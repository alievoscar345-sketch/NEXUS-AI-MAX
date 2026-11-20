from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from typing import Any

import httpx


class ModelGatewayError(RuntimeError):
    """Normalized provider failure safe to expose in task diagnostics."""


@dataclass(frozen=True)
class ModelConfig:
    id: str
    base_url: str
    model: str
    api_key: str
    priority: int = 100
    purposes: tuple[str, ...] = ("general", "planning", "evaluation")
    enabled: bool = True
    # Scores are normalized to 0..100 and should come from dated, named benchmarks.
    benchmark_scores: dict[str, float] = field(default_factory=dict)
    cost_per_million_tokens: float | None = None
    latency_ms: float | None = None
    ranking_source: str = ""
    ranking_updated_at: str = ""


@dataclass(frozen=True)
class ModelResult:
    content: str
    provider_id: str
    model: str
    usage: dict[str, Any]


TASK_KEYWORDS = {
    "coding": ("code", "coding", "program", "debug", "bug", "реализац", "код", "программ", "ошибк", "разработ"),
    "research": ("research", "sources", "citation", "paper", "study", "исслед", "источник", "научн", "стать", "найди информацию"),
    "math": ("math", "calculate", "equation", "proof", "математ", "посчитай", "вычисл", "уравнен", "доказательств"),
    "vision": ("image", "photo", "picture", "screenshot", "изображен", "фото", "картин", "скриншот"),
    "writing": ("rewrite", "write", "draft", "редактир", "напиши", "перепиши", "составь текст"),
}


def infer_task_type(text: str) -> str:
    """Lightweight deterministic classifier; no extra model call or token cost."""
    value = text.lower()
    # Resolve ties toward the more operationally specific category (e.g. "write Python code").
    tie_break = {"coding": 5, "math": 4, "research": 3, "vision": 2, "writing": 1}
    matches = [(sum(1 for word in words if word in value), tie_break[task], task) for task, words in TASK_KEYWORDS.items()]
    count, _, task = max(matches, default=(0, 0, "general"))
    return task if count else "general"


def _clean_json_text(value: str) -> str:
    value = value.strip()
    if value.startswith("```"):
        value = value.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
    return value


def load_models() -> list[ModelConfig]:
    """Load provider definitions from env; preserve the legacy single-provider setup."""
    raw = os.getenv("NEXUS_MODEL_PROVIDERS", "").strip()
    configs: list[ModelConfig] = []
    if raw:
        try:
            entries = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ModelGatewayError("NEXUS_MODEL_PROVIDERS must be valid JSON") from exc
        if not isinstance(entries, list):
            raise ModelGatewayError("NEXUS_MODEL_PROVIDERS must be a JSON array")
        for item in entries:
            if not isinstance(item, dict):
                continue
            key_env = str(item.get("api_key_env", ""))
            purposes = item.get("purposes", ["general", "planning", "evaluation"])
            if not isinstance(purposes, list):
                purposes = ["general"]
            scores = item.get("benchmark_scores", {})
            if not isinstance(scores, dict):
                scores = {}
            try:
                normalized_scores = {str(k): max(0.0, min(100.0, float(v))) for k, v in scores.items()}
                cost = float(item["cost_per_million_tokens"]) if item.get("cost_per_million_tokens") is not None else None
                latency = float(item["latency_ms"]) if item.get("latency_ms") is not None else None
                if cost is not None and cost < 0 or latency is not None and latency < 0:
                    raise ValueError("Cost and latency cannot be negative")
            except (TypeError, ValueError) as exc:
                raise ModelGatewayError("Invalid benchmark, cost, or latency metadata") from exc
            configs.append(ModelConfig(
                id=str(item.get("id", "")).strip(),
                base_url=str(item.get("base_url", "")).rstrip("/"),
                model=str(item.get("model", "")).strip(),
                api_key=os.getenv(key_env, "") if key_env else "",
                priority=int(item.get("priority", 100)),
                purposes=tuple(str(x) for x in purposes),
                enabled=bool(item.get("enabled", True)),
                benchmark_scores=normalized_scores,
                cost_per_million_tokens=cost,
                latency_ms=latency,
                ranking_source=str(item.get("ranking_source", ""))[:200],
                ranking_updated_at=str(item.get("ranking_updated_at", ""))[:40],
            ))
    else:
        configs.append(ModelConfig(
            id="default",
            base_url=os.getenv("NEXUS_LLM_BASE_URL", "https://anymodel.org/v1").rstrip("/"),
            model=os.getenv("NEXUS_LLM_MODEL", "claude-fable-5-2"),
            api_key=os.getenv("NEXUS_LLM_API_KEY", ""),
            priority=100,
        ))

    valid = [m for m in configs if m.enabled and m.id and m.base_url and m.model and m.api_key]
    if not valid:
        raise ModelGatewayError("No enabled model provider has a complete URL, model, and API key")
    return valid


class ModelGateway:
    def __init__(self, models: list[ModelConfig] | None = None, transport: httpx.AsyncBaseTransport | None = None):
        self.models = models
        self.transport = transport

    @staticmethod
    def _quality(model: ModelConfig, task_type: str) -> float:
        if task_type in model.benchmark_scores:
            return model.benchmark_scores[task_type]
        if "general" in model.benchmark_scores:
            return model.benchmark_scores["general"]
        return 50.0

    def _select(self, purpose: str, task_type: str = "general") -> list[ModelConfig]:
        models = self.models if self.models is not None else load_models()
        eligible = [m for m in models if m.enabled and purpose in m.purposes]
        if not eligible:
            eligible = [m for m in models if m.enabled and "general" in m.purposes]
        if not eligible:
            raise ModelGatewayError(f"No configured model supports purpose: {purpose}")

        route = [x.strip() for x in os.getenv(f"NEXUS_MODEL_ROUTE_{purpose.upper()}", "").split(",") if x.strip()]
        route_rank = {name: index for index, name in enumerate(route)}
        known_costs = [m.cost_per_million_tokens for m in eligible if m.cost_per_million_tokens is not None]
        known_latencies = [m.latency_ms for m in eligible if m.latency_ms is not None]
        min_cost, max_cost = (min(known_costs), max(known_costs)) if known_costs else (None, None)
        min_latency, max_latency = (min(known_latencies), max(known_latencies)) if known_latencies else (None, None)

        def efficiency(value: float | None, low: float | None, high: float | None) -> float:
            if value is None or low is None or high is None or high == low:
                return 0.5
            return 1.0 - (value - low) / (high - low)

        def score(model: ModelConfig) -> tuple[float, float, int]:
            quality = self._quality(model, task_type) / 100.0
            cost = efficiency(model.cost_per_million_tokens, min_cost, max_cost)
            speed = efficiency(model.latency_ms, min_latency, max_latency)
            # Quality dominates; cost and speed decide among similarly capable models.
            combined = 0.75 * quality + 0.15 * cost + 0.10 * speed
            return (combined, -float(model.priority), -route_rank.get(model.id, len(route_rank)))

        if route:
            eligible.sort(key=lambda m: (route_rank.get(m.id, len(route_rank)), -score(m)[0], m.priority))
        else:
            eligible.sort(key=lambda m: (-score(m)[0], m.priority))
        return eligible

    async def complete(self, system: str, user: str, purpose: str = "general") -> ModelResult:
        task_type = infer_task_type(user)
        candidates = self._select(purpose, task_type)
        last_error: Exception | None = None
        for index, model in enumerate(candidates):
            try:
                async with httpx.AsyncClient(timeout=90, transport=self.transport) as client:
                    response = await client.post(
                        model.base_url + "/chat/completions",
                        headers={"Authorization": "Bearer " + model.api_key},
                        json={
                            "model": model.model,
                            "temperature": 0.2,
                            "messages": [
                                {"role": "system", "content": system},
                                {"role": "user", "content": user},
                            ],
                        },
                    )
                    response.raise_for_status()
                    payload = response.json()
                choices = payload.get("choices")
                if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
                    raise ModelGatewayError("Provider returned no completion choices")
                message = choices[0].get("message", {})
                content = message.get("content") if isinstance(message, dict) else None
                if isinstance(content, list):
                    content = "".join(
                        part.get("text", "") for part in content if isinstance(part, dict)
                    )
                if not isinstance(content, str) or not content.strip():
                    raise ModelGatewayError("Provider returned an empty completion")
                usage = payload.get("usage", {})
                return ModelResult(
                    content=content.strip(),
                    provider_id=model.id,
                    model=model.model,
                    usage=usage if isinstance(usage, dict) else {},
                )
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                last_error = exc
            except httpx.HTTPStatusError as exc:
                last_error = exc
                status = exc.response.status_code
                if status not in (408, 429, 500, 502, 503, 504):
                    break
            except (ValueError, KeyError, TypeError) as exc:
                last_error = exc
                break
            if index == len(candidates) - 1:
                break
        detail = type(last_error).__name__ if last_error else "unknown provider error"
        if isinstance(last_error, httpx.HTTPStatusError):
            detail = f"provider returned HTTP {last_error.response.status_code}"
        raise ModelGatewayError(f"All eligible model providers failed ({detail})") from last_error

    async def complete_json(self, system: str, user: str, purpose: str = "general") -> tuple[dict[str, Any], ModelResult]:
        result = await self.complete(system, user, purpose)
        try:
            parsed = json.loads(_clean_json_text(result.content))
        except json.JSONDecodeError as exc:
            raise ModelGatewayError("Model response was not valid JSON") from exc
        if not isinstance(parsed, dict):
            raise ModelGatewayError("Model response must be a JSON object")
        return parsed, result
