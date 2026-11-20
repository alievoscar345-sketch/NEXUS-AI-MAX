from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
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


@dataclass(frozen=True)
class ModelResult:
    content: str
    provider_id: str
    model: str
    usage: dict[str, Any]


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
            configs.append(ModelConfig(
                id=str(item.get("id", "")).strip(),
                base_url=str(item.get("base_url", "")).rstrip("/"),
                model=str(item.get("model", "")).strip(),
                api_key=os.getenv(key_env, "") if key_env else "",
                priority=int(item.get("priority", 100)),
                purposes=tuple(str(x) for x in purposes),
                enabled=bool(item.get("enabled", True)),
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

    def _select(self, purpose: str) -> list[ModelConfig]:
        models = self.models if self.models is not None else load_models()
        eligible = [m for m in models if m.enabled and purpose in m.purposes]
        if not eligible:
            eligible = [m for m in models if m.enabled and "general" in m.purposes]
        if not eligible:
            raise ModelGatewayError(f"No configured model supports purpose: {purpose}")
        route = [x.strip() for x in os.getenv(f"NEXUS_MODEL_ROUTE_{purpose.upper()}", "").split(",") if x.strip()]
        if route:
            rank = {name: index for index, name in enumerate(route)}
            eligible.sort(key=lambda m: (rank.get(m.id, len(rank)), m.priority))
        else:
            eligible.sort(key=lambda m: m.priority)
        return eligible

    async def complete(self, system: str, user: str, purpose: str = "general") -> ModelResult:
        candidates = self._select(purpose)
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
