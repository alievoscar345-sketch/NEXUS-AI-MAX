import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx

from app import main
from app.model_gateway import ModelConfig, ModelGateway, ModelGatewayError, infer_task_type


def config(name="test", purposes=("general", "planning", "evaluation"), priority=1):
    return ModelConfig(
        id=name,
        base_url="https://provider.example/v1",
        model="test-model",
        api_key="test-secret",
        priority=priority,
        purposes=purposes,
    )


class GatewayTests(unittest.IsolatedAsyncioTestCase):
    async def test_stream_parses_content_and_done(self):
        body = (
            'data: {"choices":[{"index":0,"delta":{"role":"assistant"}}]}\n\n'
            'data: {"choices":[{"index":0,"delta":{"content":"hello"}}]}\n\n'
            'data: [DONE]\n\n'
        )
        def handler(request):
            self.assertTrue(json.loads(request.content)["stream"])
            return httpx.Response(200, text=body, headers={"content-type": "text/event-stream"})
        gateway = ModelGateway([config()], transport=httpx.MockTransport(handler))
        chunks = [chunk async for chunk in gateway.stream_messages([{"role":"user","content":"hi"}])]
        self.assertEqual(chunks[1]["choices"][0]["delta"]["content"], "hello")
        self.assertEqual(len(chunks), 2)

    async def test_stream_rejects_oversized_sse_event(self):
        body = "data: " + ("x" * 1_000_001) + "\n\n"
        gateway = ModelGateway([config()], transport=httpx.MockTransport(
            lambda request: httpx.Response(200, text=body)
        ))
        with self.assertRaises(ModelGatewayError):
            _ = [chunk async for chunk in gateway.stream_messages([{"role":"user","content":"hi"}])]

    async def test_stream_rejects_invalid_choice_index(self):
        body = 'data: {"choices":[{"index":"bad","delta":{"content":"x"}}]}\n\n'
        gateway = ModelGateway([config()], transport=httpx.MockTransport(
            lambda request: httpx.Response(200, text=body)
        ))
        with self.assertRaises(ModelGatewayError):
            _ = [chunk async for chunk in gateway.stream_messages([{"role":"user","content":"hi"}])]

    async def test_stream_falls_back_before_first_chunk(self):
        calls = []
        def handler(request):
            calls.append(1)
            if len(calls) == 1:
                raise httpx.ConnectError("offline", request=request)
            return httpx.Response(200, text='data: {"choices":[{"delta":{"content":"ok"},"finish_reason":null}]}\n\ndata: [DONE]\n\n')
        gateway = ModelGateway([config("first"), config("second", priority=2)],
                               transport=httpx.MockTransport(handler))
        chunks = [chunk async for chunk in gateway.stream_messages([{"role":"user","content":"hi"}])]
        self.assertEqual(len(calls), 2)
        self.assertEqual(chunks[0]["choices"][0]["delta"]["content"], "ok")

    async def test_stream_does_not_fallback_after_partial_output(self):
        calls = []
        class BrokenStream(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield b'data: {"choices":[{"delta":{"content":"partial"},"finish_reason":null}]}\n\n'
                raise httpx.ReadError("connection dropped")
            async def aclose(self):
                return None
        def broken_handler(request):
            calls.append(1)
            return httpx.Response(200, stream=BrokenStream())
        gateway = ModelGateway([config("first"), config("second", priority=2)],
                               transport=httpx.MockTransport(broken_handler))
        chunks = [chunk async for chunk in gateway.stream_messages([{"role":"user","content":"hi"}])]
        self.assertEqual(len(calls), 1)
        self.assertEqual(chunks[0]["choices"][0]["delta"]["content"], "partial")
        self.assertEqual(chunks[-1]["error"]["type"], "provider_stream_error")

    async def test_chat_endpoint_returns_sse_when_stream_requested(self):
        provider_body = (
            'data: {"choices":[{"index":0,"delta":{"content":"streamed"}}]}\n\n'
            'data: [DONE]\n\n'
        )
        transport = httpx.MockTransport(
            lambda request: httpx.Response(200, text=provider_body,
                                           headers={"content-type": "text/event-stream"})
        )
        gateway = ModelGateway([config()], transport=transport)
        with tempfile.TemporaryDirectory() as temp:
            with patch.object(main, "DB_PATH", Path(temp) / "nexus.sqlite3"), \
                 patch.object(main, "API_KEY", "test-key"), \
                 patch.object(main, "provider_models_for_owner", return_value=[config()]), \
                 patch.object(main, "ModelGateway", return_value=gateway):
                main.init_db()
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=main.app), base_url="http://nexus.test"
                ) as client:
                    response = await client.post(
                        "/v1/chat/completions",
                        headers={"Authorization": "Bearer test-key"},
                        json={"messages": [{"role": "user", "content": "hello"}], "stream": True},
                    )
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/event-stream", response.headers["content-type"])
        self.assertIn('"content":"streamed"', response.text)
        # The Android client reads this metadata from SSE and renders the actual
        # responding model below the assistant message.
        self.assertIn('"provider_id":"test"', response.text)
        self.assertIn('"display_name":"test-model"', response.text)
        self.assertIn('"model":"test-model"', response.text)
        self.assertTrue(response.text.rstrip().endswith("data: [DONE]"))

    async def test_manual_chat_selection_is_strict_and_returns_named_model(self):
        calls = []
        def handler(request):
            calls.append(json.loads(request.content)["model"])
            return httpx.Response(200, json={"choices":[{"message":{"content":"selected"}}]})
        selected = ModelConfig(
            id="user-selected", base_url="https://provider.example/v1",
            model="selected-model", api_key="key", display_name="Claude — анализ",
        )
        other = ModelConfig(
            id="user-other", base_url="https://provider.example/v1",
            model="other-model", api_key="key",
        )
        gateway = ModelGateway([selected, other], transport=httpx.MockTransport(handler))
        result = await gateway.complete_messages(
            [{"role":"user","content":"hello"}], selected_model_id="user-selected"
        )
        self.assertEqual(calls, ["selected-model"])
        self.assertEqual(result.provider_id, "user-selected")
        self.assertEqual(result.display_name, "Claude — анализ")

    async def test_manual_chat_selection_does_not_fallback_when_missing(self):
        gateway = ModelGateway([config("configured")])
        with self.assertRaises(ModelGatewayError):
            await gateway.complete_messages(
                [{"role":"user","content":"hello"}], selected_model_id="user-missing"
            )

    async def test_stream_reports_actual_model_identity(self):
        body = 'data: {"choices":[{"index":0,"delta":{"content":"ok"}}]}\\n\\n'
        model = ModelConfig(
            id="user-visible", base_url="https://provider.example/v1",
            model="visible-model", api_key="key", display_name="Модель для кода",
        )
        gateway = ModelGateway([model], transport=httpx.MockTransport(
            lambda request: httpx.Response(200, text=body)
        ))
        chunks = [chunk async for chunk in gateway.stream_messages(
            [{"role":"user","content":"hello"}]
        )]
        self.assertEqual(chunks[0]["provider_id"], "user-visible")
        self.assertEqual(chunks[0]["display_name"], "Модель для кода")

    async def test_parses_json_completion(self):
        def handler(request):
            payload = {"choices":[{"message":{"content":'{"summary":"ok"}'}}],"usage":{"total_tokens":12}}
            return httpx.Response(200, json=payload)
        gateway = ModelGateway([config()], transport=httpx.MockTransport(handler))
        parsed, result = await gateway.complete_json("system", "user", "planning")
        self.assertEqual(parsed["summary"], "ok")
        self.assertEqual(result.provider_id, "test")
        self.assertEqual(result.usage["total_tokens"], 12)

    async def test_strips_markdown_json_fence(self):
        def handler(request):
            return httpx.Response(200, json={"choices":[{"message":{"content":'\x60\x60\x60json\n{"ok":true}\n\x60\x60\x60'}}]})
        gateway = ModelGateway([config()], transport=httpx.MockTransport(handler))
        parsed, _ = await gateway.complete_json("system", "user")
        self.assertTrue(parsed["ok"])

    async def test_falls_back_on_rate_limit(self):
        calls = []
        def handler(request):
            calls.append(request.headers.get("Authorization"))
            if len(calls) == 1:
                return httpx.Response(429, json={"error":"rate limited"})
            return httpx.Response(200, json={"choices":[{"message":{"content":"fallback"}}]})
        models = [config("first", priority=1), config("second", priority=2)]
        gateway = ModelGateway(models, transport=httpx.MockTransport(handler))
        result = await gateway.complete("system", "user", "general")
        self.assertEqual(result.provider_id, "second")
        self.assertEqual(len(calls), 2)

    async def test_does_not_fallback_on_auth_failure(self):
        calls = []
        def handler(request):
            calls.append(1)
            return httpx.Response(401, json={"error":"unauthorized"})
        models = [config("first", priority=1), config("second", priority=2)]
        gateway = ModelGateway(models, transport=httpx.MockTransport(handler))
        with self.assertRaises(ModelGatewayError):
            await gateway.complete("system", "user")
        self.assertEqual(len(calls), 1)

    async def test_task_manual_model_selection_restricts_all_task_llm_calls(self):
        models = [
            ModelConfig(id="user-selected", base_url="https://provider.example/v1",
                        model="selected-model", api_key="secret"),
            ModelConfig(id="user-other", base_url="https://provider.example/v1",
                        model="other-model", api_key="secret"),
        ]
        observed = {}

        class CapturingGateway:
            async def complete_json(self, system, user, purpose):
                return {"ok": True}, SimpleNamespace(
                    provider_id=self.models[0].id, model=self.models[0].model, usage={}
                )

            def __init__(self, models):
                self.models = models
                observed["models"] = models

        token = main._TASK_MODEL_SELECTION.set("user-selected")
        try:
            with patch.object(main, "run_db", new=AsyncMock(return_value=models)), \
                 patch.object(main, "ModelGateway", CapturingGateway):
                result = await main.llm_json("system", "user", purpose="planning")
        finally:
            main._TASK_MODEL_SELECTION.reset(token)

        self.assertEqual(result, {"ok": True})
        self.assertEqual([model.id for model in observed["models"]], ["user-selected"])

    async def test_rejects_non_object_json(self):
        def handler(request):
            return httpx.Response(200, json={"choices":[{"message":{"content":"[1,2]"}}]})
        gateway = ModelGateway([config()], transport=httpx.MockTransport(handler))
        with self.assertRaises(ModelGatewayError):
            await gateway.complete_json("system", "user")

    async def test_rejects_missing_purpose(self):
        gateway = ModelGateway([config(purposes=("general",))])
        with self.assertRaises(ModelGatewayError):
            await gateway.complete("system", "user", "coding")


    def test_rejects_non_finite_benchmark_and_cost_metadata(self):
        cases = [
            {"benchmark_scores": {"general": float("nan")}},
            {"benchmark_scores": {"general": float("inf")}},
            {"cost_per_million_tokens": float("inf")},
            {"latency_ms": float("nan")},
            {"priority": -1},
            {"priority": 100001},
        ]
        for metadata in cases:
            with self.subTest(metadata=metadata):
                item = {
                    "id": "invalid", "base_url": "https://provider.example/v1",
                    "model": "model", "api_key_env": "NEXUS_TEST_KEY",
                }
                item.update(metadata)
                with patch.dict(
                    os.environ,
                    {"NEXUS_MODEL_PROVIDERS": json.dumps([item]), "NEXUS_TEST_KEY": "secret"},
                ):
                    with self.assertRaises(ModelGatewayError):
                        from app.model_gateway import load_models
                        load_models()

    async def test_selects_highest_benchmark_fit_for_task(self):
        calls = []
        def handler(request):
            calls.append(json.loads(request.content)["model"])
            return httpx.Response(200, json={"choices":[{"message":{"content":"ok"}}]})
        models = [
            ModelConfig(id="generalist", base_url="https://provider.example/v1", model="general-model",
                        api_key="key", benchmark_scores={"general": 95, "coding": 55}),
            ModelConfig(id="coder", base_url="https://provider.example/v1", model="coding-model",
                        api_key="key", benchmark_scores={"general": 70, "coding": 96}),
        ]
        gateway = ModelGateway(models, transport=httpx.MockTransport(handler))
        result = await gateway.complete("system", "Please debug this Python code")
        self.assertEqual(result.provider_id, "coder")
        self.assertEqual(calls, ["coding-model"])

    async def test_chat_router_ignores_system_prompt_when_classifying_user_intent(self):
        calls = []
        def handler(request):
            calls.append(json.loads(request.content)["model"])
            return httpx.Response(200, json={"choices":[{"message":{"content":"ok"}}]})
        coding = ModelConfig(
            id="coding", base_url="https://provider.example/v1", model="coding-model",
            api_key="key", benchmark_scores={"general": 20, "coding": 99},
        )
        general = ModelConfig(
            id="general", base_url="https://provider.example/v1", model="general-model",
            api_key="key", benchmark_scores={"general": 95, "coding": 20},
        )
        gateway = ModelGateway([coding, general], transport=httpx.MockTransport(handler))
        await gateway.complete_messages([
            {"role":"system","content":"Always write Python code and debug programs."},
            {"role":"user","content":"Расскажи, что такое северное сияние."},
        ])
        self.assertEqual(calls, ["general-model"])

    async def test_cost_and_latency_break_quality_tie(self):
        calls = []
        def handler(request):
            calls.append(json.loads(request.content)["model"])
            return httpx.Response(200, json={"choices":[{"message":{"content":"ok"}}]})
        models = [
            ModelConfig(id="expensive", base_url="https://provider.example/v1", model="expensive",
                        api_key="key", benchmark_scores={"general": 90}, cost_per_million_tokens=10, latency_ms=900),
            ModelConfig(id="efficient", base_url="https://provider.example/v1", model="efficient",
                        api_key="key", benchmark_scores={"general": 90}, cost_per_million_tokens=1, latency_ms=200),
        ]
        gateway = ModelGateway(models, transport=httpx.MockTransport(handler))
        result = await gateway.complete("system", "Explain this concept")
        self.assertEqual(result.provider_id, "efficient")

    async def test_multimodal_request_selects_vision_capable_provider(self):
        calls = []
        def handler(request):
            payload = json.loads(request.content)
            calls.append(payload)
            return httpx.Response(200, json={"choices":[{"message":{"content":"image analyzed"}}]})
        text_model = ModelConfig(
            id="text", base_url="https://provider.example/v1", model="text-model",
            api_key="key", benchmark_scores={"vision": 99},
        )
        vision_model = ModelConfig(
            id="vision", base_url="https://provider.example/v1", model="vision-model",
            api_key="key", capabilities=("vision",), benchmark_scores={"vision": 80},
        )
        gateway = ModelGateway([text_model, vision_model], transport=httpx.MockTransport(handler))
        messages = [{"role":"user","content":[
            {"type":"text","text":"Describe this image"},
            {"type":"image_url","image_url":{"url":"data:image/png;base64,aGVsbG8="}},
        ]}]
        result = await gateway.complete_messages(messages)
        self.assertEqual(result.provider_id, "vision")
        self.assertEqual(calls[0]["model"], "vision-model")
        self.assertEqual(calls[0]["messages"], messages)

    async def test_multimodal_request_fails_without_vision_capability(self):
        gateway = ModelGateway([config()], transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"choices":[{"message":{"content":"unexpected"}}]})
        ))
        messages = [{"role":"user","content":[
            {"type":"image_url","image_url":{"url":"data:image/png;base64,aGVsbG8="}}
        ]}]
        with self.assertRaises(ModelGatewayError):
            await gateway.complete_messages(messages)

    def test_infers_task_categories_without_extra_model_call(self):
        self.assertEqual(infer_task_type("Проведи научное исследование и найди источники"), "research")
        self.assertEqual(infer_task_type("Реши математическое уравнение"), "math")
        self.assertEqual(infer_task_type("Напиши Python программу"), "coding")
        self.assertEqual(infer_task_type("Собери Android-приложение"), "coding")
        self.assertEqual(infer_task_type("Проведи рефакторинг репозитория"), "coding")
        self.assertEqual(infer_task_type("Найди научные данные и источники"), "research")
        self.assertEqual(infer_task_type("Переведи этот текст на английский"), "writing")


if __name__ == "__main__":
    unittest.main()
