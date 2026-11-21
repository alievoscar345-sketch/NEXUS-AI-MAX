import json
import unittest
import httpx

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
                with unittest.mock.patch.dict(
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


if __name__ == "__main__":
    unittest.main()
