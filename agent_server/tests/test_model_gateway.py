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

    def test_infers_task_categories_without_extra_model_call(self):
        self.assertEqual(infer_task_type("Проведи научное исследование и найди источники"), "research")
        self.assertEqual(infer_task_type("Реши математическое уравнение"), "math")
        self.assertEqual(infer_task_type("Напиши Python программу"), "coding")


if __name__ == "__main__":
    unittest.main()
