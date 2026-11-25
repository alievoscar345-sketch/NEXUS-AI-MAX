"""P1 integration coverage: auth, provider CRUD, catalog, and chat endpoints
wired through the real FastAPI app (ASGI transport), not mocked handlers."""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from app import main
from app.credentials import decrypt_secret
from app.model_gateway import ModelConfig, ModelGateway

ENCRYPTION_KEY = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="
PLAINTEXT_KEY = "integration-provider-secret-123"


def config(name="test", model="test-model"):
    return ModelConfig(
        id=name,
        base_url="https://provider.example/v1",
        model=model,
        api_key="test-secret",
        priority=1,
        purposes=("general", "planning", "evaluation"),
    )


class ApiIntegrationTests(unittest.IsolatedAsyncioTestCase):
    def _setup_env(self, temp_path: str):
        """Patch DB, API key, and credential encryption onto a fresh instance."""
        return (
            patch.object(main, "DB_PATH", Path(temp_path) / "nexus.sqlite3"),
            patch.object(main, "API_KEY", "test-key"),
            patch.dict(os.environ, {"NEXUS_CREDENTIAL_ENCRYPTION_KEY": ENCRYPTION_KEY}),
        )

    async def test_health_endpoints_are_public_and_worker_fails_safe(self):
        with tempfile.TemporaryDirectory() as temp:
            db_patch, api_patch, env_patch = self._setup_env(temp)
            with db_patch, api_patch, env_patch:
                main.init_db()
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=main.app), base_url="http://nexus.test"
                ) as client:
                    health = await client.get("/health")
                    worker = await client.get("/health/worker")
        self.assertEqual(health.status_code, 200)
        self.assertEqual(health.json()["status"], "ok")
        # No heartbeat recorded yet: monitoring must fail closed with 503.
        self.assertEqual(worker.status_code, 503)
        self.assertIn("heartbeat", worker.json()["detail"].lower())

    async def test_auth_matrix_on_protected_routes(self):
        with tempfile.TemporaryDirectory() as temp:
            db_patch, api_patch, env_patch = self._setup_env(temp)
            with db_patch, api_patch, env_patch:
                main.init_db()
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=main.app), base_url="http://nexus.test"
                ) as client:
                    no_key = await client.get("/v1/models")
                    wrong_key = await client.get(
                        "/v1/models", headers={"Authorization": "Bearer wrong"}
                    )
                    with patch.object(main, "API_KEY", ""):
                        unconfigured = await client.get(
                            "/v1/models", headers={"Authorization": "Bearer anything"}
                        )
                    good_key = await client.get(
                        "/v1/models", headers={"Authorization": "Bearer test-key"}
                    )
                    no_header_chat = await client.post(
                        "/v1/chat/completions", json={"messages": [{"role": "user", "content": "hi"}]}
                    )
        self.assertEqual(no_key.status_code, 401)
        self.assertEqual(wrong_key.status_code, 401)
        self.assertEqual(unconfigured.status_code, 503)
        self.assertEqual(good_key.status_code, 200)
        self.assertEqual(no_header_chat.status_code, 401)

    async def test_provider_crud_roundtrip_encrypts_at_rest(self):
        with tempfile.TemporaryDirectory() as temp:
            db_patch, api_patch, env_patch = self._setup_env(temp)
            with db_patch, api_patch, env_patch:
                main.init_db()
                db_path = Path(temp) / "nexus.sqlite3"
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=main.app), base_url="http://nexus.test"
                ) as client:
                    headers = {"Authorization": "Bearer test-key"}
                    saved = await client.post(
                        "/v1/providers",
                        headers=headers,
                        json={
                            "provider_id": "anymodel",
                            "display_name": "AnyModel",
                            "base_url": "https://anymodel.org/v1/",
                            "model": "claude-fable-5-2",
                            "api_key": PLAINTEXT_KEY,
                            "supports_vision": True,
                            "specialties": ["coding", "research"],
                            "context_window_tokens": 128000,
                        },
                    )
                    listed = await client.get("/v1/providers", headers=headers)

                    # Update without a key must reuse the stored ciphertext.
                    updated = await client.post(
                        "/v1/providers",
                        headers=headers,
                        json={
                            "provider_id": "anymodel",
                            "display_name": "AnyModel renamed",
                            "base_url": "https://anymodel.org/v1",
                            "model": "claude-fable-5-2",
                            "specialties": ["coding"],
                        },
                    )
                    listed_again = await client.get("/v1/providers", headers=headers)

                    # Read ciphertext before deleting the row.
                    with main.db() as conn:
                        row = conn.execute(
                            "SELECT encrypted_api_key FROM provider_credentials"
                        ).fetchone()

                    deleted = await client.delete("/v1/providers/anymodel", headers=headers)
                    deleted_again = await client.delete("/v1/providers/anymodel", headers=headers)

                # WAL mode may hold recent pages in the -wal file; check both.
                raw_db = db_path.read_bytes()
                wal = db_path.with_name(db_path.name + "-wal")
                if wal.exists():
                    raw_db += wal.read_bytes()
                # Decrypt while the encryption-key env patch is still active.
                decrypted_key = decrypt_secret(bytes(row["encrypted_api_key"]))

            self.assertEqual(saved.status_code, 200, saved.text)
            self.assertEqual(saved.json()["status"], "saved")
            self.assertFalse(saved.json()["secret_returned"])

            self.assertEqual(listed.status_code, 200)
            providers = listed.json()["providers"]
            self.assertEqual(len(providers), 1)
            entry = providers[0]
            # Catalog must never leak secrets or ciphertext.
            self.assertNotIn("api_key", entry)
            self.assertNotIn("encrypted_api_key", entry)
            self.assertEqual(entry["base_url"], "https://anymodel.org/v1")  # trailing slash stripped
            self.assertEqual(entry["context_window_tokens"], 128000)
            self.assertTrue(entry["supports_vision"])
            self.assertEqual(entry["specialties"], ["coding", "research"])

            self.assertEqual(updated.status_code, 200, updated.text)
            self.assertEqual(listed_again.json()["providers"][0]["display_name"], "AnyModel renamed")

            self.assertEqual(deleted.status_code, 200)
            self.assertEqual(deleted_again.status_code, 404)

            # Encryption at rest: plaintext must not exist in the DB file, and the
            # stored ciphertext must decrypt back to the original key.
            self.assertNotIn(PLAINTEXT_KEY.encode(), raw_db)
            self.assertIsNotNone(row)
            self.assertEqual(decrypted_key, PLAINTEXT_KEY)

    async def test_provider_validation_rejects_bad_input(self):
        with tempfile.TemporaryDirectory() as temp:
            db_patch, api_patch, env_patch = self._setup_env(temp)
            with db_patch, api_patch, env_patch:
                main.init_db()
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=main.app), base_url="http://nexus.test"
                ) as client:
                    headers = {"Authorization": "Bearer test-key"}
                    base = {
                        "provider_id": "p1",
                        "base_url": "https://anymodel.org/v1",
                        "model": "m",
                    }

                    http_scheme = await client.post(
                        "/v1/providers", headers=headers,
                        json={**base, "base_url": "http://anymodel.org/v1", "api_key": "x" * 12},
                    )
                    unlisted_host = await client.post(
                        "/v1/providers", headers=headers,
                        json={**base, "base_url": "https://evil.example/v1", "api_key": "x" * 12},
                    )
                    missing_key_new = await client.post(
                        "/v1/providers", headers=headers, json=base,
                    )
                    short_key = await client.post(
                        "/v1/providers", headers=headers, json={**base, "api_key": "short"},
                    )
                    bad_specialty = await client.post(
                        "/v1/providers", headers=headers,
                        json={**base, "api_key": "x" * 12, "specialties": ["hacking"]},
                    )
                    bad_window = await client.post(
                        "/v1/providers", headers=headers,
                        json={**base, "api_key": "x" * 12, "context_window_tokens": 100},
                    )

            self.assertEqual(http_scheme.status_code, 422, http_scheme.text)
            self.assertEqual(unlisted_host.status_code, 422, unlisted_host.text)
            self.assertEqual(missing_key_new.status_code, 422, missing_key_new.text)
            self.assertEqual(short_key.status_code, 422, short_key.text)
            self.assertEqual(bad_specialty.status_code, 422, bad_specialty.text)
            self.assertEqual(bad_window.status_code, 422, bad_window.text)

    async def test_chat_non_stream_returns_actual_model_and_context(self):
        def handler(request):
            payload = json.loads(request.content)
            self.assertEqual(payload["model"], "test-model")
            self.assertFalse(payload.get("stream", False))
            return httpx.Response(200, json={
                "choices": [{"message": {"role": "assistant", "content": "pong"}}],
                "usage": {"prompt_tokens": 5, "completion_tokens": 1},
            })

        gateway = ModelGateway([config()], transport=httpx.MockTransport(handler))
        with tempfile.TemporaryDirectory() as temp:
            db_patch, api_patch, env_patch = self._setup_env(temp)
            with db_patch, api_patch, env_patch, \
                 patch.object(main, "provider_models_for_owner", return_value=[config()]), \
                 patch.object(main, "ModelGateway", return_value=gateway):
                main.init_db()
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=main.app), base_url="http://nexus.test"
                ) as client:
                    response = await client.post(
                        "/v1/chat/completions",
                        headers={"Authorization": "Bearer test-key"},
                        json={"messages": [{"role": "user", "content": "ping"}]},
                    )
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertEqual(body["model"], "test-model")
        self.assertEqual(body["provider"], "test")
        self.assertEqual(body["display_name"], "test-model")
        self.assertEqual(body["choices"][0]["message"]["content"], "pong")
        self.assertFalse(body["vision"])
        self.assertGreaterEqual(body["context"]["context_window_tokens"], 4096)
        self.assertGreater(body["context"]["input_budget_tokens"], 0)

    async def test_chat_manual_selection_is_strict(self):
        gateway = ModelGateway([config()], transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"choices": [{"message": {"content": "x"}}]})
        ))
        with tempfile.TemporaryDirectory() as temp:
            db_patch, api_patch, env_patch = self._setup_env(temp)
            with db_patch, api_patch, env_patch, \
                 patch.object(main, "provider_models_for_owner", return_value=[config()]), \
                 patch.object(main, "ModelGateway", return_value=gateway):
                main.init_db()
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=main.app), base_url="http://nexus.test"
                ) as client:
                    unknown = await client.post(
                        "/v1/chat/completions",
                        headers={"Authorization": "Bearer test-key"},
                        json={
                            "messages": [{"role": "user", "content": "hi"}],
                            "model_selection": "ghost-model",
                        },
                    )
                    known = await client.post(
                        "/v1/chat/completions",
                        headers={"Authorization": "Bearer test-key"},
                        json={
                            "messages": [{"role": "user", "content": "hi"}],
                            "model_selection": "test",
                        },
                    )
        # Strict manual selection: no fallback to another configured model.
        self.assertEqual(unknown.status_code, 422, unknown.text)
        self.assertIn("not configured", unknown.json()["detail"])
        self.assertEqual(known.status_code, 200, known.text)
        self.assertEqual(known.json()["model"], "test-model")

    async def test_chat_stream_exposes_context_headers_and_actual_model(self):
        provider_body = (
            'data: {"choices":[{"index":0,"delta":{"content":"streamed"}}]}\n\n'
            "data: [DONE]\n\n"
        )
        gateway = ModelGateway([config()], transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, text=provider_body, headers={"content-type": "text/event-stream"}
            )
        ))
        with tempfile.TemporaryDirectory() as temp:
            db_patch, api_patch, env_patch = self._setup_env(temp)
            with db_patch, api_patch, env_patch, \
                 patch.object(main, "provider_models_for_owner", return_value=[config()]), \
                 patch.object(main, "ModelGateway", return_value=gateway):
                main.init_db()
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=main.app), base_url="http://nexus.test"
                ) as client:
                    response = await client.post(
                        "/v1/chat/completions",
                        headers={"Authorization": "Bearer test-key"},
                        json={"messages": [{"role": "user", "content": "hi"}], "stream": True},
                    )
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/event-stream", response.headers["content-type"])
        # Android renders these context headers as the compaction banner.
        self.assertGreater(int(response.headers["x-nexus-context-window"]), 4096)
        self.assertIn(response.headers["x-nexus-context-compacted"], ("true", "false"))
        self.assertIn("x-nexus-context-dropped", response.headers)
        # Actual-model metadata travels inside SSE chunks.
        self.assertIn('"provider_id":"test"', response.text)
        self.assertIn('"display_name":"test-model"', response.text)
        self.assertTrue(response.text.rstrip().endswith("data: [DONE]"))

    async def test_models_catalog_marks_source_and_never_returns_secrets(self):
        env_providers = json.dumps([{
            "id": "built-in-primary",
            "base_url": "https://anymodel.org/v1",
            "model": "claude-fable-5-2",
            "api_key_env": "NEXUS_INTEGRATION_KEY",
            "purposes": ["general", "planning", "evaluation"],
        }])
        with tempfile.TemporaryDirectory() as temp:
            db_patch, api_patch, env_patch = self._setup_env(temp)
            with db_patch, api_patch, env_patch, patch.dict(os.environ, {
                "NEXUS_MODEL_PROVIDERS": env_providers,
                "NEXUS_INTEGRATION_KEY": "env-catalog-secret-value",
            }):
                main.init_db()
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=main.app), base_url="http://nexus.test"
                ) as client:
                    response = await client.get(
                        "/v1/models", headers={"Authorization": "Bearer test-key"}
                    )
        self.assertEqual(response.status_code, 200, response.text)
        models = response.json()["models"]
        self.assertEqual(len(models), 1)
        entry = models[0]
        self.assertEqual(entry["provider_id"], "built-in-primary")
        self.assertEqual(entry["source"], "built_in")
        self.assertEqual(entry["display_name"], "claude-fable-5-2")
        self.assertNotIn("api_key", entry)
        self.assertNotIn("env-catalog-secret-value", response.text)


if __name__ == "__main__":
    unittest.main()
