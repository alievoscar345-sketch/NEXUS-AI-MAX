#!/usr/bin/env python3
"""Live end-to-end smoke test for the P1 MVP surface.

Runs a real uvicorn instance of app.main:app together with a mock OpenAI-compatible
provider in one process, then exercises: health endpoints, bearer auth, provider
CRUD with encryption at rest, the model catalog, manual/auto chat routing with
actual-model metadata (non-stream and SSE), and the worker heartbeat.

Usage (from agent_server/):  PYTHONPATH=. python scripts/smoke_e2e.py
Exit code 0 = all checks passed; 1 = at least one check failed.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import socket
import sys
import tempfile
from pathlib import Path

import httpx
import uvicorn

# ---------------------------------------------------------------------------
# Environment must be configured before importing app.main (it reads API key,
# DB path, workspace, and worker flag at import time).
# ---------------------------------------------------------------------------
def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


MOCK_PORT = _free_port()
APP_PORT = _free_port()

_TMP = tempfile.mkdtemp(prefix="nexus-smoke-")
os.environ["NEXUS_AGENT_API_KEY"] = "smoke-api-key"
os.environ["NEXUS_CREDENTIAL_ENCRYPTION_KEY"] = base64.b64encode(
    b"0123456789abcdef0123456789abcdef"
).decode()
os.environ["NEXUS_AGENT_DB"] = str(Path(_TMP) / "nexus.sqlite3")
os.environ["NEXUS_AGENT_WORKSPACE"] = str(Path(_TMP) / "workspace")
os.environ["NEXUS_AGENT_WORKER_ENABLED"] = "true"
os.environ["NEXUS_SMOKE_MOCK_KEY"] = "mock-provider-secret"
os.environ["NEXUS_MODEL_PROVIDERS"] = json.dumps([{
    "id": "smoke-primary",
    "base_url": f"http://127.0.0.1:{MOCK_PORT}/v1",
    "model": "smoke-model",
    "api_key_env": "NEXUS_SMOKE_MOCK_KEY",
    "purposes": ["general", "planning", "evaluation"],
}])

from app import main  # noqa: E402  (env must be set first)
from app.credentials import decrypt_secret  # noqa: E402

PLAINTEXT_PROVIDER_KEY = "smoke-plaintext-secret-123"

MOCK_STATE = {"last_model": None, "request_count": 0}

FAILURES: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    if condition:
        print(f"PASS: {name}")
    else:
        message = f"FAIL: {name}" + (f" — {detail}" if detail else "")
        print(message)
        FAILURES.append(message)


# ---------------------------------------------------------------------------
# Mock OpenAI-compatible provider
# ---------------------------------------------------------------------------
async def mock_provider_app(scope, receive, send):  # noqa: ANN001
    if scope["type"] != "http":
        return
    method = scope["method"]
    path = scope["path"]

    async def respond(status: int, body: bytes, content_type: str = "application/json") -> None:
        await send({
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", content_type.encode()),
                (b"content-length", str(len(body)).encode()),
            ],
        })
        await send({"type": "http.response.body", "body": body})

    if method == "GET" and path == "/health":
        await respond(200, b'{"status":"ok"}')
        return

    if method == "POST" and path.endswith("/chat/completions"):
        raw = b""
        while True:
            message = await receive()
            if message["type"] == "http.request":
                raw += message.get("body", b"")
                if not message.get("more_body"):
                    break
            elif message["type"] == "http.disconnect":
                return
        payload = json.loads(raw or b"{}")
        MOCK_STATE["last_model"] = payload.get("model")
        MOCK_STATE["request_count"] += 1
        if payload.get("stream"):
            sse = (
                'data: {"choices":[{"index":0,"delta":{"role":"assistant"}}]}\n\n'
                'data: {"choices":[{"index":0,"delta":{"content":"smoke-ok"}}]}\n\n'
                "data: [DONE]\n\n"
            )
            await respond(200, sse.encode(), "text/event-stream")
        else:
            body = json.dumps({
                "id": "mock-1",
                "object": "chat.completion",
                "model": payload.get("model", "smoke-model"),
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": "smoke-ok"},
                    "finish_reason": "stop",
                }],
                "usage": {"prompt_tokens": 3, "completion_tokens": 2},
            }).encode()
            await respond(200, body)
        return

    await respond(404, b'{"detail":"not found"}')


class _NoSignalServer(uvicorn.Server):
    """uvicorn.Server without signal handlers (two servers share one loop)."""

    def install_signal_handlers(self) -> None:  # noqa: D102
        pass


async def _wait_for(client: httpx.AsyncClient, url: str, timeout: float = 30.0) -> bool:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        try:
            response = await client.get(url)
            if response.status_code < 500:
                return True
        except httpx.HTTPError:
            pass
        await asyncio.sleep(0.2)
    return False


async def run_checks(client: httpx.AsyncClient, db_path: Path) -> None:
    headers = {"Authorization": "Bearer smoke-api-key"}

    # 1. Health endpoints.
    health = await client.get("/health")
    check("GET /health is public and ok", health.status_code == 200 and health.json().get("status") == "ok",
          f"status={health.status_code}")

    # 2. Worker heartbeat (worker is enabled via lifespan).
    worker_ok = False
    worker_detail = ""
    deadline = asyncio.get_event_loop().time() + 20.0
    while asyncio.get_event_loop().time() < deadline:
        worker = await client.get("/health/worker")
        if worker.status_code == 200:
            worker_ok = True
            worker_detail = worker.text
            break
        worker_detail = worker.text
        await asyncio.sleep(0.5)
    check("GET /health/worker reports a live worker", worker_ok, worker_detail)

    # 3. Auth matrix.
    no_auth = await client.get("/v1/models")
    wrong_auth = await client.get("/v1/models", headers={"Authorization": "Bearer nope"})
    good_auth = await client.get("/v1/models", headers=headers)
    check("protected route rejects missing bearer", no_auth.status_code == 401, str(no_auth.status_code))
    check("protected route rejects wrong bearer", wrong_auth.status_code == 401, str(wrong_auth.status_code))
    check("protected route accepts configured bearer", good_auth.status_code == 200, good_auth.text)

    # 4. Provider CRUD + encryption at rest.
    saved = await client.post("/v1/providers", headers=headers, json={
        "provider_id": "anymodel",
        "display_name": "AnyModel",
        "base_url": "https://anymodel.org/v1/",
        "model": "claude-fable-5-2",
        "api_key": PLAINTEXT_PROVIDER_KEY,
        "supports_vision": True,
        "specialties": ["coding"],
    })
    check("POST /v1/providers saves a provider", saved.status_code == 200, saved.text)

    listed = await client.get("/v1/providers", headers=headers)
    providers = listed.json().get("providers", []) if listed.status_code == 200 else []
    entry = providers[0] if providers else {}
    check("GET /v1/providers returns the row without secrets",
          listed.status_code == 200 and "api_key" not in entry and "encrypted_api_key" not in entry,
          listed.text)
    check("provider base_url is normalized (trailing slash stripped)",
          entry.get("base_url") == "https://anymodel.org/v1", str(entry.get("base_url")))

    raw = db_path.read_bytes()
    wal = db_path.with_name(db_path.name + "-wal")
    if wal.exists():
        raw += wal.read_bytes()
    with main.db() as conn:
        row = conn.execute("SELECT encrypted_api_key FROM provider_credentials").fetchone()
    check("provider key is encrypted at rest (no plaintext in DB)",
          PLAINTEXT_PROVIDER_KEY.encode() not in raw and row is not None)
    decrypted = decrypt_secret(bytes(row["encrypted_api_key"])) if row else ""
    check("stored ciphertext decrypts to the original key",
          decrypted == PLAINTEXT_PROVIDER_KEY, decrypted[:8] if decrypted else "no row")

    # 5. Catalog.
    catalog = await client.get("/v1/models", headers=headers)
    catalog_text = catalog.text
    check("GET /v1/models lists the env provider",
          catalog.status_code == 200 and "smoke-primary" in catalog_text, catalog_text)
    check("catalog does not leak provider secrets",
          "mock-provider-secret" not in catalog_text and PLAINTEXT_PROVIDER_KEY not in catalog_text)

    # 6. Auto chat (actual-model metadata).
    auto_chat = await client.post("/v1/chat/completions", headers=headers, json={
        "messages": [{"role": "user", "content": "ping"}],
    })
    auto_body = auto_chat.json() if auto_chat.status_code == 200 else {}
    check("POST /v1/chat/completions auto-routing returns 200",
          auto_chat.status_code == 200, auto_chat.text)
    check("response names the actual model/provider",
          auto_body.get("model") == "smoke-model" and auto_body.get("provider") == "smoke-primary",
          json.dumps(auto_body)[:300])
    check("response content comes from the provider",
          auto_body.get("choices", [{}])[0].get("message", {}).get("content") == "smoke-ok",
          json.dumps(auto_body)[:300])
    check("response exposes context stats",
          isinstance(auto_body.get("context"), dict) and auto_body["context"].get("context_window_tokens", 0) >= 4096,
          json.dumps(auto_body.get("context")))
    check("gateway actually called the mock provider with the routed model",
          MOCK_STATE["last_model"] == "smoke-model", str(MOCK_STATE["last_model"]))

    # 7. Strict manual selection.
    ghost = await client.post("/v1/chat/completions", headers=headers, json={
        "messages": [{"role": "user", "content": "ping"}],
        "model_selection": "ghost-model",
    })
    check("manual selection of an unconfigured model is rejected (422)",
          ghost.status_code == 422, ghost.text)
    manual = await client.post("/v1/chat/completions", headers=headers, json={
        "messages": [{"role": "user", "content": "ping"}],
        "model_selection": "smoke-primary",
    })
    check("manual selection of the configured model succeeds",
          manual.status_code == 200 and manual.json().get("model") == "smoke-model", manual.text)

    # 8. Streaming with context headers and actual-model metadata.
    stream = await client.post("/v1/chat/completions", headers=headers, json={
        "messages": [{"role": "user", "content": "ping"}],
        "stream": True,
    })
    stream_text = stream.text
    check("stream returns text/event-stream",
          stream.status_code == 200 and "text/event-stream" in stream.headers.get("content-type", ""),
          f"status={stream.status_code}")
    check("stream carries X-Nexus-Context headers",
          "x-nexus-context-window" in stream.headers and "x-nexus-context-dropped" in stream.headers,
          str(dict(stream.headers)))
    check("stream chunks carry actual-model metadata and terminate with [DONE]",
          '"provider_id":"smoke-primary"' in stream_text
          and '"display_name":"smoke-model"' in stream_text
          and stream_text.rstrip().endswith("data: [DONE]"),
          stream_text[-200:])

    # 9. Cleanup CRUD.
    deleted = await client.delete("/v1/providers/anymodel", headers=headers)
    deleted_again = await client.delete("/v1/providers/anymodel", headers=headers)
    check("DELETE /v1/providers removes the row",
          deleted.status_code == 200 and deleted_again.status_code == 404,
          f"{deleted.status_code}/{deleted_again.status_code}")


async def amain() -> int:
    mock_server = _NoSignalServer(uvicorn.Config(
        mock_provider_app, host="127.0.0.1", port=MOCK_PORT, log_level="warning", lifespan="off",
    ))
    app_server = _NoSignalServer(uvicorn.Config(
        "app.main:app", host="127.0.0.1", port=APP_PORT, log_level="warning",
    ))
    tasks = [
        asyncio.create_task(mock_server.serve(), name="mock-provider"),
        asyncio.create_task(app_server.serve(), name="nexus-app"),
    ]
    try:
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{APP_PORT}", timeout=30.0) as client:
            app_ready = await _wait_for(client, "/health")
            check("uvicorn app started and serves /health", app_ready)
            async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{MOCK_PORT}", timeout=10.0) as mock_client:
                mock_ready = await _wait_for(mock_client, "/health")
            check("mock provider started", mock_ready)
            if app_ready and mock_ready:
                await run_checks(client, Path(os.environ["NEXUS_AGENT_DB"]))
    finally:
        mock_server.should_exit = True
        app_server.should_exit = True
        await asyncio.gather(*tasks, return_exceptions=True)

    if FAILURES:
        print(f"\n{len(FAILURES)} check(s) FAILED")
        return 1
    print("\nAll smoke checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(amain()))
