from __future__ import annotations

import asyncio
import base64
import hashlib
import re
import ipaddress
import json
import os
import socket
import sqlite3
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, Field, SecretStr

from .model_gateway import ModelConfig, ModelGateway, ModelGatewayError, load_models
from .credentials import CredentialEncryptionError, decrypt_secret, encrypt_secret

DB_PATH = Path(os.getenv("NEXUS_AGENT_DB", "./data/nexus-agent.db"))
WORKSPACE = Path(os.getenv("NEXUS_AGENT_WORKSPACE", "./data/workspace")).resolve()
API_KEY = os.getenv("NEXUS_AGENT_API_KEY", "")
TAVILY_KEY = os.getenv("TAVILY_API_KEY", "")
MAX_STEPS = max(1, min(int(os.getenv("NEXUS_AGENT_MAX_STEPS", "12")), 30))
MAX_SECONDS = max(60, min(int(os.getenv("NEXUS_AGENT_MAX_TASK_SECONDS", "1800")), 7200))
MAX_STEP_ATTEMPTS = max(1, min(int(os.getenv("NEXUS_AGENT_STEP_MAX_ATTEMPTS", "3")), 8))
RETRY_BASE_SECONDS = max(1, min(int(os.getenv("NEXUS_AGENT_RETRY_BASE_SECONDS", "10")), 300))
LEASE_SECONDS = MAX_SECONDS + 180
WORKER_ID = str(uuid.uuid4())
WORKSPACE.mkdir(parents=True, exist_ok=True)
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

_model_gateway = ModelGateway()


class _ClosingConnection(sqlite3.Connection):
    """Commit or roll back on context exit, then always close the connection."""

    def __exit__(self, exc_type, exc, tb):
        try:
            return super().__exit__(exc_type, exc, tb)
        finally:
            self.close()


def db():
    c = sqlite3.connect(DB_PATH, timeout=30, factory=_ClosingConnection)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA foreign_keys=ON")
    c.execute("PRAGMA busy_timeout=30000")
    return c


async def run_db(fn, *args):
    """Run a complete synchronous DB operation outside the event-loop thread."""
    return await asyncio.to_thread(fn, *args)


def _columns(c: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in c.execute(f"PRAGMA table_info({table})").fetchall()}


def init_db():
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS tasks(
            id TEXT PRIMARY KEY,prompt TEXT NOT NULL,status TEXT NOT NULL,
            created_at REAL NOT NULL,updated_at REAL NOT NULL,
            plan_json TEXT NOT NULL DEFAULT '[]',results_json TEXT NOT NULL DEFAULT '[]',
            summary TEXT NOT NULL DEFAULT '',error TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,task_id TEXT NOT NULL,
            created_at REAL NOT NULL,kind TEXT NOT NULL,message TEXT NOT NULL,
            FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS approvals(
            id TEXT PRIMARY KEY,task_id TEXT NOT NULL,description TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',created_at REAL NOT NULL,
            FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE
        );
        """)
        migrations = {
            "lease_owner": "TEXT NOT NULL DEFAULT ''",
            "lease_expires_at": "REAL",
            "attempt_count": "INTEGER NOT NULL DEFAULT 0",
            "next_attempt_at": "REAL",
            "max_attempts": "INTEGER NOT NULL DEFAULT 3",
            "cancel_requested": "INTEGER NOT NULL DEFAULT 0",
            "max_steps": "INTEGER NOT NULL DEFAULT 8",
            "client_request_key": "TEXT NOT NULL DEFAULT ''",
            "owner_id": "TEXT NOT NULL DEFAULT 'personal'",
        }
        existing = _columns(c, "tasks")
        for name, definition in migrations.items():
            if name not in existing:
                c.execute(f"ALTER TABLE tasks ADD COLUMN {name} {definition}")
        c.execute("""
        CREATE TABLE IF NOT EXISTS task_steps(
            id TEXT PRIMARY KEY,
            task_id TEXT NOT NULL,
            step_index INTEGER NOT NULL,
            title TEXT NOT NULL,
            action TEXT NOT NULL,
            input_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            attempt_count INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 3,
            idempotency_key TEXT NOT NULL,
            result_json TEXT NOT NULL DEFAULT '',
            error TEXT NOT NULL DEFAULT '',
            next_attempt_at REAL,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            UNIQUE(task_id, step_index),
            UNIQUE(idempotency_key),
            FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE
        );
        """)
        approval_cols = _columns(c, "approvals")
        if "idempotency_key" not in approval_cols:
            c.execute("ALTER TABLE approvals ADD COLUMN idempotency_key TEXT NOT NULL DEFAULT ''")
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_approvals_idempotency ON approvals(idempotency_key) WHERE idempotency_key <> ''")
        c.execute("DROP INDEX IF EXISTS idx_tasks_client_request_key")
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_owner_request_key ON tasks(owner_id,client_request_key) WHERE client_request_key <> ''")
        c.execute("""
        CREATE TABLE IF NOT EXISTS provider_credentials(
            owner_id TEXT NOT NULL, provider_id TEXT NOT NULL, base_url TEXT NOT NULL,
            model TEXT NOT NULL, encrypted_api_key BLOB NOT NULL,
            created_at REAL NOT NULL, updated_at REAL NOT NULL,
            supports_vision INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(owner_id, provider_id)
        )
        """)
        if "supports_vision" not in _columns(c, "provider_credentials"):
            c.execute("ALTER TABLE provider_credentials ADD COLUMN supports_vision INTEGER NOT NULL DEFAULT 0")
        c.execute("CREATE INDEX IF NOT EXISTS idx_provider_credentials_owner ON provider_credentials(owner_id,provider_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_tasks_queue ON tasks(status,next_attempt_at,created_at)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_steps_task ON task_steps(task_id,step_index)")


def event(tid: str, kind: str, message: str):
    with db() as c:
        now = time.time()
        c.execute("INSERT INTO events(task_id,created_at,kind,message) VALUES(?,?,?,?)",
                  (tid, now, kind, message[:2000]))
        c.execute("UPDATE tasks SET updated_at=? WHERE id=?", (now, tid))


def serialize(row):
    return {
        "id": row["id"], "prompt": row["prompt"], "status": row["status"],
        "created_at": row["created_at"], "updated_at": row["updated_at"],
        "plan": json.loads(row["plan_json"]), "results": json.loads(row["results_json"]),
        "summary": row["summary"], "error": row["error"],
        "attempt_count": row["attempt_count"] if "attempt_count" in row.keys() else 0,
        "next_attempt_at": row["next_attempt_at"] if "next_attempt_at" in row.keys() else None,
    }


def auth(authorization: str | None = Header(default=None)) -> str:
    if not API_KEY:
        raise HTTPException(503, "Agent API is not configured")
    if authorization != "Bearer " + API_KEY:
        raise HTTPException(401, "Invalid agent API key")
    # Single-user deployment identity. Replace with a verified JWT/account subject before onboarding users.
    return "personal"


class NewTask(BaseModel):
    prompt: str = Field(min_length=3, max_length=12000)
    autonomy: str = Field(default="level3", pattern="^level[123]$")
    max_steps: int = Field(default=8, ge=1, le=30)
    idempotency_key: str | None = Field(default=None, min_length=8, max_length=128)


class Approval(BaseModel):
    approved: bool
    note: str = Field(default="", max_length=1000)


class ProviderCredentialInput(BaseModel):
    provider_id: str = Field(min_length=2, max_length=64, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$")
    base_url: str = Field(min_length=12, max_length=300)
    model: str = Field(min_length=1, max_length=160)
    api_key: SecretStr
    supports_vision: bool = False


class ChatCompletionInput(BaseModel):
    messages: list[dict[str, Any]] = Field(min_length=1, max_length=40)


ALLOWED_PROVIDER_HOSTS = {
    "anymodel.org", "openrouter.ai", "api.openai.com",
    "generativelanguage.googleapis.com", "api.deepseek.com", "api.x.ai", "api.mistral.ai",
}


def validate_provider_base_url(value: str) -> str:
    try:
        parsed = urlparse(value)
        port = parsed.port
    except ValueError as exc:
        raise HTTPException(422, "Provider URL is malformed") from exc
    if (parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password
            or port not in (None, 443) or parsed.query or parsed.fragment
            or parsed.hostname.lower() not in ALLOWED_PROVIDER_HOSTS):
        raise HTTPException(422, "Provider URL must use HTTPS on a supported provider host")
    return value.rstrip("/")


def provider_models_for_owner(owner_id: str) -> list[ModelConfig]:
    models = load_models(allow_empty=True)
    with db() as c:
        rows = c.execute(
            "SELECT provider_id,base_url,model,encrypted_api_key,supports_vision FROM provider_credentials WHERE owner_id=?",
            (owner_id,),
        ).fetchall()
    for row in rows:
        try:
            key = decrypt_secret(bytes(row["encrypted_api_key"]))
        except CredentialEncryptionError as exc:
            raise ModelGatewayError("Stored provider credential could not be decrypted") from exc
        models.append(ModelConfig(
            id="user-" + row["provider_id"], base_url=row["base_url"], model=row["model"],
            api_key=key, priority=50, purposes=("general", "planning", "evaluation"),
            capabilities=("vision",) if row["supports_vision"] else (),
        ))
    return models


async def llm_json(system: str, user: str, purpose: str = "general",
                   task_id: str | None = None, owner_id: str = "personal") -> dict[str, Any]:
    models = await run_db(provider_models_for_owner, owner_id)
    if not models:
        raise ModelGatewayError("No model provider is configured for this account")
    parsed, result = await ModelGateway(models=models).complete_json(system, user, purpose)
    if task_id:
        await run_db(event, task_id, "model_call", json.dumps({
            "purpose": purpose, "provider": result.provider_id,
            "model": result.model, "usage": result.usage,
        }, ensure_ascii=False)[:1500])
    return parsed


def safe_url(value: str) -> str:
    p = urlparse(value)
    if p.scheme != "https" or not p.hostname or p.username or p.password:
        raise ValueError("Only public HTTPS URLs are allowed")
    host = p.hostname.lower()
    if host in {"localhost", "metadata.google.internal"} or host.endswith((".local", ".internal")):
        raise ValueError("Private network destinations are blocked")
    try:
        for item in socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM):
            if not ipaddress.ip_address(item[4][0]).is_global:
                raise ValueError("Private IP destinations are blocked")
    except socket.gaierror as exc:
        raise ValueError("Unable to resolve destination host") from exc
    return value


def _step_key(tid: str, index: int, action: str, args: dict[str, Any]) -> str:
    payload = json.dumps({"task": tid, "index": index, "action": action, "input": args},
                         sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _persist_plan(tid: str, steps: list[Any], max_attempts: int):
    now = time.time()
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
        existing = c.execute("SELECT COUNT(*) AS n FROM task_steps WHERE task_id=?", (tid,)).fetchone()["n"]
        if existing:
            c.execute("UPDATE tasks SET status='running',updated_at=? WHERE id=? AND status='planning'", (now, tid))
            return
        safe_steps = [x for x in steps if isinstance(x, dict)]
        for index, step in enumerate(safe_steps):
            action = str(step.get("action", ""))[:80]
            args = step.get("input") if isinstance(step.get("input"), dict) else {}
            key = _step_key(tid, index, action, args)
            c.execute("""
                INSERT INTO task_steps(id,task_id,step_index,title,action,input_json,status,
                    max_attempts,idempotency_key,created_at,updated_at)
                VALUES(?,?,?,?,?,?,'pending',?,?,?,?)
            """, (str(uuid.uuid4()), tid, index, str(step.get("title", action))[:160],
                  action, json.dumps(args, ensure_ascii=False), max_attempts, key, now, now))
        c.execute("UPDATE tasks SET plan_json=?,status='running',updated_at=? WHERE id=?",
                  (json.dumps(safe_steps, ensure_ascii=False), now, tid))


def _create_approval(tid: str, description: str, idempotency_key: str) -> str:
    approval_id = str(uuid.uuid5(uuid.NAMESPACE_URL, idempotency_key))
    with db() as c:
        c.execute("""INSERT OR IGNORE INTO approvals(id,task_id,description,status,created_at,idempotency_key)
                     VALUES(?,?,?,'pending',?,?)""",
                  (approval_id, tid, description, time.time(), idempotency_key))
        approval = c.execute("SELECT id,status FROM approvals WHERE idempotency_key=?",
                             (idempotency_key,)).fetchone()
        c.execute("UPDATE tasks SET status='waiting_for_approval',updated_at=? WHERE id=?",
                  (time.time(), tid))
    event(tid, "approval_required", description)
    return approval["id"]


async def run_step(step: dict[str, Any], tid: str, idempotency_key: str):
    action = str(step.get("action", ""))
    args = step.get("input") if isinstance(step.get("input"), dict) else {}
    title = str(step.get("title", action))[:160]
    if action == "web_search":
        if not TAVILY_KEY:
            return {"title": title, "status": "blocked", "detail": "TAVILY_API_KEY is not configured"}
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post("https://api.tavily.com/search",
                json={"api_key": TAVILY_KEY, "query": str(args.get("query", ""))[:500], "max_results": 5})
            response.raise_for_status()
        return {"title": title, "status": "completed", "data": response.json().get("results", [])}
    if action == "fetch_url":
        url = safe_url(str(args.get("url", "")))
        async with httpx.AsyncClient(timeout=25, follow_redirects=False,
                                     headers={"User-Agent": "NEXUS-Agent/0.1"}) as client:
            response = await client.get(url)
            response.raise_for_status()
            content_type = response.headers.get("content-type", "")
            if "text" not in content_type and "json" not in content_type:
                raise ValueError("Only text/JSON pages can be fetched")
        return {"title": title, "status": "completed", "url": url, "content": response.text[:20000]}
    if action == "save_artifact":
        name = Path(str(args.get("filename", "result.md"))).name[:100]
        if not name or name in {".", ".."}:
            raise ValueError("Invalid filename")
        target = (WORKSPACE / (tid + "-" + name)).resolve()
        if target.parent != WORKSPACE:
            raise ValueError("Invalid artifact path")
        target.write_text(str(args.get("content", ""))[:100000], encoding="utf-8")
        return {"title": title, "status": "completed", "artifact": target.name}
    if action == "draft_external_action":
        description = str(args.get("description", "External action requested"))[:1000]
        approval_id = await run_db(_create_approval, tid, description, idempotency_key)
        return {"title": title, "status": "awaiting_approval", "approval_id": approval_id,
                "detail": "No external action was executed. A service-specific connector is required."}
    raise ValueError("Unsupported tool: " + action)


def _retry_delay(attempt: int) -> float:
    return min(RETRY_BASE_SECONDS * (2 ** max(0, attempt - 1)), 900)


def _is_transient(exc: Exception) -> bool:
    if isinstance(exc, (httpx.TimeoutException, httpx.NetworkError)):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in {408, 425, 429, 500, 502, 503, 504}
    return False


def _release_task(tid: str):
    with db() as c:
        c.execute("UPDATE tasks SET lease_owner='',lease_expires_at=NULL WHERE id=? AND lease_owner=?",
                  (tid, WORKER_ID))


def _task_cancelled(tid: str) -> bool:
    with db() as c:
        row = c.execute("SELECT status,cancel_requested FROM tasks WHERE id=?", (tid,)).fetchone()
    return not row or row["status"] == "cancelled" or bool(row["cancel_requested"])



def _get_task_row(tid: str):
    with db() as c:
        return c.execute("SELECT * FROM tasks WHERE id=?", (tid,)).fetchone()


def _set_task_planning(tid: str):
    with db() as c:
        c.execute("UPDATE tasks SET status='planning',updated_at=? WHERE id=? AND status!='cancelled'",
                  (time.time(), tid))


def _mark_task_cancelled(tid: str):
    with db() as c:
        c.execute("UPDATE tasks SET status='cancelled',lease_owner='',lease_expires_at=NULL,updated_at=? WHERE id=?",
                  (time.time(), tid))


def _next_pending_step(tid: str, now: float):
    with db() as c:
        return c.execute("""SELECT * FROM task_steps WHERE task_id=?
            AND status IN ('pending','retry_wait') AND (next_attempt_at IS NULL OR next_attempt_at<=?)
            ORDER BY step_index LIMIT 1""", (tid, now)).fetchone()


def _step_summary(tid: str):
    with db() as c:
        waiting = c.execute("SELECT COUNT(*) AS n FROM task_steps WHERE task_id=? AND status='waiting_for_approval'",
                            (tid,)).fetchone()["n"]
        unfinished = c.execute("SELECT COUNT(*) AS n FROM task_steps WHERE task_id=? AND status NOT IN ('completed','skipped')",
                               (tid,)).fetchone()["n"]
        results_rows = c.execute("SELECT result_json FROM task_steps WHERE task_id=? AND result_json!='' ORDER BY step_index",
                                 (tid,)).fetchall()
    return waiting, unfinished, results_rows


def _set_task_waiting_for_approval(tid: str):
    with db() as c:
        c.execute("UPDATE tasks SET status='waiting_for_approval',lease_owner='',lease_expires_at=NULL,updated_at=? WHERE id=?",
                  (time.time(), tid))


def _set_task_retry_wait(tid: str, next_at: float):
    with db() as c:
        c.execute("UPDATE tasks SET status='retry_wait',next_attempt_at=?,lease_owner='',lease_expires_at=NULL,updated_at=? WHERE id=?",
                  (next_at, time.time(), tid))


def _complete_task(tid: str, summary: str, results_json: str):
    with db() as c:
        c.execute("UPDATE tasks SET status='completed',summary=?,results_json=?,error='',next_attempt_at=NULL,lease_owner='',lease_expires_at=NULL,updated_at=? WHERE id=?",
                  (summary[:12000], results_json, time.time(), tid))


def _claim_step(step_id: str):
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
        changed = c.execute("""UPDATE task_steps SET status='running',attempt_count=attempt_count+1,
            error='',updated_at=? WHERE id=? AND status IN ('pending','retry_wait')""",
            (time.time(), step_id)).rowcount
        if not changed:
            return None
        return c.execute("SELECT * FROM task_steps WHERE id=?", (step_id,)).fetchone()


def _save_step_result(tid: str, step_id: str, status: str, result_json: str):
    with db() as c:
        c.execute("UPDATE task_steps SET status=?,result_json=?,updated_at=? WHERE id=?",
                  (status, result_json, time.time(), step_id))
        rows = c.execute("SELECT result_json FROM task_steps WHERE task_id=? AND result_json!='' ORDER BY step_index",
                         (tid,)).fetchall()
        all_results = [json.loads(x["result_json"]) for x in rows]
        c.execute("UPDATE tasks SET results_json=?,updated_at=? WHERE id=?",
                  (json.dumps(all_results, ensure_ascii=False), time.time(), tid))


def _record_step_failure(tid: str, step_id: str, retry: bool, next_at: float | None, error: str):
    with db() as c:
        c.execute("UPDATE task_steps SET status=?,error=?,next_attempt_at=?,updated_at=? WHERE id=?",
                  ("retry_wait" if retry else "failed", error[:2000], next_at, time.time(), step_id))
        if retry:
            c.execute("UPDATE tasks SET status='retry_wait',next_attempt_at=?,lease_owner='',lease_expires_at=NULL,updated_at=? WHERE id=?",
                      (next_at, time.time(), tid))
        else:
            c.execute("UPDATE tasks SET status='failed',error=?,lease_owner='',lease_expires_at=NULL,updated_at=? WHERE id=?",
                      (error[:2000], time.time(), tid))


def _record_task_failure(tid: str, exc: Exception, error: str) -> bool:
    with db() as c:
        current = c.execute("SELECT attempt_count,max_attempts FROM tasks WHERE id=?", (tid,)).fetchone()
        retry = bool(current and _is_transient(exc) and current["attempt_count"] < current["max_attempts"])
        next_at = time.time() + _retry_delay(current["attempt_count"]) if retry else None
        c.execute("UPDATE tasks SET status=?,error=?,next_attempt_at=?,lease_owner='',lease_expires_at=NULL,updated_at=? WHERE id=? AND status!='cancelled'",
                  ("retry_wait" if retry else "failed", error[:2000], next_at, time.time(), tid))
    return retry


async def process(tid: str):
    row = await run_db(_get_task_row, tid)
    if not row:
        return
    started = time.monotonic()
    try:
        if not row["plan_json"] or row["plan_json"] == "[]":
            await run_db(_set_task_planning, tid)
            await run_db(event, tid, "status", "Planning task")
            plan = await llm_json(
                'You are NEXUS Agent Planner. Return strict JSON only: {"summary":"...","steps":[{"title":"...","action":"web_search|fetch_url|save_artifact|draft_external_action","input":{}}]}. Use only listed actions. For publishing, messaging, purchases or account changes use draft_external_action. Never claim external actions succeeded.',
                row["prompt"], purpose="planning", task_id=tid, owner_id=row["owner_id"])
            steps = plan.get("steps", [])
            if not isinstance(steps, list):
                raise ValueError("Planner did not return steps")
            await run_db(_persist_plan, tid, steps[:min(MAX_STEPS, int(row['max_steps']), 8)], MAX_STEP_ATTEMPTS)

        while True:
            if await run_db(_task_cancelled, tid):
                await run_db(_mark_task_cancelled, tid)
                await run_db(event, tid, "cancelled", "Task cancellation acknowledged")
                return
            if time.monotonic() - started > MAX_SECONDS:
                raise TimeoutError("Task time limit reached")
            step = await run_db(_next_pending_step, tid, time.time())
            if not step:
                waiting, unfinished, results_rows = await run_db(_step_summary, tid)
                if waiting:
                    await run_db(_set_task_waiting_for_approval, tid)
                    return
                if unfinished:
                    await run_db(_set_task_retry_wait, tid, time.time() + RETRY_BASE_SECONDS)
                    return
                results = [json.loads(x["result_json"]) for x in results_rows]
                final = await llm_json(
                    'Return strict JSON only: {"summary":"..."}. Summarize only supplied results. Explicitly identify blocked or unavailable actions; do not claim they succeeded.',
                    json.dumps({"goal": row["prompt"], "results": results}, ensure_ascii=False)[:30000],
                    purpose="evaluation", task_id=tid, owner_id=row["owner_id"])
                await run_db(_complete_task, tid, str(final.get("summary", "Task finished")),
                             json.dumps(results, ensure_ascii=False))
                await run_db(event, tid, "completed", "Task completed")
                return

            claimed = await run_db(_claim_step, step["id"])
            if not claimed:
                continue
            await run_db(event, tid, "step_started", f"{claimed['step_index'] + 1}: {claimed['title']}")
            try:
                step_data = {"title": claimed["title"], "action": claimed["action"],
                             "input": json.loads(claimed["input_json"])}
                result = await run_step(step_data, tid, claimed["idempotency_key"])
                status = "waiting_for_approval" if result.get("status") == "awaiting_approval" else "completed"
                await run_db(_save_step_result, tid, claimed["id"], status,
                             json.dumps(result, ensure_ascii=False))
                await run_db(event, tid, "step_finished", f"{claimed['step_index'] + 1}: {status}")
                if status == "waiting_for_approval":
                    await run_db(_set_task_waiting_for_approval, tid)
                    return
            except Exception as exc:
                attempts = claimed["attempt_count"]
                retry = _is_transient(exc) and attempts < claimed["max_attempts"]
                next_at = time.time() + _retry_delay(attempts) if retry else None
                await run_db(_record_step_failure, tid, claimed["id"], retry, next_at, str(exc)[:2000])
                await run_db(event, tid, "step_retry" if retry else "error", str(exc)[:1000])
                if retry:
                    return
                return
    except Exception as exc:
        retry = await run_db(_record_task_failure, tid, exc, str(exc)[:2000])
        await run_db(event, tid, "task_retry" if retry else "error", str(exc)[:1000])


def recover_interrupted_tasks():
    """Single-worker SQLite recovery. Safe because supported actions are read-only or idempotent."""
    now = time.time()
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
        rows = c.execute("SELECT id FROM tasks WHERE status IN ('planning','running')").fetchall()
        for row in rows:
            tid = row["id"]
            c.execute("""UPDATE task_steps SET status='retry_wait',next_attempt_at=?,updated_at=?,
                error='Recovered after worker interruption'
                WHERE task_id=? AND status='running'""", (now, now, tid))
            c.execute("""UPDATE tasks SET status='retry_wait',next_attempt_at=?,lease_owner='',
                lease_expires_at=NULL,updated_at=? WHERE id=?""", (now, now, tid))
            c.execute("INSERT INTO events(task_id,created_at,kind,message) VALUES(?,?,?,?)",
                      (tid, now, "recovered", "Task recovered after worker restart"))
        # A task with an expired lease can be reclaimed even if it was marked running by an older version.
        c.execute("""UPDATE tasks SET status='retry_wait',next_attempt_at=?,lease_owner='',
            lease_expires_at=NULL,updated_at=? WHERE status='running' AND lease_expires_at IS NOT NULL
            AND lease_expires_at<?""", (now, now, now))


def claim_task() -> str | None:
    now = time.time()
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
        row = c.execute("""SELECT id FROM tasks WHERE status IN ('queued','retry_wait')
            AND (next_attempt_at IS NULL OR next_attempt_at<=?)
            ORDER BY created_at LIMIT 1""", (now,)).fetchone()
        if not row:
            return None
        tid = row["id"]
        changed = c.execute("""UPDATE tasks SET status='running',lease_owner=?,lease_expires_at=?,
            attempt_count=attempt_count+1,next_attempt_at=NULL,updated_at=?
            WHERE id=? AND status IN ('queued','retry_wait')""",
            (WORKER_ID, now + LEASE_SECONDS, now, tid)).rowcount
        return tid if changed else None


async def worker():
    while True:
        tid = await run_db(claim_task)
        if tid:
            await process(tid)
        else:
            await asyncio.sleep(2)


@asynccontextmanager
async def lifespan(app):
    await run_db(init_db)
    await run_db(recover_interrupted_tasks)
    task = asyncio.create_task(worker())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="NEXUS Agent Core", version="0.2.0", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "nexus-agent-core", "version": "0.2.0"}


def validate_chat_messages(messages: list[dict[str, Any]]) -> bool:
    if len(json.dumps(messages, ensure_ascii=False).encode("utf-8")) > 12 * 1024 * 1024:
        raise HTTPException(413, "Chat payload exceeds the 12 MB limit")
    requires_vision = False
    for message in messages:
        if not isinstance(message, dict) or message.get("role") not in {"system", "user", "assistant"}:
            raise HTTPException(422, "Only system, user, and assistant messages are supported")
        content = message.get("content")
        if isinstance(content, str):
            if len(content) > 200000:
                raise HTTPException(413, "A chat message is too large")
            continue
        if not isinstance(content, list):
            raise HTTPException(422, "Message content must be text or multimodal content parts")
        for part in content:
            if not isinstance(part, dict):
                raise HTTPException(422, "Invalid multimodal content part")
            if part.get("type") == "text":
                if not isinstance(part.get("text"), str) or len(part["text"]) > 200000:
                    raise HTTPException(422, "Invalid or oversized text content")
            elif part.get("type") == "image_url":
                image = part.get("image_url")
                data_url = image.get("url") if isinstance(image, dict) else None
                if not isinstance(data_url, str):
                    raise HTTPException(422, "Image must be supplied as a data URL")
                match = re.fullmatch(r"data:image/(png|jpeg|webp|gif);base64,([A-Za-z0-9+/]+={0,2})", data_url)
                if not match or len(match.group(2)) > 11 * 1024 * 1024:
                    raise HTTPException(422, "Only base64 PNG, JPEG, WEBP, or GIF images up to 8 MB are supported")
                try:
                    decoded = base64.b64decode(match.group(2), validate=True)
                except (ValueError, base64.binascii.Error) as exc:
                    raise HTTPException(422, "Image data is not valid base64") from exc
                if len(decoded) > 8 * 1024 * 1024:
                    raise HTTPException(413, "An image exceeds the 8 MB limit")
                requires_vision = True
            else:
                raise HTTPException(422, "Unsupported multimodal content part")
    return requires_vision


@app.post("/v1/chat/completions", dependencies=[Depends(auth)])
async def chat_completions(body: ChatCompletionInput, owner_id: str = Depends(auth)):
    requires_vision = validate_chat_messages(body.messages)
    models = await run_db(provider_models_for_owner, owner_id)
    if not models:
        raise HTTPException(503, "No model provider is configured for this account")
    try:
        result = await ModelGateway(models=models).complete_messages(body.messages, purpose="general")
    except ModelGatewayError as exc:
        status = 503 if "No configured model supports" in str(exc) else 502
        raise HTTPException(status, str(exc)) from exc
    return {
        "id": "nexus-" + str(uuid.uuid4()),
        "object": "chat.completion",
        "model": result.model,
        "provider": result.provider_id,
        "choices": [{"index": 0, "message": {"role": "assistant", "content": result.content},
                     "finish_reason": "stop"}],
        "usage": result.usage,
        "vision": requires_vision,
    }


@app.get("/v1/models", dependencies=[Depends(auth)])
async def list_available_models(owner_id: str = Depends(auth)):
    models = await run_db(provider_models_for_owner, owner_id)
    return {"models": [{
        "provider_id": model.id,
        "model": model.model,
        "source": "user" if model.id.startswith("user-") else "built_in",
        "capabilities": list(model.capabilities),
        "purposes": list(model.purposes),
    } for model in models]}


@app.get("/v1/providers", dependencies=[Depends(auth)])
def list_providers(owner_id: str = Depends(auth)):
    with db() as c:
        rows = c.execute("""SELECT provider_id,base_url,model,supports_vision,created_at,updated_at
            FROM provider_credentials WHERE owner_id=? ORDER BY provider_id""", (owner_id,)).fetchall()
    return {"providers": [{**dict(row), "supports_vision": bool(row["supports_vision"])} for row in rows]}


@app.post("/v1/providers", dependencies=[Depends(auth)])
def save_provider(body: ProviderCredentialInput, owner_id: str = Depends(auth)):
    base_url = validate_provider_base_url(body.base_url)
    provider_key = body.api_key.get_secret_value()
    if not 8 <= len(provider_key) <= 4096:
        raise HTTPException(422, "Provider API key must be between 8 and 4096 characters")
    try:
        encrypted = encrypt_secret(provider_key)
    except CredentialEncryptionError as exc:
        raise HTTPException(503, "Credential encryption is not configured") from exc
    now = time.time()
    with db() as c:
        c.execute("""INSERT INTO provider_credentials(owner_id,provider_id,base_url,model,
            encrypted_api_key,supports_vision,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(owner_id,provider_id) DO UPDATE SET base_url=excluded.base_url,
            model=excluded.model,encrypted_api_key=excluded.encrypted_api_key,
            supports_vision=excluded.supports_vision,updated_at=excluded.updated_at""",
            (owner_id, body.provider_id, base_url, body.model, encrypted,
             1 if body.supports_vision else 0, now, now))
    return {"provider_id": body.provider_id, "status": "saved", "secret_returned": False}


@app.delete("/v1/providers/{provider_id}", dependencies=[Depends(auth)])
def delete_provider(provider_id: str, owner_id: str = Depends(auth)):
    with db() as c:
        changed = c.execute("DELETE FROM provider_credentials WHERE owner_id=? AND provider_id=?",
                            (owner_id, provider_id)).rowcount
    if not changed:
        raise HTTPException(404, "Provider not found")
    return {"provider_id": provider_id, "status": "deleted"}


@app.post("/v1/providers/{provider_id}/test", dependencies=[Depends(auth)])
async def test_provider(provider_id: str, owner_id: str = Depends(auth)):
    models = [m for m in await run_db(provider_models_for_owner, owner_id) if m.id == "user-" + provider_id]
    if not models:
        raise HTTPException(404, "Provider not found")
    started = time.monotonic()
    try:
        result = await ModelGateway(models=models).complete(
            "You are a connection test. Reply with exactly: OK",
            "Reply with exactly: OK", purpose="general")
    except ModelGatewayError as exc:
        raise HTTPException(502, str(exc)) from exc
    return {"provider_id": provider_id, "model": result.model,
            "status": "connected", "latency_ms": round((time.monotonic() - started) * 1000)}


def _create_task_record(body: NewTask, owner_id: str) -> dict[str, Any]:
    tid = str(uuid.uuid4())
    now = time.time()
    with db() as c:
        # Serialize the idempotency check and insert to make concurrent retries deterministic.
        c.execute("BEGIN IMMEDIATE")
        if body.idempotency_key:
            existing = c.execute("SELECT id,status FROM tasks WHERE owner_id=? AND client_request_key=?",
                                 (owner_id, body.idempotency_key)).fetchone()
            if existing:
                return {"id": existing["id"], "status": existing["status"], "deduplicated": True}
        c.execute("""INSERT INTO tasks(id,prompt,status,created_at,updated_at,max_attempts,
            max_steps,client_request_key,owner_id) VALUES(?,?,?,?,?,?,?,?,?)""",
            (tid, body.prompt, "queued", now, now, MAX_STEP_ATTEMPTS,
             body.max_steps, body.idempotency_key or "", owner_id))
        event_time = time.time()
        c.execute("INSERT INTO events(task_id,created_at,kind,message) VALUES(?,?,?,?)",
                  (tid, event_time, "created", "Task queued in durable cloud worker"))
        c.execute("UPDATE tasks SET updated_at=? WHERE id=?", (event_time, tid))
    return {"id": tid, "status": "queued"}


@app.post("/v1/tasks", dependencies=[Depends(auth)])
def create_task(body: NewTask, owner_id: str = Depends(auth)):
    return _create_task_record(body, owner_id)


@app.get("/v1/tasks", dependencies=[Depends(auth)])
def list_tasks(limit: int = 30, owner_id: str = Depends(auth)):
    with db() as c:
        rows = c.execute("SELECT * FROM tasks WHERE owner_id=? ORDER BY created_at DESC LIMIT ?",
                         (owner_id, max(1, min(limit, 100)))).fetchall()
    return {"tasks": [serialize(r) for r in rows]}


@app.get("/v1/tasks/{tid}", dependencies=[Depends(auth)])
def get_task(tid: str, owner_id: str = Depends(auth)):
    with db() as c:
        row = c.execute("SELECT * FROM tasks WHERE id=? AND owner_id=?", (tid, owner_id)).fetchone()
        if not row:
            raise HTTPException(404, "Task not found")
        events = c.execute("SELECT created_at,kind,message FROM events WHERE task_id=? ORDER BY id",
                           (tid,)).fetchall()
        steps = c.execute("""SELECT step_index,title,action,status,attempt_count,max_attempts,
            error,next_attempt_at,result_json FROM task_steps WHERE task_id=? ORDER BY step_index""",
            (tid,)).fetchall()
    return {**serialize(row), "events": [dict(x) for x in events],
            "steps": [{**dict(x), "result": json.loads(x["result_json"]) if x["result_json"] else None}
                      for x in steps]}


@app.post("/v1/tasks/{tid}/cancel", dependencies=[Depends(auth)])
def cancel_task(tid: str, owner_id: str = Depends(auth)):
    with db() as c:
        row = c.execute("SELECT status FROM tasks WHERE id=? AND owner_id=?", (tid, owner_id)).fetchone()
        if not row:
            raise HTTPException(404, "Task not found")
        if row["status"] in {"completed", "failed", "cancelled"}:
            raise HTTPException(409, "Task already finished")
        c.execute("UPDATE tasks SET cancel_requested=1,updated_at=? WHERE id=?", (time.time(), tid))
        if row["status"] in {"queued", "retry_wait", "waiting_for_approval", "paused"}:
            c.execute("UPDATE tasks SET status='cancelled',lease_owner='',lease_expires_at=NULL WHERE id=?", (tid,))
    event(tid, "cancel_requested", "Cancellation requested by user")
    return {"id": tid, "status": "cancellation_requested"}


@app.get("/v1/approvals", dependencies=[Depends(auth)])
def list_approvals(owner_id: str = Depends(auth)):
    with db() as c:
        rows = c.execute("""SELECT a.* FROM approvals a JOIN tasks t ON t.id=a.task_id
            WHERE a.status='pending' AND t.owner_id=? ORDER BY a.created_at""", (owner_id,)).fetchall()
    return {"approvals": [dict(r) for r in rows]}


def _decide_approval_record(approval_id: str, owner_id: str, approved: bool, note: str) -> dict[str, str]:
    with db() as c:
        # Lock before reading pending status so concurrent decisions have a single winner.
        c.execute("BEGIN IMMEDIATE")
        row = c.execute("""SELECT a.* FROM approvals a JOIN tasks t ON t.id=a.task_id
            WHERE a.id=? AND t.owner_id=?""", (approval_id, owner_id)).fetchone()
        if not row:
            raise HTTPException(404, "Approval not found")
        if row["status"] != "pending":
            raise HTTPException(409, "Approval already decided")
        status = "approved" if approved else "rejected"
        now = time.time()
        c.execute("UPDATE approvals SET status=? WHERE id=?", (status, approval_id))
        c.execute("""UPDATE task_steps SET status='completed',result_json=?,updated_at=?
            WHERE task_id=? AND status='waiting_for_approval'""",
            (json.dumps({"status": status, "detail": "Approval recorded; no external connector is configured."}),
             now, row["task_id"]))
        c.execute("UPDATE tasks SET status='paused',lease_owner='',lease_expires_at=NULL,updated_at=? WHERE id=?",
                  (now, row["task_id"]))
        c.execute("INSERT INTO events(task_id,created_at,kind,message) VALUES(?,?,?,?)",
                  (row["task_id"], now, "approval_decision", status + ": " + note))
    return {"approval_id": approval_id, "status": status,
            "detail": "Decision recorded. No third-party action is executed by this generic backend."}


@app.post("/v1/approvals/{approval_id}", dependencies=[Depends(auth)])
def decide_approval(approval_id: str, body: Approval, owner_id: str = Depends(auth)):
    return _decide_approval_record(approval_id, owner_id, body.approved, body.note)