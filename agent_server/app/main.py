from __future__ import annotations

import asyncio
import contextvars
import base64
import hashlib
import hmac
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
import aiohttp
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import StreamingResponse
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
_LEASE_OWNER = contextvars.ContextVar("nexus_lease_owner", default=WORKER_ID)


def _current_lease_owner() -> str:
    return _LEASE_OWNER.get()
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


def _worker_event(tid: str, kind: str, message: str) -> bool:
    now = time.time()
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
        owned = c.execute("""SELECT 1 FROM tasks WHERE id=? AND status IN ('running','planning')
            AND cancel_requested=0 AND lease_owner=? AND lease_expires_at>?""",
            (tid, _current_lease_owner(), now)).fetchone()
        if not owned:
            return False
        c.execute("INSERT INTO events(task_id,created_at,kind,message) VALUES(?,?,?,?)",
                  (tid, now, kind, message[:2000]))
        c.execute("UPDATE tasks SET updated_at=? WHERE id=?", (now, tid))
        return True


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
    if not hmac.compare_digest(authorization or "", "Bearer " + API_KEY):
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
    stream: bool = False


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


def _resolve_public_addresses(host: str, port: int = 443) -> list[ipaddress.IPv4Address | ipaddress.IPv6Address]:
    try:
        try:
            addresses = [ipaddress.ip_address(host)]
        except ValueError:
            records = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
            addresses = list({ipaddress.ip_address(item[4][0]) for item in records})
        if not addresses or any(not address.is_global for address in addresses):
            raise ValueError("Private IP destinations are blocked")
        return addresses
    except socket.gaierror as exc:
        raise ValueError("Unable to resolve destination host") from exc


class _PinnedPublicResolver(aiohttp.abc.AbstractResolver):
    """Return only the IP addresses validated for this request; never re-resolve DNS."""

    def __init__(self, addresses: list[ipaddress.IPv4Address | ipaddress.IPv6Address]):
        self.addresses = addresses

    async def resolve(self, host: str, port: int = 0, family: int = socket.AF_INET):
        results = []
        for address in self.addresses:
            address_family = socket.AF_INET6 if address.version == 6 else socket.AF_INET
            if family not in (socket.AF_UNSPEC, address_family):
                continue
            results.append({
                "hostname": host,
                "host": str(address),
                "port": port,
                "family": address_family,
                "proto": socket.IPPROTO_TCP,
                "flags": socket.AI_NUMERICHOST,
            })
        if not results:
            raise OSError("No validated address is compatible with the requested address family")
        return results

    def close(self):
        return None


def safe_url(value: str) -> str:
    try:
        p = urlparse(value)
        port = p.port
    except ValueError as exc:
        raise ValueError("Malformed URL") from exc
    if (p.scheme != "https" or not p.hostname or p.username or p.password
            or port not in (None, 443)):
        raise ValueError("Only public HTTPS URLs on port 443 are allowed")
    host = p.hostname.lower().rstrip(".")
    if not host or host in {"localhost", "metadata.google.internal"} or host.endswith((".local", ".internal")):
        raise ValueError("Private network destinations are blocked")
    _resolve_public_addresses(host, 443)
    return value


async def _fetch_public_text(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.hostname.lower().rstrip(".")
    # Resolve again immediately before connecting, then pin these exact validated IPs
    # in the connector. TLS still verifies the original URL hostname/SNI.
    addresses = _resolve_public_addresses(host, 443)
    resolver = _PinnedPublicResolver(addresses)
    connector = aiohttp.TCPConnector(resolver=resolver, use_dns_cache=False, ssl=True, family=socket.AF_UNSPEC)
    timeout = aiohttp.ClientTimeout(total=25, connect=10, sock_read=15)
    async with aiohttp.ClientSession(
        timeout=timeout, connector=connector,
        headers={"User-Agent": "NEXUS-Agent/0.1"},
    ) as session:
        async with session.get(url, allow_redirects=False) as response:
            response.raise_for_status()
            content_type = response.headers.get("content-type", "").lower()
            if "text" not in content_type and "json" not in content_type:
                raise ValueError("Only text/JSON pages can be fetched")
            body = await response.content.read(20001)
            if len(body) > 20000:
                body = body[:20000]
            encoding = response.charset or "utf-8"
            return body.decode(encoding, errors="replace")


def _step_key(tid: str, index: int, action: str, args: dict[str, Any]) -> str:
    payload = json.dumps({"task": tid, "index": index, "action": action, "input": args},
                         sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _persist_plan(tid: str, steps: list[Any], max_attempts: int) -> bool:
    now = time.time()
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
        owned = c.execute("""SELECT 1 FROM tasks WHERE id=? AND status='planning'
            AND cancel_requested=0 AND lease_owner=? AND lease_expires_at>?""",
            (tid, _current_lease_owner(), now)).fetchone()
        if not owned:
            return False
        existing = c.execute("SELECT COUNT(*) AS n FROM task_steps WHERE task_id=?", (tid,)).fetchone()["n"]
        if existing:
            c.execute("""UPDATE tasks SET status='running',updated_at=? WHERE id=?
                AND status='planning' AND lease_owner=? AND lease_expires_at>?""",
                (now, tid, _current_lease_owner(), now))
            return True
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
        changed = c.execute("""UPDATE tasks SET plan_json=?,status='running',updated_at=?
            WHERE id=? AND status='planning' AND cancel_requested=0
            AND lease_owner=? AND lease_expires_at>?""",
            (json.dumps(safe_steps, ensure_ascii=False), now, tid, _current_lease_owner(), now)).rowcount
        return bool(changed)


def _create_approval(tid: str, description: str, idempotency_key: str) -> str | None:
    now = time.time()
    approval_id = str(uuid.uuid5(uuid.NAMESPACE_URL, idempotency_key))
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
        owned = c.execute("""SELECT 1 FROM tasks WHERE id=? AND status='running'
            AND cancel_requested=0 AND lease_owner=? AND lease_expires_at>?""",
            (tid, _current_lease_owner(), now)).fetchone()
        if not owned:
            return None
        inserted = c.execute("""INSERT OR IGNORE INTO approvals(id,task_id,description,status,created_at,idempotency_key)
                     VALUES(?,?,?,'pending',?,?)""",
                  (approval_id, tid, description, now, idempotency_key)).rowcount
        approval = c.execute("SELECT id FROM approvals WHERE idempotency_key=?",
                             (idempotency_key,)).fetchone()
        if not approval:
            raise RuntimeError("Approval record could not be created")
        if inserted:
            c.execute("INSERT INTO events(task_id,created_at,kind,message) VALUES(?,?,?,?)",
                      (tid, now, "approval_required", description[:2000]))
            c.execute("UPDATE tasks SET updated_at=? WHERE id=?", (now, tid))
        return approval["id"]


def _save_artifact(tid: str, filename: str, content: str) -> str | None:
    target = (WORKSPACE / (tid + "-" + filename)).resolve()
    if target.parent != WORKSPACE:
        raise ValueError("Invalid artifact path")
    temporary = target.with_name(target.name + ".tmp-" + uuid.uuid4().hex)
    now = time.time()
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
        changed = c.execute("""UPDATE tasks SET lease_expires_at=?,updated_at=?
            WHERE id=? AND status='running' AND cancel_requested=0
            AND lease_owner=? AND lease_expires_at>?""",
            (now + LEASE_SECONDS, now, tid, _current_lease_owner(), now)).rowcount
        if not changed:
            return None
        try:
            temporary.write_text(content[:100000], encoding="utf-8")
            os.replace(temporary, target)
        finally:
            if temporary.exists():
                temporary.unlink()
    return target.name


def _task_workspace_root(tid: str) -> Path:
    root = (WORKSPACE / tid).resolve()
    if root.parent != WORKSPACE.resolve():
        raise ValueError("Invalid task workspace")
    return root


def _workspace_target(tid: str, relative_path: str) -> tuple[Path, Path]:
    if not isinstance(relative_path, str) or not relative_path.strip() or len(relative_path) > 240:
        raise ValueError("Workspace path must be a non-empty relative path up to 240 characters")
    if "\x00" in relative_path or relative_path.startswith(("/", "\\")) or "\\" in relative_path:
        raise ValueError("Workspace path must be a safe relative POSIX path")
    parts = relative_path.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError("Workspace path contains an invalid component")
    root = _task_workspace_root(tid)
    target = root.joinpath(*parts)
    resolved = target.resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError("Workspace path escapes the task workspace") from exc
    current = root
    for part in parts:
        current = current / part
        if current.is_symlink():
            raise ValueError("Symbolic links are not allowed in the task workspace")
    return root, resolved


def _workspace_write(tid: str, relative_path: str, content: str) -> dict[str, Any]:
    if not isinstance(content, str) or len(content.encode("utf-8")) > 100_000:
        raise ValueError("Workspace files are limited to 100 KB of UTF-8 text")
    root, target = _workspace_target(tid, relative_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    # Revalidate after directory creation before writing.
    _, target = _workspace_target(tid, relative_path)
    temporary = target.with_name(target.name + ".tmp-" + uuid.uuid4().hex)
    try:
        temporary.write_text(content, encoding="utf-8")
        os.replace(temporary, target)
    finally:
        if temporary.exists():
            temporary.unlink()
    return {"path": relative_path, "bytes": len(content.encode("utf-8"))}


def _leased_workspace_write(tid: str, relative_path: str, content: str) -> dict[str, Any] | None:
    now = time.time()
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
        owned = c.execute("""SELECT 1 FROM tasks WHERE id=? AND status='running'
            AND cancel_requested=0 AND lease_owner=? AND lease_expires_at>?""",
            (tid, _current_lease_owner(), now)).fetchone()
        if not owned:
            return None
        return _workspace_write(tid, relative_path, content)


def _workspace_read(tid: str, relative_path: str) -> dict[str, Any]:
    _, target = _workspace_target(tid, relative_path)
    if not target.is_file():
        raise FileNotFoundError("Workspace file not found")
    size = target.stat().st_size
    if size > 100_000:
        raise ValueError("Workspace files are limited to 100 KB")
    return {"path": relative_path, "content": target.read_text(encoding="utf-8")}


def _workspace_list(tid: str, relative_path: str = ".") -> dict[str, Any]:
    if relative_path == ".":
        root = _task_workspace_root(tid)
        root.mkdir(parents=True, exist_ok=True)
        target = root
    else:
        _, target = _workspace_target(tid, relative_path)
    if not target.is_dir():
        raise NotADirectoryError("Workspace directory not found")
    entries = []
    for child in sorted(target.iterdir(), key=lambda item: item.name)[:100]:
        if child.is_symlink():
            continue
        entries.append({"name": child.name, "type": "directory" if child.is_dir() else "file",
                        "bytes": child.stat().st_size if child.is_file() else None})
    return {"path": relative_path, "entries": entries}


def _workspace_search(tid: str, query: str) -> dict[str, Any]:
    if not isinstance(query, str) or not query or len(query) > 200:
        raise ValueError("Search query must contain 1 to 200 characters")
    root = _task_workspace_root(tid)
    matches = []
    scanned_bytes = 0
    for path in sorted(root.rglob("*")):
        if len(matches) >= 100 or path.is_symlink() or not path.is_file():
            continue
        try:
            resolved = path.resolve()
            resolved.relative_to(root)
            size = path.stat().st_size
            if size > 100_000 or scanned_bytes + size > 2_000_000:
                continue
            scanned_bytes += size
            content = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError, ValueError):
            continue
        for line_number, line in enumerate(content.splitlines(), 1):
            if query.casefold() in line.casefold():
                matches.append({"path": path.relative_to(root).as_posix(),
                                "line": line_number, "text": line[:500]})
                if len(matches) >= 100:
                    break
    return {"query": query, "matches": matches}


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
        content = await _fetch_public_text(url)
        return {"title": title, "status": "completed", "url": url, "content": content}
    if action == "workspace_list":
        result = await run_db(_workspace_list, tid, str(args.get("path", ".")))
        return {"title": title, "status": "completed", **result}
    if action == "workspace_read":
        result = await run_db(_workspace_read, tid, str(args.get("path", "")))
        return {"title": title, "status": "completed", **result}
    if action == "workspace_write":
        result = await run_db(_leased_workspace_write, tid, str(args.get("path", "")),
                              str(args.get("content", "")))
        if result is None:
            raise RuntimeError("Worker lease lost before workspace write")
        return {"title": title, "status": "completed", **result}
    if action == "workspace_search":
        result = await run_db(_workspace_search, tid, str(args.get("query", "")))
        return {"title": title, "status": "completed", **result}
    if action == "save_artifact":
        name = Path(str(args.get("filename", "result.md"))).name[:100]
        if not name or name in {".", ".."}:
            raise ValueError("Invalid filename")
        artifact = await run_db(_save_artifact, tid, name, str(args.get("content", "")))
        if artifact is None:
            raise RuntimeError("Worker lease lost before artifact persistence")
        return {"title": title, "status": "completed", "artifact": artifact}
    if action == "draft_external_action":
        description = str(args.get("description", "External action requested"))[:1000]
        approval_id = await run_db(_create_approval, tid, description, idempotency_key)
        if approval_id is None:
            raise RuntimeError("Worker lease lost before approval creation")
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
                  (tid, _current_lease_owner()))


def _task_cancelled(tid: str) -> bool:
    with db() as c:
        row = c.execute("SELECT status,cancel_requested FROM tasks WHERE id=?", (tid,)).fetchone()
    return not row or row["status"] == "cancelled" or bool(row["cancel_requested"])



def _get_task_row(tid: str):
    with db() as c:
        return c.execute("SELECT * FROM tasks WHERE id=?", (tid,)).fetchone()


def _set_task_planning(tid: str) -> bool:
    now = time.time()
    with db() as c:
        changed = c.execute("""UPDATE tasks SET status='planning',updated_at=? WHERE id=?
            AND status='running' AND cancel_requested=0 AND lease_owner=? AND lease_expires_at>?""",
            (now, tid, _current_lease_owner(), now)).rowcount
    return bool(changed)


def _mark_task_cancelled(tid: str) -> bool:
    """Finalize an explicit cancellation without allowing a stale worker to cancel a reclaimed task."""
    now = time.time()
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
        task = c.execute("SELECT status,cancel_requested FROM tasks WHERE id=?", (tid,)).fetchone()
        if not task or (task["status"] != "cancelled" and not task["cancel_requested"]):
            return False
        changed = c.execute(
            """UPDATE tasks SET status='cancelled',cancel_requested=1,lease_owner='',
                lease_expires_at=NULL,updated_at=?
                WHERE id=? AND (status='cancelled' OR cancel_requested=1)""",
            (now, tid),
        ).rowcount
        if changed:
            c.execute("UPDATE approvals SET status='cancelled' WHERE task_id=? AND status='pending'",
                      (tid,))
        return bool(changed)


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


def _set_task_waiting_for_approval(tid: str) -> bool:
    now = time.time()
    with db() as c:
        changed = c.execute("""UPDATE tasks SET status='waiting_for_approval',lease_owner='',
            lease_expires_at=NULL,updated_at=? WHERE id=? AND status='running'
            AND cancel_requested=0 AND lease_owner=? AND lease_expires_at>?""",
            (now, tid, _current_lease_owner(), now)).rowcount
    return bool(changed)


def _set_task_retry_wait(tid: str, next_at: float) -> bool:
    now = time.time()
    with db() as c:
        changed = c.execute("""UPDATE tasks SET status='retry_wait',next_attempt_at=?,lease_owner='',
            lease_expires_at=NULL,updated_at=? WHERE id=? AND status='running'
            AND cancel_requested=0 AND lease_owner=? AND lease_expires_at>?""",
            (next_at, now, tid, _current_lease_owner(), now)).rowcount
    return bool(changed)


def _complete_task(tid: str, summary: str, results_json: str) -> bool:
    now = time.time()
    with db() as c:
        changed = c.execute("""UPDATE tasks SET status='completed',summary=?,results_json=?,
            error='',next_attempt_at=NULL,lease_owner='',lease_expires_at=NULL,updated_at=?
            WHERE id=? AND status='running' AND cancel_requested=0
            AND lease_owner=? AND lease_expires_at>?""",
            (summary[:12000], results_json, now, tid, _current_lease_owner(), now)).rowcount
    return bool(changed)


def _claim_step(tid: str, step_id: str):
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
        changed = c.execute("""UPDATE task_steps SET status='running',attempt_count=attempt_count+1,
            error='',updated_at=? WHERE id=? AND task_id=? AND status IN ('pending','retry_wait')
            AND EXISTS (
                SELECT 1 FROM tasks WHERE id=? AND status='running'
                AND cancel_requested=0 AND lease_owner=? AND lease_expires_at>?
            )""",
            (time.time(), step_id, tid, tid, _current_lease_owner(), time.time())).rowcount
        if not changed:
            return None
        return c.execute("SELECT * FROM task_steps WHERE id=? AND task_id=?",
                         (step_id, tid)).fetchone()


def _save_step_result(tid: str, step_id: str, status: str, result_json: str) -> bool:
    now = time.time()
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
        owned = c.execute("""SELECT 1 FROM tasks WHERE id=? AND status='running'
            AND cancel_requested=0 AND lease_owner=? AND lease_expires_at>?""",
            (tid, _current_lease_owner(), now)).fetchone()
        if not owned:
            return False
        changed = c.execute("""UPDATE task_steps SET status=?,result_json=?,updated_at=?
            WHERE id=? AND task_id=? AND status='running'""",
            (status, result_json, now, step_id, tid)).rowcount
        if not changed:
            return False
        rows = c.execute("SELECT result_json FROM task_steps WHERE task_id=? AND result_json!='' ORDER BY step_index",
                         (tid,)).fetchall()
        all_results = [json.loads(x["result_json"]) for x in rows]
        c.execute("UPDATE tasks SET results_json=?,updated_at=? WHERE id=? AND status='running' AND lease_owner=?",
                  (json.dumps(all_results, ensure_ascii=False), now, tid, _current_lease_owner()))
        return True


def _record_step_failure(tid: str, step_id: str, retry: bool, next_at: float | None, error: str) -> bool:
    now = time.time()
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
        owned = c.execute("""SELECT 1 FROM tasks WHERE id=? AND status='running'
            AND cancel_requested=0 AND lease_owner=? AND lease_expires_at>?""",
            (tid, _current_lease_owner(), now)).fetchone()
        if not owned:
            return False
        changed = c.execute("""UPDATE task_steps SET status=?,error=?,next_attempt_at=?,updated_at=?
            WHERE id=? AND task_id=? AND status='running'""",
            ("retry_wait" if retry else "failed", error[:2000], next_at, now, step_id, tid)).rowcount
        if not changed:
            return False
        if retry:
            task_changed = c.execute("""UPDATE tasks SET status='retry_wait',next_attempt_at=?,
                lease_owner='',lease_expires_at=NULL,updated_at=?
                WHERE id=? AND status='running' AND cancel_requested=0
                AND lease_owner=? AND lease_expires_at>?""",
                (next_at, now, tid, _current_lease_owner(), now)).rowcount
        else:
            task_changed = c.execute("""UPDATE tasks SET status='failed',error=?,lease_owner='',
                lease_expires_at=NULL,updated_at=? WHERE id=? AND status='running'
                AND cancel_requested=0 AND lease_owner=? AND lease_expires_at>?""",
                (error[:2000], now, tid, _current_lease_owner(), now)).rowcount
        if not task_changed:
            c.rollback()
            return False
        return True


def _record_task_failure(tid: str, exc: Exception, error: str) -> bool | None:
    now = time.time()
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
        current = c.execute("""SELECT attempt_count,max_attempts FROM tasks WHERE id=?
            AND status='running' AND cancel_requested=0 AND lease_owner=?
            AND lease_expires_at>?""", (tid, _current_lease_owner(), now)).fetchone()
        if not current:
            return None
        retry = bool(_is_transient(exc) and current["attempt_count"] < current["max_attempts"])
        next_at = now + _retry_delay(current["attempt_count"]) if retry else None
        status = "retry_wait" if retry else "failed"
        changed = c.execute("""UPDATE tasks SET status=?,error=?,next_attempt_at=?,
            lease_owner='',lease_expires_at=NULL,updated_at=? WHERE id=? AND status='running'
            AND cancel_requested=0 AND lease_owner=? AND lease_expires_at>?""",
            (status, error[:2000], next_at, now, tid, _current_lease_owner(), now)).rowcount
        return retry if changed else None


async def process(tid: str):
    row = await run_db(_get_task_row, tid)
    if not row:
        return
    started = time.monotonic()
    try:
        if not row["plan_json"] or row["plan_json"] == "[]":
            planning = await run_db(_set_task_planning, tid)
            if not planning:
                return
            await run_db(_worker_event, tid, "status", "Planning task")
            plan = await llm_json(
                'You are NEXUS Agent Planner. Return strict JSON only: {"summary":"...","steps":[{"title":"...","action":"web_search|fetch_url|save_artifact|workspace_list|workspace_read|workspace_write|workspace_search|draft_external_action","input":{}}]}. Use only listed actions. Use workspace_list/read/search to inspect task files and workspace_write to create or update files. Keep file paths relative to the task workspace. For publishing, messaging, purchases or account changes use draft_external_action. Never claim external actions succeeded.',
                row["prompt"], purpose="planning", task_id=tid, owner_id=row["owner_id"])
            steps = plan.get("steps", [])
            if not isinstance(steps, list):
                raise ValueError("Planner did not return steps")
            persisted = await run_db(_persist_plan, tid, steps[:min(MAX_STEPS, int(row['max_steps']), 8)], MAX_STEP_ATTEMPTS)
            if not persisted:
                return

        while True:
            if await run_db(_task_cancelled, tid):
                await run_db(_mark_task_cancelled, tid)
                await run_db(_worker_event, tid, "cancelled", "Task cancellation acknowledged")
                return
            if time.monotonic() - started > MAX_SECONDS:
                raise TimeoutError("Task time limit reached")
            step = await run_db(_next_pending_step, tid, time.time())
            if not step:
                waiting, unfinished, results_rows = await run_db(_step_summary, tid)
                if waiting:
                    transitioned = await run_db(_set_task_waiting_for_approval, tid)
                    if not transitioned:
                        await run_db(_mark_task_cancelled, tid)
                        await run_db(_worker_event, tid, "cancelled", "Task cancellation acknowledged")
                    return
                if unfinished:
                    transitioned = await run_db(_set_task_retry_wait, tid, time.time() + RETRY_BASE_SECONDS)
                    if not transitioned:
                        await run_db(_mark_task_cancelled, tid)
                        await run_db(_worker_event, tid, "cancelled", "Task cancellation acknowledged")
                    return
                results = [json.loads(x["result_json"]) for x in results_rows]
                final = await llm_json(
                    'Return strict JSON only: {"summary":"..."}. Summarize only supplied results. Explicitly identify blocked or unavailable actions; do not claim they succeeded.',
                    json.dumps({"goal": row["prompt"], "results": results}, ensure_ascii=False)[:30000],
                    purpose="evaluation", task_id=tid, owner_id=row["owner_id"])
                completed = await run_db(_complete_task, tid, str(final.get("summary", "Task finished")),
                                         json.dumps(results, ensure_ascii=False))
                if completed:
                    await run_db(_worker_event, tid, "completed", "Task completed")
                else:
                    await run_db(_mark_task_cancelled, tid)
                    await run_db(_worker_event, tid, "cancelled", "Task cancellation acknowledged")
                return

            claimed = await run_db(_claim_step, tid, step["id"])
            if not claimed:
                # Another worker claimed it, or this task lost its lease / was cancelled.
                return
            await run_db(_worker_event, tid, "step_started", f"{claimed['step_index'] + 1}: {claimed['title']}")
            try:
                step_data = {"title": claimed["title"], "action": claimed["action"],
                             "input": json.loads(claimed["input_json"])}
                result = await run_step(step_data, tid, claimed["idempotency_key"])
                status = "waiting_for_approval" if result.get("status") == "awaiting_approval" else "completed"
                saved = await run_db(_save_step_result, tid, claimed["id"], status,
                                     json.dumps(result, ensure_ascii=False))
                if not saved:
                    return
                await run_db(_worker_event, tid, "step_finished", f"{claimed['step_index'] + 1}: {status}")
                if status == "waiting_for_approval":
                    transitioned = await run_db(_set_task_waiting_for_approval, tid)
                    if not transitioned:
                        await run_db(_mark_task_cancelled, tid)
                        await run_db(_worker_event, tid, "cancelled", "Task cancellation acknowledged")
                    return
            except Exception as exc:
                attempts = claimed["attempt_count"]
                retry = _is_transient(exc) and attempts < claimed["max_attempts"]
                next_at = time.time() + _retry_delay(attempts) if retry else None
                recorded = await run_db(_record_step_failure, tid, claimed["id"], retry, next_at, str(exc)[:2000])
                if not recorded:
                    return
                if await run_db(_task_cancelled, tid):
                    await run_db(_mark_task_cancelled, tid)
                    await run_db(_worker_event, tid, "cancelled", "Task cancellation acknowledged")
                else:
                    await run_db(_worker_event, tid, "step_retry" if retry else "error", str(exc)[:1000])
                return
    except Exception as exc:
        retry = await run_db(_record_task_failure, tid, exc, str(exc)[:2000])
        if retry is None:
            return
        if await run_db(_task_cancelled, tid):
            await run_db(_mark_task_cancelled, tid)
            await run_db(_worker_event, tid, "cancelled", "Task cancellation acknowledged")
        else:
            await run_db(_worker_event, tid, "task_retry" if retry else "error", str(exc)[:1000])


def recover_interrupted_tasks():
    """Single-worker SQLite recovery. Safe because supported actions are read-only or idempotent."""
    now = time.time()
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
        # Preserve work that may still be owned by another live worker.
        # Missing leases are legacy/interrupted work; future leases remain valid.
        rows = c.execute("""SELECT id,cancel_requested FROM tasks
            WHERE status IN ('planning','running')
            AND (lease_expires_at IS NULL OR lease_expires_at<=?)""", (now,)).fetchall()
        for row in rows:
            tid = row["id"]
            cancelled = bool(row["cancel_requested"])
            step_status = "skipped" if cancelled else "retry_wait"
            c.execute("""UPDATE task_steps SET status=?,next_attempt_at=?,updated_at=?,
                error=? WHERE task_id=? AND status='running'""",
                (step_status, None if cancelled else now, now,
                 "Task cancelled during worker interruption" if cancelled
                 else "Recovered after worker interruption", tid))
            c.execute("""UPDATE tasks SET status=?,next_attempt_at=?,lease_owner='',
                lease_expires_at=NULL,updated_at=? WHERE id=?""",
                ("cancelled" if cancelled else "retry_wait",
                 None if cancelled else now, now, tid))
            c.execute("INSERT INTO events(task_id,created_at,kind,message) VALUES(?,?,?,?)",
                      (tid, now, "cancelled" if cancelled else "recovered",
                       "Cancellation finalized after worker interruption" if cancelled
                       else "Task recovered after worker restart"))
        # Legacy running tasks with expired leases are also fenced from re-claim after cancellation.
        c.execute("""UPDATE tasks SET status=CASE WHEN cancel_requested=1 THEN 'cancelled' ELSE 'retry_wait' END,
            next_attempt_at=CASE WHEN cancel_requested=1 THEN NULL ELSE ? END,
            lease_owner='',lease_expires_at=NULL,updated_at=?
            WHERE status='running' AND lease_expires_at IS NOT NULL AND lease_expires_at<?""",
            (now, now, now))


def _claim_task_with_lease() -> tuple[str, str] | None:
    now = time.time()
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
        row = c.execute("""SELECT id FROM tasks WHERE status IN ('queued','retry_wait')
            AND cancel_requested=0
            AND (next_attempt_at IS NULL OR next_attempt_at<=?)
            ORDER BY created_at LIMIT 1""", (now,)).fetchone()
        if not row:
            return None
        tid = row["id"]
        owner = f"{WORKER_ID}:{uuid.uuid4()}"
        changed = c.execute("""UPDATE tasks SET status='running',lease_owner=?,lease_expires_at=?,
            attempt_count=attempt_count+1,next_attempt_at=NULL,updated_at=?
            WHERE id=? AND status IN ('queued','retry_wait') AND cancel_requested=0""",
            (owner, now + LEASE_SECONDS, now, tid)).rowcount
        return (tid, owner) if changed else None


def claim_task() -> str | None:
    claimed = _claim_task_with_lease()
    return claimed[0] if claimed else None


async def worker():
    while True:
        claimed = await run_db(_claim_task_with_lease)
        if claimed:
            tid, owner = claimed
            context_token = _LEASE_OWNER.set(owner)
            try:
                await process(tid)
            finally:
                _LEASE_OWNER.reset(context_token)
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
    gateway = ModelGateway(models=models)
    if body.stream:
        stream_iterator = gateway.stream_messages(body.messages, purpose="general")
        try:
            first_chunk = await anext(stream_iterator)
        except ModelGatewayError as exc:
            status = 503 if "No configured model supports" in str(exc) else 502
            raise HTTPException(status, str(exc)) from exc

        async def event_stream():
            try:
                yield "data: " + json.dumps(first_chunk, ensure_ascii=False, separators=(",", ":")) + "\n\n"
                async for chunk in stream_iterator:
                    yield "data: " + json.dumps(chunk, ensure_ascii=False, separators=(",", ":")) + "\n\n"
            except ModelGatewayError:
                error = {"error": {"message": "The model stream was interrupted.",
                                   "type": "provider_stream_error"}}
                yield "data: " + json.dumps(error, separators=(",", ":")) + "\n\n"
            finally:
                await stream_iterator.aclose()
            yield "data: [DONE]\n\n"

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no",
                     "Connection": "keep-alive"},
        )
    try:
        result = await gateway.complete_messages(body.messages, purpose="general")
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


def _read_task_event_batch(tid: str, owner_id: str, after_event_id: int) -> dict[str, Any] | None:
    with db() as c:
        task = c.execute("SELECT status FROM tasks WHERE id=? AND owner_id=?",
                         (tid, owner_id)).fetchone()
        if not task:
            return None
        rows = c.execute("""SELECT id,created_at,kind,message FROM events
            WHERE task_id=? AND id>? ORDER BY id LIMIT 200""",
            (tid, after_event_id)).fetchall()
    return {"status": task["status"], "events": [dict(row) for row in rows]}


async def _task_event_stream(tid: str, owner_id: str, after_event_id: int, request: Request):
    cursor = after_event_id
    last_heartbeat = time.monotonic()
    terminal_statuses = {"completed", "failed", "cancelled"}
    while True:
        if await request.is_disconnected():
            break
        batch = await run_db(_read_task_event_batch, tid, owner_id, cursor)
        if batch is None:
            break
        for item in batch["events"]:
            cursor = item["id"]
            payload = json.dumps(item, ensure_ascii=False, separators=(",", ":"))
            yield f"id: {cursor}\nevent: task_event\ndata: {payload}\n\n"
        if batch["status"] in terminal_statuses and not batch["events"]:
            break
        now = time.monotonic()
        if now - last_heartbeat >= 15:
            yield ": keep-alive\n\n"
            last_heartbeat = now
        await asyncio.sleep(1)


@app.get("/v1/tasks/{tid}/events", dependencies=[Depends(auth)])
async def stream_task_events(
    tid: str,
    request: Request,
    after_event_id: int = 0,
    owner_id: str = Depends(auth),
):
    if after_event_id < 0:
        raise HTTPException(422, "after_event_id must be non-negative")
    exists = await run_db(
        lambda: _read_task_event_batch(tid, owner_id, after_event_id) is not None
    )
    if not exists:
        raise HTTPException(404, "Task not found")
    return StreamingResponse(
        _task_event_stream(tid, owner_id, after_event_id, request),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/v1/tasks/{tid}", dependencies=[Depends(auth)])
def get_task(tid: str, after_event_id: int | None = None, owner_id: str = Depends(auth)):
    if after_event_id is not None and after_event_id < 0:
        raise HTTPException(422, "after_event_id must be non-negative")
    with db() as c:
        row = c.execute("SELECT * FROM tasks WHERE id=? AND owner_id=?", (tid, owner_id)).fetchone()
        if not row:
            raise HTTPException(404, "Task not found")
        events = c.execute("""SELECT id,created_at,kind,message FROM events
            WHERE task_id=? AND (? IS NULL OR id>?) ORDER BY id LIMIT 200""",
            (tid, after_event_id, after_event_id)).fetchall()
        steps = c.execute("""SELECT step_index,title,action,status,attempt_count,max_attempts,
            error,next_attempt_at,result_json FROM task_steps WHERE task_id=? ORDER BY step_index""",
            (tid,)).fetchall()
    return {**serialize(row), "events": [dict(x) for x in events],
            "steps": [{**dict(x), "result": json.loads(x["result_json"]) if x["result_json"] else None}
                      for x in steps]}


def _cancel_task_record(tid: str, owner_id: str) -> dict[str, str]:
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
        row = c.execute("SELECT status FROM tasks WHERE id=? AND owner_id=?", (tid, owner_id)).fetchone()
        if not row:
            raise HTTPException(404, "Task not found")
        if row["status"] in {"completed", "failed", "cancelled"}:
            raise HTTPException(409, "Task already finished")
        now = time.time()
        if row["status"] in {"queued", "retry_wait", "waiting_for_approval", "paused"}:
            c.execute("UPDATE tasks SET cancel_requested=1,status='cancelled',lease_owner='',lease_expires_at=NULL,updated_at=? WHERE id=?",
                      (now, tid))
        else:
            c.execute("UPDATE tasks SET cancel_requested=1,updated_at=? WHERE id=?", (now, tid))
        c.execute("UPDATE approvals SET status='cancelled' WHERE task_id=? AND status='pending'",
                  (tid,))
        c.execute("INSERT INTO events(task_id,created_at,kind,message) VALUES(?,?,?,?)",
                  (tid, now, "cancel_requested", "Cancellation requested by user"))
    return {"id": tid, "status": "cancellation_requested"}


@app.post("/v1/tasks/{tid}/cancel", dependencies=[Depends(auth)])
def cancel_task(tid: str, owner_id: str = Depends(auth)):
    return _cancel_task_record(tid, owner_id)


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
        # Resume the durable worker after the approval decision. The approved/rejected
        # decision is persisted in the step result before the task becomes claimable.
        c.execute("""UPDATE tasks SET status='queued',next_attempt_at=NULL,lease_owner='',
            lease_expires_at=NULL,updated_at=? WHERE id=? AND status='waiting_for_approval'
            AND cancel_requested=0""", (now, row["task_id"]))
        c.execute("INSERT INTO events(task_id,created_at,kind,message) VALUES(?,?,?,?)",
                  (row["task_id"], now, "approval_decision", status + ": " + note))
    return {"approval_id": approval_id, "status": status,
            "detail": "Decision recorded and task re-queued. No third-party action is executed by this generic backend."}


@app.post("/v1/approvals/{approval_id}", dependencies=[Depends(auth)])
def decide_approval(approval_id: str, body: Approval, owner_id: str = Depends(auth)):
    return _decide_approval_record(approval_id, owner_id, body.approved, body.note)