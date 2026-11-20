# NEXUS Agent Core

First cloud backend milestone for long-running NEXUS tasks. It runs separately from Android so queued work can continue when the phone app is closed.

## Implemented
- Persistent SQLite task queue and event log.
- Authenticated API to create, list, inspect, and cancel tasks.
- Background worker and task planning through a provider-agnostic Model Gateway (OpenAI-compatible chat-completions adapters).\n- Optional multi-provider registry with purpose-based routing, explicit priority, and limited fallback for transient failures. The legacy single-provider environment variables remain supported.\n- Normalized provider errors, JSON response validation, and token-usage capture when the provider returns usage metadata.
- Allowlisted actions: optional Tavily web search, HTTPS text/JSON fetching with private-network IP blocking, and task-scoped artifact creation.
- Approval records for external side effects. This generic worker does not send messages, publish listings, make purchases, or modify third-party accounts.
- Docker image and persistent data directory.

## Durable task execution

- Plans are persisted as ordered task steps before execution; each step records status, attempts, result/error, retry time, and a deterministic idempotency key.
- Worker startup recovers tasks left in planning/running states after a process interruption. Supported current actions are read-only (search/fetch) or idempotent artifact writes; approval records are deduplicated by the step key.
- The SQLite worker claims due tasks in a short BEGIN IMMEDIATE transaction, records a lease owner/expiry, and uses bounded exponential backoff for transient network/HTTP failures.
- Task inspection includes per-step status and retry metadata. Cancellation is cooperative for active work.
- API task creation accepts an optional idempotency_key (8–128 characters) to avoid duplicate submissions; max_steps is persisted per task.
- This remains a single-process, single-replica SQLite prototype. The lease is not distributed coordination; do not run multiple replicas. PostgreSQL plus a durable queue/locking strategy is required for multi-worker production.

## Not yet implemented
Avito Messenger/listing connectors, browser automation, isolated code execution, Android task dashboard, push notifications, and automatic execution of approved external actions (approval records currently pause the task; the generic backend does not execute third-party side effects). Do not connect a real Avito account until official API credentials and permissions are configured.

## Model Gateway configuration\n\nThe default configuration remains the legacy AnyModel endpoint in `.env.example`. To register multiple compatible providers, set `NEXUS_MODEL_PROVIDERS` to a JSON array. Each entry requires `id`, `base_url`, `model`, and `api_key_env`; optional fields are `priority`, `purposes`, and `enabled`. The API key is read from the named server environment variable and is never included in the registry JSON. Purpose routing can be overridden with `NEXUS_MODEL_ROUTE_PLANNING` and `NEXUS_MODEL_ROUTE_EVALUATION` (comma-separated provider IDs).\n\nFallback is attempted only for network failures, timeouts, and transient HTTP statuses (408, 429, 5xx). Authentication and other permanent HTTP errors stop routing to avoid hiding configuration problems. This first gateway milestone does not yet provide provider price accounting, hard token budgets, streaming, vision, or vendor-specific tool calling; those require verified provider metadata and separate tests.\n\n## Deploy
1. Deploy this directory as a Docker service on a host with persistent volumes.
2. Mount persistent storage at /data.
3. Configure environment variables from .env.example. Set a strong random NEXUS_AGENT_API_KEY and NEXUS_LLM_API_KEY. Set model and base URL; add TAVILY_API_KEY for search.
4. Expose port 8080 over HTTPS only.
5. Verify GET /health.

Keep model provider credentials on the server; never embed them in the APK or commit them to Git.

## API
Every /v1 route requires Authorization: Bearer with NEXUS_AGENT_API_KEY.
- POST /v1/tasks with JSON {"prompt":"Research competitors and prepare a report","autonomy":"level3","max_steps":8}
- GET /v1/tasks
- GET /v1/tasks/{id} (includes event log)
- POST /v1/tasks/{id}/cancel
- GET /v1/approvals
- POST /v1/approvals/{id} with JSON {"approved":true,"note":"Reviewed"}

## Operations and security
Use persistent disk, HTTPS, strong API key, provider spend limits, backups, and host resource limits. The worker is single-process and SQLite-backed; use one service replica until shared queue/database and distributed locking exist. The service is not an arbitrary shell sandbox and must not be exposed to untrusted users.

## Benchmark-aware model selection

The gateway now infers a coarse task category (coding, research, math, vision, writing, or general) without an extra model call. It ranks eligible providers using configured category benchmark scores, then cost and latency metadata. Quality has the dominant weight; cost and latency help choose between similarly capable candidates. Existing explicit purpose-route environment variables remain manual overrides, and provider priority remains the deterministic tie-breaker.

Add `benchmark_scores` (0..100), `cost_per_million_tokens`, `latency_ms`, `ranking_source`, and `ranking_updated_at` to each provider entry in `NEXUS_MODEL_PROVIDERS`. Scores must be traceable to named, dated public benchmarks or NEXUS internal evaluations. The gateway does not currently fetch leaderboard data live, validate benchmark methodology, or automatically refresh model rankings. Missing metadata falls back to neutral scoring and then configured priority; it must not be interpreted as evidence that a model is objectively better. Vision routing metadata is preparatory: image payload handling is not implemented yet.

A benchmark score is not a universal intelligence score. Maintain separate measurements by task category, account for benchmark freshness and contamination risk, and run NEXUS-specific evaluations before promoting a model. Never put provider API keys in the registry JSON or commit secrets.

Retry controls: NEXUS_AGENT_STEP_MAX_ATTEMPTS (default 3, capped at 8) and NEXUS_AGENT_RETRY_BASE_SECONDS (default 10 seconds, exponential backoff capped at 15 minutes). Only transient network failures and selected HTTP statuses are retried. Permanent HTTP/authentication errors and validation failures are terminal.
