# NEXUS Agent Core

First cloud backend milestone for long-running NEXUS tasks. It runs separately from Android so queued work can continue when the phone app is closed.

## Implemented
- Persistent SQLite task queue and event log.
- Authenticated API to create, list, inspect, and cancel tasks.
- Background worker and task planning through a provider-agnostic Model Gateway (OpenAI-compatible chat-completions adapters).\n- Optional multi-provider registry with purpose-based routing, explicit priority, and limited fallback for transient failures. The legacy single-provider environment variables remain supported.\n- Normalized provider errors, JSON response validation, and token-usage capture when the provider returns usage metadata.
- Allowlisted actions: optional Tavily web search, HTTPS text/JSON fetching with private-network IP blocking, and task-scoped artifact creation.
- Approval records for external side effects. This generic worker does not send messages, publish listings, make purchases, or modify third-party accounts.
- Docker image and persistent data directory.

## Not yet implemented
Avito Messenger/listing connectors, browser automation, isolated code execution, Android task dashboard, push notifications, and resumable approval continuation. Do not connect a real Avito account until official API credentials and permissions are configured.

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
