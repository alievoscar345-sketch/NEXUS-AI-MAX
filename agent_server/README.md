# NEXUS Agent Core

First cloud backend milestone for long-running NEXUS tasks. It runs separately from Android so queued work can continue when the phone app is closed.

## Implemented
- Persistent SQLite task queue and event log.
- Authenticated API to create, list, inspect, and cancel tasks.
- Background worker and task planning through an OpenAI-compatible chat-completions API.
- Allowlisted actions: optional Tavily web search, HTTPS text/JSON fetching with private-network IP blocking, and task-scoped artifact creation.
- Approval records for external side effects. This generic worker does not send messages, publish listings, make purchases, or modify third-party accounts.
- Docker image and persistent data directory.

## Not yet implemented
Avito Messenger/listing connectors, browser automation, isolated code execution, Android task dashboard, push notifications, and resumable approval continuation. Do not connect a real Avito account until official API credentials and permissions are configured.

## Deploy
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
