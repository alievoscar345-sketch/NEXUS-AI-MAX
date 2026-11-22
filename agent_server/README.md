# NEXUS Agent Core

Cloud backend for NEXUS AI MAX. It runs separately from Android so queued agent tasks can continue while the phone app is closed. The API and durable task worker can run as separate restartable services.

## Implemented

- Authenticated API for creating, listing, inspecting, and cancelling tasks.
- Persistent SQLite task queue, ordered task steps, event log, retry scheduling, safe pause/resume, and lease-fenced worker writes.
- A baseline checkpoint is created after project files are materialized and before planning starts, including for tasks created before checkpoint support.
- The Android agent console exposes `GET /v1/tasks/{task_id}/checkpoints` history with verified workspace-file SHA-256 manifests, project-file revisions, artifact manifests, timestamps, and step counts. Two snapshots can be compared by file hash in the app.
- Manual rollback uses `POST /v1/tasks/{task_id}/checkpoints/{checkpoint_id}/restore` with the latest history sequence as a concurrency guard. Only paused or finished tasks can be restored; the server reserves the task during restore, verifies the archive, restores workspace/project/artifact state and plan/steps, then leaves the task paused for review before the user resumes it. The restore target and lease are persisted so a worker can replay an interrupted manual restore after a process crash; an unverifiable restore fails closed instead of allowing the task to continue with mixed state. The Android client incrementally requests only checkpoints newer than its last sequence, avoiding repeated hashing of the entire history during polling.
- Immutable task checkpoints archive the complete task workspace and every published task artifact (including release APKs), and persist the matching plan, results, ordered step state, artifact manifest, and immutable project-file revision manifest after initial planning, dynamic plan extension, and each completed/failed step boundary.
- Checkpoint archives are SHA-256 verified before restore, extracted into staging directories with path/symlink checks, then used to restore workspace files and published artifact bytes. Recovery also restores project files as new immutable revisions matching the checkpoint, including tombstones for files created after it, and restores the checkpointed plan/step database state before re-queueing interrupted work.
- Failed or interrupted step mutations are rolled back to the last confirmed workspace snapshot; the failed step result remains recorded for planner-driven repair. A corrupt or missing latest checkpoint stops the task rather than continuing against inconsistent files.
- Checkpoints live beside the SQLite database on the persistent /data volume. NEXUS_AGENT_MAX_CHECKPOINT_BYTES and NEXUS_AGENT_MAX_CHECKPOINT_FILES set archive safety bounds; NEXUS_AGENT_CHECKPOINT_RETENTION (default 5) controls rolling per-task history.
- Configurable in-process worker concurrency (`NEXUS_AGENT_WORKER_CONCURRENCY`, default 2; supported range 1–8) with isolated per-task lease context; one worker process remains required for this SQLite deployment.
- Long tasks retain durable checkpoints and automatically recover after backend restarts; the active worker renews its lease rather than stopping at a fixed task-duration budget.
- Task planning and evaluation through a provider-agnostic OpenAI-compatible Model Gateway.
- Multi-provider routing using task category, configured benchmark metadata, cost/latency metadata, and provider priority.
- Autonomous tasks accept a persisted per-task model selection: `auto` uses the task-category router, while an explicit provider model ID pins every planner, continuation, specialist-review, and evaluation call to that model without cross-model fallback. Model IDs are validated when the task is queued.
- Limited provider fallback for network failures, timeouts, and transient HTTP errors. No fallback after streamed output has started.
- Server-sent event streams for chat responses and task events.
- Encrypted per-owner provider credentials and GitHub repository tokens; secrets are never returned by status or listing endpoints.
- Standard chat endpoint with bounded text and inline image payloads. The Android app extracts supported document text locally and sends it as message context.
- Provider connectivity checks and an image-understanding probe that can confirm and enable the provider's vision capability.
- Task-scoped workspace files plus binary artifact publication, listing, text preview, SHA-256 metadata, and authenticated downloads; Android can save original files such as APKs to device storage.
- Persistent project file storage in the shared SQLite database with immutable per-file revisions, SHA-256 metadata, version history, and restore-as-new-revision; project files are materialized into linked task workspaces and can be explicitly saved back with lease-fenced `project_save_file` steps.
- Optional isolated Docker code execution with digest-pinned images, no network egress, read-only container root, no fixed CPU/memory/PID/wall-clock ceilings, and task-workspace-only writable mount; requires explicit operator configuration.
- Optional Tavily search and HTTPS text/JSON fetching with private-network destination protections.
- Approval records for proposed external side effects.
- Optional isolated code execution through a digest-pinned Docker runner with no default command timeout or fixed CPU/memory/PID ceilings, no network egress, bounded output capture, and task-workspace-only write access.

## Not yet implemented

Avito Messenger/listing connectors, general browser automation, automatic pull-request creation, production-grade VM/microVM isolation, push notifications, and automatic execution of approved external actions. Approval currently records the decision and resumes the task; it does not send messages, publish listings, make purchases, or modify third-party accounts. Do not connect a real Avito account until official API credentials and permissions are configured.

## Always-on background worker

For phone-independent execution, deploy both services from `docker-compose.yml` on an always-on Docker host with a persistent disk:

1. Copy `.env.example` to `.env` and set the API key, credential-encryption key, and model-provider credentials. Keep `.env` private and out of Git.
2. Start both services with `docker compose up -d --build`.
3. Confirm the API health check is healthy and the `worker` service remains running: `docker compose ps`.
4. Keep the named `nexus_data` volume. It contains the SQLite database, task checkpoints, workspace files, and artifacts.
5. Verify `GET /health/worker` returns HTTP 200; it reports a durable worker heartbeat, running/pending task counts, and the age of the oldest pending task. It returns HTTP 503 if the worker has stopped or its heartbeat is older than 90 seconds. The Compose worker health check uses this endpoint.
6. Configure host-level backups for that volume and securely back up the encryption key separately.

The API service has `NEXUS_AGENT_WORKER_ENABLED=false`; it accepts requests but does not execute tasks. The separate `worker` service runs `python -m app.worker`, claims durable tasks, renews leases, and recovers expired in-progress tasks after restart. Within that process, `NEXUS_AGENT_WORKER_CONCURRENCY` controls how many independent tasks can execute concurrently (default 2; set to 1 for strictly sequential work), while `NEXUS_AGENT_WORKER_POLL_SECONDS` controls the idle queue polling interval. Both services must use the same persistent volume and identical environment configuration. Run exactly one worker replica with this SQLite deployment. Do not deploy this Compose file to a scale-to-zero/serverless platform: both services must remain running. Android is only a client; closing or powering off the phone does not stop work already accepted by the server. Tasks that require files or hardware available only on the phone cannot continue while it is offline.

This is a single-host deployment profile, not high availability. A host outage pauses execution until the host returns. Multi-host workers require moving from SQLite/local volume to PostgreSQL and a shared durable workspace/object store before enabling replicas.

## Deployment and security

1. Deploy this directory as a Docker service with persistent storage mounted at `/data`.
2. Configure environment variables from `.env.example`.
3. Set a strong random `NEXUS_AGENT_API_KEY` and `NEXUS_CREDENTIAL_ENCRYPTION_KEY`.
4. Expose port 8080 over HTTPS only and verify `GET /health`.
5. Back up the database and encryption key separately. If the encryption key is lost, stored provider credentials cannot be recovered.

The worker is a single-process, single-replica SQLite prototype. Do not run multiple replicas. PostgreSQL plus a durable queue and distributed locking are required before multi-worker production deployment. Code execution is disabled by default and experimental. Do not expose this prototype to untrusted users. A Docker daemon socket grants highly privileged control of its host; do not mount the host Docker socket into a publicly reachable API container. Prefer a separately administered sandbox daemon/host reachable through a tightly restricted, authenticated Docker endpoint (for example, a TLS-protected DOCKER_HOST), and treat the runner host as a separate security boundary. For hostile multi-tenant workloads, replace this Docker runner with a dedicated VM/microVM service.

## Model Gateway configuration

The legacy single-provider configuration remains supported through `NEXUS_LLM_BASE_URL`, `NEXUS_LLM_MODEL`, and `NEXUS_LLM_API_KEY`. The default endpoint is the compatible AnyModel gateway.

For multiple server-configured providers, set `NEXUS_MODEL_PROVIDERS` to a JSON array. Each entry requires `id`, `base_url`, `model`, and `api_key_env`. Optional fields include `priority`, `purposes`, `enabled`, `capabilities`, `benchmark_scores`, `cost_per_million_tokens`, `latency_ms`, `ranking_source`, and `ranking_updated_at`. Never place API keys directly in this JSON; the gateway reads them from the named environment variables.

Use `"capabilities":["vision"]` only when the selected model is confirmed to accept image input. User-managed providers can be tested from Android settings; a successful green-square vision probe enables the saved vision capability. The probe makes a small provider request and may incur token charges.

Task routing infers a coarse category (coding, research, math, vision, writing, or general) without an additional model call. Quality benchmark scores dominate the ranking; cost and latency help break ties. These scores are configuration metadata, not a universal intelligence score. Record the benchmark name, date, and methodology. Missing scores are neutral, not evidence of model quality.

Purpose routing can be overridden with `NEXUS_MODEL_ROUTE_PLANNING` and `NEXUS_MODEL_ROUTE_EVALUATION` (comma-separated provider IDs). Retry controls are `NEXUS_AGENT_STEP_MAX_ATTEMPTS` (default 3, capped at 8) and `NEXUS_AGENT_RETRY_BASE_SECONDS` (default 10 seconds, exponential backoff capped at 15 minutes).

## User-managed provider credentials

The backend supports encrypted per-owner credentials through:

- `GET /v1/providers` — provider metadata only; secrets are never returned.
- `GET /v1/models` — configured model IDs, source, purpose, and declared capabilities.
- `POST /v1/providers` — add or replace a credential using `provider_id`, `base_url`, `model`, and `api_key`.
- `POST /v1/providers/{provider_id}/test` — verify ordinary text connectivity.
- `POST /v1/providers/{provider_id}/test-vision` — submit a small generated image and verify that the model identifies its color. Only a confirmed result enables `supports_vision`.
- `DELETE /v1/providers/{provider_id}` — remove a stored credential.

Provider base URLs are HTTPS-only and host-allowlisted: AnyModel, OpenRouter, OpenAI, Google Gemini OpenAI-compatible endpoint, DeepSeek, xAI, and Mistral. Direct Anthropic Messages API is not supported by this OpenAI-compatible gateway; Claude models can be reached through compatible gateways such as AnyModel or OpenRouter.

Provider API keys are encrypted in SQLite using AES-256-GCM with a fresh 96-bit nonce per write. Set `NEXUS_CREDENTIAL_ENCRYPTION_KEY` to a base64-encoded 32-byte random key (for example, generate one with `openssl rand -base64 32`). Keep it outside the database and repository.

The current bearer-token authentication maps requests to the fixed owner `personal`. This is a single-user prototype, not multi-account authentication. A verified account/JWT layer must replace this mapping before the service is opened to multiple users; client-supplied owner IDs are never trusted.

## GitHub repository integration

A single repository can be connected to the single-user account through the Android settings screen:

- `POST /v1/github/connect` — verify a GitHub token and selected `owner/repository`, then encrypt and store the token.
- `GET /v1/github/status` — return repository/account metadata only; never returns the token.
- `DELETE /v1/github/connect` — delete the stored encrypted token and integration.
- `GET /v1/github/files` and `GET /v1/github/file?path=...&branch=...` — inspect repository files.
- Task actions `github_create_branch`, `github_checkout`, `github_list_files`, `github_read_file`, `github_write_file`, `github_commit_workspace`, and `github_build_android` let the agent work with the connected repository and build the feature branch. Checkout extracts the selected branch into the task workspace without restoring `.git` history, rejects traversal entries and skips symlinks, and uses configurable archive, extracted-size, and entry-count safety bounds.

Use a GitHub fine-grained personal access token restricted to the one selected repository, with Metadata read-only, Contents read/write, and Actions read/write. The token is transmitted only to the configured HTTPS NEXUS server and encrypted at rest using the same AES-256-GCM credential key as provider secrets. Never put it in a task prompt or chat message.

Each coding task gets a deterministic `nexus-agent/task-<task-id>` feature branch based on the repository's default branch. The worker checks its active lease before creating a branch or committing a file. Writes are accepted only on `nexus-agent/*` branches; `main`, `master`, and `nexus-1.0-development` are never write targets. Protected/default branches remain readable for inspection. The integration does not merge branches or create pull requests. GitHub's own token permissions and repository branch rules remain authoritative.

The Android workflow accepts `require_signed_release=true` for strict release builds. In this mode it skips the debug APK path and fails if any signing secret is absent. Configure these repository Actions secrets before requesting the final release APK: `NEXUS_KEYSTORE_BASE64`, `NEXUS_KEYSTORE_PASSWORD`, `NEXUS_KEY_ALIAS`, and `NEXUS_KEY_PASSWORD`. Do not send keystore files or passwords in chat. GitHub Actions artifacts are temporary (currently 14-day retention); NEXUS copies the APK into the task's persistent artifact storage after a successful workflow.


The current implementation commits changed and deleted workspace source files as one Git tree/commit update; individual UTF-8 file writes use the Contents API (1 MB per-file text limit). Checkout is a source snapshot rather than a full Git clone and preserves executable file bits; local Git history, branches, and merge state are not synchronized. Build outputs are excluded from repository commits. `github_build_android` dispatches `.github/workflows/build-android.yml`, waits for the run, downloads the release APK artifact, and publishes it as a task artifact. Failed CI runs return bounded, token-redacted log excerpts to the autonomous repair planner. This requires GitHub Actions write permission and the workflow's signing secrets.

## Chat model selection and response attribution

The ordinary Android chat can use the server model catalog in two modes. The request field `model_selection` accepts `auto` or an exact configured model ID (for user-managed providers this is `user-<provider_id>`). Manual selection is strict: the gateway filters to that exact model and does not silently fall back to another provider. If the selected model is unavailable or lacks a required capability (for example, image input), the request fails with a clear error.

In automatic mode, the Model Gateway classifies the request category, filters by required capabilities and configured purposes, then ranks eligible models using benchmark fit, cost, latency, provider priority, and optional route order. The current category classifier is deterministic and keyword-based; it is not yet a semantic LLM router. Streaming chat chunks include the actual selected `provider_id`, technical `model` ID, and configured `display_name`, so the Android client can show the model that really produced the answer beneath the response.

User-managed provider records have a separate human-readable `display_name` in addition to provider ID and technical model ID. The provider settings API never returns API keys. Editing an existing provider with a blank API-key field preserves its encrypted key; supplying a nonblank key replaces it. Multiple model configurations from the same provider can be stored under distinct provider IDs.

## Chat and attachments

`POST /v1/chat/completions` accepts OpenAI-style message history and returns a normalized completion. Set `stream=true` for an SSE response. The endpoint uses the same Model Gateway and merges built-in server providers with the authenticated owner's encrypted credentials.

Images are accepted only as base64 data URLs for PNG, JPEG, WEBP, or GIF, with an 8 MB decoded limit per image and a 12 MB total request limit. Remote image URLs are rejected to prevent server-side URL fetching. Vision requests are routed only to providers declaring the `vision` capability.

The Android app currently extracts text locally from TXT, MD, CSV, JSON, XML, PDF, and DOCX files. Scanned PDFs use on-device OCR. Extracted document text is bounded before being sent to the model; the app displays a truncation notice when the limit is reached. Image and document files are not uploaded to a generic server-side file store.

The chat gateway does not yet proxy provider-specific web-search or reasoning tools. Those remain in the direct-provider mode until compatible server-side tool adapters are implemented.

## Personal memory

The memory store is owner-scoped and supports creating, editing, pinning, searching, and deleting user-approved records. Relevant records are retrieved for later chats and inserted as untrusted context, so current user instructions take priority. Automatic context retrieval uses BM25 term ranking with title weighting and a small pin preference; it is not embedding-based semantic search.

- `GET /v1/memories`, `POST /v1/memories`, `PATCH /v1/memories/{id}`, and `DELETE /v1/memories/{id}` manage saved records.
- `POST /v1/memories/suggest` analyzes user-authored messages from the current chat and proposes durable preferences, goals, facts, project context, or instructions.
- Suggestions are drafts only. NEXUS does not save them until the user explicitly accepts each one in the Android memory manager.
- The extractor is instructed to exclude credentials and sensitive personal categories; users should still review each proposed item before saving.

### Personal skills and episodic recall

Enabled user-created skills are now selected by relevance to the current chat or agent task instead of injecting the newest skills indiscriminately. Matching uses field-weighted BM25 over skill names, descriptions, categories, and instructions; only a small relevant subset is added to normal chat and task-planning context. Skills remain user-managed records and can be disabled or deleted.

Completed agent tasks also provide a bounded episodic recall source. NEXUS retrieves a few earlier completed tasks with relevant goal/outcome terms and supplies their summaries as historical context during new task planning. Episodes are explicitly marked as potentially incomplete or outdated; they are not treated as instructions or proof that a new task has succeeded. Retrieval uses BM25 term ranking and is owner-scoped, not embedding-based semantic search.

## Scheduled automations

The backend now stores owner-scoped automations and dispatches due schedules into the same durable task queue used by the autonomous agent. Supported schedules are one-time timestamps, daily local times, selected weekdays, and recurring intervals (5 minutes to 7 days). Daily and weekly schedules use an IANA time-zone identifier, so their wall-clock time follows daylight-saving changes. The scheduler runs inside the single backend worker; the service must remain online for a scheduled run to be dispatched. Missed runs are dispatched once after the service returns rather than replayed repeatedly.

- `GET /v1/automations` — list schedules and latest task status.
- `GET /v1/automations/{id}/runs` — inspect recent executions, including task status, summary, and errors.
- `POST /v1/automations` — create a schedule.
- `PUT /v1/automations/{id}` — replace its configuration.
- `POST /v1/automations/{id}/pause` and `/resume` — pause or resume a schedule.
- `POST /v1/automations/{id}/run` — trigger it immediately.
- `DELETE /v1/automations/{id}` — delete the schedule and its run index; already-created tasks remain in task history.

A schedule creates ordinary NEXUS agent tasks. It does not grant new tool permissions: existing restrictions and approval gates still apply. This is a single-instance prototype scheduler, not a highly available distributed scheduler.

### Isolated code execution

The task worker supports the `code_execute` action through `app.sandbox`. It never evaluates task commands in the backend process or through a shell. The action accepts an argument array (not a shell string). It has no default wall-clock deadline; an explicit positive `timeout_seconds` may be supplied only when a task requires a deadline. The executable must be one of: `python`, `python3`, `pytest`, `node`, `npm`, `java`, `javac`, `gradle`, or `./gradlew`.

Execution is disabled unless all prerequisites are explicitly configured:
- Docker CLI and daemon are available to the backend.
- The backend runs as a non-root user.
- `NEXUS_SANDBOX_IMAGE` names a preloaded image pinned by its full SHA-256 digest. The worker uses `--pull=never`; it does not download arbitrary images.
- The selected image contains the requested executable and required dependencies.
The repository includes a starter image definition at `agent_server/sandbox/Dockerfile` with Python, pytest, Node.js/npm, a JDK, and Gradle. Build and push it to a registry you control, then configure `NEXUS_SANDBOX_IMAGE` with the exact registry image reference plus its immutable `@sha256:<64-hex-digest>`. Do not use a mutable tag by itself. The starter image does not include the Android SDK; Android builds require a separately maintained runner image with the required Android SDK/build-tools and preloaded Gradle dependencies. Since runtime networking is disabled, all build dependencies must be baked into the image or otherwise made available locally.

Each invocation uses a disposable container with network and IPC disabled, a read-only container root, all Linux capabilities dropped, no-new-privileges, a non-root container user matching the backend UID/GID, and only the current task workspace mounted writable. Fixed CPU, memory, PID-count, and wall-clock ceilings are not imposed by this runner; actual capacity remains bounded by the host and its administrator. Stdout and stderr are each capped at 64 KiB; the worker attempts to stop timed-out containers. The runner image itself must be built and audited by the operator. This is a constrained Docker boundary, not a substitute for a dedicated VM/microVM isolation service against hostile multi-tenant workloads.

The feature returns a blocked result instead of pretending execution occurred when Docker, the image, or safe backend-user configuration is missing. The Docker CLI honors the standard `DOCKER_HOST` environment variable, allowing an operator to target a separately administered runner daemon rather than mounting the host socket into the API container. Access to that daemon must be restricted because Docker control is effectively host-level authority. The Android app does not need Docker access.

### Autonomous software project planning

For software-development tasks, the planner now prioritizes explicit requirements and acceptance criteria, workspace inspection, architecture/file layout, ordered file-level implementation, build configuration, tests, and setup documentation. It maintains a requirements-to-deliverables progress document, reviews written files, and can request independent architect/implementer/critic review before finalizing. The planner has no fixed total-step ceiling. `max_steps` is legacy request metadata only and does not truncate plans or dynamically appended work.

Important limitation: the agent can now execute allowlisted commands in an optional isolated Docker runner when the operator provisions it. Without that runner, it can author source code and test files but cannot execute them. GitHub integration is a source snapshot and Git API workflow, not a full Git clone with local history; automatic pull-request creation and production-grade VM/microVM isolation are still missing. The current GitHub API integration can inspect a selected repository, create a per-task feature branch, check out a ZIP snapshot into the task workspace, read UTF-8 files, and commit UTF-8 file updates.

### Long-running background work

The Android app is not the worker. Once a task is accepted by the deployed backend, its plan, ordered steps, results, and events are persisted in SQLite. The backend worker runs independently of the phone. It renews its lease while active, checkpoints durable work, and recovers unfinished work after a backend restart. Whenever the current plan is exhausted, it evaluates the original requirements and dynamically creates additional steps until it determines that the task is complete. There is no fixed total step count or task wall-clock budget in the worker.

Completion still depends on a continuously deployed backend, persistent storage, valid model-provider credentials, available tools, and sufficient provider quota. The agent cannot manufacture missing infrastructure or bypass external service limits. Optional code execution uses the isolated Docker runner described below; it is blocked until explicitly configured. Full browser automation, repository checkout/branch integration, and multi-day coding loops with repository integration are not implemented. Android uses periodic WorkManager polling for task-completion notifications (15-minute minimum interval); server-push/FCM notifications are not implemented. A requested minimum duration should be interpreted as an availability/work window, not as a reason to waste compute after all acceptance criteria are met.

## Task API

Every `/v1` route requires `Authorization: Bearer` with `NEXUS_AGENT_API_KEY`.

- `POST /v1/tasks` — create a task; accepts an optional idempotency key, optional max_steps metadata, optional active `project_id`, and up to 24 KB of separate user-supplied text-file context (`context_text`). The file context is treated as untrusted content during planning.
- `GET /v1/tasks` — list tasks for the authenticated owner.
- `GET /v1/tasks/{id}` — inspect a task and its event log.
- `GET /v1/tasks/{id}/artifacts` — list published task artifacts with byte size, MIME type, and SHA-256.
- `GET /v1/tasks/{id}/artifacts/file?name=...` — preview UTF-8 text artifacts up to 100 KB.
- `GET /v1/tasks/{id}/artifacts/download?name=...` — download the original artifact bytes with an attachment filename.
- `GET /v1/tasks/{id}/events?after_event_id=N` — resume the task event stream from a cursor.
- `POST /v1/tasks/{id}/cancel` — request cooperative cancellation.
- `POST /v1/tasks/{id}/pause` and `/resume` — pause at a durable step boundary and continue later; an in-flight step is allowed to finish before pausing.
- `GET /v1/approvals` — list pending approvals.
- `POST /v1/approvals/{id}` — record an approval decision.
- Task workspace and artifact endpoints are task- and owner-scoped. The planner can publish a file from its task workspace with the `publish_artifact` action (`input.path`, optional `input.filename`). Binary files are copied atomically into persistent artifact storage; the default publication size ceiling is 2 GiB and can be changed with `NEXUS_AGENT_MAX_ARTIFACT_BYTES`. This storage ceiling is a configurable safety bound, not a limit on task steps or duration.

The supported worker actions are deliberately constrained. Do not interpret approval records as evidence that an external action was executed.


## Projects and reusable skills

Projects are owner-scoped containers for a durable name, description, and context. A task or scheduled automation can be linked to an active project ID; the planner receives that project's context before producing a task plan. Scheduled runs inherit the project association. Android also allows selecting a persistent active-project context for ordinary chat; that context is added alongside relevant personal memory and skills. Archiving prevents new tasks from being attached while preserving project history. Deleting a project leaves its tasks intact and clears their project association.

- `GET /v1/projects`, `POST /v1/projects`, `PATCH /v1/projects/{id}`, and `DELETE /v1/projects/{id}` manage projects.
- `GET /v1/projects/{id}/files` lists current project files; `GET /v1/projects/{id}/files/content?path=...` reads UTF-8 text; `PUT /v1/projects/{id}/files` creates a text revision; `PUT/GET /v1/projects/{id}/files/binary?path=...` uploads/downloads raw binary revisions; `GET /v1/projects/{id}/files/versions?path=...` lists history; `POST /v1/projects/{id}/files/restore?path=...` restores a prior revision as a new revision.
- Project file revisions support UTF-8 text plus raw binary upload/download endpoints (`/files/binary`); the configurable `NEXUS_AGENT_MAX_PROJECT_FILE_BYTES` limit defaults to 50 MiB per revision. Binary and text revisions share immutable history, SHA-256 integrity checks, restore, and task-workspace materialization. File contents and history are stored transactionally in the persistent SQLite database, not in temporary task folders. SHA-256 and byte length are checked before reads, task materialization, and restore; integrity mismatches fail closed. Tasks linked to a project receive a snapshot of its current files on first execution; the planner can persist changed workspace files with `project_save_file`. Existing task workspace files are not overwritten during snapshot materialization. Android's project manager exposes file browsing, editing, revision history, and restore.
- `POST /v1/tasks` and `POST/PUT /v1/automations` accept an optional `project_id`; it must refer to an active project owned by the authenticated account.
- The Android project manager includes a file browser/editor, creates new revisions on save, displays SHA-256 metadata and revision history, and can restore an earlier revision without deleting later history.

Reusable skills are owner-scoped instruction sets with name, description, category, instructions, and enabled state. The Android client can create, edit, enable/disable, and delete skills. Relevant enabled skills are selected with field-weighted BM25 ranking and supplied to the ordinary chat system context and autonomous task planner. Relevant user-approved memories are also retrieved for chat and autonomous task planning. This is lexical ranking, not semantic embedding search. Skills are treated as user preferences and cannot override system rules, safety controls, or tool permissions.

- `GET /v1/skills`, `POST /v1/skills`, `PATCH /v1/skills/{id}`, and `DELETE /v1/skills/{id}` manage skills.
- Maximum 100 projects and 100 skills per account; skill instructions are capped at 6 KB.


## Isolated code execution

The autonomous planner can request a `code_execute` step. The backend never evaluates task commands in its own process; it starts a short-lived Docker container with a preloaded image and an argument-array command. No shell string is accepted. Executable names are allowlisted to Python, pytest, Node/npm, Java/javac, Gradle, and `./gradlew`.

The runner image definition is in `sandbox/Dockerfile`. Build it, push it to a registry, and configure the exact `repository@sha256:...` digest as `NEXUS_SANDBOX_IMAGE`. The image must already be present on the Docker host because execution uses `--pull=never`.

The backend image includes the Docker CLI. To enable execution, the deployment operator must provide access to a dedicated Docker daemon:
1. Prefer a separately administered runner daemon over mounting the host socket into a broadly exposed API container. Configure it through Docker's standard `DOCKER_HOST` setting using a protected SSH or TLS connection.
2. If using a local Unix socket, mount it at `/var/run/docker.sock` and add the socket's host group ID as a supplementary group for the backend's non-root `nexus` user.
3. Ensure the workspace is mounted at a stable host/runner path. Set `NEXUS_SANDBOX_HOST_WORKSPACE_ROOT` to the daemon-side path corresponding to the backend's `NEXUS_AGENT_WORKSPACE` directory when paths differ.
4. Preload the exact sandbox image digest on the selected daemon.

Docker daemon access is privileged host-level authority. Restrict it to this trusted single-user service, protect its credentials, and never expose the Docker socket to the sandbox container. Do not enable this on a shared or untrusted multi-user service. If any prerequisite is missing, code execution returns a blocked result and the agent can continue file-based work without claiming that code was run.
