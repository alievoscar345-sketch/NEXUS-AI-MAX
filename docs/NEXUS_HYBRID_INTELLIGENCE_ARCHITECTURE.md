# NEXUS Hybrid-to-Single Intelligence Architecture

Status: target architecture and implementation roadmap. This document is a design specification, not a claim that every component is implemented.

## Product decision

NEXUS presents one assistant in one conversation. The user does not select or manage agents. Internally, the assistant may plan work, route subtasks to specialist models, call tools, request approvals, and continue long-running jobs in the cloud.

The selected strategy is **hybrid first, own model later**:
- Start with provider adapters and a model router over available hosted and open-weight models.
- Build durable memory, reusable skills, task execution, and evaluation before attempting weight updates.
- Only consider fine-tuning/distillation when there is a lawful, high-quality dataset, a measurable capability gap, an evaluation suite, and affordable compute.
- Never assume that combining model weights automatically combines their best capabilities. Weight merging is an experimental, architecture- and license-dependent research track, not the initial implementation.

## Current repository baseline (2026-11-20)

The development branch contains:
- Android app source under `app/src/main`.
- A FastAPI service under `agent_server/` with SQLite task/event/approval records, one background worker, one configured OpenAI-compatible chat-completions endpoint, basic planning, Tavily search (optional), safe HTTPS text fetching, and task-scoped artifact writing.
- Agent server CI and Android build workflows.

Important limitations of the current server:
- It is a prototype, not a production multi-user service.
- The worker is single-process and SQLite-backed; run one replica only.
- Interrupted task/step recovery is implemented for the currently supported read-only and deterministic artifact actions; this is not distributed recovery and does not make external side effects resumable.
- The Model Gateway now has a configurable provider registry, deterministic purpose/task routing, capability-aware selection, and transient-error fallback. Benchmark scores remain operator-supplied; there is no live global leaderboard integration or full cost ledger.
- It does not yet execute arbitrary code, operate a browser, or connect to Avito.
- Approval records are not yet a complete resumable human-in-the-loop workflow.
- Android now has provider-vault settings and can route standard text/image chat through the cloud Model Gateway when configured. The durable long-running task API is not yet integrated into chat as resumable task cards.

## Target architecture

```text
Android NEXUS client
  └─ Conversation API / streaming updates
      └─ NEXUS Orchestrator (cloud)
          ├─ Policy & permission gate
          ├─ Task planner / durable state machine
          ├─ Model Gateway
          │   ├─ Provider adapters (OpenAI-compatible, Anthropic-style, etc.)
          │   ├─ Model registry + capability metadata
          │   ├─ Task router + fallback policy
          │   ├─ Budget / rate / timeout controls
          │   └─ Quality evaluator
          ├─ Tool registry
          │   ├─ Web research
          │   ├─ File/artifact tools
          │   ├─ Sandboxed code runner (later)
          │   ├─ Browser automation (later)
          │   └─ Service connectors: Avito, GitHub, etc. (later)
          ├─ Memory & Skill Store
          │   ├─ User-approved profile memory
          │   ├─ Project/task memory
          │   ├─ Episodic execution history
          │   └─ Versioned, evaluated reusable skills
          ├─ Human approval inbox
          └─ Durable job queue + scheduler
              ├─ PostgreSQL (production state)
              ├─ Object storage (artifacts)
              └─ Worker processes
```

### 1. Android client

Responsibilities:
- Keep the familiar single-chat experience.
- Send a user request and receive task state, progress events, and final output.
- Render compact inline task cards in the conversation (queued, planning, running, waiting for approval, completed, failed, cancelled).
- Provide a separate **Work / Activity** screen only for operational visibility: active jobs, history, approvals, logs, and artifacts. It is not an agent picker.
- Store only the user's session credential securely; never store provider API keys in the APK.

The Android app must remain usable as a normal chat if the cloud agent service is unavailable.

### 2. Orchestrator and durable execution

Replace the current one-shot worker flow gradually with a persisted state machine:
- Task states: queued, planning, running, waiting_for_approval, retry_wait, completed, failed, cancelled.
- Each step has an immutable input snapshot, tool/model identity, attempt count, start/end timestamps, output reference, and error.
- Persist before and after every side effect.
- Use idempotency keys for external actions.
- On restart, reclaim expired worker leases and resume only steps declared safe to retry.
- Use bounded retries with exponential backoff and a task deadline.
- Support pause, cancel, resume, and approval continuation.
- Production deployment uses PostgreSQL and a durable queue; SQLite remains suitable only for local/single-instance prototype use.

### 3. Model Gateway and Hybrid Intelligence

All model calls go through one internal interface. Android and business logic must not depend directly on a vendor SDK.

Provider adapter contract should normalize:
- provider and model IDs;
- text and structured JSON output;
- streaming (when supported);
- tool/function calling (when supported);
- image/file input capability flags;
- context-window metadata;
- timeout, token usage, latency, and estimated cost;
- retryable vs non-retryable errors.

Model registry records capabilities and operational metadata. Do not infer vision, tools, streaming, or file support from a model name; verify provider documentation and controlled tests.

Router policy:
1. Classify the request and required capabilities.
2. Select the least expensive model that meets the quality/capability threshold.
3. Use a stronger model for high-complexity planning, code review, or ambiguous high-impact decisions when budget allows.
4. Route independent subtasks in parallel only when this improves latency or quality.
5. Fall back only to a compatible provider/model and record the reason.
6. Never silently exceed the user's configured budget.

Initial routing classes: general_chat, reasoning, coding, research_synthesis, vision, structured_extraction, and tool_planning. These are routing labels, not separate user-facing agents.

### 4. Memory and progressive learning

Keep memory distinct from the model's training weights.

Memory layers:
- User memory: explicit preferences and stable instructions; user can inspect and correct it.
- Project memory: repository conventions, architecture decisions, and current milestones.
- Episodic memory: task summaries, tool traces, and outcomes with retention limits.
- Skill library: versioned procedures with prerequisites, examples, tests, and provenance.

Learning loop:
1. Capture a task trace and outcome.
2. Evaluate outcome using deterministic tests, source checks, user feedback, or a second independent review.
3. Extract a candidate lesson or skill.
4. Check for duplicates, secrets, prompt injection, and unsupported claims.
5. Store as a draft skill.
6. Promote only after passing tests and, for consequential workflows, user approval.
7. Track regressions and allow rollback to earlier skill versions.

Do not train on raw private conversations or external content by default. Do not treat a model's self-rating as proof of correctness.

### 5. Own-model path (later, optional)

Progression:
- Stage A: use hosted APIs and open-weight models through the same gateway.
- Stage B: benchmark candidate open models on a NEXUS task suite.
- Stage C: collect permitted, redacted, high-quality examples and synthetic tasks; maintain data lineage and licenses.
- Stage D: fine-tune a small specialist model using LoRA/QLoRA on rented or otherwise available GPU compute, only after a measurable need is established.
- Stage E: evaluate against frozen holdout tests, safety tests, cost, latency, and hosted baselines.
- Stage F: deploy as one candidate in the model registry; router uses it only where it meets quality thresholds.

Weight merging (SLERP/TIES/DARE/MoE) is an experimental research option. It is not a guaranteed route to a stronger general model, and model licenses/architectures must be compatible. Do not merge proprietary API models; API access does not grant weight access.

### 6. Tools and external actions

Every tool declares:
- name, schema, required permissions, risk class, timeout, idempotency behavior, and audit fields.

Risk classes:
- Read-only: search, fetch public pages, inspect permitted data.
- Reversible write: draft, edit unpublished content, create a local artifact.
- External side effect: send message, publish listing, modify account, spend money, delete data.

Approval policy:
- Read-only work can run autonomously within policy.
- Reversible writes may run within explicitly granted scopes.
- External side effects require approval when material, unusual, irreversible, financial, legal, or outside a user-approved rule.
- Approval is bound to the exact action payload, target account, and expiration; changes after approval invalidate it.
- Secrets are stored server-side and scoped per connector. Never ask users to paste secrets into public repository files.

Avito integration must use official APIs and permissions where available. Browser automation is a fallback only where permitted by the service terms and technically reliable; do not bypass authentication, anti-abuse controls, CAPTCHAs, or access restrictions.

### 7. Security and reliability baseline

Before public deployment:
- HTTPS only; strong user authentication, per-user authorization, and key rotation.
- Provider and connector secrets in host secret storage, never Git or APK.
- Per-user rate limits, task quotas, token/cost budgets, concurrency limits, and maximum runtime.
- SSRF protection including DNS rebinding/redirect revalidation; strict outbound allowlists for tools.
- Sandboxed code execution with no host secrets, no privileged container, resource/network limits, and disposable workspaces.
- Audit log for tool calls, approvals, and external side effects.
- Database backups, retention policy, deletion/export controls, and incident logging.
- Treat web pages, uploaded files, and tool outputs as untrusted data; defend against prompt injection.
- Do not expose an unauthenticated task API or run multiple replicas against SQLite.

## Low-cost implementation roadmap

### Milestone 0 — Baseline and contract tests
- Confirm Android build and agent-server CI.
- Add API contract tests and task-state tests.
- Document environment variables, provider compatibility, and local/mobile deployment steps.
- No new paid services.

Acceptance: CI green; no secrets in tracked files; API behavior documented.

### Milestone 1 — Model Gateway
- Add provider adapter interface and one adapter matching the currently configured AnyModel endpoint.
- Add a model registry with explicit capability metadata.
- Add timeout/error normalization, usage capture, and a hard per-task budget.
- Keep current configured model as default; do not assume unverified endpoint features.

Acceptance: mocked tests for provider success, malformed response, timeout, rate limit, unsupported capability, and budget exhaustion.

### Milestone 2 — Durable task engine
- Introduce persisted step records, leases, idempotency keys, retry policy, cancellation, and resume.
- Move production state to PostgreSQL when deploying beyond one instance; retain SQLite for development.
- Add approval continuation and event streaming/polling contract.

Acceptance: kill/restart worker tests prove safe recovery without duplicate external actions.

### Milestone 3 — One-chat Android integration
- Add a backend configuration/onboarding path that works from Android.
- Add task submission and inline progress cards in the existing conversation.
- Add Work / Activity screen for history, approvals, and artifacts.
- Keep provider keys server-side and preserve normal chat fallback.

Acceptance: user can submit, close app, reopen, see the same job state, approve a pending action, and retrieve output.

### Milestone 4 — Memory and skills
- Add project/user memory APIs with inspect/edit/delete controls.
- Add versioned skill records and provenance.
- Add evaluator pipeline and promotion/rollback workflow.

Acceptance: no skill is promoted without tests; memory can be inspected and corrected.

### Milestone 5 — Tool execution
- Harden web research and artifact tools.
- Add isolated code execution and automated test runner.
- Add browser automation only after sandbox and policy controls.
- Add connectors one by one, starting with read-only scopes.

Acceptance: tools pass security tests; external side effects are approval-gated and auditable.

### Milestone 6 — Avito business integration
- Verify the user's account type, official API access, scopes, and current Avito API contract.
- Implement message retrieval and draft replies first.
- Add sending replies behind explicit policy/approval.
- Add listing import/update/publish only through supported official flows.
- Add customer-response evaluation and human escalation.

Acceptance: sandbox/test account validation, idempotency, audit trail, approval checks, and rate-limit handling.

### Milestone 7 — Model selection and own-model research
- Create a benchmark suite from representative NEXUS tasks.
- Compare hosted and open-weight candidates by quality, latency, context, reliability, and total cost.
- Add routing only when benchmark results justify it.
- Consider LoRA/QLoRA for a narrow specialist task only if a model demonstrably improves cost or quality.

Acceptance: reproducible benchmark report and no regression against current baseline.

## Cost policy

- Default to the current provider until a tested alternative has a clear benefit.
- Prefer free/open-weight models for experiments, but account for GPU hosting, storage, bandwidth, and operations.
- Use small/cheap models for classification and extraction; reserve stronger models for difficult reasoning and review.
- Set daily and monthly hard spending caps, per-task limits, and user-visible cost estimates.
- If a cap is reached, pause and ask; do not silently switch to an expensive provider.
- Keep the cloud service scale-to-zero only if the chosen host can wake it reliably and preserve durable jobs. True 24/7 execution requires an always-available worker or a reliable scheduler.

## Immediate next implementation order

1. Add provider abstraction and tests without changing Android.
2. Add task-step persistence and restart recovery.
3. Add budget/usage accounting and permission scopes.
4. Add Android conversation integration and Work / Activity view.
5. Add memory/skills and evaluation.
6. Add sandboxed tools, then Avito.

This order minimizes spend and avoids exposing unfinished autonomy or external actions.


## Dynamic benchmark-aware model routing (implementation milestone)

The Model Gateway has an initial deterministic scoring router. It infers a coarse task category (coding, research, math, vision, writing, general) from the request without an extra LLM call, filters providers by purpose, and ranks them using explicitly configured per-category benchmark scores, token cost, latency, and provider priority. Quality is weighted most heavily; cost and latency are secondary factors. Explicit purpose route variables remain operator overrides.

Benchmark metadata is supplied per provider in `NEXUS_MODEL_PROVIDERS`: `benchmark_scores` (0–100), `cost_per_million_tokens`, `latency_ms`, `ranking_source`, and `ranking_updated_at`. This is an initial scoring mechanism, not a live leaderboard integration. It does not claim to fetch or validate current global rankings. Production rollout requires a maintained, dated benchmark catalog, source-specific normalization, freshness controls, and NEXUS task-level evaluations. Missing benchmark data is neutral and falls back to configured priority; it is not evidence of model quality. Vision categorization is preparatory until image input support is implemented.


## Durable Task Engine milestone

The SQLite prototype now persists the generated plan as ordered task-step rows before execution. Each step records its action/input, status, attempt count, bounded retry policy, deterministic idempotency key, result/error, and next retry time. Task rows record lease owner/expiry, attempts, cancellation request, per-task step limit, and optional client idempotency key. Task inspection exposes per-step state.

At worker startup, tasks left in planning/running states are recovered into retry_wait; interrupted running steps are reset to retry_wait. This is safe for the currently supported action set: search and fetch are read-only, artifact writes target deterministic task-scoped paths, and approval records are deduplicated by step idempotency key. Transient network errors and selected HTTP statuses use bounded exponential backoff. Cancellation is cooperative.

The SQLite claim transaction and lease metadata are suitable only for the documented single-process/single-replica prototype. They are not distributed locking. Production multi-worker execution requires PostgreSQL transactions/locking and a durable queue or equivalent coordination. Approval records only record consent; the generic backend does not execute third-party actions, and approved tasks remain paused pending a service-specific connector and explicit action implementation.


## Provider credential vault milestone

The Agent Server now includes an initial owner-scoped provider credential vault:
- Provider API keys are encrypted with AES-256-GCM before persistence; the encryption key is supplied separately through `NEXUS_CREDENTIAL_ENCRYPTION_KEY`.
- Provider metadata can be listed without returning secret material. Credentials can be added/replaced, tested with a minimal completion request, and deleted through authenticated API endpoints.
- The Model Gateway combines server-configured providers with credentials belonging to the authenticated owner.
- Provider base URLs are restricted to an explicit HTTPS host allowlist to reduce SSRF risk.
- Task listing, inspection, cancellation, and approval access are scoped by owner identity. Idempotency keys are unique within an owner scope.

Current boundary: the prototype bearer-token authentication maps requests to the fixed `personal` owner. This is suitable only for a single-user deployment. Before multi-user access, replace it with verified account authentication (e.g. JWT subject mapping), apply owner scoping to every resource and background job, and add per-user rate/cost quotas. Do not accept a tenant/user ID directly from an untrusted client. Android settings UI, deployment secret provisioning, key rotation, and billing controls are not yet implemented.


## Standard chat and vision gateway milestone

The Agent Server now exposes a normalized authenticated chat-completions endpoint. Standard text chat and inline image attachments can use the same Model Gateway as background tasks. The gateway routes image requests only to provider configurations explicitly marked with the `vision` capability. Image payloads must be bounded base64 data URLs; remote image URLs are rejected to avoid server-side fetch/SSRF behavior.

Android settings now include the Agent Server connection, an encrypted local server access token (Android Keystore), provider credential management, and a vision-capability flag. When the server is configured, ordinary chat and supported image attachments are routed through the server gateway. Provider-specific web search and reasoning still use the legacy direct-provider path. The cloud chat endpoint is currently non-streaming.

Remaining boundaries: the server bearer token still maps to the single `personal` owner; account/JWT authentication is required before multi-user exposure. The backend must be deployed and its encryption key configured before the Android vault can connect. Streaming, server-side web-search adapters, reasoning controls, provider capability verification, quotas, and billing are future work.


## Implementation update — 2026-11-21

The current feature branch now includes the following product-facing pieces in addition to the baseline above:

- Android chat can extract candidate long-term memories from the current conversation. Candidates are user-reviewed drafts; saving requires an explicit action. Existing memory retrieval remains lexical/title-weighted and is not yet semantic embedding search.
- The cloud agent backend stores one-time, daily, weekly, and interval automations. Daily/weekly schedules use IANA time zones; due schedules are converted into normal durable agent tasks.
- Android exposes an automation manager for creation, listing, pause/resume, immediate execution, and deletion.
- Scheduled execution currently depends on the single backend instance remaining online. It is not a distributed/high-availability scheduler.
- The autonomous agent still has a constrained tool set. Arbitrary code execution and general browser automation are not implemented; do not represent the current workspace file tools as a secure code sandbox.

The user's current delivery order is feature-first: finish the agreed product functionality before the comprehensive test pass, APK assembly, and release verification. Do not use earlier CI results as validation of newer commits. Keep the pull request in Draft and do not merge without explicit permission.
