# CURRENT_STATE.md — verified snapshot (2026-11-25)

All facts below were verified live against GitHub and by running the test suite
locally on the stated commits. Historical claims from earlier chat exports are
explicitly marked as unverified.

> **Update (2026-11-25, after user-approved merge):** PR #18 was merged into
> the PR #17 head branch with user permission. PR #17 head is now `a54ef69`;
> CI (`Backend syntax and tests`, `Android unit tests and debug APK`) passes on
> that head; `mergeStateStatus=CLEAN`; PR #17 remains Draft and unmerged.

> **Update 2 (2026-11-25, P1 hardening session):** PR **#19** opened from
> `arena/e327ab55-nexus-ai-max` (head `5c92066`) into
> `nexus-agent/per-claim-lease-fencing-20261121`; **both CI jobs green**
> (run `45540362054`), `mergeStateStatus=CLEAN`, not Draft, **not merged**
> (awaiting user permission). Contents:
> 1. **24-message limit removed** — `MainActivity.kt` now selects chat history
>    with `selectHistoryForRequest` (token budgets: 24 000 direct-connect,
>    100 000 Anthropic web-search; estimator mirrors backend
>    `context_manager.py`: `ceil(UTF-8 bytes/3.2)` + 8/message, images 4 096,
>    preserved `providerMessagesJson` estimated verbatim; newest turn always
>    kept). Covered by `ChatHistorySelectionTest` (11 tests, green in CI).
> 2. **Backend integration tests** — `agent_server/tests/test_api_integration.py`
>    (8 tests, ASGI transport): auth matrix 401/503, provider CRUD roundtrip,
>    encryption at rest (no plaintext in DB; ciphertext decrypts), validation
>    rejections, catalog secret redaction, chat auto/manual/strict-422,
>    SSE `X-Nexus-Context-*` headers + actual-model metadata.
>    Suite grew **186 → 194 tests, all green** locally (Py3.11) and in CI (Py3.12).
> 3. **Live-server smoke test** — `agent_server/scripts/smoke_e2e.py` runs a real
>    uvicorn `app.main:app` + mock OpenAI provider in one process (25 checks:
>    health, worker heartbeat, auth, CRUD+encryption, catalog, auto/manual chat,
>    streaming, cleanup) and is now a **CI step** after unit tests.
>    Two CI-only bugs were found and fixed via workflow-command annotations
>    (job logs unreachable from sandbox): a `_free_port()` race (both servers
>    could get the same port → gateway 401 against the app itself; fixed by
>    binding port 0 inside uvicorn), and chat checks running after a saved
>    `anymodel.org` provider existed (CI has live internet → its non-retryable
>    HTTP 401 aborted gateway fallback before the mock; chat checks now run on
>    an empty provider table).
> 4. **Debug APK refreshed from green CI**: run `45540362054`, artifact
>    `nexus-ai-debug-apk` id `14988459485`, 39 597 023 B,
>    `sha256:9092544c3487cece62951dee1a3322081497ddb5181e9a182475e8cddb8b3bd1`,
>    built from head `5c92066`, expires 2026-12-02 (download blocked from this
>    sandbox — blob host not on egress allowlist; fetch from the Actions page).

## Repository

- Repo: `alievoscar345-sketch/NEXUS-AI-MAX`, default branch `main`.
- `main` contains only `README.md`, `NEXUS_AI_MAX_0.2.0_source.zip`,
  `.github/workflows/build-android.yml` (single commit `e78cede`).
- Real development lives on branch chains diverged from `main`.

## PR #17 (Draft — do not merge without user permission)

- State: OPEN · Draft · mergeStateStatus **CLEAN** (all checks green).
- Base: `nexus-agent/recover-cancelled-tasks-20261121`.
- Head: `nexus-agent/per-claim-lease-fencing-20261121` @ `a54ef69`
  (540 commits, 41 files vs base; includes the CI fix and this doc).
- CI on `a54ef69` (run `45522880946`): **Backend syntax and tests — success**;
  **Android unit tests and debug APK — success**.
- Prior head `cff7bd4` had a failing backend job (fixed, see below).

## CI fix (this session)

- Reproduced locally on `cff7bd4`: 186 backend tests, 1 failure —
  `test_compaction_digest_includes_every_turn_before_recent_suffix`
  asserted `dropped_messages > 6`; contiguous-suffix compaction correctly
  drops exactly 6 (5 history turns + oversized gap) and keeps all 20 recent
  messages.
- Fix pushed as commit `77bdcaf` on `arena/e327ab55-nexus-ai-max`,
  PR #18 (base = PR #17 head branch, contains only the fix).
- **PR #18 merged (user-approved) → PR #17 head `a54ef69`.**
- Verified: `python -m compileall -q app tests` OK;
  `python -m unittest discover -s tests` → **186 tests OK**.
- CI on PR #18 run `45522294576` and on PR #17 head run `45522880946`:
  **both jobs pass**; artifact `nexus-ai-debug-apk` (~39.6 MB) built.

## Artifacts

- Latest debug APK: PR #19 run `45540362054`, artifact id `14988459485`,
  39 597 023 B, sha256 `9092544c…b3bd1`, head `5c92066`, expires 2026-12-02
  (see Update 2 above). Prior: run `45521632148` (~39.6 MB).
- APKs are debug builds; no signed release APK verified.

## Branch inventory (verified)

- `nexus-1.0-development` @ `f263542` (protected, historical base).
- `gemini-hydrogen-development` @ `f263542` — same tip as
  `nexus-1.0-development`; the seven "Gemini commits" (base `f263542…`,
  local end `295225f`) are **not present** in the repository
  (`295225f` does not resolve). Transfer never happened.
- 24 `nexus-agent/*` branches (lease-fencing, sqlite-async-offload,
  recovery, cancellation, guards, GitHub build runs, etc.).
- `nexus-developer-agent/bootstrap`.

## Secrets scan

- No real secrets in working tree or diff history. The single
  `github_pat_…` match is a deliberate redaction-test fixture in
  `agent_server/tests/test_task_durability.py`.
- Old chat-export API keys are masked in the handoff; if that key was ever
  exposed, the user should rotate it.

## Module map (verified against head `cff7bd4` + fix)

Backend `agent_server/app/` (FastAPI, SQLite/WAL, 56 routes):
`main.py` (API + durable worker), `model_gateway.py` (provider routing,
manual/auto selection), `context_manager.py` (token estimation, bounded
extractive compaction, per-model context windows), `credentials.py`
(encrypted provider keys), `sandbox.py` (Docker limits), `worker.py`,
`automation.py`.

Android `app/src/main/java/ai/nexus/max/` (Kotlin, Compose):
`MainActivity.kt`, `AgentTaskClient.kt`, `SseDataParser.kt`,
`ChatCompletionSseDecoder.kt`, `SecureSecretStore.kt`, `PdfOcr.kt`,
`ReminderScheduler.kt`, `OfficeDocumentTextExtractor.kt`,
`AgentTaskNotificationWorker.kt`.

CI: `.github/workflows/` — `android-pr.yml` (backend tests + live server
smoke test + Android unit tests + debug APK; path-filtered to
`app/** agent_server/**`), `agent-server.yml`, `build-android.yml`.

## Known gaps (verified, not yet fixed)

1. ~~24-message request limit~~ **Fixed in PR #19** (head `5c92066`): both the
   direct-connect and web-search paths use token-budgeted selection; cloud
   path remains `takeLast(2000)` + server-side compaction. Awaiting merge.
2. Backend is not deployed; no persistent HTTPS server, encryption key or
   backups exist outside this repo.
3. No signed release APK; only debug artifacts.
4. Node 20 deprecation warnings in CI (non-blocking).
5. Android unit tests were not re-run locally this session (no Android SDK
   in sandbox); CI job passed on PR #18 and PR #19 (includes
   `ChatHistorySelectionTest`).

## Not verified / out of scope this session

- Runtime behavior on a real device or against a deployed backend.
- End-to-end autonomous task with phone off.
- AnyModel base URL / model ID (must be re-checked against provider docs).
