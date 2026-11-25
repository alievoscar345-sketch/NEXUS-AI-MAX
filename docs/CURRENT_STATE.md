# CURRENT_STATE.md — verified snapshot (2026-11-25)

All facts below were verified live against GitHub and by running the test suite
locally on the stated commits. Historical claims from earlier chat exports are
explicitly marked as unverified.

## Repository

- Repo: `alievoscar345-sketch/NEXUS-AI-MAX`, default branch `main`.
- `main` contains only `README.md`, `NEXUS_AI_MAX_0.2.0_source.zip`,
  `.github/workflows/build-android.yml` (single commit `e78cede`).
- Real development lives on branch chains diverged from `main`.

## PR #17 (Draft — do not merge without user permission)

- State: OPEN · Draft · MERGEABLE (no conflicts).
- Base: `nexus-agent/recover-cancelled-tasks-20261121`.
- Head: `nexus-agent/per-claim-lease-fencing-20261121` @ `cff7bd4`.
- 537 commits, 40 changed files vs base (+14418 / −2346).
- CI on `cff7bd4`: **Android unit tests and debug APK — success**;
  **Backend syntax and tests — failure** (one assertion, see below).

## CI fix (this session)

- Reproduced locally on `cff7bd4`: 186 backend tests, 1 failure —
  `test_compaction_digest_includes_every_turn_before_recent_suffix`
  asserted `dropped_messages > 6`; contiguous-suffix compaction correctly
  drops exactly 6 (5 history turns + oversized gap) and keeps all 20 recent
  messages.
- Fix pushed as commit `77bdcaf` on `arena/e327ab55-nexus-ai-max`,
  PR #18 (base = PR #17 head branch, contains only the fix).
- Verified: `python -m compileall -q app tests` OK;
  `python -m unittest discover -s tests` → **186 tests OK**.
- CI on PR #18 run `45521632148`: **both jobs pass**; artifact
  `nexus-ai-debug-apk` (39 593 359 bytes) built from this commit.

## Artifacts

- Latest debug APK: run `45521632148`, not expired, ~39.6 MB
  (build-android.yml `nexus-ai-debug-apk`).
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

CI: `.github/workflows/` — `android-pr.yml` (backend tests + Android unit
tests + debug APK), `agent-server.yml`, `build-android.yml`.

## Known gaps (verified, not yet fixed)

1. **24-message request limit** for direct (non-cloud-gateway) chat:
   `MainActivity.kt:2275` sends `history.takeLast(24)` unless
   `useCloudGateway` (then 2000). Cloud path also caps at 2000.
2. Backend is not deployed; no persistent HTTPS server, encryption key or
   backups exist outside this repo.
3. No signed release APK; only debug artifacts.
4. Node 20 deprecation warnings in CI (non-blocking).
5. Android unit tests were not re-run locally this session (no Android SDK
   in sandbox); CI job passed on PR #18.

## Not verified / out of scope this session

- Runtime behavior on a real device or against a deployed backend.
- End-to-end autonomous task with phone off.
- AnyModel base URL / model ID (must be re-checked against provider docs).
