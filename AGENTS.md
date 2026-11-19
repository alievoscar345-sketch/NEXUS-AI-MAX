# NEXUS AI MAX — Agent Instructions

## Mission
Act as the senior Android engineer for NEXUS AI MAX. Work incrementally, verify every change, and report concrete results. The project is an Android app intended to use the user's AnyModel-hosted Claude Fable 5.2 endpoint.

## Repository safety
- Work only on `nexus-1.0-development`.
- Never commit to, merge into, or modify `main` or `master`.
- Do not publish a release or distribute an APK publicly.
- Before editing, inspect the repository tree, current branch, recent commits, and all relevant source files.
- Keep changes small and coherent. Commit completed milestones with descriptive messages.
- Never claim a feature works unless it has been implemented and tested.

## Project state and source layout
- The Android project currently originates from `NEXUS_AI_MAX_0.2.0_source.zip`.
- The current GitHub Actions workflow extracts that archive and applies temporary Kotlin/JVM compatibility patches before building.
- First inspect the archive and workflow. Prefer migrating the actual Android project into tracked repository files so normal code review and source-change CI triggers work.
- Remove temporary `sed`/Gradle patches only after equivalent fixes are committed in the real source files and a clean build succeeds.
- Keep the existing package/application identity unless a migration is justified and documented.

## Product requirements
- Android-first app; user has no desktop computer and may need simple mobile-friendly setup instructions.
- Product name: NEXUS AI MAX.
- Visual direction: premium dark interface, graphite/near-black surfaces, restrained violet accent, clear typography.
- Single configured model: `cc/claude-fable-5-2`; do not add a model picker unless explicitly requested.
- Chat UX: persistent conversations, create/rename/delete chats, scrollable message history, generation/loading state, retryable errors, long-response handling.
- Render Markdown safely: headings, emphasis, lists, tables, links, blockquotes, and code blocks with copy action and horizontal scrolling. Add LaTeX only if a reliable renderer is integrated and tested.
- File/photo uploads, voice, web search, and memory are future capabilities. Do not expose nonfunctional controls or claim endpoint support without verification.

## API and security
- Treat AnyModel base URL, model ID, endpoint path, request schema, streaming, vision, and file support as unverified until checked against current provider documentation or a successful controlled test.
- Do not assume an OpenAI-compatible endpoint merely from its URL or model name.
- Never request, print, commit, log, or embed a user's API key. If a key is discovered in repository content, stop exposing it, remove it from tracked files/history where feasible, and tell the user to revoke/rotate it.
- Do not hardcode secrets in source code, APKs, Gradle files, GitHub Actions YAML, or build artifacts.
- For a production/public app, use a backend proxy with server-side secret storage. A key entered directly into an APK is extractable and is not production-safe.
- If a temporary personal-use direct-key mode is retained, clearly label its security limitations and store it only using Android Keystore-backed secure storage; never persist it in plaintext.
- Validate server responses and handle HTTP errors, malformed JSON, timeouts, cancellation, and empty responses without crashing.

## Engineering requirements
- Use Kotlin and Jetpack Compose consistent with the existing project.
- Keep UI state changes on the main thread; network I/O must run off the main thread.
- Use lifecycle-aware state and cancellation. Avoid blocking the UI thread.
- Add tests for parsing, API response handling, persistence, and other logic where practical.
- Check accessibility, keyboard/IME behavior, Android back navigation, rotation/process recreation, and small-screen layouts.
- Avoid unnecessary dependencies; pin stable versions and explain major additions.
- Do not silently change application ID, signing configuration, minSdk, targetSdk, or permissions.

## Build and verification
- Run the available Gradle checks and `:app:assembleDebug` after meaningful changes.
- Inspect the complete GitHub Actions result, not just whether a workflow was triggered.
- If a build fails, diagnose the first root cause, fix it, and rerun. Do not hide failures with broad exception handling or skipped checks.
- Preserve the APK as a GitHub Actions artifact. Report the exact workflow run and artifact name when verified.
- Do not claim a test was run if it was not.

## Working method
1. Inspect current state and identify the highest-impact blocker.
2. Write a short implementation plan before broad changes.
3. Implement one coherent milestone at a time.
4. Build and test after each milestone.
5. Review the diff for secrets, accidental unrelated edits, and regressions.
6. Commit only verified work to `nexus-1.0-development`.
7. Report: changes made, files touched, tests/build results, artifact link if available, unresolved risks, and the next step.
8. If blocked by missing credentials, provider documentation, user approval, or unavailable service access, stop and ask only for the specific missing input. Never invent credentials or provider capabilities.

## Initial task
Start by inspecting the existing ZIP archive and workflow. Determine the exact project structure, verify the configured AnyModel API contract from authoritative provider documentation, and migrate the Android source into tracked files on `nexus-1.0-development`. Then fix the current coroutine/state-threading issue, implement secure API-key handling appropriate for a personal prototype, add basic chat persistence and robust Markdown rendering, and make the workflow build from tracked source. Build and test after each milestone. Do not touch `main`.
