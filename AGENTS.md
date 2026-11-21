# NEXUS AI MAX — Working Instructions

## Mission
Build NEXUS AI MAX as an Android-first personal AI assistant with a cloud agent backend. Prioritize complete user-facing functionality over cosmetic progress or repeated CI runs.

## Repository safety
- For the current development effort, work only on `nexus-agent/per-claim-lease-fencing-20261121` and update Draft PR #17.
- Never modify protected `main` or `nexus-1.0-development` branches.
- Never merge PR #17 or publish a release without the user's explicit permission.
- Keep API keys and signing credentials out of source, logs, commits, and APK assets.
- The user works from Android and has no desktop computer; avoid workflows that require local PC commands.

## Product direction
- One primary NEXUS assistant in one conversation; specialist agents and model routing operate behind the scenes.
- Support multiple providers and task-aware model selection, with explicit capability metadata and safe fallback.
- Build durable personal memory, reviewed memory suggestions, projects, reusable skills, and task history.
- Provide scheduled automation and long-running cloud tasks with visible status, pause/cancel controls, and approval gates.
- Expand tools incrementally: research, files, code execution in an isolated sandbox, and browser automation only after security boundaries exist.
- Support images, voice, and common office/document formats without exposing nonfunctional controls.
- Preserve the existing Android package identity and premium dark UI.

## Security and reliability
- Provider credentials remain server-side and encrypted; the Android client stores only its NEXUS server token in Android Keystore-backed storage.
- Enforce owner-scoped API access, HTTPS, bounded payloads, task timeouts, and permission checks.
- Treat uploaded files, webpages, and model/tool outputs as untrusted input.
- Do not execute arbitrary code on the backend host. Code execution requires a separately isolated runner with resource and network limits.
- External side effects require explicit approval bound to the exact action and target.
- The current SQLite worker and scheduler are single-instance prototypes; do not describe them as distributed or highly available.

## Working order
1. Implement the agreed user-facing feature set in coherent groups.
2. Keep documentation aligned with actual behavior and list limitations honestly.
3. Defer the comprehensive test pass, Android build, APK assembly, and release verification until the product feature set is implemented, as requested by the user.
4. After feature completion, run the full tests, resolve failures, build and verify the signed release APK.
5. Keep PR #17 in Draft throughout development; report actual changed functionality and any remaining blockers.

## Reporting
- Do not send frequent progress messages.
- Do not call infrastructure checks or test runs "features".
- Never claim a feature is complete until the code path and user-facing flow exist.
- Clearly distinguish implemented, partially implemented, and not yet implemented functionality.
