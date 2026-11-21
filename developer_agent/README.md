# NEXUS Developer Agent (bootstrap)

A mobile-triggered GitHub Actions coding agent. It uses AnyModel's OpenAI-compatible Chat Completions endpoint with function calling and a narrowly scoped set of GitHub tools. No desktop computer or local installation is required.

## Current MVP boundaries

- Triggered manually from the GitHub Actions page in a mobile browser.
- Uses `ANYMODEL_API_KEY` from GitHub Actions secrets; never expose it to the Android client or repository.
- Uses the workflow-provided `GITHUB_TOKEN` for repository access.
- Creates an isolated branch named `nexus-agent/<run-id>` from `gemini-hydrogen-development`.
- Can list files, read text files, search code, create/update text files, and open a draft PR.
- Cannot execute shell commands, run tests itself, delete files, change workflow files, or merge PRs.
- PR CI is responsible for validation. A human reviews and merges.
- Maximum 16 model tool rounds, 12 changed files, 180 KB per file.

## One-time setup (Android)

1. In GitHub, open the repository **Settings → Secrets and variables → Actions → New repository secret**.
2. Create secret named `ANYMODEL_API_KEY`; paste the AnyModel key directly into GitHub's secret field. Never paste it into chat, issues, commits, or workflow inputs.
3. Open the repository in the GitHub mobile app or browser and switch to branch `nexus-developer-agent/bootstrap`.
4. Edit `developer_agent/TASK.md`, replace the example with your task, and commit the change to that same branch.
5. The push automatically starts the agent. Open Actions → NEXUS Developer Agent to follow the run.
6. When it finishes, open the draft PR created against `gemini-hydrogen-development`.
7. Review the PR and CI checks. Do not enable auto-merge.

The manual-dispatch trigger is retained. GitHub's web UI requires workflow_dispatch to exist on the default branch; the task-file push trigger avoids changing the default branch.

## Models

Use the exact model IDs listed by AnyModel at `GET https://anymodel.org/v1/models`. The default ID is the existing NEXUS setting `claude-fable-5-2`; change it only to an ID confirmed in the AnyModel dashboard/API.

## Security

The first version intentionally does not provide terminal or arbitrary code execution. Repository text is untrusted input and may contain prompt injection. The agent cannot modify workflow files or secret files. Keep the repository private if its source is confidential. Review every generated PR.

## Known limitations

- The GitHub Actions run is a batch job, not a persistent chat session.
- Tool calling depends on AnyModel forwarding OpenAI-compatible `tools` and `tool_calls` for the selected model. This must be verified with the real account/model.
- GitHub's REST API code-search endpoint may require code-search permissions and may be unavailable for some token types; file listing and direct reads remain available.
- The first version has no browser automation, build/test tool, long-term conversation memory, or interactive live progress UI.
