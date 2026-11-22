import asyncio
import hashlib
import json
import ipaddress
import socket
import tempfile
import zipfile
import time
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
import unittest
from pathlib import Path
from unittest.mock import patch

from app import main


class DurableTaskTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp.name) / "tasks.sqlite3"
        self.patch = patch.object(main, "DB_PATH", self.db_path)
        self.patch.start()
        main.init_db()

    def tearDown(self):
        self.patch.stop()
        self.temp.cleanup()

    def test_workspace_write_read_and_list_are_scoped_to_task(self):
        workspace = Path(self.temp.name) / "workspace"
        with patch.object(main, "WORKSPACE", workspace):
            written = main._workspace_write("task-1", "notes/report.md", "NEXUS report")
            read = main._workspace_read("task-1", "notes/report.md")
            listing = main._workspace_list("task-1", "notes")
        self.assertEqual(written["bytes"], len("NEXUS report"))
        self.assertEqual(read["content"], "NEXUS report")
        self.assertEqual(listing["entries"][0]["name"], "report.md")
        self.assertFalse((workspace / "task-2" / "notes" / "report.md").exists())

    def test_workspace_rejects_traversal_and_absolute_paths(self):
        workspace = Path(self.temp.name) / "workspace"
        with patch.object(main, "WORKSPACE", workspace):
            for path in ("../outside.txt", "/etc/passwd", "a/../../outside", "a\\b"):
                with self.subTest(path=path):
                    with self.assertRaises(ValueError):
                        main._workspace_write("task-1", path, "blocked")

    def test_workspace_enforces_file_size_limit(self):
        workspace = Path(self.temp.name) / "workspace"
        with patch.object(main, "WORKSPACE", workspace):
            with self.assertRaises(ValueError):
                main._workspace_write("task-1", "large.txt", "x" * 100_001)

    def test_workspace_search_finds_text_with_line_numbers(self):
        workspace = Path(self.temp.name) / "workspace"
        with patch.object(main, "WORKSPACE", workspace):
            main._workspace_write("task-1", "src/readme.md", "first line\nNEXUS agent\nlast line")
            result = main._workspace_search("task-1", "nexus")
        self.assertEqual(len(result["matches"]), 1)
        self.assertEqual(result["matches"][0]["line"], 2)
        self.assertEqual(result["matches"][0]["path"], "src/readme.md")

    def test_stale_worker_cannot_write_to_task_workspace(self):
        now = time.time()
        current_owner = main.WORKER_ID + ":current-claim"
        stale_owner = main.WORKER_ID + ":stale-claim"
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?)",
                ("task-workspace-fence", "workspace fencing", "running", now, now,
                 current_owner, now + 600),
            )
        workspace = Path(self.temp.name) / "workspace"
        token = main._LEASE_OWNER.set(stale_owner)
        try:
            with patch.object(main, "WORKSPACE", workspace):
                result = main._leased_workspace_write("task-workspace-fence", "late.txt", "stale write")
        finally:
            main._LEASE_OWNER.reset(token)
        self.assertIsNone(result)
        self.assertFalse((workspace / "task-workspace-fence" / "late.txt").exists())

    def test_workspace_rejects_symlink_escape(self):
        workspace = Path(self.temp.name) / "workspace"
        outside = Path(self.temp.name) / "outside"
        outside.mkdir()
        (workspace / "task-1").mkdir(parents=True)
        (workspace / "task-1" / "escape").symlink_to(outside, target_is_directory=True)
        with patch.object(main, "WORKSPACE", workspace):
            with self.assertRaises(ValueError):
                main._workspace_write("task-1", "escape/secret.txt", "blocked")

    def test_workspace_api_lists_and_reads_files_for_task_owner(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id) VALUES(?,?,?,?,?,?)",
                ("workspace-api", "workspace api", "completed", now, now, "owner-a"),
            )
        workspace = Path(self.temp.name) / "workspace"
        with patch.object(main, "WORKSPACE", workspace):
            main._workspace_write("workspace-api", "docs/summary.md", "NEXUS output")
            listing = main.list_task_workspace("workspace-api", owner_id="owner-a")
            read = main.read_task_workspace_file("workspace-api", "docs/summary.md", owner_id="owner-a")
            with self.assertRaises(main.HTTPException) as denied:
                main.list_task_workspace("workspace-api", owner_id="owner-b")
            self.assertEqual(denied.exception.status_code, 404)
        self.assertEqual(listing["entries"][0]["name"], "docs")
        self.assertEqual(read["content"], "NEXUS output")

    def test_workspace_api_allows_owner_edits_only_after_task_finishes(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id) VALUES(?,?,?,?,?,?)",
                ("workspace-edit", "workspace edit", "running", now, now, "owner-a"),
            )
        body = main.WorkspaceFileInput(path="report.md", content="edited")
        with patch.object(main, "WORKSPACE", Path(self.temp.name) / "workspace"):
            with self.assertRaises(main.HTTPException) as conflict:
                main.write_task_workspace_file("workspace-edit", body, owner_id="owner-a")
            self.assertEqual(conflict.exception.status_code, 409)
            with main.db() as conn:
                conn.execute("UPDATE tasks SET status='completed' WHERE id='workspace-edit'")
            result = main.write_task_workspace_file("workspace-edit", body, owner_id="owner-a")
            self.assertEqual(result["bytes"], len("edited".encode("utf-8")))
            self.assertEqual(main._workspace_read("workspace-edit", "report.md")["content"], "edited")

    def test_task_artifact_api_lists_reads_and_enforces_owner_scope(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id) VALUES(?,?,?,?,?,?)",
                ("artifact-api", "artifact api", "completed", now, now, "owner-a"),
            )
        workspace = Path(self.temp.name) / "workspace"
        with patch.object(main, "WORKSPACE", workspace):
            workspace.mkdir()
            (workspace / "artifact-api-report.md").write_text("Generated NEXUS report", encoding="utf-8")
            (workspace / "another-task-secret.txt").write_text("private", encoding="utf-8")
            listing = main.list_task_artifacts("artifact-api", owner_id="owner-a")
            read = main.read_task_artifact("artifact-api", "report.md", owner_id="owner-a")
            self.assertEqual([item["name"] for item in listing["artifacts"]], ["report.md"])
            self.assertEqual(read["content"], "Generated NEXUS report")
            with self.assertRaises(main.HTTPException) as denied:
                main.list_task_artifacts("artifact-api", owner_id="owner-b")
            self.assertEqual(denied.exception.status_code, 404)
            with self.assertRaises(main.HTTPException) as traversal:
                main.read_task_artifact("artifact-api", "../another-task-secret.txt", owner_id="owner-a")
            self.assertEqual(traversal.exception.status_code, 422)


    def test_binary_artifact_publication_is_lease_fenced_and_downloadable(self):
        now = time.time()
        worker = main.WORKER_ID + ":artifact-publisher"
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?,?)",
                ("binary-artifact", "publish APK", "running", now, now, "owner-a", worker, now + 600),
            )
        workspace = Path(self.temp.name) / "workspace"
        task_root = workspace / "binary-artifact"
        task_root.mkdir(parents=True)
        payload = b"\x00\x01NEXUS-APK\xff"
        (task_root / "app-release.apk").write_bytes(payload)
        token = main._LEASE_OWNER.set(worker)
        try:
            with patch.object(main, "WORKSPACE", workspace):
                published = main._publish_workspace_artifact(
                    "binary-artifact", "app-release.apk", ""
                )
                listing = main.list_task_artifacts("binary-artifact", owner_id="owner-a")
                response = main.download_task_artifact(
                    "binary-artifact", "app-release.apk", owner_id="owner-a"
                )
                self.assertEqual(published["bytes"], len(payload))
                self.assertEqual(published["sha256"], __import__("hashlib").sha256(payload).hexdigest())
                self.assertEqual(listing["artifacts"][0]["content_type"], "application/vnd.android.package-archive")
                self.assertEqual(Path(response.path).read_bytes(), payload)
                self.assertEqual(response.filename, "app-release.apk")
        finally:
            main._LEASE_OWNER.reset(token)

    def test_binary_artifact_publication_rejects_traversal_and_symlinks(self):
        workspace = Path(self.temp.name) / "workspace"
        task_root = workspace / "artifact-security"
        task_root.mkdir(parents=True)
        outside = Path(self.temp.name) / "outside.apk"
        outside.write_bytes(b"private")
        (task_root / "escape.apk").symlink_to(outside)
        with patch.object(main, "WORKSPACE", workspace):
            with self.assertRaises(ValueError):
                main._publish_workspace_artifact("artifact-security", "../outside.apk")
            with self.assertRaises(ValueError):
                main._publish_workspace_artifact("artifact-security", "escape.apk")

    def test_binary_artifact_publication_rejects_stale_worker(self):
        now = time.time()
        worker = main.WORKER_ID + ":current"
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?)",
                ("artifact-fence", "publish artifact", "running", now, now, worker, now + 600),
            )
        workspace = Path(self.temp.name) / "workspace"
        root = workspace / "artifact-fence"
        root.mkdir(parents=True)
        (root / "report.bin").write_bytes(b"content")
        stale = main._LEASE_OWNER.set(main.WORKER_ID + ":stale")
        try:
            with patch.object(main, "WORKSPACE", workspace):
                self.assertIsNone(main._publish_workspace_artifact("artifact-fence", "report.bin"))
                self.assertFalse((workspace / "artifact-fence-report.bin").exists())
        finally:
            main._LEASE_OWNER.reset(stale)

    def test_github_connect_encrypts_token_and_status_never_returns_it(self):
        from unittest.mock import AsyncMock
        with patch.object(main, "_github_api", new=AsyncMock(side_effect=[
            {"login": "nexus-owner"},
            {"full_name": "nexus-owner/sample-app", "default_branch": "main",
             "permissions": {"push": True}},
        ])), patch.object(main, "encrypt_secret", return_value=b"encrypted-token"):
            result = asyncio.run(main.connect_github(
                main.GitHubConnectInput(repo_full_name="nexus-owner/sample-app",
                                        token="ghp_" + "x" * 36),
                owner_id="owner-a",
            ))
        self.assertEqual(result["status"], "connected")
        self.assertFalse(result["secret_returned"])
        status = main.github_status(owner_id="owner-a")
        self.assertTrue(status["connected"])
        self.assertEqual(status["integration"]["repo_full_name"], "nexus-owner/sample-app")
        self.assertNotIn("token", str(status))
        with main.db() as conn:
            stored = conn.execute("SELECT encrypted_token FROM github_integrations WHERE owner_id='owner-a'").fetchone()
        self.assertEqual(bytes(stored["encrypted_token"]), b"encrypted-token")
        self.assertEqual(main.disconnect_github(owner_id="owner-a")["status"], "disconnected")
        self.assertFalse(main.github_status(owner_id="owner-a")["connected"])

    def test_github_zipball_checkout_extracts_files_safely_and_is_idempotent(self):
        now = time.time()
        worker = main.WORKER_ID + ":checkout"
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?)",
                ("checkout-task", "checkout repo", "running", now, now, worker, now + 600),
            )
        archive_path = Path(self.temp.name) / "repo.zip"
        with zipfile.ZipFile(archive_path, "w") as archive:
            archive.writestr("owner-repo-sha/README.md", "# NEXUS")
            archive.writestr("owner-repo-sha/app/src/Main.kt", "package sample")
        workspace = Path(self.temp.name) / "workspace"
        lease = main._LEASE_OWNER.set(worker)
        try:
            with patch.object(main, "WORKSPACE", workspace):
                result = main._extract_github_zipball(
                    "checkout-task", archive_path, "nexus-agent/task-checkout-task", "commit-sha"
                )
                self.assertFalse(result["already_checked_out"])
                self.assertEqual(result["files"], 2)
                self.assertEqual((workspace / "checkout-task" / "README.md").read_text(), "# NEXUS")
                self.assertEqual(
                    (workspace / "checkout-task" / "app" / "src" / "Main.kt").read_text(),
                    "package sample",
                )
                again = main._extract_github_zipball(
                    "checkout-task", archive_path, "nexus-agent/task-checkout-task", "commit-sha"
                )
                self.assertTrue(again["already_checked_out"])
                self.assertEqual(again["files"], 0)
        finally:
            main._LEASE_OWNER.reset(lease)

    def test_github_zipball_checkout_rejects_path_traversal(self):
        now = time.time()
        worker = main.WORKER_ID + ":checkout-security"
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?)",
                ("checkout-security", "checkout untrusted archive", "running", now, now, worker, now + 600),
            )
        archive_path = Path(self.temp.name) / "unsafe.zip"
        with zipfile.ZipFile(archive_path, "w") as archive:
            archive.writestr("repo-root/../../escaped.txt", "must not escape")
        workspace = Path(self.temp.name) / "workspace"
        lease = main._LEASE_OWNER.set(worker)
        try:
            with patch.object(main, "WORKSPACE", workspace):
                with self.assertRaises(ValueError):
                    main._extract_github_zipball(
                        "checkout-security", archive_path, "nexus-agent/task-checkout-security", "sha"
                    )
            self.assertFalse((Path(self.temp.name) / "escaped.txt").exists())
        finally:
            main._LEASE_OWNER.reset(lease)

    def test_github_workspace_commit_creates_one_feature_branch_commit(self):
        from unittest.mock import AsyncMock
        now = time.time()
        worker = main.WORKER_ID + ":workspace-commit"
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?)",
                ("workspace-commit", "commit workspace", "running", now, now, worker, now + 600),
            )
        workspace = Path(self.temp.name) / "workspace"
        task_root = workspace / "workspace-commit"
        (task_root / "app" / "src").mkdir(parents=True)
        (task_root / "app" / "src" / "Main.kt").write_text("package sample", encoding="utf-8")
        (task_root / "app" / "src" / "Main.kt").chmod(0o755)
        (task_root / ".nexus-github-checkout.json").write_text("{}", encoding="utf-8")
        token = main._LEASE_OWNER.set(worker)
        try:
            with patch.object(main, "WORKSPACE", workspace), \
                 patch.object(main, "_github_api", new=AsyncMock(side_effect=[
                     {"object": {"sha": "parent-sha"}},
                     {"tree": {"sha": "base-tree"}},
                     {"tree": [], "truncated": False},
                     {"sha": "blob-sha"},
                     {"sha": "new-tree"},
                     {"sha": "new-commit", "html_url": "https://github.com/owner/repo/commit/new-commit"},
                     {},
                 ])) as github_api:
                result = asyncio.run(main._github_commit_task_workspace(
                    "workspace-commit",
                    {"token": "ghp_test", "repo_full_name": "owner/repo"},
                    "nexus-agent/task-workspace-commit",
                    "NEXUS: implement source",
                ))
            self.assertEqual(result["files_committed"], 1)
            self.assertEqual(result["commit_sha"], "new-commit")
            self.assertEqual(github_api.await_count, 7)
            self.assertEqual(github_api.await_args_list[-1].args[1], "PATCH")
            self.assertEqual(github_api.await_args_list[-1].args[3]["force"], False)
            tree_payload = github_api.await_args_list[4].args[3]
            self.assertEqual(tree_payload["tree"][0]["mode"], "100755")
        finally:
            main._LEASE_OWNER.reset(token)

    def test_github_workspace_commit_does_not_update_branch_after_lease_loss(self):
        from unittest.mock import AsyncMock
        now = time.time()
        current = main.WORKER_ID + ":workspace-current"
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?)",
                ("workspace-stale-commit", "stale workspace commit", "running", now, now, current, now + 600),
            )
        workspace = Path(self.temp.name) / "workspace"
        task_root = workspace / "workspace-stale-commit"
        task_root.mkdir(parents=True)
        (task_root / "README.md").write_text("updated", encoding="utf-8")
        stale = main._LEASE_OWNER.set(main.WORKER_ID + ":workspace-stale")
        try:
            with patch.object(main, "WORKSPACE", workspace), \
                 patch.object(main, "_github_api", new=AsyncMock(side_effect=[
                     {"object": {"sha": "parent-sha"}},
                     {"tree": {"sha": "base-tree"}},
                     {"tree": [], "truncated": False},
                     {"sha": "blob-sha"},
                     {"sha": "new-tree"},
                     {"sha": "new-commit", "html_url": "https://github.com/owner/repo/commit/new-commit"},
                 ])) as github_api:
                with self.assertRaises(RuntimeError):
                    asyncio.run(main._github_commit_task_workspace(
                        "workspace-stale-commit",
                        {"token": "ghp_test", "repo_full_name": "owner/repo"},
                        "nexus-agent/task-workspace-stale-commit",
                    ))
                self.assertEqual(github_api.await_count, 0)
                self.assertTrue(all(call.args[1] != "PATCH" for call in github_api.await_args_list))
        finally:
            main._LEASE_OWNER.reset(stale)

    def test_github_actions_log_parser_redacts_tokens_and_bounds_excerpts(self):
        archive_path = Path(self.temp.name) / "actions-logs.zip"
        secret = "github_pat_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        with zipfile.ZipFile(archive_path, "w") as archive:
            archive.writestr("1_build/step.txt", "build failed with " + secret + "\n" + ("x" * 20000))
            archive.writestr("2_build/metadata.json", "{\"not\":\"a log\"}")
        logs = main._read_github_actions_log_archive(archive_path, secret)
        self.assertEqual(len(logs), 1)
        self.assertEqual(logs[0]["file"], "step.txt")
        self.assertNotIn(secret, logs[0]["tail"])
        self.assertIn("[REDACTED_GITHUB_TOKEN]", logs[0]["tail"])
        self.assertLessEqual(len(logs[0]["tail"]), 12000)

    def test_github_android_build_dispatch_waits_and_publishes_signed_apk(self):
        from unittest.mock import AsyncMock
        now = time.time()
        worker = main.WORKER_ID + ":android-build"
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?)",
                ("android-build-task", "build release apk", "running", now, now, worker, now + 600),
            )
        created_at = datetime.fromtimestamp(now, tz=timezone.utc).isoformat()
        token = main._LEASE_OWNER.set(worker)
        try:
            with patch.object(main, "_github_api", new=AsyncMock(side_effect=[
                {"state": "active"},
                {"object": {"sha": "0123456789abcdef0123456789abcdef01234567"}},
                {"workflow_runs": []},
                {},
                {"workflow_runs": [{"id": 123, "display_title": "NEXUS-android-build-task-0123456789ab",
                                    "created_at": created_at,
                                    "head_branch": "nexus-agent/task-android-build-task",
                                    "html_url": "https://github.com/owner/repo/actions/runs/123"}]},
                {"status": "completed", "conclusion": "success"},
                {"artifacts": [{"id": 77, "name": "NEXUS-AI-MAX-release-apk", "expired": False}]},
            ])) as github_api, patch.object(
                main, "_download_github_actions_artifact",
                new=AsyncMock(return_value=[{"name": "app-release.apk", "bytes": 1234}]),
            ) as download:
                result = asyncio.run(main._github_build_android(
                    "android-build-task",
                    {"token": "ghp_test", "repo_full_name": "owner/repo"},
                    "nexus-agent/task-android-build-task",
                    True,
                ))
            self.assertEqual(result["build_status"], "success")
            self.assertEqual(result["artifacts"][0]["name"], "app-release.apk")
            dispatch_payload = github_api.await_args_list[3].args[3]
            self.assertEqual(dispatch_payload["ref"], "nexus-agent/task-android-build-task")
            self.assertEqual(dispatch_payload["inputs"]["require_signed_release"], "true")
            self.assertEqual(dispatch_payload["inputs"]["nexus_task_id"], "android-build-task")
            download.assert_awaited_once()
        finally:
            main._LEASE_OWNER.reset(token)

    def test_github_workspace_commit_tracks_deleted_source_files(self):
        from unittest.mock import AsyncMock
        now = time.time()
        worker = main.WORKER_ID + ":workspace-delete"
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?)",
                ("workspace-delete", "delete source", "running", now, now, worker, now + 600),
            )
        workspace = Path(self.temp.name) / "workspace"
        (workspace / "workspace-delete").mkdir(parents=True)
        token = main._LEASE_OWNER.set(worker)
        try:
            with patch.object(main, "WORKSPACE", workspace), \
                 patch.object(main, "_github_api", new=AsyncMock(side_effect=[
                     {"object": {"sha": "parent-sha"}},
                     {"tree": {"sha": "base-tree"}},
                     {"tree": [{"path": "README.md", "type": "blob", "sha": "old-blob"}],
                      "truncated": False},
                     {"sha": "deleted-tree"},
                     {"sha": "delete-commit", "html_url": "https://github.com/owner/repo/commit/delete-commit"},
                     {},
                 ])) as github_api:
                result = asyncio.run(main._github_commit_task_workspace(
                    "workspace-delete",
                    {"token": "ghp_test", "repo_full_name": "owner/repo"},
                    "nexus-agent/task-workspace-delete",
                ))
            self.assertEqual(result["files_committed"], 0)
            self.assertEqual(result["files_deleted"], 1)
            tree_payload = github_api.await_args_list[3].args[3]
            self.assertEqual(tree_payload["tree"], [
                {"path": "README.md", "mode": "100644", "type": "blob", "sha": None}
            ])
        finally:
            main._LEASE_OWNER.reset(token)

    def test_github_write_actions_reject_protected_branches(self):
        for branch_name in ("main", "master", "nexus-1.0-development", "feature/unsafe"):
            with self.subTest(branch=branch_name):
                with self.assertRaises(ValueError):
                    main._github_branch_name(branch_name, require_feature=True)
        self.assertEqual(main._github_branch_name("nexus-agent/task-123", require_feature=True),
                         "nexus-agent/task-123")
        self.assertEqual(main._github_branch_name("main"), "main")
        with self.assertRaises(ValueError):
            main._github_branch_name("main", require_feature=True)

    def test_github_write_is_fenced_to_current_task_lease(self):
        from unittest.mock import AsyncMock
        now = time.time()
        worker = main.WORKER_ID + ":github-write"
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?,?)",
                ("github-write-task", "commit code", "running", now, now, "owner-a", worker, now + 600),
            )
            conn.execute(
                "INSERT INTO github_integrations(owner_id,repo_full_name,github_login,default_branch,"
                "encrypted_token,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                ("owner-a", "nexus-owner/sample-app", "nexus-owner", "main", b"encrypted", now, now),
            )
        token_context = main._LEASE_OWNER.set(worker)
        try:
            with patch.object(main, "decrypt_secret", return_value="ghp_test-token"), \
                 patch.object(main, "_github_api", new=AsyncMock(side_effect=[
                     None,
                     {"commit": {"html_url": "https://github.com/nexus-owner/sample-app/commit/abc"},
                      "content": {"sha": "new-sha"}},
                 ])) as github_api:
                result = asyncio.run(main._github_task_action(
                    "github-write-task", "owner-a", "github_write_file",
                    {"branch": "nexus-agent/task-github-write-task",
                     "path": "src/Main.kt", "content": "package sample"},
                ))
            self.assertFalse(result["unchanged"])
            self.assertEqual(result["sha"], "new-sha")
            self.assertEqual(github_api.await_count, 2)
            self.assertEqual(github_api.await_args_list[-1].args[1], "PUT")
        finally:
            main._LEASE_OWNER.reset(token_context)

    def test_github_write_refuses_stale_task_lease_before_commit(self):
        from unittest.mock import AsyncMock
        now = time.time()
        current = main.WORKER_ID + ":github-current"
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?,?)",
                ("github-stale-task", "stale commit", "running", now, now, "owner-a", current, now + 600),
            )
            conn.execute(
                "INSERT INTO github_integrations(owner_id,repo_full_name,github_login,default_branch,"
                "encrypted_token,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                ("owner-a", "nexus-owner/sample-app", "nexus-owner", "main", b"encrypted", now, now),
            )
        stale = main._LEASE_OWNER.set(main.WORKER_ID + ":github-stale")
        try:
            with patch.object(main, "decrypt_secret", return_value="ghp_test-token"), \
                 patch.object(main, "_github_api", new=AsyncMock(side_effect=[
                     None, {"object": {"sha": "base-sha"}}
                 ])) as github_api:
                with self.assertRaises(RuntimeError):
                    asyncio.run(main._github_task_action(
                        "github-stale-task", "owner-a", "github_create_branch", {}
                    ))
                self.assertEqual(github_api.await_count, 2)
                self.assertTrue(all(call.args[1] == "GET" for call in github_api.await_args_list))
        finally:
            main._LEASE_OWNER.reset(stale)

    def test_memory_crud_is_scoped_to_owner(self):
        created = main.create_memory(
            main.MemoryInput(title="Nutrition preference", content="Prefers evidence-based nutrition advice",
                             category="preference", pinned=True),
            owner_id="owner-a",
        )["memory"]
        memory_id = created["id"]
        self.assertTrue(created["pinned"])
        self.assertEqual(main.list_memories(owner_id="owner-a")["memories"][0]["id"], memory_id)
        self.assertEqual(main.list_memories(owner_id="owner-b")["memories"], [])
        updated = main.update_memory(
            memory_id, main.MemoryUpdateInput(content="Wants peer-reviewed nutrition research", pinned=False),
            owner_id="owner-a",
        )["memory"]
        self.assertEqual(updated["content"], "Wants peer-reviewed nutrition research")
        self.assertFalse(updated["pinned"])
        with self.assertRaises(main.HTTPException) as denied:
            main.update_memory(memory_id, main.MemoryUpdateInput(title="Hijack"), owner_id="owner-b")
        self.assertEqual(denied.exception.status_code, 404)
        self.assertEqual(main.delete_memory(memory_id, owner_id="owner-a")["status"], "deleted")
        with self.assertRaises(main.HTTPException) as missing:
            main.delete_memory(memory_id, owner_id="owner-a")
        self.assertEqual(missing.exception.status_code, 404)

    def test_memory_retrieval_matches_recent_user_context_and_is_owner_scoped(self):
        main.create_memory(
            main.MemoryInput(title="NEXUS project", content="Building an Android AI assistant with long-term memory",
                             category="project", pinned=True),
            owner_id="owner-a",
        )
        main.create_memory(
            main.MemoryInput(title="Travel", content="Prefers quiet hotels near public transport",
                             category="preference"),
            owner_id="owner-a",
        )
        main.create_memory(
            main.MemoryInput(title="NEXUS project", content="Private memory from another account",
                             category="project"),
            owner_id="owner-b",
        )
        messages = [{"role": "user", "content": "How should we implement NEXUS Android memory retrieval?"}]
        results = main._retrieve_relevant_memories("owner-a", messages)
        self.assertEqual([item["title"] for item in results], ["NEXUS project"])
        self.assertIn("Android AI assistant", results[0]["content"])
        self.assertIsNotNone(results[0]["last_accessed_at"])
        self.assertEqual(main._retrieve_relevant_memories("owner-b", messages)[0]["content"],
                         "Private memory from another account")

    def test_memory_search_ranks_title_matches(self):
        main.create_memory(
            main.MemoryInput(title="Cooking", content="General food notes"),
            owner_id="personal",
        )
        main.create_memory(
            main.MemoryInput(title="NEXUS roadmap", content="Android app and agent memory"),
            owner_id="personal",
        )
        results = main.list_memories(q="NEXUS", owner_id="personal")["memories"]
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["title"], "NEXUS roadmap")

    def test_get_task_returns_event_ids_and_supports_incremental_cursor(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id) VALUES(?,?,?,?,?,?)",
                ("task-event-cursor", "inspect event cursor", "running", now, now, "personal"),
            )
            for index in range(3):
                conn.execute(
                    "INSERT INTO events(task_id,created_at,kind,message) VALUES(?,?,?,?)",
                    ("task-event-cursor", now + index, "progress", f"event-{index}"),
                )
        initial = main.get_task("task-event-cursor", owner_id="personal")
        self.assertEqual([item["id"] for item in initial["events"]], [1, 2, 3])
        self.assertEqual(initial["events"][0]["message"], "event-0")
        incremental = main.get_task("task-event-cursor", after_event_id=2, owner_id="personal")
        self.assertEqual([item["id"] for item in incremental["events"]], [3])
        self.assertEqual(incremental["events"][0]["message"], "event-2")

    def test_task_event_stream_emits_ordered_sse_events_and_stops_on_terminal_task(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id) VALUES(?,?,?,?,?,?)",
                ("task-sse", "stream events", "completed", now, now, "personal"),
            )
            for index in range(2):
                conn.execute(
                    "INSERT INTO events(task_id,created_at,kind,message) VALUES(?,?,?,?)",
                    ("task-sse", now + index, "progress", f"event-{index}"),
                )

        request = SimpleNamespace(is_disconnected=AsyncMock(return_value=False))

        async def collect():
            return [chunk async for chunk in main._task_event_stream(
                "task-sse", "personal", 0, request
            )]

        chunks = asyncio.run(collect())
        self.assertEqual(len(chunks), 2)
        self.assertIn("id: 1\nevent: task_event\ndata:", chunks[0])
        self.assertIn('"message":"event-0"', chunks[0])
        self.assertIn("id: 2\nevent: task_event\ndata:", chunks[1])
        self.assertIn('"message":"event-1"', chunks[1])

    def test_task_event_batch_resumes_after_last_seen_event_id(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id) VALUES(?,?,?,?,?,?)",
                ("task-sse-cursor", "resume stream", "running", now, now, "personal"),
            )
            for index in range(4):
                conn.execute(
                    "INSERT INTO events(task_id,created_at,kind,message) VALUES(?,?,?,?)",
                    ("task-sse-cursor", now + index, "progress", f"event-{index}"),
                )
        batch = main._read_task_event_batch("task-sse-cursor", "personal", 2)
        self.assertEqual([item["id"] for item in batch["events"]], [3, 4])
        self.assertEqual(batch["status"], "running")

    def test_task_event_stream_stops_when_client_disconnects(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id) VALUES(?,?,?,?,?,?)",
                ("task-sse-disconnect", "disconnect stream", "running", now, now, "personal"),
            )
        request = SimpleNamespace(is_disconnected=AsyncMock(return_value=True))

        async def collect():
            return [chunk async for chunk in main._task_event_stream(
                "task-sse-disconnect", "personal", 0, request
            )]

        self.assertEqual(asyncio.run(collect()), [])

    def test_task_event_stream_does_not_expose_another_owner_task(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id) VALUES(?,?,?,?,?,?)",
                ("task-sse-private", "private stream", "completed", now, now, "owner-a"),
            )
        self.assertIsNone(main._read_task_event_batch("task-sse-private", "owner-b", 0))

    def test_get_task_without_cursor_returns_most_recent_event_page(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id) VALUES(?,?,?,?,?,?)",
                ("task-event-page", "bounded event history", "completed", now, now, "personal"),
            )
            for index in range(205):
                conn.execute(
                    "INSERT INTO events(task_id,created_at,kind,message) VALUES(?,?,?,?)",
                    ("task-event-page", now + index, "progress", f"event-{index}"),
                )
        recent = main.get_task("task-event-page", owner_id="personal")["events"]
        self.assertEqual(len(recent), 200)
        self.assertEqual(recent[0]["message"], "event-5")
        self.assertEqual(recent[-1]["message"], "event-204")
        next_page = main.get_task("task-event-page", after_event_id=200, owner_id="personal")["events"]
        self.assertEqual([item["message"] for item in next_page], [
            "event-200", "event-201", "event-202", "event-203", "event-204"
        ])

    def test_get_task_rejects_negative_event_cursor(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id) VALUES(?,?,?,?,?,?)",
                ("task-event-negative", "invalid cursor", "running", now, now, "personal"),
            )
        with self.assertRaises(main.HTTPException) as raised:
            main.get_task("task-event-negative", after_event_id=-1, owner_id="personal")
        self.assertEqual(raised.exception.status_code, 422)

    def test_migrates_legacy_task_schema(self):
        with main.db() as conn:
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(tasks)")}
        self.assertIn("lease_owner", columns)
        self.assertIn("attempt_count", columns)
        self.assertIn("max_steps", columns)
        self.assertIn("client_request_key", columns)

    def test_claims_queued_task_and_increments_attempt(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at) VALUES(?,?,?,?,?)",
                ("task-a", "do a task", "queued", now, now),
            )
        claimed = main.claim_task()
        self.assertEqual(claimed, "task-a")
        with main.db() as conn:
            row = conn.execute("SELECT status,attempt_count,lease_owner,lease_expires_at FROM tasks WHERE id=?",
                               ("task-a",)).fetchone()
        self.assertEqual(row["status"], "running")
        self.assertEqual(row["attempt_count"], 1)
        self.assertTrue(row["lease_owner"].startswith(main.WORKER_ID + ":"))
        self.assertGreater(row["lease_expires_at"], now)

    def test_concurrent_workers_never_claim_the_same_task(self):
        now = time.time()
        with main.db() as conn:
            for index in range(2):
                conn.execute(
                    "INSERT INTO tasks(id,prompt,status,created_at,updated_at) VALUES(?,?,?,?,?)",
                    (f"task-concurrent-{index}", "concurrent claim", "queued", now, now),
                )

        with ThreadPoolExecutor(max_workers=8) as pool:
            claimed = list(pool.map(lambda _: main.claim_task(), range(8)))

        claimed_ids = [task_id for task_id in claimed if task_id is not None]
        self.assertEqual(len(claimed_ids), 2)
        self.assertEqual(len(set(claimed_ids)), 2)
        with main.db() as conn:
            rows = conn.execute(
                "SELECT id,lease_owner,status FROM tasks WHERE id LIKE 'task-concurrent-%'"
            ).fetchall()
        self.assertEqual({row["status"] for row in rows}, {"running"})
        owners = [row["lease_owner"] for row in rows]
        self.assertEqual(len(set(owners)), 2)
        self.assertTrue(all(owner.startswith(main.WORKER_ID + ":") for owner in owners))

    def test_concurrent_workers_never_claim_the_same_step(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?)",
                ("task-concurrent-step", "concurrent step claim", "planning", now, now,
                 main.WORKER_ID, now + 600),
            )
        main._persist_plan("task-concurrent-step", [
            {"title": "Single step", "action": "fetch_url",
             "input": {"url": "https://example.com"}}
        ], 3)
        with main.db() as conn:
            step = conn.execute(
                "SELECT id FROM task_steps WHERE task_id=?", ("task-concurrent-step",)
            ).fetchone()

        with ThreadPoolExecutor(max_workers=8) as pool:
            claims = list(pool.map(
                lambda _: main._claim_step("task-concurrent-step", step["id"]), range(8)
            ))

        accepted = [claim for claim in claims if claim is not None]
        self.assertEqual(len(accepted), 1)
        with main.db() as conn:
            row = conn.execute(
                "SELECT status,attempt_count FROM task_steps WHERE id=?", (step["id"],)
            ).fetchone()
        self.assertEqual(row["status"], "running")
        self.assertEqual(row["attempt_count"], 1)

    def test_persists_steps_and_stable_idempotency_key(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) VALUES(?,?,?,?,?,?,?)",
                ("task-b", "write file", "planning", now, now, main.WORKER_ID, now + 600),
            )
        steps = [{"title": "Save", "action": "save_artifact",
                  "input": {"filename": "report.md", "content": "hello"}}]
        main._persist_plan("task-b", steps, 3)
        with main.db() as conn:
            first = conn.execute("SELECT * FROM task_steps WHERE task_id=?", ("task-b",)).fetchone()
        main._persist_plan("task-b", steps, 3)
        with main.db() as conn:
            rows = conn.execute("SELECT * FROM task_steps WHERE task_id=?", ("task-b",)).fetchall()
            task = conn.execute("SELECT status,plan_json FROM tasks WHERE id=?", ("task-b",)).fetchone()
        self.assertEqual(len(rows), 1)
        self.assertEqual(first["idempotency_key"], rows[0]["idempotency_key"])
        self.assertEqual(task["status"], "running")

    def test_recovers_interrupted_task_and_running_step(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?)",
                ("task-c", "continue work", "planning", now, now, main.WORKER_ID, now + 600),
            )
        main._persist_plan("task-c", [{"title": "Read", "action": "fetch_url",
            "input": {"url": "https://example.com"}}], 3)
        with main.db() as conn:
            conn.execute("UPDATE task_steps SET status='running' WHERE task_id=?", ("task-c",))
            conn.execute("UPDATE tasks SET status='running',lease_owner='dead-worker',lease_expires_at=? WHERE id=?",
                         (now - 1, "task-c"))
        main.recover_interrupted_tasks()
        with main.db() as conn:
            task = conn.execute("SELECT status,lease_owner FROM tasks WHERE id=?", ("task-c",)).fetchone()
            step = conn.execute("SELECT status,error FROM task_steps WHERE task_id=?", ("task-c",)).fetchone()
        self.assertEqual(task["status"], "retry_wait")
        self.assertEqual(task["lease_owner"], "")
        self.assertEqual(step["status"], "retry_wait")
        self.assertIn("Recovered", step["error"])

    def test_step_claim_rejects_expired_lease(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?)",
                ("task-expired-claim", "expired lease", "planning", now, now, main.WORKER_ID, now + 600),
            )
        main._persist_plan("task-expired-claim", [
            {"title": "Read", "action": "fetch_url", "input": {"url": "https://example.com"}}
        ], 3)
        with main.db() as conn:
            step = conn.execute("SELECT id FROM task_steps WHERE task_id=?",
                                ("task-expired-claim",)).fetchone()
            conn.execute("UPDATE tasks SET status='running',lease_expires_at=? WHERE id=?",
                         (now - 1, "task-expired-claim"))
        self.assertIsNone(main._claim_step("task-expired-claim", step["id"]))

    def test_worker_task_transitions_reject_lost_lease(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?)",
                ("task-stale-transition", "stale transition", "running", now, now, "new-worker", now + 600),
            )
        self.assertFalse(main._set_task_planning("task-stale-transition"))
        self.assertFalse(main._set_task_waiting_for_approval("task-stale-transition"))
        self.assertFalse(main._set_task_retry_wait("task-stale-transition", now + 30))
        self.assertFalse(main._complete_task("task-stale-transition", "late", "[]"))
        with main.db() as conn:
            row = conn.execute("SELECT status,lease_owner,summary FROM tasks WHERE id=?",
                               ("task-stale-transition",)).fetchone()
        self.assertEqual(row["status"], "running")
        self.assertEqual(row["lease_owner"], "new-worker")
        self.assertEqual(row["summary"], "")

    def test_persist_plan_rejects_lost_or_expired_lease(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?)",
                ("task-stale-plan", "stale plan", "planning", now, now, "different-worker", now + 600),
            )
        accepted = main._persist_plan("task-stale-plan", [
            {"title": "Read", "action": "fetch_url", "input": {"url": "https://example.com"}}
        ], 3)
        self.assertFalse(accepted)
        with main.db() as conn:
            count = conn.execute("SELECT COUNT(*) AS n FROM task_steps WHERE task_id=?",
                                 ("task-stale-plan",)).fetchone()["n"]
        self.assertEqual(count, 0)

    def test_worker_event_rejects_previous_claim_from_same_process(self):
        now = time.time()
        old_owner = main.WORKER_ID + ":old-claim"
        new_owner = main.WORKER_ID + ":new-claim"
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?)",
                ("task-stale-same-process", "stale same-process claim", "running", now, now,
                 new_owner, now + 600),
            )
        context_token = main._LEASE_OWNER.set(old_owner)
        try:
            accepted = main._worker_event(
                "task-stale-same-process", "step_finished", "late event from previous claim"
            )
        finally:
            main._LEASE_OWNER.reset(context_token)
        self.assertFalse(accepted)
        with main.db() as conn:
            count = conn.execute(
                "SELECT COUNT(*) AS n FROM events WHERE task_id=? AND kind='step_finished'",
                ("task-stale-same-process",),
            ).fetchone()["n"]
        self.assertEqual(count, 0)

    def test_worker_event_rejects_lost_lease(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?)",
                ("task-stale-event", "stale event", "running", now, now, "different-worker", now + 600),
            )
        accepted = main._worker_event("task-stale-event", "step_finished", "late event")
        self.assertFalse(accepted)
        with main.db() as conn:
            count = conn.execute("SELECT COUNT(*) AS n FROM events WHERE task_id=? AND kind='step_finished'",
                                 ("task-stale-event",)).fetchone()["n"]
        self.assertEqual(count, 0)

    def test_step_failure_write_rejects_lost_lease(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?)",
                ("task-stale-failure", "stale failure", "running", now, now, "new-worker", now + 600),
            )
            conn.execute(
                "INSERT INTO task_steps(id,task_id,step_index,title,action,input_json,idempotency_key,status,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                ("step-stale-failure", "task-stale-failure", 0, "Read", "fetch_url", "{}", "stale-failure-key",
                 "running", now, now),
            )
        saved = main._record_step_failure(
            "task-stale-failure", "step-stale-failure", False, None, "late failure"
        )
        self.assertFalse(saved)
        with main.db() as conn:
            task = conn.execute("SELECT status,lease_owner FROM tasks WHERE id=?",
                                ("task-stale-failure",)).fetchone()
            step = conn.execute("SELECT status,error FROM task_steps WHERE id=?",
                                ("step-stale-failure",)).fetchone()
        self.assertEqual(task["status"], "running")
        self.assertEqual(task["lease_owner"], "new-worker")
        self.assertEqual(step["status"], "running")
        self.assertEqual(step["error"], "")

    def test_task_failure_write_rejects_lost_lease(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?)",
                ("task-stale-task-failure", "stale task failure", "running", now, now, "new-worker", now + 600),
            )
        result = main._record_task_failure(
            "task-stale-task-failure", RuntimeError("late failure"), "late failure"
        )
        self.assertIsNone(result)
        with main.db() as conn:
            task = conn.execute("SELECT status,lease_owner,error FROM tasks WHERE id=?",
                                ("task-stale-task-failure",)).fetchone()
        self.assertEqual(task["status"], "running")
        self.assertEqual(task["lease_owner"], "new-worker")
        self.assertEqual(task["error"], "")

    def test_step_result_write_rejects_lost_lease(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?)",
                ("task-stale-result", "stale result", "running", now, now, "former-worker", now + 600),
            )
            conn.execute(
                "INSERT INTO task_steps(id,task_id,step_index,title,action,input_json,idempotency_key,status,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                ("step-stale-result", "task-stale-result", 0, "Read", "fetch_url", "{}", "stale-result-key",
                 "running", now, now),
            )
        saved = main._save_step_result(
            "task-stale-result", "step-stale-result", "completed", json.dumps({"ok": True})
        )
        self.assertFalse(saved)
        with main.db() as conn:
            row = conn.execute("SELECT status,result_json FROM task_steps WHERE id=?",
                                ("step-stale-result",)).fetchone()
        self.assertEqual(row["status"], "running")
        self.assertEqual(row["result_json"], "")

    def test_previous_claim_from_same_process_cannot_write_step_result_or_failure(self):
        now = time.time()
        old_owner = main.WORKER_ID + ":previous-claim"
        current_owner = main.WORKER_ID + ":current-claim"
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?)",
                ("task-reclaimed-write", "reclaimed task", "running", now, now,
                 current_owner, now + 600),
            )
            conn.execute(
                "INSERT INTO task_steps(id,task_id,step_index,title,action,input_json,idempotency_key,status,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                ("step-reclaimed-write", "task-reclaimed-write", 0, "Read", "fetch_url", "{}",
                 "reclaimed-write-key", "running", now, now),
            )

        token = main._LEASE_OWNER.set(old_owner)
        try:
            result_saved = main._save_step_result(
                "task-reclaimed-write", "step-reclaimed-write", "completed",
                json.dumps({"late": True}),
            )
            failure_saved = main._record_step_failure(
                "task-reclaimed-write", "step-reclaimed-write", False, None, "late failure",
            )
        finally:
            main._LEASE_OWNER.reset(token)

        self.assertFalse(result_saved)
        self.assertFalse(failure_saved)
        with main.db() as conn:
            task = conn.execute(
                "SELECT status,lease_owner FROM tasks WHERE id=?", ("task-reclaimed-write",)
            ).fetchone()
            step = conn.execute(
                "SELECT status,result_json,error FROM task_steps WHERE id=?",
                ("step-reclaimed-write",),
            ).fetchone()
        self.assertEqual(task["status"], "running")
        self.assertEqual(task["lease_owner"], current_owner)
        self.assertEqual(step["status"], "running")
        self.assertEqual(step["result_json"], "")
        self.assertEqual(step["error"], "")

    def test_step_claim_requires_live_task_lease_and_no_cancellation(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?)",
                ("task-claim-guard", "guard step claim", "planning", now, now, main.WORKER_ID, now + 600),
            )
        main._persist_plan("task-claim-guard", [
            {"title": "Read", "action": "fetch_url", "input": {"url": "https://example.com"}}
        ], 3)
        with main.db() as conn:
            conn.execute("UPDATE tasks SET status='running',cancel_requested=1 WHERE id=?",
                         ("task-claim-guard",))
            step = conn.execute(
                "SELECT id,status,attempt_count FROM task_steps WHERE task_id=?",
                ("task-claim-guard",),
            ).fetchone()
        claimed = main._claim_step("task-claim-guard", step["id"])
        self.assertIsNone(claimed)
        with main.db() as conn:
            after = conn.execute(
                "SELECT status,attempt_count FROM task_steps WHERE id=?",
                (step["id"],),
            ).fetchone()
        self.assertEqual(after["status"], "pending")
        self.assertEqual(after["attempt_count"], 0)

    def test_recovery_is_idempotent_and_does_not_duplicate_recovery_events(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?)",
                ("task-recovery-idempotent", "recover once", "running", now, now,
                 "expired-worker", now - 1),
            )
            conn.execute(
                "INSERT INTO task_steps(id,task_id,step_index,title,action,input_json,idempotency_key,status,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                ("step-recovery-idempotent", "task-recovery-idempotent", 0, "Read", "fetch_url",
                 "{}", "recovery-idempotent-key", "running", now, now),
            )

        main.recover_interrupted_tasks()
        main.recover_interrupted_tasks()

        with main.db() as conn:
            task = conn.execute(
                "SELECT status,lease_owner FROM tasks WHERE id=?",
                ("task-recovery-idempotent",),
            ).fetchone()
            step = conn.execute(
                "SELECT status FROM task_steps WHERE id=?", ("step-recovery-idempotent",)
            ).fetchone()
            events = conn.execute(
                "SELECT COUNT(*) AS n FROM events WHERE task_id=? AND kind='recovered'",
                ("task-recovery-idempotent",),
            ).fetchone()["n"]
        self.assertEqual(task["status"], "retry_wait")
        self.assertEqual(task["lease_owner"], "")
        self.assertEqual(step["status"], "retry_wait")
        self.assertEqual(events, 1)

    def test_recovery_preserves_task_with_active_lease(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?)",
                ("task-live-lease", "still running elsewhere", "running", now, now,
                 "other-live-worker", now + 600),
            )
        main.recover_interrupted_tasks()
        with main.db() as conn:
            row = conn.execute(
                "SELECT status,lease_owner,lease_expires_at FROM tasks WHERE id=?",
                ("task-live-lease",),
            ).fetchone()
        self.assertEqual(row["status"], "running")
        self.assertEqual(row["lease_owner"], "other-live-worker")
        self.assertGreater(row["lease_expires_at"], now)

    def test_recovery_finalizes_cancelled_task_instead_of_requeueing(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,cancel_requested,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?,?)",
                ("task-cancelled-recovery", "cancel me", "running", now, now, 1,
                 "dead-worker", now - 10),
            )
            conn.execute(
                "INSERT INTO task_steps(id,task_id,step_index,title,action,input_json,idempotency_key,status,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                ("step-cancelled-recovery", "task-cancelled-recovery", 0, "Work", "fetch_url",
                 "{}", "cancel-recovery-key", "running", now, now),
            )
        main.recover_interrupted_tasks()
        with main.db() as conn:
            task = conn.execute("SELECT status,cancel_requested,lease_owner FROM tasks WHERE id=?",
                                ("task-cancelled-recovery",)).fetchone()
            step = conn.execute("SELECT status FROM task_steps WHERE id=?",
                                ("step-cancelled-recovery",)).fetchone()
        self.assertEqual(task["status"], "cancelled")
        self.assertEqual(task["cancel_requested"], 1)
        self.assertEqual(task["lease_owner"], "")
        self.assertEqual(step["status"], "skipped")
        self.assertIsNone(main.claim_task())

    def test_claim_skips_cancel_requested_retry_task(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,cancel_requested,next_attempt_at) "
                "VALUES(?,?,?,?,?,?,?)",
                ("task-cancelled-queued", "cancel me", "retry_wait", now, now, 1, now - 1),
            )
        self.assertIsNone(main.claim_task())

    def test_claim_respects_retry_schedule(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,next_attempt_at) "
                "VALUES(?,?,?,?,?,?)",
                ("task-d", "retry later", "retry_wait", now, now, now + 3600),
            )
        self.assertIsNone(main.claim_task())

    def test_new_task_defaults_to_automatic_model_selection(self):
        body = main.NewTask(prompt="Research this subject")
        self.assertEqual(body.model_selection, "auto")
        created = main.create_task(body, owner_id="personal")
        with main.db() as conn:
            row = conn.execute("SELECT model_selection FROM tasks WHERE id=?", (created["id"],)).fetchone()
        self.assertEqual(row["model_selection"], "auto")

    def test_manual_model_selection_is_validated_and_persisted(self):
        body = main.NewTask(prompt="Build an Android application", model_selection="user-claude")
        with patch.object(main, "provider_models_for_owner",
                          return_value=[SimpleNamespace(id="user-claude")]):
            created = main.create_task(body, owner_id="personal")
        with main.db() as conn:
            row = conn.execute("SELECT model_selection FROM tasks WHERE id=?", (created["id"],)).fetchone()
        self.assertEqual(row["model_selection"], "user-claude")

    def test_unknown_manual_model_selection_is_rejected(self):
        body = main.NewTask(prompt="Build an Android application", model_selection="user-missing")
        with patch.object(main, "provider_models_for_owner",
                          return_value=[SimpleNamespace(id="user-claude")]):
            with self.assertRaises(main.HTTPException) as raised:
                main.create_task(body, owner_id="personal")
        self.assertEqual(raised.exception.status_code, 422)

    def test_create_task_deduplicates_client_request_key(self):
        body = main.NewTask(prompt="Research this subject", idempotency_key="client-key-0001")
        first = main.create_task(body, owner_id="personal")
        second = main.create_task(body, owner_id="personal")
        self.assertEqual(first["id"], second["id"])
        self.assertTrue(second["deduplicated"])
        with main.db() as conn:
            count = conn.execute("SELECT COUNT(*) AS n FROM tasks").fetchone()["n"]
        self.assertEqual(count, 1)

    def test_cancel_request_cannot_be_overwritten_by_task_completion(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id) VALUES(?,?,?,?,?,?)",
                ("task-cancel-race", "cancel before completion", "running", now, now, "tenant-cancel"),
            )
            conn.execute(
                "INSERT INTO approvals(id,task_id,description,status,created_at,idempotency_key) "
                "VALUES(?,?,?,?,?,?)",
                ("approval-cancel-race", "task-cancel-race", "Pending action", "pending", now,
                 "approval-cancel-race-key"),
            )

        response = main.cancel_task("task-cancel-race", owner_id="tenant-cancel")
        self.assertEqual(response["status"], "cancellation_requested")
        completed = main._complete_task("task-cancel-race", "should not complete", "[]")
        self.assertFalse(completed)
        main._mark_task_cancelled("task-cancel-race")

        with main.db() as conn:
            task = conn.execute("SELECT status,cancel_requested FROM tasks WHERE id=?",
                                ("task-cancel-race",)).fetchone()
            event_count = conn.execute(
                "SELECT COUNT(*) AS n FROM events WHERE task_id=? AND kind='cancel_requested'",
                ("task-cancel-race",),
            ).fetchone()["n"]
            approval = conn.execute(
                "SELECT status FROM approvals WHERE id=?", ("approval-cancel-race",)
            ).fetchone()
        self.assertEqual(approval["status"], "cancelled")
        self.assertEqual(task["status"], "cancelled")
        self.assertEqual(task["cancel_requested"], 1)
        self.assertEqual(event_count, 1)

    def test_stale_worker_cannot_finalize_cancellation_without_cancel_request(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?)",
                ("task-stale-cancel-finalize", "reclaimed by another worker", "running",
                 now, now, "current-worker-claim", now + 600),
            )
            conn.execute(
                "INSERT INTO approvals(id,task_id,description,status,created_at,idempotency_key) "
                "VALUES(?,?,?,?,?,?)",
                ("approval-stale-cancel-finalize", "task-stale-cancel-finalize",
                 "Pending action", "pending", now, "stale-cancel-finalize-key"),
            )

        finalized = main._mark_task_cancelled("task-stale-cancel-finalize")

        self.assertFalse(finalized)
        with main.db() as conn:
            task = conn.execute(
                "SELECT status,cancel_requested,lease_owner FROM tasks WHERE id=?",
                ("task-stale-cancel-finalize",),
            ).fetchone()
            approval = conn.execute(
                "SELECT status FROM approvals WHERE id=?",
                ("approval-stale-cancel-finalize",),
            ).fetchone()
        self.assertEqual(task["status"], "running")
        self.assertEqual(task["cancel_requested"], 0)
        self.assertEqual(task["lease_owner"], "current-worker-claim")
        self.assertEqual(approval["status"], "pending")

    def test_worker_cancellation_finalization_cancels_pending_approvals(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,cancel_requested) "
                "VALUES(?,?,?,?,?,1)",
                ("task-worker-cancel-approval", "cancel pending action", "running", now, now),
            )
            conn.execute(
                "INSERT INTO approvals(id,task_id,description,status,created_at,idempotency_key) "
                "VALUES(?,?,?,?,?,?)",
                ("approval-worker-cancel", "task-worker-cancel-approval", "Pending action",
                 "pending", now, "approval-worker-cancel-key"),
            )

        main._mark_task_cancelled("task-worker-cancel-approval")

        with main.db() as conn:
            task = conn.execute(
                "SELECT status,cancel_requested FROM tasks WHERE id=?",
                ("task-worker-cancel-approval",),
            ).fetchone()
            approval = conn.execute(
                "SELECT status FROM approvals WHERE id=?", ("approval-worker-cancel",)
            ).fetchone()
        self.assertEqual(task["status"], "cancelled")
        self.assertEqual(task["cancel_requested"], 1)
        self.assertEqual(approval["status"], "cancelled")

    def test_cancel_wins_before_approval_decision(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id) VALUES(?,?,?,?,?,?)",
                ("task-cancel-before-approval", "cancel before decision", "waiting_for_approval",
                 now, now, "tenant-cancel-first"),
            )
            conn.execute(
                "INSERT INTO approvals(id,task_id,description,status,created_at,idempotency_key) "
                "VALUES(?,?,?,?,?,?)",
                ("approval-cancel-before", "task-cancel-before-approval", "Pending action",
                 "pending", now, "approval-cancel-before-key"),
            )

        main.cancel_task("task-cancel-before-approval", owner_id="tenant-cancel-first")
        with self.assertRaises(main.HTTPException) as raised:
            main.decide_approval(
                "approval-cancel-before", main.Approval(approved=True),
                owner_id="tenant-cancel-first",
            )
        self.assertEqual(raised.exception.status_code, 409)
        with main.db() as conn:
            task = conn.execute(
                "SELECT status,cancel_requested FROM tasks WHERE id=?",
                ("task-cancel-before-approval",),
            ).fetchone()
            approval = conn.execute(
                "SELECT status FROM approvals WHERE id=?", ("approval-cancel-before",)
            ).fetchone()
        self.assertEqual(task["status"], "cancelled")
        self.assertEqual(task["cancel_requested"], 1)
        self.assertEqual(approval["status"], "cancelled")

    def test_approval_decision_wins_before_later_task_cancellation(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id) VALUES(?,?,?,?,?,?)",
                ("task-approval-before-cancel", "decision before cancel", "waiting_for_approval",
                 now, now, "tenant-approval-first"),
            )
            conn.execute(
                "INSERT INTO approvals(id,task_id,description,status,created_at,idempotency_key) "
                "VALUES(?,?,?,?,?,?)",
                ("approval-before-cancel", "task-approval-before-cancel", "Pending action",
                 "pending", now, "approval-before-cancel-key"),
            )

        decision = main.decide_approval(
            "approval-before-cancel", main.Approval(approved=True),
            owner_id="tenant-approval-first",
        )
        self.assertEqual(decision["status"], "approved")
        main.cancel_task("task-approval-before-cancel", owner_id="tenant-approval-first")
        main._mark_task_cancelled("task-approval-before-cancel")

        with main.db() as conn:
            task = conn.execute(
                "SELECT status,cancel_requested FROM tasks WHERE id=?",
                ("task-approval-before-cancel",),
            ).fetchone()
            approval = conn.execute(
                "SELECT status FROM approvals WHERE id=?", ("approval-before-cancel",)
            ).fetchone()
        self.assertEqual(task["status"], "cancelled")
        self.assertEqual(task["cancel_requested"], 1)
        self.assertEqual(approval["status"], "approved")

    def test_concurrent_approval_decisions_have_single_winner(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id) VALUES(?,?,?,?,?,?)",
                ("task-approval-race", "approve one decision", "waiting_for_approval", now, now, "tenant-race"),
            )
            conn.execute(
                "INSERT INTO approvals(id,task_id,description,status,created_at,idempotency_key) "
                "VALUES(?,?,?,?,?,?)",
                ("approval-race", "task-approval-race", "Approve action", "pending", now, "approval-race-key"),
            )

        def decide(approved):
            try:
                return main.decide_approval(
                    "approval-race", main.Approval(approved=approved), owner_id="tenant-race"
                )
            except main.HTTPException as exc:
                return exc.status_code

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(decide, [True, False]))

        successes = [item for item in results if isinstance(item, dict)]
        conflicts = [item for item in results if item == 409]
        self.assertEqual(len(successes), 1)
        self.assertEqual(len(conflicts), 1)
        with main.db() as conn:
            approval = conn.execute("SELECT status FROM approvals WHERE id=?",
                                     ("approval-race",)).fetchone()
            event_count = conn.execute(
                "SELECT COUNT(*) AS n FROM events WHERE task_id=? AND kind='approval_decision'",
                ("task-approval-race",),
            ).fetchone()["n"]
        self.assertIn(approval["status"], {"approved", "rejected"})
        self.assertEqual(event_count, 1)

    def test_concurrent_create_task_is_idempotent_and_event_is_atomic(self):
        body = main.NewTask(
            prompt="Run a concurrent idempotency check",
            idempotency_key="parallel-client-key-0001",
        )
        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(
                lambda _: main.create_task(body, owner_id="tenant-concurrent"),
                range(16),
            ))

        task_ids = {item["id"] for item in results}
        self.assertEqual(len(task_ids), 1)
        self.assertTrue(any(item.get("deduplicated") for item in results))
        task_id = next(iter(task_ids))
        with main.db() as conn:
            task_count = conn.execute(
                "SELECT COUNT(*) AS n FROM tasks WHERE owner_id=? AND client_request_key=?",
                ("tenant-concurrent", body.idempotency_key),
            ).fetchone()["n"]
            event_count = conn.execute(
                "SELECT COUNT(*) AS n FROM events WHERE task_id=? AND kind='created'",
                (task_id,),
            ).fetchone()["n"]
        self.assertEqual(task_count, 1)
        self.assertEqual(event_count, 1)

    def test_provider_credentials_are_encrypted_and_owner_scoped(self):
        import os
        from unittest.mock import patch

        body = main.ProviderCredentialInput(
            provider_id="anymodel",
            base_url="https://anymodel.org/v1",
            model="claude-fable-5-2",
            api_key="provider-secret-key-value",
            supports_vision=True,
        )
        self.assertNotIn("provider-secret-key-value", repr(body))
        short_body = main.ProviderCredentialInput(
            provider_id="shortkey",
            base_url="https://anymodel.org/v1",
            model="test-model",
            api_key="short",
        )
        key = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="
        with patch.dict(os.environ, {"NEXUS_CREDENTIAL_ENCRYPTION_KEY": key}):
            with self.assertRaises(main.HTTPException) as invalid_key:
                main.save_provider(short_body, owner_id="tenant-a")
            self.assertNotIn("short", str(invalid_key.exception.detail))
            result = main.save_provider(body, owner_id="tenant-a")
            self.assertEqual(result["status"], "saved")
            with main.db() as conn:
                row = conn.execute("SELECT encrypted_api_key FROM provider_credentials WHERE owner_id=?",
                                    ("tenant-a",)).fetchone()
            self.assertNotIn(b"provider-secret-key-value", bytes(row["encrypted_api_key"]))
            models = main.provider_models_for_owner("tenant-a")
            self.assertTrue(any(m.id == "user-anymodel" and m.api_key == body.api_key.get_secret_value() and "vision" in m.capabilities for m in models))
            self.assertEqual(main.provider_models_for_owner("tenant-b"), [])
            catalog = asyncio.run(main.list_available_models(owner_id="tenant-a"))
            self.assertTrue(any(item["source"] == "user" and "api_key" not in item for item in catalog["models"]))

    def test_task_lists_are_owner_scoped(self):
        first = main.NewTask(prompt="Task for tenant A")
        second = main.NewTask(prompt="Task for tenant B")
        main.create_task(first, owner_id="tenant-a")
        main.create_task(second, owner_id="tenant-b")
        tenant_a = main.list_tasks(owner_id="tenant-a")
        tenant_b = main.list_tasks(owner_id="tenant-b")
        self.assertEqual(len(tenant_a["tasks"]), 1)
        self.assertEqual(len(tenant_b["tasks"]), 1)
        self.assertNotEqual(tenant_a["tasks"][0]["id"], tenant_b["tasks"][0]["id"])

    def test_chat_validator_accepts_inline_image_and_rejects_remote_image_url(self):
        valid = [{"role": "user", "content": [
            {"type": "text", "text": "Describe this"},
            {"type": "image_url", "image_url": {"url": "data:image/png;base64,aGVsbG8="}},
        ]}]
        self.assertTrue(main.validate_chat_messages(valid))
        invalid = [{"role": "user", "content": [
            {"type": "image_url", "image_url": {"url": "https://example.com/image.png"}},
        ]}]
        with self.assertRaises(main.HTTPException):
            main.validate_chat_messages(invalid)

    def test_safe_url_rejects_private_and_malformed_destinations(self):
        blocked = [
            "http://example.com",
            "https://example.com:8443/path",
            "https://user:pass@example.com/path",
            "https://localhost./",
            "https://127.0.0.1/",
            "https://[::1]/",
            "https://metadata.google.internal/",
            "https://example.com:invalid/",
        ]
        for url in blocked:
            with self.subTest(url=url), self.assertRaises(ValueError):
                main.safe_url(url)

    def test_pinned_resolver_returns_only_prevalidated_addresses_without_dns(self):
        addresses = [ipaddress.ip_address("93.184.216.34"), ipaddress.ip_address("2606:2800:220:1:248:1893:25c8:1946")]
        resolver = main._PinnedPublicResolver(addresses)

        async def run():
            with patch.object(main.socket, "getaddrinfo", side_effect=AssertionError("DNS must not be repeated")):
                results = await resolver.resolve("example.test", 443, socket.AF_UNSPEC)
            resolver.close()
            return results

        results = asyncio.run(run())
        self.assertEqual({item["host"] for item in results},
                         {"93.184.216.34", "2606:2800:220:1:248:1893:25c8:1946"})
        self.assertTrue(all(item["flags"] == socket.AI_NUMERICHOST for item in results))

    def test_pinned_resolver_filters_incompatible_address_family(self):
        resolver = main._PinnedPublicResolver([ipaddress.ip_address("93.184.216.34")])

        async def run():
            return await resolver.resolve("example.test", 443, socket.AF_INET6)

        with self.assertRaisesRegex(OSError, "compatible"):
            asyncio.run(run())

    def test_safe_url_rejects_mixed_public_and_private_dns_answers(self):
        records = [
            (None, None, None, None, ("93.184.216.34", 443)),
            (None, None, None, None, ("10.0.0.8", 443)),
        ]
        with patch.object(main.socket, "getaddrinfo", return_value=records):
            with self.assertRaisesRegex(ValueError, "Private IP"):
                main.safe_url("https://example.test/path")

    def test_safe_url_accepts_public_dns_and_normalizes_hostname_for_policy(self):
        records = [(None, None, None, None, ("93.184.216.34", 443))]
        with patch.object(main.socket, "getaddrinfo", return_value=records):
            self.assertEqual(main.safe_url("https://example.test./path"),
                             "https://example.test./path")

    def test_agent_auth_rejects_invalid_and_accepts_exact_bearer_token(self):
        with patch.object(main, "API_KEY", "test-secret-token"):
            with self.assertRaises(main.HTTPException) as raised:
                main.auth("Bearer test-secret-tokeN")
            self.assertEqual(raised.exception.status_code, 401)
            self.assertEqual(main.auth("Bearer test-secret-token"), "personal")

    def test_rejects_unlisted_provider_hosts(self):
        with self.assertRaises(main.HTTPException):
            main.validate_provider_base_url("https://attacker.example/v1")
        with self.assertRaises(main.HTTPException):
            main.validate_provider_base_url("https://api.openai.com:invalid/v1")
        with self.assertRaises(main.HTTPException):
            main.validate_provider_base_url("https://[invalid/v1")

    def test_approval_record_is_idempotent_for_retried_step(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) VALUES(?,?,?,?,?,?,?)",
                ("task-approval", "send a message", "running", now, now, main.WORKER_ID, now + 600),
            )
        step = {"title": "Prepare message", "action": "draft_external_action",
                "input": {"description": "Send approved message"}}
        first = asyncio.run(main.run_step(step, "task-approval", "same-idempotency-key"))
        second = asyncio.run(main.run_step(step, "task-approval", "same-idempotency-key"))
        self.assertEqual(first["approval_id"], second["approval_id"])
        with main.db() as conn:
            count = conn.execute("SELECT COUNT(*) AS n FROM approvals WHERE task_id=?",
                                 ("task-approval",)).fetchone()["n"]
            event_count = conn.execute(
                "SELECT COUNT(*) AS n FROM events WHERE task_id=? AND kind='approval_required'",
                ("task-approval",),
            ).fetchone()["n"]
        self.assertEqual(count, 1)
        self.assertEqual(event_count, 1)


    def test_stale_worker_cannot_create_approval(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) VALUES(?,?,?,?,?,?,?)",
                ("task-stale-approval", "draft", "running", now, now, "new-worker", now + 600),
            )
        step = {"title": "Draft", "action": "draft_external_action",
                "input": {"description": "Do something"}}
        with self.assertRaisesRegex(RuntimeError, "lease lost"):
            asyncio.run(main.run_step(step, "task-stale-approval", "stale-approval-key"))
        with main.db() as conn:
            count = conn.execute("SELECT COUNT(*) AS n FROM approvals WHERE task_id=?",
                                 ("task-stale-approval",)).fetchone()["n"]
            task = conn.execute("SELECT status,lease_owner FROM tasks WHERE id=?",
                                ("task-stale-approval",)).fetchone()
        self.assertEqual(count, 0)
        self.assertEqual(task["status"], "running")
        self.assertEqual(task["lease_owner"], "new-worker")

    def test_artifact_replace_failure_preserves_previous_file_and_cleans_temp(self):
        now = time.time()
        workspace = Path(self.temp.name) / "workspace-atomic"
        workspace.mkdir()
        target = workspace / "task-atomic-report.md"
        target.write_text("previous complete content", encoding="utf-8")
        with patch.object(main, "WORKSPACE", workspace):
            with main.db() as conn:
                conn.execute(
                    "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    ("task-atomic", "replace artifact", "running", now, now,
                     main.WORKER_ID, now + 600),
                )
            with patch.object(main.os, "replace", side_effect=OSError("simulated replace failure")):
                with self.assertRaisesRegex(OSError, "simulated replace failure"):
                    main._save_artifact("task-atomic", "report.md", "new content")

        self.assertEqual(target.read_text(encoding="utf-8"), "previous complete content")
        self.assertEqual(list(workspace.glob("*.tmp-*")), [])

    def test_artifact_write_replaces_target_with_complete_content(self):
        now = time.time()
        workspace = Path(self.temp.name) / "workspace-replace"
        workspace.mkdir()
        target = workspace / "task-replace-report.md"
        target.write_text("old", encoding="utf-8")
        with patch.object(main, "WORKSPACE", workspace):
            with main.db() as conn:
                conn.execute(
                    "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    ("task-replace", "replace artifact", "running", now, now,
                     main.WORKER_ID, now + 600),
                )
            saved = main._save_artifact("task-replace", "report.md", "complete new content")

        self.assertEqual(saved, target.name)
        self.assertEqual(target.read_text(encoding="utf-8"), "complete new content")
        self.assertEqual(list(workspace.glob("*.tmp-*")), [])

    def test_stale_worker_cannot_write_artifact(self):
        now = time.time()
        workspace = Path(self.temp.name) / "workspace"
        workspace.mkdir()
        with patch.object(main, "WORKSPACE", workspace):
            with main.db() as conn:
                conn.execute(
                    "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) VALUES(?,?,?,?,?,?,?)",
                    ("task-stale-artifact", "save", "running", now, now, "new-worker", now + 600),
                )
            step = {"title": "Save", "action": "save_artifact",
                    "input": {"filename": "report.md", "content": "late write"}}
            with self.assertRaisesRegex(RuntimeError, "lease lost"):
                asyncio.run(main.run_step(step, "task-stale-artifact", "stale-artifact-key"))
            self.assertFalse((workspace / "task-stale-artifact-report.md").exists())

    def test_run_db_executes_in_worker_thread(self):
        loop_thread = threading.get_ident()
        worker_thread = asyncio.run(main.run_db(threading.get_ident))
        self.assertNotEqual(worker_thread, loop_thread)

    def test_llm_json_offloads_provider_lookup_and_event_write(self):
        loop_thread = threading.get_ident()
        observed = {}

        def provider_lookup(owner_id):
            observed["provider_thread"] = threading.get_ident()
            return [object()]

        def write_event(*args):
            observed["event_thread"] = threading.get_ident()

        result = SimpleNamespace(provider_id="test", model="test-model", usage={})

        async def run():
            with patch.object(main, "provider_models_for_owner", side_effect=provider_lookup), \
                 patch.object(main, "event", side_effect=write_event), \
                 patch.object(main.ModelGateway, "complete_json",
                              new=AsyncMock(return_value=({"ok": True}, result))):
                return await main.llm_json("system", "user", task_id="task-thread")

        parsed = asyncio.run(run())
        self.assertEqual(parsed, {"ok": True})
        self.assertNotEqual(observed["provider_thread"], loop_thread)
        self.assertNotEqual(observed["event_thread"], loop_thread)

    def test_process_offloads_database_helpers_from_event_loop(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at) VALUES(?,?,?,?,?)",
                ("task-thread-process", "finish an empty task", "queued", now, now),
            )
        self.assertEqual(main.claim_task(), "task-thread-process")
        with main.db() as conn:
            lease_owner = conn.execute(
                "SELECT lease_owner FROM tasks WHERE id=?", ("task-thread-process",)
            ).fetchone()["lease_owner"]
        lease_context = main._LEASE_OWNER.set(lease_owner)

        loop_thread = threading.get_ident()
        observed = []
        helper_names = [
            "_get_task_row", "_set_task_planning", "_persist_plan",
            "_task_cancelled", "_next_pending_step", "_step_summary",
            "_complete_task", "_worker_event",
        ]
        patches = []
        for name in helper_names:
            original = getattr(main, name)

            def wrapped(*args, _original=original, _name=name, **kwargs):
                observed.append((_name, threading.get_ident()))
                return _original(*args, **kwargs)

            patches.append(patch.object(main, name, side_effect=wrapped))

        async def run():
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], \
                 patch.object(main, "llm_json",
                              new=AsyncMock(side_effect=[{"steps": []}, {"summary": "finished"}])):
                await main.process("task-thread-process")

        try:
            asyncio.run(run())
        finally:
            main._LEASE_OWNER.reset(lease_context)
        self.assertTrue(observed)
        self.assertTrue(all(thread_id != loop_thread for _, thread_id in observed))
        with main.db() as conn:
            task = conn.execute("SELECT status,summary FROM tasks WHERE id=?",
                                ("task-thread-process",)).fetchone()
        self.assertEqual(task["status"], "completed", task["error"])
        self.assertEqual(task["summary"], "finished")

    def test_run_step_offloads_approval_database_work(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,lease_owner,lease_expires_at) VALUES(?,?,?,?,?,?,?)",
                ("task-approval-thread", "draft a message", "running", now, now, main.WORKER_ID, now + 600),
            )
        loop_thread = threading.get_ident()
        observed = []
        original = main._create_approval

        def capture_thread(*args):
            observed.append(threading.get_ident())
            return original(*args)

        step = {"title": "Draft", "action": "draft_external_action",
                "input": {"description": "Prepare a draft"}}
        with patch.object(main, "_create_approval", side_effect=capture_thread):
            result = asyncio.run(main.run_step(step, "task-approval-thread", "approval-thread-key"))
        self.assertEqual(result["status"], "awaiting_approval")
        self.assertTrue(observed)
        self.assertNotEqual(observed[0], loop_thread)

    def test_approval_decision_requeues_waiting_task_for_worker(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id) VALUES(?,?,?,?,?,?)",
                ("task-approval-resume", "continue after approval", "waiting_for_approval",
                 now, now, "tenant-approval"),
            )
            conn.execute(
                "INSERT INTO approvals(id,task_id,description,status,created_at,idempotency_key) "
                "VALUES(?,?,?,?,?,?)",
                ("approval-resume", "task-approval-resume", "Review action", "pending",
                 now, "approval-resume-key"),
            )

        result = main._decide_approval_record(
            "approval-resume", "tenant-approval", True, "approved by owner"
        )
        self.assertEqual(result["status"], "approved")
        self.assertIn("re-queued", result["detail"])
        with main.db() as conn:
            task = conn.execute(
                "SELECT status,next_attempt_at,lease_owner FROM tasks WHERE id=?",
                ("task-approval-resume",),
            ).fetchone()
            approval = conn.execute(
                "SELECT status FROM approvals WHERE id=?", ("approval-resume",)
            ).fetchone()
        self.assertEqual(task["status"], "queued")
        self.assertIsNone(task["next_attempt_at"])
        self.assertEqual(task["lease_owner"], "")
        self.assertEqual(approval["status"], "approved")
        claimed = main._claim_task_with_lease()
        self.assertEqual(claimed[0], "task-approval-resume")

    def test_rejected_approval_also_requeues_task_with_decision_recorded(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id) VALUES(?,?,?,?,?,?)",
                ("task-approval-reject", "continue after rejection", "waiting_for_approval",
                 now, now, "tenant-approval"),
            )
            conn.execute(
                "INSERT INTO approvals(id,task_id,description,status,created_at,idempotency_key) "
                "VALUES(?,?,?,?,?,?)",
                ("approval-reject", "task-approval-reject", "Review action", "pending",
                 now, "approval-reject-key"),
            )

        result = main._decide_approval_record(
            "approval-reject", "tenant-approval", False, "rejected by owner"
        )
        self.assertEqual(result["status"], "rejected")
        with main.db() as conn:
            task = conn.execute("SELECT status FROM tasks WHERE id=?",
                                ("task-approval-reject",)).fetchone()
            approval = conn.execute("SELECT status FROM approvals WHERE id=?",
                                     ("approval-reject",)).fetchone()
        self.assertEqual(task["status"], "queued")
        self.assertEqual(approval["status"], "rejected")

    def test_worker_metrics_report_queue_depth_and_oldest_pending_age(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at) VALUES(?,?,?,?,?)",
                ("queued-1", "queued task", "queued", now - 120, now),
            )
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at) VALUES(?,?,?,?,?)",
                ("retry-1", "retry task", "retry_wait", now - 60, now),
            )
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at) VALUES(?,?,?,?,?)",
                ("running-1", "running task", "running", now - 30, now),
            )
        heartbeat, counts, oldest = main._read_worker_metrics()
        self.assertIsNone(heartbeat)
        self.assertEqual(counts["queued"], 1)
        self.assertEqual(counts["retry_wait"], 1)
        self.assertEqual(counts["running"], 1)
        self.assertAlmostEqual(oldest, now - 120, delta=1)

    def test_worker_heartbeat_is_persisted_and_can_be_marked_stale(self):
        main._write_worker_heartbeat("running")
        heartbeat = main._read_worker_heartbeat()
        self.assertEqual(heartbeat["state"], "running")
        self.assertEqual(heartbeat["instance_id"], main.WORKER_ID)
        with main.db() as conn:
            conn.execute(
                "UPDATE worker_heartbeats SET updated_at=? WHERE name='default'",
                (time.time() - 120,),
            )
        stale = main._read_worker_heartbeat()
        self.assertLess(stale["updated_at"], time.time() - 90)
        main._write_worker_heartbeat("stopped")
        self.assertEqual(main._read_worker_heartbeat()["state"], "stopped")

    def _create_test_project(self, owner_id="tenant-project"):
        result = main.create_project(
            main.ProjectInput(name="Persistent project", description="Versioned files"),
            owner_id=owner_id,
        )
        return result["project"]["id"]

    def test_binary_project_files_round_trip_with_immutable_revisions(self):
        project_id = self._create_test_project()
        payload = bytes([0, 255, 1, 2, 128, 10])
        first = main._write_project_file_bytes(
            project_id, "tenant-project", "assets/icon.bin", payload,
            "Upload icon", "application/octet-stream", True,
        )
        second = main._write_project_file_bytes(
            project_id, "tenant-project", "assets/icon.bin", b"new-binary",
            "Replace icon", "application/octet-stream", True,
        )
        self.assertEqual(first["revision"], 1)
        self.assertEqual(first["bytes"], len(payload))
        self.assertTrue(first["is_binary"])
        self.assertEqual(second["revision"], 2)
        with main.db() as conn:
            rows = conn.execute(
                "SELECT content,sha256,size_bytes FROM project_file_versions "
                "WHERE project_id=? AND path=? ORDER BY revision",
                (project_id, "assets/icon.bin"),
            ).fetchall()
        self.assertEqual(bytes(rows[0]["content"]), payload)
        self.assertEqual(hashlib.sha256(payload).hexdigest(), rows[0]["sha256"])
        self.assertEqual(rows[0]["size_bytes"], len(payload))

    def test_binary_project_file_restore_preserves_binary_metadata(self):
        project_id = self._create_test_project()
        main._write_project_file_bytes(
            project_id, "tenant-project", "assets/data.bin", bytes([0, 255]),
            "Original binary", "image/png", True,
        )
        main._write_project_file_bytes(
            project_id, "tenant-project", "assets/data.bin", b"replacement",
            "Replacement", "application/octet-stream", True,
        )
        restored = main.restore_project_file(
            project_id, "assets/data.bin",
            main.ProjectFileRestoreInput(revision=1),
            owner_id="tenant-project",
        )
        self.assertEqual(restored["file"]["revision"], 3)
        self.assertTrue(restored["file"]["is_binary"])
        self.assertEqual(restored["file"]["content_type"], "image/png")
        with main.db() as conn:
            row = conn.execute(
                "SELECT content,sha256 FROM project_file_versions WHERE project_id=? "
                "AND path=? AND revision=3",
                (project_id, "assets/data.bin"),
            ).fetchone()
        self.assertEqual(bytes(row["content"]), bytes([0, 255]))
        self.assertEqual(hashlib.sha256(bytes([0, 255])).hexdigest(), row["sha256"])

    def test_binary_project_file_write_is_owner_scoped_and_size_limited(self):
        project_id = self._create_test_project()
        with self.assertRaises(main.HTTPException) as raised:
            main._write_project_file_bytes(
                project_id, "another-owner", "private.bin", b"secret",
                "Upload", "application/octet-stream", True,
            )
        self.assertEqual(raised.exception.status_code, 404)
        with patch.object(main, "MAX_PROJECT_FILE_BYTES", 4):
            with self.assertRaises(main.HTTPException) as raised:
                main._write_project_file_bytes(
                    project_id, "tenant-project", "large.bin", b"12345",
                    "Upload", "application/octet-stream", True,
                )
        self.assertEqual(raised.exception.status_code, 413)

    def test_project_files_are_persisted_and_versioned(self):
        project_id = self._create_test_project()
        first = main.write_project_file(
            project_id,
            main.ProjectFileWriteInput(path="src/main.py", content="print('v1')", message="Initial"),
            owner_id="tenant-project",
        )
        second = main.write_project_file(
            project_id,
            main.ProjectFileWriteInput(path="src/main.py", content="print('v2')", message="Update"),
            owner_id="tenant-project",
        )
        self.assertEqual(first["file"]["revision"], 1)
        self.assertEqual(second["file"]["revision"], 2)
        current = main.read_project_file(project_id, "src/main.py", owner_id="tenant-project")
        self.assertEqual(current["content"], "print('v2')")
        listing = main.list_project_files(project_id, owner_id="tenant-project")
        self.assertEqual(len(listing["files"]), 1)
        self.assertEqual(listing["files"][0]["revision"], 2)
        history = main.list_project_file_versions(
            project_id, "src/main.py", owner_id="tenant-project"
        )
        self.assertEqual([item["revision"] for item in history["versions"]], [2, 1])

    def test_project_file_restore_creates_new_revision_without_erasing_history(self):
        project_id = self._create_test_project()
        main.write_project_file(
            project_id, main.ProjectFileWriteInput(path="README.md", content="old"),
            owner_id="tenant-project",
        )
        main.write_project_file(
            project_id, main.ProjectFileWriteInput(path="README.md", content="new"),
            owner_id="tenant-project",
        )
        restored = main.restore_project_file(
            project_id, "README.md",
            main.ProjectFileRestoreInput(revision=1, message="Rollback"),
            owner_id="tenant-project",
        )
        self.assertEqual(restored["file"]["revision"], 3)
        self.assertEqual(restored["restored_from_revision"], 1)
        current = main.read_project_file(project_id, "README.md", owner_id="tenant-project")
        self.assertEqual(current["content"], "old")
        history = main.list_project_file_versions(
            project_id, "README.md", owner_id="tenant-project"
        )
        self.assertEqual([item["revision"] for item in history["versions"]], [3, 2, 1])

    def test_project_file_access_is_owner_scoped(self):
        project_id = self._create_test_project()
        with self.assertRaises(main.HTTPException) as raised:
            main.write_project_file(
                project_id, main.ProjectFileWriteInput(path="secret.txt", content="private"),
                owner_id="other-owner",
            )
        self.assertEqual(raised.exception.status_code, 404)

    def test_project_file_paths_reject_traversal_and_reserved_names(self):
        for path in ("../outside.txt", "/absolute.txt", "a/../../outside", "a\\b", ".nexus/history"):
            with self.subTest(path=path):
                with self.assertRaises(main.HTTPException):
                    main._validate_project_file_path(path)

    def test_project_file_read_rejects_corrupted_content_hash(self):
        project_id = self._create_test_project()
        main.write_project_file(
            project_id, main.ProjectFileWriteInput(path="integrity.txt", content="trusted"),
            owner_id="tenant-project",
        )
        with main.db() as conn:
            conn.execute(
                "UPDATE project_file_versions SET content=? WHERE project_id=? AND path=?",
                (sqlite3.Binary(b"tampered"), project_id, "integrity.txt"),
            )
        with self.assertRaises(main.HTTPException) as raised:
            main.read_project_file(project_id, "integrity.txt", owner_id="tenant-project")
        self.assertEqual(raised.exception.status_code, 500)

    def test_project_file_restore_rejects_corrupted_source_version(self):
        project_id = self._create_test_project()
        main.write_project_file(
            project_id, main.ProjectFileWriteInput(path="integrity.txt", content="trusted"),
            owner_id="tenant-project",
        )
        with main.db() as conn:
            conn.execute(
                "UPDATE project_file_versions SET content=? WHERE project_id=? AND path=?",
                (sqlite3.Binary(b"tampered"), project_id, "integrity.txt"),
            )
        with self.assertRaises(main.HTTPException) as raised:
            main.restore_project_file(
                project_id, "integrity.txt", main.ProjectFileRestoreInput(revision=1),
                owner_id="tenant-project",
            )
        self.assertEqual(raised.exception.status_code, 500)

    def test_project_file_revision_size_is_checked_before_database_write(self):
        project_id = self._create_test_project()
        with self.assertRaises(main.HTTPException) as raised:
            main.write_project_file(
                project_id,
                main.ProjectFileWriteInput(path="large.txt", content="x" * (2 * 1024 * 1024 + 1)),
                owner_id="tenant-project",
            )
        self.assertEqual(raised.exception.status_code, 413)
        with main.db() as conn:
            count = conn.execute(
                "SELECT COUNT(*) AS n FROM project_file_versions WHERE project_id=?",
                (project_id,),
            ).fetchone()["n"]
        self.assertEqual(count, 0)

    def test_project_files_are_materialized_into_attached_task_workspace(self):
        project_id = self._create_test_project()
        main.write_project_file(
            project_id,
            main.ProjectFileWriteInput(path="src/app.py", content="print('project source')"),
            owner_id="tenant-project",
        )
        task = main._create_task_record(
            main.NewTask(prompt="Improve the project", project_id=project_id),
            "tenant-project",
        )
        workspace = Path(self.temp.name) / "workspace"
        with patch.object(main, "WORKSPACE", workspace):
            first = main._materialize_project_files(task["id"], "tenant-project", project_id)
            second = main._materialize_project_files(task["id"], "tenant-project", project_id)
            content = (workspace / task["id"] / "src" / "app.py").read_text(encoding="utf-8")
        self.assertEqual(first["files"], 1)
        self.assertTrue(second["already_materialized"])
        self.assertEqual(content, "print('project source')")

    def test_task_can_save_workspace_file_as_a_new_project_revision(self):
        project_id = self._create_test_project()
        task = main._create_task_record(
            main.NewTask(prompt="Edit the project", project_id=project_id),
            "tenant-project",
        )
        owner = "worker:project-save-test"
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "UPDATE tasks SET status='running',lease_owner=?,lease_expires_at=? WHERE id=?",
                (owner, now + 300, task["id"]),
            )
        workspace = Path(self.temp.name) / "workspace"
        task_root = workspace / task["id"]
        task_root.mkdir(parents=True)
        (task_root / "README.md").write_text("saved from task", encoding="utf-8")
        with patch.object(main, "WORKSPACE", workspace):
            token = main._LEASE_OWNER.set(owner)
            try:
                saved = main._save_project_workspace_file(
                    task["id"], "README.md", "Task result"
                )
            finally:
                main._LEASE_OWNER.reset(token)
        current = main.read_project_file(project_id, "README.md", owner_id="tenant-project")
        self.assertEqual(saved["revision"], 1)
        self.assertEqual(current["content"], "saved from task")
        self.assertEqual(current["message"], "Task result")

    def test_stale_task_lease_cannot_save_project_file(self):
        project_id = self._create_test_project()
        task = main._create_task_record(
            main.NewTask(prompt="Stale writer", project_id=project_id),
            "tenant-project",
        )
        with main.db() as conn:
            conn.execute(
                "UPDATE tasks SET status='running',lease_owner=?,lease_expires_at=? WHERE id=?",
                ("new-owner", time.time() + 300, task["id"]),
            )
        workspace = Path(self.temp.name) / "workspace"
        task_root = workspace / task["id"]
        task_root.mkdir(parents=True)
        (task_root / "README.md").write_text("stale write", encoding="utf-8")
        with patch.object(main, "WORKSPACE", workspace):
            token = main._LEASE_OWNER.set("stale-owner")
            try:
                saved = main._save_project_workspace_file(task["id"], "README.md")
            finally:
                main._LEASE_OWNER.reset(token)
        self.assertIsNone(saved)
        self.assertEqual(
            main.list_project_files(project_id, owner_id="tenant-project")["files"], []
        )


    def test_task_checkpoint_restores_workspace_and_persisted_plan_state(self):
        now = time.time()
        owner = main.WORKER_ID + ":checkpoint-test"
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id,plan_json,results_json,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                ("checkpoint-task", "checkpoint test", "running", now, now, "owner-a",
                 '[{"title":"first"}]', '[]', owner, now + 600),
            )
            conn.execute(
                "INSERT INTO task_steps(id,task_id,step_index,title,action,input_json,status,attempt_count,"
                "max_attempts,idempotency_key,result_json,error,next_attempt_at,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("step-1", "checkpoint-task", 0, "Write file", "workspace_write", '{}',
                 "completed", 1, 3, "checkpoint-key", '{"status":"completed"}', "", None, now, now),
            )
        workspace = Path(self.temp.name) / "workspace"
        checkpoint_root = Path(self.temp.name) / "checkpoints"
        task_root = workspace / "checkpoint-task"
        task_root.mkdir(parents=True)
        (task_root / "src").mkdir()
        (task_root / "src" / "main.py").write_text("stable version", encoding="utf-8")
        token = main._LEASE_OWNER.set(owner)
        try:
            with patch.object(main, "WORKSPACE", workspace), patch.object(main, "CHECKPOINT_ROOT", checkpoint_root):
                saved = main._create_task_checkpoint("checkpoint-task", "step_completed", "step-1")
                self.assertEqual(saved["sequence"], 1)
                (task_root / "src" / "main.py").write_text("uncommitted mutation", encoding="utf-8")
                (task_root / "untracked.tmp").write_text("must disappear", encoding="utf-8")
                with main.db() as conn:
                    conn.execute("UPDATE tasks SET plan_json=? WHERE id='checkpoint-task'",
                                 ('[{"title":"corrupt future plan"}]',))
                    conn.execute("UPDATE task_steps SET status='running',result_json='' WHERE id='step-1'")
                restored = main._restore_latest_task_checkpoint("checkpoint-task")
                self.assertEqual(restored["sequence"], 1)
                self.assertEqual((task_root / "src" / "main.py").read_text(encoding="utf-8"), "stable version")
                self.assertFalse((task_root / "untracked.tmp").exists())
                with main.db() as conn:
                    task = conn.execute("SELECT plan_json FROM tasks WHERE id='checkpoint-task'").fetchone()
                    step = conn.execute("SELECT status,result_json FROM task_steps WHERE id='step-1'").fetchone()
                self.assertEqual(task["plan_json"], '[{"title":"first"}]')
                self.assertEqual(step["status"], "completed")
                self.assertEqual(json.loads(step["result_json"])["status"], "completed")
        finally:
            main._LEASE_OWNER.reset(token)

    def test_interrupted_task_recovery_restores_latest_checkpoint_before_requeue(self):
        now = time.time()
        owner = main.WORKER_ID + ":checkpoint-recovery"
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id,plan_json,results_json,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                ("checkpoint-recovery", "recover me", "running", now, now, "owner-a",
                 '[{"title":"durable plan"}]', '[]', owner, now + 600),
            )
            conn.execute(
                "INSERT INTO task_steps(id,task_id,step_index,title,action,input_json,status,attempt_count,"
                "max_attempts,idempotency_key,result_json,error,next_attempt_at,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("recovery-step", "checkpoint-recovery", 0, "Build", "code_execute", '{}',
                 "pending", 0, 3, "recovery-key", "", "", None, now, now),
            )
        workspace = Path(self.temp.name) / "workspace"
        checkpoint_root = Path(self.temp.name) / "checkpoints"
        task_root = workspace / "checkpoint-recovery"
        task_root.mkdir(parents=True)
        (task_root / "README.md").write_text("checkpoint content", encoding="utf-8")
        token = main._LEASE_OWNER.set(owner)
        try:
            with patch.object(main, "WORKSPACE", workspace), patch.object(main, "CHECKPOINT_ROOT", checkpoint_root):
                main._create_task_checkpoint("checkpoint-recovery", "initial_plan_persisted")
                (task_root / "README.md").write_text("partial work from crashed step", encoding="utf-8")
                with main.db() as conn:
                    conn.execute("UPDATE tasks SET lease_expires_at=? WHERE id=?",
                                 (time.time() - 1, "checkpoint-recovery"))
                    conn.execute("UPDATE task_steps SET status='running' WHERE id='recovery-step'")
                main.recover_interrupted_tasks()
                self.assertEqual((task_root / "README.md").read_text(encoding="utf-8"), "checkpoint content")
                with main.db() as conn:
                    task = conn.execute("SELECT status FROM tasks WHERE id='checkpoint-recovery'").fetchone()
                    step = conn.execute("SELECT status FROM task_steps WHERE id='recovery-step'").fetchone()
                self.assertEqual(task["status"], "retry_wait")
                self.assertEqual(step["status"], "pending")
        finally:
            main._LEASE_OWNER.reset(token)

    def test_interrupted_task_with_corrupt_checkpoint_is_stopped_safely(self):
        now = time.time()
        owner = main.WORKER_ID + ":checkpoint-corrupt"
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?,?)",
                ("checkpoint-corrupt", "stop safely", "running", now, now, "owner-a", owner, now + 600),
            )
        workspace = Path(self.temp.name) / "workspace"
        checkpoint_root = Path(self.temp.name) / "checkpoints"
        task_root = workspace / "checkpoint-corrupt"
        task_root.mkdir(parents=True)
        (task_root / "file.txt").write_text("known good", encoding="utf-8")
        token = main._LEASE_OWNER.set(owner)
        try:
            with patch.object(main, "WORKSPACE", workspace), patch.object(main, "CHECKPOINT_ROOT", checkpoint_root):
                saved = main._create_task_checkpoint("checkpoint-corrupt")
                archive = Path(checkpoint_root / "checkpoint-corrupt" / "000000000001.zip")
                self.assertTrue(archive.exists())
                archive.write_bytes(b"tampered")
                with main.db() as conn:
                    conn.execute("UPDATE tasks SET lease_expires_at=? WHERE id=?",
                                 (time.time() - 1, "checkpoint-corrupt"))
                main.recover_interrupted_tasks()
                with main.db() as conn:
                    task = conn.execute("SELECT status,error FROM tasks WHERE id='checkpoint-corrupt'").fetchone()
                self.assertEqual(task["status"], "failed")
                self.assertIn("Checkpoint restore failed", task["error"])
        finally:
            main._LEASE_OWNER.reset(token)


    def test_failed_step_rolls_back_workspace_but_preserves_failure_result(self):
        now = time.time()
        owner = main.WORKER_ID + ":failed-step-checkpoint"
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id,plan_json,results_json,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                ("failed-step-checkpoint", "rollback failed step", "running", now, now, "owner-a",
                 '[{"title":"mutate"}]', '[]', owner, now + 600),
            )
            conn.execute(
                "INSERT INTO task_steps(id,task_id,step_index,title,action,input_json,status,attempt_count,"
                "max_attempts,idempotency_key,result_json,error,next_attempt_at,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("failed-mutation", "failed-step-checkpoint", 0, "Mutate", "code_execute", '{}',
                 "pending", 0, 3, "failed-mutation-key", "", "", None, now, now),
            )
        workspace = Path(self.temp.name) / "workspace"
        checkpoint_root = Path(self.temp.name) / "checkpoints"
        task_root = workspace / "failed-step-checkpoint"
        task_root.mkdir(parents=True)
        target = task_root / "source.txt"
        target.write_text("last confirmed version", encoding="utf-8")
        token = main._LEASE_OWNER.set(owner)
        try:
            with patch.object(main, "WORKSPACE", workspace), patch.object(main, "CHECKPOINT_ROOT", checkpoint_root):
                main._create_task_checkpoint("failed-step-checkpoint", "initial_plan_persisted")
                with main.db() as conn:
                    conn.execute("UPDATE task_steps SET status='running' WHERE id='failed-mutation'")
                target.write_text("partial failed mutation", encoding="utf-8")
                self.assertTrue(main._save_step_result(
                    "failed-step-checkpoint", "failed-mutation", "failed",
                    '{"status":"execution_failed","error":"compile failed"}',
                ))
                main._restore_latest_task_checkpoint("failed-step-checkpoint", False)
                main._create_task_checkpoint(
                    "failed-step-checkpoint", "step_failed", "failed-mutation"
                )
                self.assertEqual(target.read_text(encoding="utf-8"), "last confirmed version")
                with main.db() as conn:
                    step = conn.execute(
                        "SELECT status,result_json FROM task_steps WHERE id='failed-mutation'"
                    ).fetchone()
                self.assertEqual(step["status"], "failed")
                self.assertEqual(json.loads(step["result_json"])["status"], "execution_failed")
        finally:
            main._LEASE_OWNER.reset(token)


    def test_checkpoint_restores_project_file_revisions_and_published_artifacts(self):
        now = time.time()
        owner = main.WORKER_ID + ":project-artifact-checkpoint"
        project_id = "checkpoint-project"
        task_id = "project-artifact-task"
        with main.db() as conn:
            conn.execute(
                "INSERT INTO projects(id,owner_id,name,description,context,status,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (project_id, "owner-a", "Checkpoint project", "", "", "active", now, now),
            )
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id,project_id,plan_json,results_json,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (task_id, "restore complete project state", "running", now, now, "owner-a",
                 project_id, "[]", "[]", owner, now + 600),
            )
        main._write_project_file_bytes(
            project_id, "owner-a", "src/main.py", b"stable project source",
            "initial source", "text/plain; charset=utf-8", False,
        )
        workspace = Path(self.temp.name) / "workspace"
        checkpoint_root = Path(self.temp.name) / "checkpoints"
        task_root = workspace / task_id
        task_root.mkdir(parents=True)
        (task_root / "src").mkdir()
        (task_root / "src" / "main.py").write_bytes(b"stable project source")
        artifact = workspace / (task_id + "-release.apk")
        artifact.write_bytes(b"stable apk bytes")
        token = main._LEASE_OWNER.set(owner)
        try:
            with patch.object(main, "WORKSPACE", workspace), patch.object(main, "CHECKPOINT_ROOT", checkpoint_root):
                saved = main._create_task_checkpoint(task_id, "release_published")
                self.assertEqual(saved["project_files"], 1)
                self.assertEqual(saved["artifacts"], 1)

                main._write_project_file_bytes(
                    project_id, "owner-a", "src/main.py", b"failed source mutation",
                    "failed build source", "text/plain; charset=utf-8", False,
                )
                main._write_project_file_bytes(
                    project_id, "owner-a", "generated/temporary.txt", b"not in checkpoint",
                    "temporary file", "text/plain; charset=utf-8", False,
                )
                (task_root / "src" / "main.py").write_bytes(b"partial workspace mutation")
                artifact.write_bytes(b"corrupted apk")
                (workspace / (task_id + "-failed.apk")).write_bytes(b"failed artifact")

                restored = main._restore_latest_task_checkpoint(task_id)
                self.assertEqual(restored["project_revisions_restored"], 2)
                self.assertEqual(restored["artifacts"], 1)
                self.assertEqual((task_root / "src" / "main.py").read_bytes(), b"stable project source")
                self.assertEqual(artifact.read_bytes(), b"stable apk bytes")
                self.assertFalse((workspace / (task_id + "-failed.apk")).exists())

                project_files = main.list_project_files(project_id, "owner-a")["files"]
                self.assertEqual([item["path"] for item in project_files], ["src/main.py"])
                latest = main.read_project_file(project_id, "src/main.py", "owner-a")
                self.assertEqual(latest["content"], "stable project source")
                with main.db() as conn:
                    history = conn.execute(
                        "SELECT revision,is_deleted FROM project_file_versions "
                        "WHERE project_id=? AND path='src/main.py' ORDER BY revision",
                        (project_id,),
                    ).fetchall()
                self.assertEqual([row["revision"] for row in history], [1, 2, 3])
                self.assertTrue(all(not row["is_deleted"] for row in history))
                with main.db() as conn:
                    tombstone = conn.execute(
                        "SELECT is_deleted FROM project_file_versions "
                        "WHERE project_id=? AND path='generated/temporary.txt' ORDER BY revision DESC LIMIT 1",
                        (project_id,),
                    ).fetchone()
                self.assertEqual(tombstone["is_deleted"], 1)
                with self.assertRaises(main.HTTPException) as missing:
                    main.read_project_file(project_id, "generated/temporary.txt", "owner-a")
                self.assertEqual(missing.exception.status_code, 404)
        finally:
            main._LEASE_OWNER.reset(token)


    def test_legacy_checkpoint_does_not_delete_artifacts_without_a_snapshot_manifest(self):
        now = time.time()
        task_id = "legacy-checkpoint-task"
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id) "
                "VALUES(?,?,?,?,?,?)",
                (task_id, "legacy restore", "retry_wait", now, now, "owner-a"),
            )
        workspace = Path(self.temp.name) / "workspace"
        checkpoint_root = Path(self.temp.name) / "checkpoints"
        task_root = workspace / task_id
        task_root.mkdir(parents=True)
        (task_root / "source.txt").write_text("confirmed", encoding="utf-8")
        artifact = workspace / (task_id + "-release.apk")
        artifact.write_bytes(b"existing artifact")
        archive_dir = checkpoint_root / task_id
        archive_dir.mkdir(parents=True)
        archive = archive_dir / "000000000001.zip"
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
            bundle.writestr("source.txt", "confirmed")
        digest = main._sha256_file(archive)
        with main.db() as conn:
            conn.execute(
                "INSERT INTO task_checkpoints(id,task_id,owner_id,sequence,step_id,reason,"
                "plan_json,results_json,steps_json,archive_path,archive_sha256,archive_bytes,"
                "project_manifest_json,artifact_manifest_json,created_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("legacy-checkpoint-1", task_id, "owner-a", 1, None, "pre-manifest",
                 "[]", "[]", "[]", str(archive), digest, archive.stat().st_size,
                 "{}", "[]", now),
            )
        with patch.object(main, "WORKSPACE", workspace), patch.object(main, "CHECKPOINT_ROOT", checkpoint_root):
            (task_root / "source.txt").write_text("unconfirmed", encoding="utf-8")
            main._restore_latest_task_checkpoint(task_id)
            self.assertEqual((task_root / "source.txt").read_text(encoding="utf-8"), "confirmed")
            self.assertEqual(artifact.read_bytes(), b"existing artifact")


    def test_checkpoint_history_lists_verified_file_manifests_and_owner_scope(self):
        now = time.time()
        task_id = "checkpoint-history-api"
        worker = main.WORKER_ID + ":checkpoint-history"
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id,plan_json,results_json,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (task_id, "checkpoint history", "running", now, now, "owner-a",
                 '[{"title":"Build"}]', "[]", worker, now + 600),
            )
        workspace = Path(self.temp.name) / "workspace"
        checkpoint_root = Path(self.temp.name) / "checkpoints"
        token = main._LEASE_OWNER.set(worker)
        try:
            with patch.object(main, "WORKSPACE", workspace), patch.object(main, "CHECKPOINT_ROOT", checkpoint_root):
                main._workspace_write(task_id, "src/main.py", "print('checkpoint')")
                saved = main._create_task_checkpoint(task_id, "step_completed", "step-1")
                self.assertEqual(saved["sequence"], 1)
                result = main.list_task_checkpoints(task_id, owner_id="owner-a")
                self.assertEqual(result["latest_sequence"], 1)
                item = result["checkpoints"][0]
                self.assertTrue(item["available"])
                self.assertEqual(item["workspace_files"][0]["path"], "src/main.py")
                self.assertEqual(item["workspace_files"][0]["sha256"],
                                 main.hashlib.sha256(b"print('checkpoint')").hexdigest())
                self.assertEqual(item["plan_steps"], 1)
                self.assertEqual(item["reason"], "step_completed")
                incremental = main.list_task_checkpoints(task_id, after_sequence=1, owner_id="owner-a")
                self.assertEqual(incremental["latest_sequence"], 1)
                self.assertEqual(incremental["checkpoints"], [])
                with self.assertRaises(main.HTTPException) as invalid_cursor:
                    main.list_task_checkpoints(task_id, after_sequence=-1, owner_id="owner-a")
                self.assertEqual(invalid_cursor.exception.status_code, 422)
                with self.assertRaises(main.HTTPException) as denied:
                    main.list_task_checkpoints(task_id, owner_id="owner-b")
                self.assertEqual(denied.exception.status_code, 404)
        finally:
            main._LEASE_OWNER.reset(token)

    def test_manual_checkpoint_restore_is_owner_scoped_guarded_and_pauses_task(self):
        now = time.time()
        task_id = "manual-checkpoint-restore"
        worker = main.WORKER_ID + ":manual-restore"
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id,plan_json,results_json,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (task_id, "manual restore", "running", now, now, "owner-a",
                 '[{"title":"Original"}]', "[]", worker, now + 600),
            )
        workspace = Path(self.temp.name) / "workspace"
        checkpoint_root = Path(self.temp.name) / "checkpoints"
        token = main._LEASE_OWNER.set(worker)
        try:
            with patch.object(main, "WORKSPACE", workspace), patch.object(main, "CHECKPOINT_ROOT", checkpoint_root):
                main._workspace_write(task_id, "src/main.py", "stable version")
                saved = main._create_task_checkpoint(task_id, "before_change")
                with main.db() as conn:
                    conn.execute("UPDATE tasks SET status='completed',lease_owner='',lease_expires_at=NULL WHERE id=?",
                                 (task_id,))
                main._workspace_write(task_id, "src/main.py", "later mutation")
                with self.assertRaises(main.HTTPException) as denied:
                    main.restore_task_checkpoint(task_id, saved["id"],
                        {"expected_latest_sequence": 1}, owner_id="owner-b")
                self.assertEqual(denied.exception.status_code, 404)
                with self.assertRaises(main.HTTPException) as stale:
                    main.restore_task_checkpoint(task_id, saved["id"],
                        {"expected_latest_sequence": 2}, owner_id="owner-a")
                self.assertEqual(stale.exception.status_code, 409)
                restored = main.restore_task_checkpoint(task_id, saved["id"],
                    {"expected_latest_sequence": 1}, owner_id="owner-a")
                self.assertEqual(restored["status"], "paused")
                self.assertEqual(main._workspace_read(task_id, "src/main.py")["content"], "stable version")
                with main.db() as conn:
                    task = conn.execute("SELECT status,plan_json FROM tasks WHERE id=?", (task_id,)).fetchone()
                    event = conn.execute("SELECT kind FROM events WHERE task_id=? ORDER BY id DESC LIMIT 1",
                                         (task_id,)).fetchone()
                self.assertEqual(task["status"], "paused")
                self.assertEqual(json.loads(task["plan_json"])[0]["title"], "Original")
                self.assertEqual(event["kind"], "checkpoint_manually_restored")
        finally:
            main._LEASE_OWNER.reset(token)


    def test_interrupted_manual_checkpoint_restore_is_replayed_on_worker_recovery(self):
        now = time.time()
        task_id = "crashed-manual-restore"
        worker = main.WORKER_ID + ":manual-restore-crash"
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id,plan_json,results_json,lease_owner,lease_expires_at,restore_checkpoint_id,restore_previous_status) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (task_id, "recover manual restore", "running", now, now, "owner-a",
                 '[{"title":"Before"}]', "[]", worker, now + 600, "", ""),
            )
        workspace = Path(self.temp.name) / "workspace"
        checkpoint_root = Path(self.temp.name) / "checkpoints"
        token = main._LEASE_OWNER.set(worker)
        try:
            with patch.object(main, "WORKSPACE", workspace), patch.object(main, "CHECKPOINT_ROOT", checkpoint_root):
                main._workspace_write(task_id, "state.txt", "checkpoint state")
                saved = main._create_task_checkpoint(task_id, "manual_restore_target")
                with main.db() as conn:
                    conn.execute("""UPDATE tasks SET status='restoring',restore_checkpoint_id=?,
                        restore_previous_status='completed',lease_owner='checkpoint-restore:crashed',
                        lease_expires_at=? WHERE id=?""",
                        (saved["id"], now - 1, task_id))
                main._workspace_write(task_id, "state.txt", "partial restore or later mutation")
                recovered = main._recover_interrupted_checkpoint_restores()
                self.assertEqual(recovered, 1)
                self.assertEqual(main._workspace_read(task_id, "state.txt")["content"], "checkpoint state")
                with main.db() as conn:
                    row = conn.execute("SELECT status,restore_checkpoint_id,lease_owner FROM tasks WHERE id=?",
                                       (task_id,)).fetchone()
                self.assertEqual(row["status"], "paused")
                self.assertEqual(row["restore_checkpoint_id"], "")
                self.assertEqual(row["lease_owner"], "")
        finally:
            main._LEASE_OWNER.reset(token)


    def test_manual_restore_of_corrupt_checkpoint_fails_task_closed(self):
        now = time.time()
        task_id = "corrupt-manual-checkpoint"
        worker = main.WORKER_ID + ":corrupt-manual"
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (task_id, "corrupt manual restore", "running", now, now, "owner-a", worker, now + 600),
            )
        workspace = Path(self.temp.name) / "workspace"
        checkpoint_root = Path(self.temp.name) / "checkpoints"
        token = main._LEASE_OWNER.set(worker)
        try:
            with patch.object(main, "WORKSPACE", workspace), patch.object(main, "CHECKPOINT_ROOT", checkpoint_root):
                main._workspace_write(task_id, "file.txt", "checkpoint")
                saved = main._create_task_checkpoint(task_id, "before_corruption")
                with main.db() as conn:
                    conn.execute("UPDATE tasks SET status='completed',lease_owner='',lease_expires_at=NULL WHERE id=?",
                                 (task_id,))
                with main.db() as conn:
                    archive_path = conn.execute(
                        "SELECT archive_path FROM task_checkpoints WHERE id=?", (saved["id"],)
                    ).fetchone()["archive_path"]
                Path(archive_path).write_bytes(b"corrupted")
                with self.assertRaises(main.HTTPException) as failure:
                    main.restore_task_checkpoint(task_id, saved["id"],
                        {"expected_latest_sequence": 1}, owner_id="owner-a")
                self.assertEqual(failure.exception.status_code, 422)
                with main.db() as conn:
                    task = conn.execute("SELECT status,restore_checkpoint_id,lease_owner FROM tasks WHERE id=?",
                                        (task_id,)).fetchone()
                self.assertEqual(task["status"], "failed")
                self.assertEqual(task["restore_checkpoint_id"], "")
                self.assertEqual(task["lease_owner"], "")
        finally:
            main._LEASE_OWNER.reset(token)

    def test_manual_checkpoint_restore_rejects_active_task(self):
        now = time.time()
        task_id = "active-checkpoint-restore"
        worker = main.WORKER_ID + ":active-restore"
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at,owner_id,lease_owner,lease_expires_at) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (task_id, "active restore", "running", now, now, "owner-a", worker, now + 600),
            )
        with self.assertRaises(main.HTTPException) as conflict:
            main.restore_task_checkpoint(task_id, "missing",
                {"expected_latest_sequence": 1}, owner_id="owner-a")
        self.assertEqual(conflict.exception.status_code, 409)

    def test_db_context_commits_and_closes_connection(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at) VALUES(?,?,?,?,?)",
                ("context-commit", "commit test", "queued", now, now),
            )
        with self.assertRaises(sqlite3.ProgrammingError):
            conn.execute("SELECT 1")
        with main.db() as check:
            row = check.execute("SELECT prompt FROM tasks WHERE id=?",
                                ("context-commit",)).fetchone()
        self.assertEqual(row["prompt"], "commit test")

    def test_db_context_rolls_back_and_closes_connection(self):
        now = time.time()
        with self.assertRaisesRegex(RuntimeError, "rollback test"):
            with main.db() as conn:
                conn.execute(
                    "INSERT INTO tasks(id,prompt,status,created_at,updated_at) VALUES(?,?,?,?,?)",
                    ("context-rollback", "rollback test", "queued", now, now),
                )
                raise RuntimeError("rollback test")
        with self.assertRaises(sqlite3.ProgrammingError):
            conn.execute("SELECT 1")
        with main.db() as check:
            row = check.execute("SELECT id FROM tasks WHERE id=?",
                                ("context-rollback",)).fetchone()
        self.assertIsNone(row)



class WorkerLeaseContextTests(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_claims_keep_independent_lease_owners(self):
        observed = []

        async def capture_owner(task_id):
            await asyncio.sleep(0)
            observed.append((task_id, main._current_lease_owner()))

        with patch.object(main, "process", new=AsyncMock(side_effect=capture_owner)):
            await asyncio.gather(
                main._run_claimed_task("task-a", "worker-a:lease-a"),
                main._run_claimed_task("task-b", "worker-b:lease-b"),
            )

        self.assertCountEqual(
            observed,
            [("task-a", "worker-a:lease-a"), ("task-b", "worker-b:lease-b")],
        )
        self.assertEqual(main._current_lease_owner(), main.WORKER_ID)

if __name__ == "__main__":
    unittest.main()
