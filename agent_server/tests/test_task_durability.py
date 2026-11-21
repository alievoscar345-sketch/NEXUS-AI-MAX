import asyncio
import json
import ipaddress
import socket
import tempfile
import time
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
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
            main._workspace_write("task-1", "src/readme.md", "first line\\nNEXUS agent\\nlast line")
            result = main._workspace_search("task-1", "nexus")
        self.assertEqual(len(result["matches"]), 1)
        self.assertEqual(result["matches"][0]["line"], 2)
        self.assertEqual(result["matches"][0]["path"], "src/readme.md")

    def test_workspace_rejects_symlink_escape(self):
        workspace = Path(self.temp.name) / "workspace"
        outside = Path(self.temp.name) / "outside"
        outside.mkdir()
        (workspace / "task-1").mkdir(parents=True)
        (workspace / "task-1" / "escape").symlink_to(outside, target_is_directory=True)
        with patch.object(main, "WORKSPACE", workspace):
            with self.assertRaises(ValueError):
                main._workspace_write("task-1", "escape/secret.txt", "blocked")

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
        self.assertTrue({name for name, _ in observed}.issuperset(set(helper_names)))
        self.assertTrue(all(thread_id != loop_thread for _, thread_id in observed))
        with main.db() as conn:
            task = conn.execute("SELECT status,summary FROM tasks WHERE id=?",
                                ("task-thread-process",)).fetchone()
        self.assertEqual(task["status"], "completed")
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


if __name__ == "__main__":
    unittest.main()
