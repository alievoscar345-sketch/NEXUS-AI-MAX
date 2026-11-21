import asyncio
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
        self.assertEqual(row["lease_owner"], main.WORKER_ID)
        self.assertGreater(row["lease_expires_at"], now)

    def test_persists_steps_and_stable_idempotency_key(self):
        now = time.time()
        with main.db() as conn:
            conn.execute(
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at) VALUES(?,?,?,?,?)",
                ("task-b", "write file", "planning", now, now),
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
                ("task-c", "continue work", "running", now, now, "dead-worker", now - 1),
            )
        main._persist_plan("task-c", [{"title": "Read", "action": "fetch_url",
            "input": {"url": "https://example.com"}}], 3)
        with main.db() as conn:
            conn.execute("UPDATE task_steps SET status='running' WHERE task_id=?", ("task-c",))
            conn.execute("UPDATE tasks SET status='running',lease_owner='dead-worker' WHERE id=?", ("task-c",))
        main.recover_interrupted_tasks()
        with main.db() as conn:
            task = conn.execute("SELECT status,lease_owner FROM tasks WHERE id=?", ("task-c",)).fetchone()
            step = conn.execute("SELECT status,error FROM task_steps WHERE task_id=?", ("task-c",)).fetchone()
        self.assertEqual(task["status"], "retry_wait")
        self.assertEqual(task["lease_owner"], "")
        self.assertEqual(step["status"], "retry_wait")
        self.assertIn("Recovered", step["error"])

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
        self.assertEqual(task["status"], "cancelled")
        self.assertEqual(task["cancel_requested"], 1)
        self.assertEqual(event_count, 1)

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
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at) VALUES(?,?,?,?,?)",
                ("task-approval", "send a message", "running", now, now),
            )
        step = {"title": "Prepare message", "action": "draft_external_action",
                "input": {"description": "Send approved message"}}
        first = asyncio.run(main.run_step(step, "task-approval", "same-idempotency-key"))
        second = asyncio.run(main.run_step(step, "task-approval", "same-idempotency-key"))
        self.assertEqual(first["approval_id"], second["approval_id"])
        with main.db() as conn:
            count = conn.execute("SELECT COUNT(*) AS n FROM approvals WHERE task_id=?",
                                 ("task-approval",)).fetchone()["n"]
        self.assertEqual(count, 1)


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

        loop_thread = threading.get_ident()
        observed = []
        helper_names = [
            "_get_task_row", "_set_task_planning", "_persist_plan",
            "_task_cancelled", "_next_pending_step", "_step_summary",
            "_complete_task", "event",
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

        asyncio.run(run())
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
                "INSERT INTO tasks(id,prompt,status,created_at,updated_at) VALUES(?,?,?,?,?)",
                ("task-approval-thread", "draft a message", "running", now, now),
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
