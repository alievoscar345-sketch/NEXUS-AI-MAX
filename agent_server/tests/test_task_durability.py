import asyncio
import tempfile
import time
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
                ("task-c", "continue work", "running", now, now, "dead-worker", now + 999),
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
        first = asyncio.run(main.create_task(body, owner_id="personal"))
        second = asyncio.run(main.create_task(body, owner_id="personal"))
        self.assertEqual(first["id"], second["id"])
        self.assertTrue(second["deduplicated"])
        with main.db() as conn:
            count = conn.execute("SELECT COUNT(*) AS n FROM tasks").fetchone()["n"]
        self.assertEqual(count, 1)

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
        key = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="
        with patch.dict(os.environ, {"NEXUS_CREDENTIAL_ENCRYPTION_KEY": key}):
            result = asyncio.run(main.save_provider(body, owner_id="tenant-a"))
            self.assertEqual(result["status"], "saved")
            with main.db() as conn:
                row = conn.execute("SELECT encrypted_api_key FROM provider_credentials WHERE owner_id=?",
                                    ("tenant-a",)).fetchone()
            self.assertNotIn(b"provider-secret-key-value", bytes(row["encrypted_api_key"]))
            models = main.provider_models_for_owner("tenant-a")
            self.assertTrue(any(m.id == "user-anymodel" and m.api_key == body.api_key and "vision" in m.capabilities for m in models))
            self.assertEqual(main.provider_models_for_owner("tenant-b"), [])

    def test_task_lists_are_owner_scoped(self):
        first = main.NewTask(prompt="Task for tenant A")
        second = main.NewTask(prompt="Task for tenant B")
        asyncio.run(main.create_task(first, owner_id="tenant-a"))
        asyncio.run(main.create_task(second, owner_id="tenant-b"))
        tenant_a = asyncio.run(main.list_tasks(owner_id="tenant-a"))
        tenant_b = asyncio.run(main.list_tasks(owner_id="tenant-b"))
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


if __name__ == "__main__":
    unittest.main()
