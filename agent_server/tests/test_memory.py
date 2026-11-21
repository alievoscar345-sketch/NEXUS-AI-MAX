import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from app import main


class MemoryRetrievalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp.name) / "memory.sqlite3"
        self.db_patch = patch.object(main, "DB_PATH", self.db_path)
        self.db_patch.start()
        main.init_db()

    def tearDown(self):
        self.db_patch.stop()
        self.temp.cleanup()

    def add_memory(self, memory_id, owner_id, title, content, category="general", pinned=False, updated_at=None):
        now = time.time() if updated_at is None else updated_at
        with main.db() as conn:
            conn.execute(
                """INSERT INTO memories(
                    id,owner_id,title,content,category,pinned,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?)""",
                (memory_id, owner_id, title, content, category, int(pinned), now, now),
            )

    def test_retrieval_is_scoped_to_owner(self):
        self.add_memory("own", "owner-a", "Training goal", "Build muscle with progressive overload", "goal")
        self.add_memory("other", "owner-b", "Training goal", "Build muscle with progressive overload", "goal")

        result = main._retrieve_relevant_memories(
            "owner-a", [{"role": "user", "content": "How should I build muscle?"}]
        )

        self.assertEqual([item["id"] for item in result], ["own"])

    def test_retrieval_prioritizes_title_matches_and_pinned_memories(self):
        self.add_memory("content", "owner-a", "General note", "I am interested in progressive overload training")
        self.add_memory("title", "owner-a", "Muscle training plan", "A short personal note")
        self.add_memory("pinned", "owner-a", "Muscle training preference", "Prefer evidence-based training", pinned=True)

        result = main._retrieve_relevant_memories(
            "owner-a", [{"role": "user", "content": "Help me with a muscle training plan"}]
        )

        ids = [item["id"] for item in result]
        self.assertEqual(ids[0], "pinned")
        self.assertIn("title", ids)
        self.assertIn("content", ids)

    def test_retrieval_uses_only_recent_user_messages_and_ignores_other_roles(self):
        messages = [
            {"role": "system", "content": "system-only-secret"},
            {"role": "user", "content": "First user request"},
            {"role": "assistant", "content": "assistant-only-secret"},
            {"role": "user", "content": "Second user request"},
            {"role": "user", "content": "Third user request"},
            {"role": "user", "content": "Fourth user request"},
        ]

        query = main._memory_user_text(messages)

        self.assertIn("Second user request", query)
        self.assertIn("Third user request", query)
        self.assertIn("Fourth user request", query)
        self.assertNotIn("First user request", query)
        self.assertNotIn("system-only-secret", query)
        self.assertNotIn("assistant-only-secret", query)

    def test_retrieval_updates_last_accessed_only_for_selected_memories(self):
        self.add_memory("match", "owner-a", "Sleep schedule", "Consistent sleep timing matters")
        self.add_memory("unrelated", "owner-a", "Cooking preference", "Likes roasted vegetables")

        result = main._retrieve_relevant_memories(
            "owner-a", [{"role": "user", "content": "How can I improve my sleep schedule?"}]
        )

        self.assertEqual([item["id"] for item in result], ["match"])
        with main.db() as conn:
            rows = {
                row["id"]: row["last_accessed_at"]
                for row in conn.execute("SELECT id,last_accessed_at FROM memories").fetchall()
            }
        self.assertIsNotNone(rows["match"])
        self.assertIsNone(rows["unrelated"])

    def test_empty_user_query_returns_no_memories(self):
        self.add_memory("pinned", "owner-a", "Important goal", "Complete the project", pinned=True)

        result = main._retrieve_relevant_memories(
            "owner-a", [{"role": "assistant", "content": "No user request here"}]
        )

        self.assertEqual(result, [])
