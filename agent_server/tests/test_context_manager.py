import os
import unittest
from unittest.mock import patch

from app.context_manager import (
    compact_messages,
    configured_context_window,
    estimate_message_tokens,
    estimate_messages_tokens,
)


class ContextManagerTests(unittest.TestCase):
    def test_estimator_counts_text_and_multimodal_images(self):
        text_only = estimate_message_tokens({"role": "user", "content": "hello"})
        with_image = estimate_message_tokens({
            "role": "user",
            "content": [
                {"type": "text", "text": "hello"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}},
            ],
        })
        self.assertGreater(with_image, text_only + 900)

    def test_under_budget_messages_are_unchanged(self):
        messages = [
            {"role": "system", "content": "You are NEXUS."},
            {"role": "user", "content": "Question"},
        ]
        compacted, stats = compact_messages(messages, 8192, 1024)
        self.assertEqual(compacted, messages)
        self.assertFalse(stats["compacted"])
        self.assertEqual(stats["dropped_messages"], 0)

    def test_compaction_preserves_system_and_latest_turn(self):
        messages = [{"role": "system", "content": "Never ignore system instructions."}]
        messages.extend(
            {"role": "user" if i % 2 == 0 else "assistant",
             "content": f"historical message {i}: " + ("important details " * 100)}
            for i in range(45)
        )
        messages.append({"role": "user", "content": "LATEST QUESTION: keep this verbatim."})
        compacted, stats = compact_messages(messages, 8192, 1024)
        self.assertTrue(stats["compacted"])
        self.assertGreater(stats["dropped_messages"], 0)
        self.assertEqual(compacted[0], messages[0])
        self.assertEqual(compacted[-1], messages[-1])
        self.assertTrue(any("[NEXUS_CONTEXT_COMPACTION]" in str(m.get("content", "")) for m in compacted))
        self.assertLessEqual(estimate_messages_tokens(compacted), stats["input_budget_tokens"])

    def test_compaction_keeps_only_contiguous_recent_suffix(self):
        messages = [{"role": "system", "content": "System."}]
        messages.extend(
            {"role": "user" if i % 2 == 0 else "assistant", "content": f"old-{i}"}
            for i in range(12)
        )
        messages.append({"role": "assistant", "content": "oversized recent turn " * 5000})
        messages.append({"role": "user", "content": "latest"})
        compacted, stats = compact_messages(messages, 4096, 1024)
        self.assertTrue(stats["compacted"])
        conversational = [m for m in compacted if m.get("role") in {"user", "assistant"}]
        self.assertEqual(conversational, [messages[-1]])
        digest = next(m["content"] for m in compacted if "[NEXUS_CONTEXT_COMPACTION]" in str(m.get("content", "")))
        self.assertIn("old-0", digest)
        self.assertIn("oversized recent turn", digest)

    def test_compaction_digest_includes_every_turn_before_recent_suffix(self):
        messages = [{"role": "system", "content": "System."}]
        messages.extend(
            {"role": "user", "content": f"history-{i} " + ("detail " * 55)}
            for i in range(5)
        )
        messages.append({"role": "assistant", "content": "oversized-gap " * 800})
        messages.extend(
            {"role": "user", "content": f"recent-{i} " + ("detail " * 55)}
            for i in range(20)
        )
        messages.append({"role": "user", "content": "latest"})
        compacted, stats = compact_messages(messages, 4096, 1024)
        self.assertTrue(stats["compacted"])
        digest = next(
            m["content"] for m in compacted
            if "[NEXUS_CONTEXT_COMPACTION]" in str(m.get("content", ""))
        )
        for expected in ["history-0", "history-4", "oversized-gap"]:
            self.assertIn(expected, digest)
        self.assertGreater(stats["dropped_messages"], 6)
        self.assertEqual(compacted[-1], messages[-1])

    def test_extractive_digest_preserves_start_and_end_of_long_turns(self):
        messages = [
            {"role": "user", "content": "START-OF-REQUEST " + ("middle " * 5000) + " END-OF-REQUEST"},
            {"role": "user", "content": "latest"},
        ]
        compacted, stats = compact_messages(messages, 8192, 1024)
        self.assertTrue(stats["compacted"])
        digest = next(m["content"] for m in compacted if "[NEXUS_CONTEXT_COMPACTION]" in str(m.get("content", "")))
        self.assertIn("START-OF-REQUEST", digest)
        self.assertIn("END-OF-REQUEST", digest)

    def test_compaction_does_not_mutate_original_messages(self):
        messages = [
            {"role": "system", "content": "System."},
            *[{"role": "user", "content": "x " * 400} for _ in range(30)],
        ]
        before = [dict(item) for item in messages]
        compact_messages(messages, 4096, 512)
        self.assertEqual(messages, before)

    def test_latest_turn_too_large_fails_closed(self):
        messages = [
            {"role": "system", "content": "System " * 1000},
            {"role": "user", "content": "Current " * 1000},
        ]
        with self.assertRaises(ValueError):
            compact_messages(messages, 4096, 1024)

    def test_model_specific_context_window_config(self):
        with patch.dict(os.environ, {
            "NEXUS_MODEL_CONTEXT_WINDOWS": '{"provider-a":65536,"model-b":131072}',
            "NEXUS_DEFAULT_CONTEXT_WINDOW_TOKENS": "32768",
        }, clear=False):
            self.assertEqual(configured_context_window("provider-a", "other"), 65536)
            self.assertEqual(configured_context_window("other", "model-b"), 131072)
            self.assertEqual(configured_context_window("other", "other"), 32768)

    def test_provider_context_window_overrides_environment_default(self):
        with patch.dict(os.environ, {
            "NEXUS_MODEL_CONTEXT_WINDOWS": '{"provider-a":65536}',
            "NEXUS_DEFAULT_CONTEXT_WINDOW_TOKENS": "32768",
        }, clear=False):
            self.assertEqual(configured_context_window("provider-a", "other", 131072), 65536)
            self.assertEqual(configured_context_window("provider-b", "other", 131072), 131072)

    def test_invalid_context_window_config_falls_back(self):
        with patch.dict(os.environ, {
            "NEXUS_MODEL_CONTEXT_WINDOWS": "not-json",
            "NEXUS_DEFAULT_CONTEXT_WINDOW_TOKENS": "not-an-int",
        }, clear=False):
            self.assertEqual(configured_context_window("provider-a"), 32768)


if __name__ == "__main__":
    unittest.main()
