package ai.nexus.max

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class ChatAttachmentTextTest {
    @Test
    fun preservesPromptAndSmallDocumentContent() {
        val result = buildChatTextWithAttachments(
            "Summarize this file",
            listOf(ChatAttachment("notes.txt", "text/plain", textContent = "First line\\nSecond line"))
        )

        assertTrue(result.startsWith("Summarize this file"))
        assertTrue(result.contains("[Прикреплённый файл: notes.txt]"))
        assertTrue(result.contains("First line\\nSecond line"))
        assertFalse(result.contains("ВНИМАНИЕ"))
    }

    @Test
    fun truncatesOversizedDocumentAndExplainsHowToContinue() {
        val result = buildChatTextWithAttachments(
            "Analyze",
            listOf(ChatAttachment("large.txt", "text/plain", textContent = "x".repeat(220_000)))
        )

        assertEquals(180_000, result.length)
        assertTrue(result.startsWith("Analyze"))
        assertTrue(result.contains("ВНИМАНИЕ"))
        assertTrue(result.contains("отправь документы по отдельности"))
    }

    @Test
    fun keepsCombinedAttachmentsWithinBackendTextLimit() {
        val result = buildChatTextWithAttachments(
            "Compare",
            listOf(
                ChatAttachment("a.txt", "text/plain", textContent = "a".repeat(100_000)),
                ChatAttachment("b.txt", "text/plain", textContent = "b".repeat(100_000))
            )
        )

        assertTrue(result.length <= 180_000)
        assertTrue(result.contains("ВНИМАНИЕ"))
    }
}
