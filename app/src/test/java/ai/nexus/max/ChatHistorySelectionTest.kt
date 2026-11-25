package ai.nexus.max

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class ChatHistorySelectionTest {
    @Test
    fun estimatesUtf8TokensLikeBackend() {
        assertEquals(0, estimateUtf8Tokens(""))
        // ceil(5 bytes / 3.2) = 2
        assertEquals(2, estimateUtf8Tokens("hello"))
        // "привет" is 12 UTF-8 bytes: ceil(12 / 3.2) = 4
        assertEquals(4, estimateUtf8Tokens("привет"))
    }

    @Test
    fun countsMessageOverheadRoleAndContent() {
        val line = ChatLine("user", "hello")
        // 8 overhead + role "user" (ceil(4/3.2)=2) + content (2)
        assertEquals(12, estimateChatLineTokens(line))
    }

    @Test
    fun countsImageAttachmentsAtFixedEstimate() {
        val plain = ChatLine("user", "hi", listOf(ChatAttachment("a.txt", "text/plain", textContent = "abcd")))
        val withImage = ChatLine("user", "hi", listOf(ChatAttachment("p.png", "image/png", localPath = "/tmp/p.png")))
        assertEquals(estimateChatLineTokens(plain) + HISTORY_IMAGE_TOKEN_ESTIMATE - estimateUtf8Tokens("abcd"),
            estimateChatLineTokens(withImage))
        assertTrue(estimateChatLineTokens(withImage) >= 4096)
    }

    @Test
    fun preservesNewestTurnWhenBudgetIsTiny() {
        val history = listOf(
            ChatLine("user", "one"),
            ChatLine("assistant", "two"),
            ChatLine("user", "three"),
        )
        val selected = selectHistoryForRequest(history, tokenBudget = 1)
        assertEquals(listOf("three"), selected.map { it.content })
    }

    @Test
    fun keepsEverythingWhenBudgetAllows() {
        val history = (1..50).map { ChatLine(if (it % 2 == 0) "assistant" else "user", "message $it") }
        val selected = selectHistoryForRequest(history, tokenBudget = 1_000_000)
        assertEquals(history, selected)
    }

    @Test
    fun selectsContiguousNewestSuffixWithinBudget() {
        val history = (1..100).map { ChatLine("user", "msg $it") }
        val selected = selectHistoryForRequest(history, tokenBudget = 300)
        // Always a contiguous suffix that starts no earlier than index 0 and ends at the newest.
        assertEquals(history.last(), selected.last())
        assertTrue(selected.size < history.size)
        assertTrue(selected.size >= 1)
        assertEquals(history.subList(history.size - selected.size, history.size), selected)
        // Dropping one more older message would fit the budget (selection is maximal).
        if (selected.size > 1) {
            val withoutOldest = selected.drop(1)
            val tokens = withoutOldest.sumOf { estimateChatLineTokens(it) }
            assertTrue(tokens <= 300)
        }
    }

    @Test
    fun respectsMaxMessagesCap() {
        val history = (1..50).map { ChatLine("user", "m$it") }
        val selected = selectHistoryForRequest(history, tokenBudget = 1_000_000, maxMessages = 10)
        assertEquals(10, selected.size)
        assertEquals("m50", selected.last().content)
    }

    @Test
    fun returnsEmptyForEmptyHistory() {
        assertTrue(selectHistoryForRequest(emptyList(), tokenBudget = 100).isEmpty())
    }

    @Test
    fun webSearchEstimateUsesPreservedProviderPayload() {
        val preservedJson = """[{"role":"assistant","content":"${"x".repeat(5000)}"}]"""
        val line = ChatLine("assistant", "short visible text", providerMessagesJson = preservedJson)
        val preservedEstimate = HISTORY_MESSAGE_OVERHEAD_TOKENS +
            estimateUtf8Tokens("assistant") + estimateUtf8Tokens(preservedJson)
        assertEquals(preservedEstimate, estimateWebSearchLineTokens(line))
        // The preserved payload dominates: counting it must exceed counting visible content.
        assertTrue(estimateWebSearchLineTokens(line) > estimateChatLineTokens(line))
    }

    @Test
    fun webSearchEstimateFallsBackToNormalLineWithoutPreservedPayload() {
        val line = ChatLine("user", "plain question")
        assertEquals(estimateChatLineTokens(line), estimateWebSearchLineTokens(line))
    }

    @Test
    fun webSearchSelectionDropsLinesWhosePreservedPayloadBlowsTheBudget() {
        // ~400k chars of preserved payload ≈ 125k estimated tokens > 100k budget.
        val giant = ChatLine(
            "assistant",
            "visible",
            providerMessagesJson = """[{"role":"assistant","content":"${"y".repeat(400_000)}"}]"""
        )
        val newest = ChatLine("user", "latest question")
        val history = listOf(giant, newest)
        val selected = selectHistoryForRequest(
            history,
            tokenBudget = WEB_SEARCH_HISTORY_TOKEN_BUDGET,
            estimateTokens = ::estimateWebSearchLineTokens,
        )
        assertEquals(listOf(newest), selected)
    }
}
