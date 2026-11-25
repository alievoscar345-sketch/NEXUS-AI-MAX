package ai.nexus.max

import org.junit.Assert.assertTrue
import org.junit.Test

class ChatPayloadSizeTest {
    @Test
    fun acceptsPayloadAtConfiguredByteLimit() {
        validateChatRequestSize("1234567890", maxBytes = 10)
    }

    @Test
    fun rejectsPayloadAboveConfiguredByteLimit() {
        var rejected = false
        try {
            validateChatRequestSize("12345678901", maxBytes = 10)
        } catch (_: IllegalArgumentException) {
            rejected = true
        }
        assertTrue(rejected)
    }

    @Test
    fun measuresUtf8BytesRatherThanCharacterCount() {
        // Six Cyrillic characters occupy twelve UTF-8 bytes.
        var rejected = false
        try {
            validateChatRequestSize("я".repeat(6), maxBytes = 10)
        } catch (_: IllegalArgumentException) {
            rejected = true
        }
        assertTrue(rejected)
    }
}
