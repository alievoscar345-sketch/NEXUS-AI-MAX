package ai.nexus.max

/**
 * Frames an OpenAI-compatible chat-completion SSE stream and recognizes its terminal marker.
 *
 * The decoder returns complete non-terminal data payloads. The caller is responsible for
 * interpreting JSON payloads. A stream is complete only after an explicit [DONE] event.
 */
internal class ChatCompletionSseDecoder(
    maxEventChars: Int = 1_048_576
) {
    private val parser = SseDataParser(maxEventChars)

    var isComplete: Boolean = false
        private set

    fun acceptLine(line: String): String? = parser.acceptLine(line)?.let(::decodeEvent)

    fun finish(): String? = parser.finish()?.let(::decodeEvent)

    private fun decodeEvent(payload: String): String? {
        if (payload == "[DONE]") {
            isComplete = true
            return null
        }
        return payload
    }
}
