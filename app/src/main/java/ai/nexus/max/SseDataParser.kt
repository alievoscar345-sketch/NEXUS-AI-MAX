package ai.nexus.max

/**
 * Incremental parser for Server-Sent Events data fields.
 * Feed one line at a time without its line terminator.
 */
internal class SseDataParser(
    private val maxEventChars: Int = 1_048_576
) {
    private val data = StringBuilder()
    private var hasDataField = false

    init {
        require(maxEventChars > 0) { "maxEventChars must be positive" }
    }

    fun acceptLine(line: String): String? {
        if (line.isEmpty()) return dispatch()
        if (line.startsWith(":")) return null

        val separator = line.indexOf(':')
        val field = if (separator >= 0) line.substring(0, separator) else line
        if (field != "data") return null

        var value = if (separator >= 0) line.substring(separator + 1) else ""
        if (value.startsWith(" ")) value = value.substring(1)

        val addedLength = value.length + if (hasDataField) 1 else 0
        require(data.length + addedLength <= maxEventChars) {
            "SSE event exceeds configured size limit"
        }
        if (hasDataField) data.append('\n')
        data.append(value)
        hasDataField = true
        return null
    }

    fun finish(): String? = dispatch()

    private fun dispatch(): String? {
        if (!hasDataField) return null
        return data.toString().also {
            data.setLength(0)
            hasDataField = false
        }
    }
}
