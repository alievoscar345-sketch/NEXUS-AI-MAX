package ai.nexus.max

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class SseDataParserTest {
    @Test
    fun dispatchesSingleDataFieldOnBlankLine() {
        val parser = SseDataParser()
        assertNull(parser.acceptLine("data: {\"choices\":[]}"))
        assertEquals("{\"choices\":[]}", parser.acceptLine(""))
        assertNull(parser.finish())
    }

    @Test
    fun joinsMultipleDataFieldsWithNewline() {
        val parser = SseDataParser()
        parser.acceptLine("data: first")
        parser.acceptLine("data:second")
        assertEquals("first\\nsecond", parser.acceptLine(""))
    }

    @Test
    fun ignoresCommentsAndNonDataFields() {
        val parser = SseDataParser()
        assertNull(parser.acceptLine(": heartbeat"))
        assertNull(parser.acceptLine("event: message"))
        assertNull(parser.acceptLine("id: 42"))
        parser.acceptLine("data: payload")
        assertEquals("payload", parser.finish())
    }

    @Test
    fun dispatchesUnterminatedFinalEventAtEof() {
        val parser = SseDataParser()
        parser.acceptLine("data: final")
        assertEquals("final", parser.finish())
        assertNull(parser.finish())
    }

    @Test
    fun emptyDataFieldIsAValidEvent() {
        val parser = SseDataParser()
        parser.acceptLine("data:")
        assertEquals("", parser.acceptLine(""))
    }

    @Test(expected = IllegalArgumentException::class)
    fun rejectsOversizedEvent() {
        val parser = SseDataParser(maxEventChars = 4)
        parser.acceptLine("data: 12345")
    }
}
