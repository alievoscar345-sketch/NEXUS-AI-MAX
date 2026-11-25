package ai.nexus.max

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class ChatCompletionSseDecoderTest {
    @Test
    fun emitsCompleteDataPayloads() {
        val decoder = ChatCompletionSseDecoder()
        assertNull(decoder.acceptLine("data: {\"choices\":[]}"))
        assertEquals("{\"choices\":[]}", decoder.acceptLine(""))
        assertFalse(decoder.isComplete)
    }

    @Test
    fun recognizesDoneMarkerAndMarksStreamComplete() {
        val decoder = ChatCompletionSseDecoder()
        decoder.acceptLine("data: [DONE]")
        assertNull(decoder.acceptLine(""))
        assertTrue(decoder.isComplete)
    }

    @Test
    fun recognizesUnterminatedDoneMarkerAtEof() {
        val decoder = ChatCompletionSseDecoder()
        decoder.acceptLine("data: [DONE]")
        assertNull(decoder.finish())
        assertTrue(decoder.isComplete)
    }

    @Test
    fun doesNotTreatPrematureEofAsSuccessfulCompletion() {
        val decoder = ChatCompletionSseDecoder()
        decoder.acceptLine("data: {\"choices\":[]}")
        assertEquals("{\"choices\":[]}", decoder.finish())
        assertFalse(decoder.isComplete)
    }

    @Test
    fun preservesEmptyNonTerminalPayload() {
        val decoder = ChatCompletionSseDecoder()
        decoder.acceptLine("data:")
        assertEquals("", decoder.acceptLine(""))
        assertFalse(decoder.isComplete)
    }
}
