package ai.nexus.max

import org.json.JSONObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class OpenRouterVisionCapabilityTest {
    @Test
    fun readsImageInputModalityFromArchitectureMetadata() {
        val model = JSONObject(
            """{"architecture":{"input_modalities":["text","image"]}}"""
        )

        assertEquals(setOf("text", "image"), parseOpenRouterInputModalities(model))
    }

    @Test
    fun doesNotTreatTextOnlyModelAsVisionCapable() {
        val model = JSONObject(
            """{"architecture":{"input_modalities":["text"]}}"""
        )

        assertTrue("image" !in parseOpenRouterInputModalities(model))
    }

    @Test
    fun safelyHandlesMissingArchitectureMetadata() {
        assertTrue(parseOpenRouterInputModalities(JSONObject("{}")).isEmpty())
    }
}
