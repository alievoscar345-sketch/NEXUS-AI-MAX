package ai.nexus.max

import okhttp3.Interceptor
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Response
import okhttp3.ResponseBody.Companion.toResponseBody
import okio.Buffer
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.IOException

class AgentTaskClientTest {
    private lateinit var transport: FakeTransport
    private lateinit var client: AgentTaskClient

    private class FakeTransport : Interceptor {
        var statusCode = 200
        var responseBody = "{}"
        var lastRequest: okhttp3.Request? = null

        override fun intercept(chain: Interceptor.Chain): Response {
            val request = chain.request()
            lastRequest = request
            return Response.Builder()
                .request(request)
                .protocol(Protocol.HTTP_1_1)
                .code(statusCode)
                .message("test")
                .body(responseBody.toResponseBody("application/json".toMediaType()))
                .build()
        }
    }

    @Before fun setUp() {
        transport = FakeTransport()
        val http = OkHttpClient.Builder().addInterceptor(transport).build()
        client = AgentTaskClient("https://agent.example", "test-token", http)
    }

    @Test fun createTaskSendsBearerAndIdempotencyKey() {
        transport.responseBody = """{"id":"task-123","status":"queued"}"""
        assertEquals("task-123", client.createTask("Build a report", idempotencyKey = "stable-key-123"))
        val request = transport.lastRequest!!
        assertEquals("POST", request.method)
        assertEquals("Bearer test-token", request.header("Authorization"))
        val buffer = Buffer()
        request.body!!.writeTo(buffer)
        assertEquals("stable-key-123", org.json.JSONObject(buffer.readUtf8()).getString("idempotency_key"))
    }

    @Test fun getTaskParsesEventsAndSteps() {
        transport.responseBody = """{"id":"t1","status":"running","prompt":"Do work","summary":"","error":"","events":[{"created_at":1.5,"kind":"step_started","message":"1: Plan"}],"steps":[{"step_index":0,"title":"Plan","action":"web_search","status":"running","error":""}]}"""
        val task = client.getTask("t1")
        assertEquals("running", task.status)
        assertEquals("step_started", task.events.single().kind)
        assertEquals("web_search", task.steps.single().action)
    }

    @Test fun taskIdIsEncodedAsOnePathSegment() {
        transport.responseBody = """{"id":"a/b","status":"queued","prompt":"x","summary":"","error":""}"""
        client.getTask("a/b")
        assertTrue(transport.lastRequest!!.url.encodedPath.contains("a%2Fb"))
    }

    @Test fun apiErrorDetailIsSurfaced() {
        transport.statusCode = 409
        transport.responseBody = """{"detail":"Task already finished"}"""
        try {
            client.cancelTask("t1")
            fail("Expected API error")
        } catch (error: IOException) {
            assertEquals("Task already finished", error.message)
        }
    }

    @Test fun rejectsNonHttpsServer() {
        try {
            AgentTaskClient("http://example.com", "token")
            fail("Expected HTTPS validation")
        } catch (error: IllegalArgumentException) {
            assertTrue(error.message!!.contains("HTTPS"))
        }
    }
}
