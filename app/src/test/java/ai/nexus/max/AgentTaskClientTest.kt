package ai.nexus.max

import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.tls.HandshakeCertificates
import okhttp3.tls.HeldCertificate
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.IOException

class AgentTaskClientTest {
    private lateinit var server: MockWebServer
    private lateinit var client: AgentTaskClient

    @Before fun setUp() {
        val heldCertificate = HeldCertificate.Builder()
            .addSubjectAlternativeName("localhost")
            .build()
        val serverCertificates = HandshakeCertificates.Builder()
            .heldCertificate(heldCertificate)
            .build()
        val clientCertificates = HandshakeCertificates.Builder()
            .addTrustedCertificate(heldCertificate.certificate)
            .build()
        server = MockWebServer()
        server.useHttps(serverCertificates.sslSocketFactory(), false)
        server.start()
        val http = okhttp3.OkHttpClient.Builder()
            .sslSocketFactory(clientCertificates.sslSocketFactory(), clientCertificates.trustManager)
            .build()
        client = AgentTaskClient(server.url("/").toString(), "test-token", http)
    }

    @After fun tearDown() { server.shutdown() }

    @Test fun createTaskSendsBearerAndIdempotencyKey() {
        server.enqueue(MockResponse().setBody("""{"id":"task-123","status":"queued"}"""))
        assertEquals("task-123", client.createTask("Build a report", idempotencyKey = "stable-key-123"))
        val request = server.takeRequest()
        assertEquals("POST", request.method)
        assertEquals("Bearer test-token", request.getHeader("Authorization"))
        assertEquals("stable-key-123", org.json.JSONObject(request.body.readUtf8()).getString("idempotency_key"))
    }

    @Test fun getTaskParsesEventsAndSteps() {
        server.enqueue(MockResponse().setBody("""{"id":"t1","status":"running","prompt":"Do work","summary":"","error":"","events":[{"created_at":1.5,"kind":"step_started","message":"1: Plan"}],"steps":[{"step_index":0,"title":"Plan","action":"web_search","status":"running","error":""}]}"""))
        val task = client.getTask("t1")
        assertEquals("running", task.status)
        assertEquals("step_started", task.events.single().kind)
        assertEquals("web_search", task.steps.single().action)
    }

    @Test fun taskIdIsEncodedAsOnePathSegment() {
        server.enqueue(MockResponse().setBody("""{"id":"a/b","status":"queued","prompt":"x","summary":"","error":""}"""))
        client.getTask("a/b")
        assertTrue(server.takeRequest().path!!.contains("a%2Fb"))
    }

    @Test fun apiErrorDetailIsSurfaced() {
        server.enqueue(MockResponse().setResponseCode(409).setBody("""{"detail":"Task already finished"}"""))
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
