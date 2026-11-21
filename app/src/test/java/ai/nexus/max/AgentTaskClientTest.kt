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
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

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
        transport.responseBody = """{"id":"t1","status":"running","prompt":"Do work","summary":"","error":"","events":[{"id":17,"created_at":1.5,"kind":"step_started","message":"1: Plan"}],"steps":[{"step_index":0,"title":"Plan","action":"web_search","status":"running","error":""}]}"""
        val task = client.getTask("t1")
        assertEquals("running", task.status)
        assertEquals("step_started", task.events.single().kind)
        assertEquals(17L, task.events.single().id)
        assertEquals("web_search", task.steps.single().action)
    }

    @Test fun getTaskRequestsOnlyEventsAfterProvidedCursor() {
        transport.responseBody = """{"id":"t1","status":"running","prompt":"Do work","summary":"","error":"","events":[{"id":22,"created_at":2.0,"kind":"progress","message":"new"}],"steps":[]}"""
        val task = client.getTask("t1", afterEventId = 21L)
        assertEquals(22L, task.events.single().id)
        assertEquals("21", transport.lastRequest!!.url.queryParameter("after_event_id"))
    }

    @Test fun streamTaskEventsParsesSseAndSendsCursor() {
        transport.responseBody = "id: 31\nevent: task_event\ndata: {\"id\":31,\"created_at\":3.1,\"kind\":\"progress\",\"message\":\"working\"}\n\n"
        val received = CountDownLatch(1)
        val failure = AtomicReference<IOException?>()
        val event = AtomicReference<AgentTaskEvent?>()
        val closed = CountDownLatch(1)
        client.streamTaskEvents(
            "task-31",
            30L,
            onEvent = { event.set(it); received.countDown() },
            onError = { failure.set(it) },
            onClosed = { closed.countDown() }
        )
        assertTrue("SSE event was not received", received.await(5, TimeUnit.SECONDS))
        assertTrue("SSE stream did not close", closed.await(5, TimeUnit.SECONDS))
        assertNull(failure.get())
        assertEquals(31L, event.get()!!.id)
        assertEquals("working", event.get()!!.message)
        assertEquals("30", transport.lastRequest!!.url.queryParameter("after_event_id"))
        assertEquals("text/event-stream", transport.lastRequest!!.header("Accept"))
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

    @Test fun decideApprovalUsesBackendRouteAndPayload() {
        transport.responseBody = """{"approval_id":"approval-7","status":"approved"}"""
        assertEquals("approved", client.decideApproval("approval/7", true, "reviewed"))
        val request = transport.lastRequest!!
        assertEquals("POST", request.method)
        assertEquals("/v1/approvals/approval%2F7", request.url.encodedPath)
        assertEquals("Bearer test-token", request.header("Authorization"))
        val buffer = Buffer()
        request.body!!.writeTo(buffer)
        val payload = org.json.JSONObject(buffer.readUtf8())
        assertTrue(payload.getBoolean("approved"))
        assertEquals("reviewed", payload.getString("note"))
    }

    @Test fun listWorkspaceRequestsTaskScopedDirectory() {
        transport.responseBody = """{"path":".","entries":[{"name":"report.md","type":"file","bytes":42}]}"""
        val entries = client.listWorkspace("task-1")
        assertEquals("report.md", entries.single().name)
        assertEquals(42L, entries.single().bytes)
        assertEquals("/v1/tasks/task-1/workspace", transport.lastRequest!!.url.encodedPath)
    }

    @Test fun readAndWriteWorkspaceFileUseAuthenticatedTaskScopedRoutes() {
        transport.responseBody = """{"path":"report.md","content":"hello"}"""
        assertEquals("hello", client.readWorkspaceFile("task-1", "report.md"))
        assertEquals("report.md", transport.lastRequest!!.url.queryParameter("path"))
        transport.responseBody = """{"path":"report.md","bytes":5}"""
        assertEquals(5L, client.writeWorkspaceFile("task-1", "report.md", "hello"))
        val request = transport.lastRequest!!
        assertEquals("PUT", request.method)
        assertEquals("/v1/tasks/task-1/workspace/file", request.url.encodedPath)
        assertEquals("Bearer test-token", request.header("Authorization"))
        val buffer = Buffer()
        request.body!!.writeTo(buffer)
        val payload = org.json.JSONObject(buffer.readUtf8())
        assertEquals("report.md", payload.getString("path"))
        assertEquals("hello", payload.getString("content"))
    }

    @Test fun listArtifactsAndReadArtifactUseTaskScopedAuthenticatedRoutes() {
        transport.responseBody = """{"artifacts":[{"name":"summary.md","bytes":128}]}"""
        val artifacts = client.listArtifacts("task-9")
        assertEquals("summary.md", artifacts.single().name)
        assertEquals(128L, artifacts.single().bytes)
        assertEquals("/v1/tasks/task-9/artifacts", transport.lastRequest!!.url.encodedPath)
        transport.responseBody = """{"name":"summary.md","content":"NEXUS result"}"""
        assertEquals("NEXUS result", client.readArtifact("task-9", "summary.md"))
        assertEquals("/v1/tasks/task-9/artifacts/file", transport.lastRequest!!.url.encodedPath)
        assertEquals("summary.md", transport.lastRequest!!.url.queryParameter("name"))
        assertEquals("Bearer test-token", transport.lastRequest!!.header("Authorization"))
    }

    @Test fun memoryCrudUsesAuthenticatedOwnerScopedApiContracts() {
        transport.responseBody = """{"memories":[{"id":"m-1","title":"NEXUS","content":"Build the Android app","category":"project","pinned":true,"updated_at":12.5}]}"""
        val listed = client.listMemories("NEXUS Android", 20).single()
        assertEquals("m-1", listed.id)
        assertTrue(listed.pinned)
        assertEquals("NEXUS Android", transport.lastRequest!!.url.queryParameter("q"))
        assertEquals("Bearer test-token", transport.lastRequest!!.header("Authorization"))

        transport.responseBody = """{"memory":{"id":"m-2","title":"Goal","content":"Build NEXUS","category":"goal","pinned":false,"updated_at":13.0}}"""
        val created = client.createMemory("Goal", "Build NEXUS", "goal")
        assertEquals("m-2", created.id)
        assertEquals("POST", transport.lastRequest!!.method)
        val createBody = Buffer()
        transport.lastRequest!!.body!!.writeTo(createBody)
        assertEquals("Build NEXUS", org.json.JSONObject(createBody.readUtf8()).getString("content"))

        transport.responseBody = """{"memory":{"id":"m-2","title":"Goal v2","content":"Build NEXUS MAX","category":"goal","pinned":true,"updated_at":14.0}}"""
        val updated = client.updateMemory("m-2", "Goal v2", "Build NEXUS MAX", "goal", true)
        assertEquals("Goal v2", updated.title)
        assertEquals("PATCH", transport.lastRequest!!.method)
        assertEquals("/v1/memories/m-2", transport.lastRequest!!.url.encodedPath)

        transport.responseBody = """{"id":"m-2","status":"deleted"}"""
        client.deleteMemory("m-2")
        assertEquals("DELETE", transport.lastRequest!!.method)
        assertEquals("/v1/memories/m-2", transport.lastRequest!!.url.encodedPath)
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
