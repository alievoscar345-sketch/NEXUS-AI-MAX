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

    @Test fun createTaskSendsExplicitManualModelSelection() {
        transport.responseBody = """{"id":"task-model","status":"queued"}"""
        client.createTask("Build an Android app", modelSelection = "user-claude")
        val buffer = Buffer()
        transport.lastRequest!!.body!!.writeTo(buffer)
        val payload = org.json.JSONObject(buffer.readUtf8())
        assertEquals("user-claude", payload.getString("model_selection"))
    }

    @Test fun createTaskDefaultsToAutomaticModelSelection() {
        transport.responseBody = """{"id":"task-auto","status":"queued"}"""
        client.createTask("Research this topic")
        val buffer = Buffer()
        transport.lastRequest!!.body!!.writeTo(buffer)
        assertEquals("auto", org.json.JSONObject(buffer.readUtf8()).getString("model_selection"))
    }

    @Test fun writeProjectFileSendsTextAndRevisionMessage() {
        transport.responseBody = """{"file":{"path":"src/main.kt","revision":2,"bytes":12,"sha256":"abc","message":"save","created_at":10.0}}"""
        val result = client.writeProjectFile("project-1", "src/main.kt", "hello world", "save")
        assertEquals(2, result.revision)
        assertEquals("PUT", transport.lastRequest!!.method)
        val payload = Buffer().also { transport.lastRequest!!.body!!.writeTo(it) }
        val json = org.json.JSONObject(payload.readUtf8())
        assertEquals("src/main.kt", json.getString("path"))
        assertEquals("hello world", json.getString("content"))
        assertEquals("save", json.getString("message"))
    }

    @Test fun readProjectFileEncodesPathAsQueryParameter() {
        transport.responseBody = """{"path":"src/main file.kt","content":"source","revision":1,"bytes":6,"sha256":"abc","message":"initial","created_at":10.0}"""
        val result = client.readProjectFile("project-1", "src/main file.kt")
        assertEquals("source", result.content)
        assertEquals("src/main file.kt", transport.lastRequest!!.url.queryParameter("path"))
    }

    @Test fun restoreProjectFileSendsRevisionAndPath() {
        transport.responseBody = """{"file":{"path":"README.md","revision":4,"bytes":4,"sha256":"abc","message":"restore","created_at":11.0}}"""
        val result = client.restoreProjectFile("project-1", "README.md", 2)
        assertEquals(4, result.revision)
        assertEquals("README.md", transport.lastRequest!!.url.queryParameter("path"))
        val payload = Buffer().also { transport.lastRequest!!.body!!.writeTo(it) }
        assertEquals(2, org.json.JSONObject(payload.readUtf8()).getInt("revision"))
    }

    @Test fun uploadProjectBinaryFileSendsRawBytesAndContentType() {
        transport.responseBody = """{"file":{"path":"assets/photo.png","revision":3,"bytes":4,"sha256":"abcd","message":"upload","created_at":12.0}}"""
        val payload = byteArrayOf(0, 1, -1, 42)
        val file = client.uploadProjectBinaryFile("project-1", "assets/photo.png", payload, "image/png")
        assertEquals(3, file.revision)
        val request = transport.lastRequest!!
        assertEquals("PUT", request.method)
        assertEquals("Bearer test-token", request.header("Authorization"))
        assertEquals("image/png", request.header("Content-Type")?.substringBefore(";"))
        assertEquals("assets/photo.png", request.url.queryParameter("path"))
        val body = Buffer()
        request.body!!.writeTo(body)
        assertArrayEquals(payload, body.readByteArray())
    }

    @Test fun downloadProjectBinaryFileReturnsExactRawBytes() {
        transport.responseBody = "raw\\u0000binary"
        val result = client.downloadProjectBinaryFile("project-1", "assets/data.bin")
        assertArrayEquals(transport.responseBody.toByteArray(Charsets.UTF_8), result)
        assertEquals("GET", transport.lastRequest!!.method)
        assertEquals("assets/data.bin", transport.lastRequest!!.url.queryParameter("path"))
        assertEquals("Bearer test-token", transport.lastRequest!!.header("Authorization"))
    }

    @Test fun binaryProjectUploadRejectsFilesAboveClientLimit() {
        val error = try {
            client.uploadProjectBinaryFile("project-1", "large.bin", ByteArray(50 * 1024 * 1024 + 1))
            null
        } catch (e: IllegalArgumentException) { e }
        assertNotNull(error)
        assertTrue(error!!.message!!.contains("50 MiB"))
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

    @Test fun downloadArtifactReturnsRawBytesAndUsesAuthenticatedDownloadRoute() {
        transport.responseBody = "APK-binary-content"
        val output = java.io.ByteArrayOutputStream()
        val copied = client.downloadArtifact("task-10", "release.apk", output)
        assertEquals("APK-binary-content", output.toString("UTF-8"))
        assertEquals("APK-binary-content".length.toLong(), copied)
        val request = transport.lastRequest!!
        assertEquals("GET", request.method)
        assertEquals("/v1/tasks/task-10/artifacts/download", request.url.encodedPath)
        assertEquals("release.apk", request.url.queryParameter("name"))
        assertEquals("Bearer test-token", request.header("Authorization"))
        assertEquals("application/octet-stream", request.header("Accept"))
    }

    @Test fun createTaskDoesNotApplyAnArtificialThirtyStepMetadataCeiling() {
        transport.responseBody = """{"id":"task-large-plan","status":"queued"}"""
        client.createTask("Build a complex application", maxSteps = 500)
        val body = Buffer()
        transport.lastRequest!!.body!!.writeTo(body)
        assertEquals(500, org.json.JSONObject(body.readUtf8()).getInt("max_steps"))
    }

    @Test fun githubStatusParsesConnectedRepositoryWithoutSecret() {
        transport.responseBody = """{"connected":true,"integration":{"repo_full_name":"owner/app","github_login":"owner","default_branch":"main"}}"""
        val status = client.githubStatus()
        assertTrue(status.connected)
        assertEquals("owner/app", status.repoFullName)
        assertEquals("main", status.defaultBranch)
        assertEquals("/v1/github/status", transport.lastRequest!!.url.encodedPath)
        assertEquals("Bearer test-token", transport.lastRequest!!.header("Authorization"))
    }

    @Test fun connectGitHubSendsTokenOnlyToAuthenticatedServerEndpoint() {
        transport.responseBody = """{"status":"connected","repo_full_name":"owner/app","github_login":"owner","default_branch":"main","secret_returned":false}"""
        val result = client.connectGitHub("owner/app", "github_pat_test-secret")
        assertTrue(result.connected)
        assertEquals("owner/app", result.repoFullName)
        val request = transport.lastRequest!!
        assertEquals("POST", request.method)
        assertEquals("/v1/github/connect", request.url.encodedPath)
        val body = Buffer()
        request.body!!.writeTo(body)
        val payload = org.json.JSONObject(body.readUtf8())
        assertEquals("owner/app", payload.getString("repo_full_name"))
        assertEquals("github_pat_test-secret", payload.getString("token"))
    }

    @Test fun connectGitHubRejectsHttpServerBeforeSendingCredential() {
        val insecureClient = AgentTaskClient("http://example.com", "test-token", transport)
        try {
            insecureClient.connectGitHub("owner/app", "github_pat_test-secret")
            fail("Expected HTTPS requirement")
        } catch (error: IllegalArgumentException) {
            assertTrue(error.message!!.contains("HTTPS"))
        }
        assertNull(transport.lastRequest)
    }

    @Test fun connectGitHubRejectsMalformedRepositoryNames() {
        try {
            client.connectGitHub("../private", "github_pat_test-secret")
            fail("Expected repository validation")
        } catch (error: IllegalArgumentException) {
            assertTrue(error.message!!.contains("repository"))
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
