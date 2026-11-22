package ai.nexus.max

import okhttp3.Call
import okhttp3.Callback
import okhttp3.Response
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.HttpUrl.Companion.toHttpUrl
import org.json.JSONArray
import org.json.JSONObject
import java.io.IOException
import java.io.OutputStream
import java.util.UUID
import java.util.concurrent.TimeUnit

data class AgentTaskSnapshot(
    val id: String,
    val status: String,
    val prompt: String,
    val summary: String,
    val error: String,
    val events: List<AgentTaskEvent>,
    val steps: List<AgentTaskStep>
)

data class AgentTaskEvent(val id: Long, val createdAt: Double, val kind: String, val message: String)
data class AgentTaskStep(val index: Int, val title: String, val action: String, val status: String, val error: String)

data class AgentApproval(val id: String, val taskId: String, val description: String, val status: String)
data class NexusGitHubIntegration(val connected: Boolean, val repoFullName: String, val githubLogin: String, val defaultBranch: String)
data class AgentWorkspaceEntry(val name: String, val type: String, val bytes: Long?)
data class AgentCheckpointFile(
    val path: String, val bytes: Long, val sha256: String,
    val revision: Int? = null, val contentType: String = "", val isBinary: Boolean = false
)
data class AgentTaskCheckpoint(
    val id: String, val sequence: Int, val stepId: String, val reason: String,
    val createdAt: Double, val archiveBytes: Long, val archiveSha256: String,
    val isLatest: Boolean, val planSteps: Int, val completedSteps: Int,
    val workspaceFiles: List<AgentCheckpointFile>, val projectFiles: List<AgentCheckpointFile>,
    val artifacts: List<AgentArtifact>, val legacy: Boolean, val available: Boolean,
    val error: String
)
data class AgentArtifact(val name: String, val bytes: Long, val contentType: String, val sha256: String)
data class NexusMemory(val id: String, val title: String, val content: String, val category: String, val pinned: Boolean, val updatedAt: Double)
data class MemorySuggestion(val title: String, val content: String, val category: String)
data class NexusSkill(val id: String, val name: String, val description: String, val instructions: String, val category: String, val enabled: Boolean)
data class NexusProject(val id: String, val name: String, val description: String, val context: String, val status: String, val taskCount: Int)
data class NexusProjectFile(
    val path: String, val revision: Int, val bytes: Long, val sha256: String,
    val message: String, val createdAt: Double,
    val contentType: String = "text/plain; charset=utf-8", val isBinary: Boolean = false
)
data class NexusProjectFileContent(val file: NexusProjectFile, val content: String)
data class NexusAutomationRun(val id: String, val taskId: String, val scheduledFor: Double, val taskStatus: String, val taskSummary: String, val taskError: String)

data class NexusAutomation(
    val id: String, val name: String, val prompt: String, val scheduleType: String,
    val localTime: String, val timezone: String, val weekdays: List<Int>, val runAt: Double?,
    val intervalMinutes: Int?, val enabled: Boolean, val nextRunAt: Double?,
    val projectId: String?,
    val lastRunAt: Double?, val lastTaskStatus: String?, val lastTaskSummary: String?
)

class AgentTaskClient(
    baseUrl: String,
    private val token: String,
    private val http: OkHttpClient = OkHttpClient.Builder()
        .connectTimeout(15, TimeUnit.SECONDS)
        .readTimeout(30, TimeUnit.SECONDS)
        .callTimeout(45, TimeUnit.SECONDS)
        .build()
) {
    private val base = baseUrl.trimEnd('/').toHttpUrl()
    private val jsonType = "application/json; charset=utf-8".toMediaType()

    init {
        require(base.isHttps) { "Agent server must use HTTPS" }
        require(token.isNotBlank()) { "Agent access token is missing" }
    }

    fun createTask(prompt: String, maxSteps: Int = 24, idempotencyKey: String = UUID.randomUUID().toString(),
                    projectId: String? = null, contextText: String = "", modelSelection: String = "auto"): String {
        require(prompt.trim().length >= 3) { "Task prompt is too short" }
        require(contextText.length <= 24000) { "Task file context exceeds 24 KB" }
        require(modelSelection == "auto" || modelSelection.matches(Regex("^[^\\s]{1,159}$"))) {
            "Invalid model selection"
        }
        val body = JSONObject()
            .put("prompt", prompt.trim())
            .put("autonomy", "level3")
            .put("max_steps", maxSteps.coerceAtLeast(1))
            .put("idempotency_key", idempotencyKey)
            .put("model_selection", modelSelection)
        projectId?.let { body.put("project_id", it) }
        if (contextText.isNotBlank()) body.put("context_text", contextText)
        return request("POST", listOf("v1", "tasks"), body)["id"]?.toString()
            ?: throw IOException("Agent API did not return a task ID")
    }

    fun getTask(taskId: String, afterEventId: Long? = null): AgentTaskSnapshot {
        val query = afterEventId?.let { mapOf("after_event_id" to it.toString()) } ?: emptyMap()
        val obj = request("GET", listOf("v1", "tasks", taskId), query = query)
        val events = obj.optJSONArray("events").toEventList()
        val steps = obj.optJSONArray("steps").toStepList()
        return AgentTaskSnapshot(
            id = obj.optString("id"),
            status = obj.optString("status"),
            prompt = obj.optString("prompt"),
            summary = obj.optString("summary"),
            error = obj.optString("error"),
            events = events,
            steps = steps
        )
    }

    fun streamTaskEvents(
        taskId: String,
        afterEventId: Long,
        onEvent: (AgentTaskEvent) -> Unit,
        onError: (IOException) -> Unit,
        onClosed: () -> Unit = {}
    ): Call {
        require(afterEventId >= 0) { "Event cursor must be non-negative" }
        val url = base.newBuilder()
            .addPathSegment("v1")
            .addPathSegment("tasks")
            .addPathSegment(taskId)
            .addPathSegment("events")
            .addQueryParameter("after_event_id", afterEventId.toString())
            .build()
        val request = Request.Builder()
            .url(url)
            .header("Authorization", "Bearer $token")
            .header("Accept", "text/event-stream")
            .get()
            .build()
        val streamHttp = http.newBuilder()
            .readTimeout(0, TimeUnit.MILLISECONDS)
            .callTimeout(0, TimeUnit.MILLISECONDS)
            .build()
        val call = streamHttp.newCall(request)
        call.enqueue(object : Callback {
            override fun onFailure(call: Call, error: IOException) {
                if (!call.isCanceled()) onError(error)
                onClosed()
            }

            override fun onResponse(call: Call, response: Response) {
                response.use {
                    try {
                        if (!it.isSuccessful) {
                            val raw = it.body?.string().orEmpty()
                            val detail = try { JSONObject(raw).optString("detail") } catch (_: Exception) { "" }
                            throw IOException(detail.ifBlank { "Agent event stream returned HTTP ${it.code}" })
                        }
                        val source = it.body?.source() ?: throw IOException("Agent event stream body is empty")
                        val parser = SseDataParser()
                        while (!call.isCanceled()) {
                            val line = source.readUtf8Line() ?: break
                            val payload = parser.acceptLine(line) ?: continue
                            val item = try { JSONObject(payload) } catch (error: Exception) {
                                throw IOException("Agent event stream returned invalid JSON", error)
                            }
                            onEvent(AgentTaskEvent(
                                item.optLong("id"),
                                item.optDouble("created_at"),
                                item.optString("kind"),
                                item.optString("message")
                            ))
                        }
                        parser.finish()?.let { payload ->
                            val item = try { JSONObject(payload) } catch (error: Exception) {
                                throw IOException("Agent event stream returned invalid JSON", error)
                            }
                            onEvent(AgentTaskEvent(item.optLong("id"), item.optDouble("created_at"),
                                item.optString("kind"), item.optString("message")))
                        }
                    } catch (error: Exception) {
                        if (!call.isCanceled()) {
                            onError(if (error is IOException) error else IOException("Agent event stream failed", error))
                        }
                    } finally {
                        onClosed()
                    }
                }
            }
        })
        return call
    }

    fun githubStatus(): NexusGitHubIntegration {
        val obj = request("GET", listOf("v1", "github", "status"))
        val connected = obj.optBoolean("connected")
        val item = obj.optJSONObject("integration")
        return NexusGitHubIntegration(
            connected,
            item?.optString("repo_full_name").orEmpty(),
            item?.optString("github_login").orEmpty(),
            item?.optString("default_branch").orEmpty()
        )
    }

    fun connectGitHub(repoFullName: String, token: String): NexusGitHubIntegration {
        require(base.isHttps) { "GitHub credentials may only be sent to an HTTPS NEXUS server" }
        val repositoryParts = repoFullName.split("/", limit = 3)
        require(repositoryParts.size == 2 && repositoryParts.all { part ->
            part.matches(Regex("[A-Za-z0-9_.-]+")) && part != "." && part != ".."
        }) {
            "GitHub repository must use owner/repository format"
        }
        require(token.isNotBlank()) { "GitHub token is missing" }
        val obj = request("POST", listOf("v1", "github", "connect"),
            JSONObject().put("repo_full_name", repoFullName.trim()).put("token", token.trim()))
        return NexusGitHubIntegration(true, obj.optString("repo_full_name"),
            obj.optString("github_login"), obj.optString("default_branch"))
    }

    fun disconnectGitHub() {
        request("DELETE", listOf("v1", "github", "connect"))
    }

    fun listTasks(limit: Int = 30): List<AgentTaskSnapshot> {
        val tasks = request("GET", listOf("v1", "tasks"), query = mapOf("limit" to limit.coerceIn(1, 100).toString()))
            .optJSONArray("tasks") ?: JSONArray()
        return (0 until tasks.length()).mapNotNull { index ->
            val item = tasks.optJSONObject(index) ?: return@mapNotNull null
            AgentTaskSnapshot(item.optString("id"), item.optString("status"), item.optString("prompt"),
                item.optString("summary"), item.optString("error"), emptyList(), emptyList())
        }
    }

    fun listTaskCheckpoints(taskId: String, afterSequence: Int = 0): Pair<Int, List<AgentTaskCheckpoint>> {
        require(afterSequence >= 0) { "Checkpoint cursor must be non-negative" }
        val obj = request("GET", listOf("v1", "tasks", taskId, "checkpoints"),
            query = mapOf("after_sequence" to afterSequence.toString()))
        val items = obj.optJSONArray("checkpoints") ?: JSONArray()
        val checkpoints = (0 until items.length()).mapNotNull { index ->
            val item = items.optJSONObject(index) ?: return@mapNotNull null
            fun files(key: String, project: Boolean = false): List<AgentCheckpointFile> {
                val array = item.optJSONArray(key) ?: JSONArray()
                return (0 until array.length()).mapNotNull { fileIndex ->
                    val file = array.optJSONObject(fileIndex) ?: return@mapNotNull null
                    AgentCheckpointFile(
                        path = file.optString(if (key == "artifacts") "name" else "path"),
                        bytes = file.optLong(if (key == "artifacts") "bytes" else if (project) "size_bytes" else "bytes"),
                        sha256 = file.optString("sha256"),
                        revision = if (file.has("revision")) file.optInt("revision") else null,
                        contentType = file.optString("content_type"),
                        isBinary = file.optBoolean("is_binary")
                    )
                }
            }
            val artifactFiles = files("artifacts")
            AgentTaskCheckpoint(
                id = item.optString("id"), sequence = item.optInt("sequence"),
                stepId = item.optString("step_id"), reason = item.optString("reason"),
                createdAt = item.optDouble("created_at"), archiveBytes = item.optLong("archive_bytes"),
                archiveSha256 = item.optString("archive_sha256"), isLatest = item.optBoolean("is_latest"),
                planSteps = item.optInt("plan_steps"), completedSteps = item.optInt("completed_steps"),
                workspaceFiles = files("workspace_files"), projectFiles = files("project_files", project = true),
                artifacts = artifactFiles.map { AgentArtifact(it.path, it.bytes, it.contentType, it.sha256) },
                legacy = item.optBoolean("legacy"), available = item.optBoolean("available", true),
                error = item.optString("error")
            )
        }
        return obj.optInt("latest_sequence") to checkpoints
    }

    fun restoreTaskCheckpoint(taskId: String, checkpointId: String, expectedLatestSequence: Int): String {
        require(expectedLatestSequence >= 1) { "Checkpoint history is empty" }
        return request("POST", listOf("v1", "tasks", taskId, "checkpoints", checkpointId, "restore"),
            JSONObject().put("expected_latest_sequence", expectedLatestSequence))["status"]?.toString().orEmpty()
    }

    fun cancelTask(taskId: String): String =
        request("POST", listOf("v1", "tasks", taskId, "cancel"), JSONObject())["status"]?.toString().orEmpty()

    fun pauseTask(taskId: String): String =
        request("POST", listOf("v1", "tasks", taskId, "pause"), JSONObject())["status"]?.toString().orEmpty()

    fun resumeTask(taskId: String): String =
        request("POST", listOf("v1", "tasks", taskId, "resume"), JSONObject())["status"]?.toString().orEmpty()

    fun listWorkspace(taskId: String, path: String = "."): List<AgentWorkspaceEntry> {
        val obj = request("GET", listOf("v1", "tasks", taskId, "workspace"), query = mapOf("path" to path))
        val entries = obj.optJSONArray("entries") ?: JSONArray()
        return (0 until entries.length()).mapNotNull { index ->
            val item = entries.optJSONObject(index) ?: return@mapNotNull null
            AgentWorkspaceEntry(item.optString("name"), item.optString("type"),
                if (item.isNull("bytes")) null else item.optLong("bytes"))
        }
    }

    fun readWorkspaceFile(taskId: String, path: String): String =
        request("GET", listOf("v1", "tasks", taskId, "workspace", "file"), query = mapOf("path" to path)).optString("content")

    fun writeWorkspaceFile(taskId: String, path: String, content: String): Long =
        request("PUT", listOf("v1", "tasks", taskId, "workspace", "file"), JSONObject().put("path", path).put("content", content)).optLong("bytes")
    fun listArtifacts(taskId: String): List<AgentArtifact> {
        val items = request("GET", listOf("v1", "tasks", taskId, "artifacts"))
            .optJSONArray("artifacts") ?: JSONArray()
        return (0 until items.length()).mapNotNull { index ->
            val item = items.optJSONObject(index) ?: return@mapNotNull null
            AgentArtifact(item.optString("name"), item.optLong("bytes"),
                item.optString("content_type", "application/octet-stream"), item.optString("sha256"))
        }
    }

    fun readArtifact(taskId: String, name: String): String =
        request("GET", listOf("v1", "tasks", taskId, "artifacts", "file"),
            query = mapOf("name" to name)).optString("content")

    fun downloadArtifact(taskId: String, name: String, output: OutputStream): Long {
        val builder = base.newBuilder()
        listOf("v1", "tasks", taskId, "artifacts", "download").forEach { builder.addPathSegment(it) }
        builder.addQueryParameter("name", name)
        val request = Request.Builder().url(builder.build())
            .header("Authorization", "Bearer $token")
            .header("Accept", "application/octet-stream")
            .get().build()
        val downloadHttp = http.newBuilder()
            .readTimeout(0, TimeUnit.MILLISECONDS)
            .callTimeout(0, TimeUnit.MILLISECONDS)
            .build()
        downloadHttp.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                val raw = response.body?.string().orEmpty()
                val detail = try { JSONObject(raw).optString("detail") } catch (_: Exception) { "" }
                throw IOException(detail.ifBlank { "Artifact download returned HTTP ${response.code}" })
            }
            val input = response.body?.byteStream() ?: throw IOException("Artifact download body is empty")
            var copied = 0L
            val buffer = ByteArray(64 * 1024)
            input.use { source ->
                while (true) {
                    val count = source.read(buffer)
                    if (count < 0) break
                    output.write(buffer, 0, count)
                    copied += count
                }
            }
            output.flush()
            return copied
        }
    }

    fun listProjects(): List<NexusProject> {
        val items = request("GET", listOf("v1", "projects")).optJSONArray("projects") ?: JSONArray()
        return (0 until items.length()).mapNotNull { index ->
            val item = items.optJSONObject(index) ?: return@mapNotNull null
            NexusProject(item.optString("id"), item.optString("name"), item.optString("description"),
                item.optString("context"), item.optString("status"), item.optInt("task_count"))
        }
    }

    fun createProject(name: String, description: String, context: String): NexusProject {
        val body = JSONObject().put("name", name.trim()).put("description", description.trim()).put("context", context.trim())
        val item = request("POST", listOf("v1", "projects"), body).optJSONObject("project")
            ?: throw IOException("Projects API did not return a record")
        return item.toNexusProject()
    }

    fun updateProject(project: NexusProject): NexusProject {
        val body = JSONObject().put("name", project.name).put("description", project.description)
            .put("context", project.context).put("status", project.status)
        val item = request("PATCH", listOf("v1", "projects", project.id), body).optJSONObject("project")
            ?: throw IOException("Projects API did not return an updated record")
        return item.toNexusProject()
    }

    fun deleteProject(id: String) { request("DELETE", listOf("v1", "projects", id)) }

    fun listProjectFiles(projectId: String): List<NexusProjectFile> {
        val items = request("GET", listOf("v1", "projects", projectId, "files"))
            .optJSONArray("files") ?: JSONArray()
        return (0 until items.length()).mapNotNull { items.optJSONObject(it)?.toNexusProjectFile() }
    }

    fun readProjectFile(projectId: String, path: String): NexusProjectFileContent {
        val response = request("GET", listOf("v1", "projects", projectId, "files", "content"),
            query = mapOf("path" to path))
        val item = response
        return NexusProjectFileContent(item.toNexusProjectFile(), item.optString("content"))
    }

    fun writeProjectFile(projectId: String, path: String, content: String,
                         message: String = "Updated from NEXUS Android"): NexusProjectFile {
        require(content.toByteArray(Charsets.UTF_8).size <= 2 * 1024 * 1024) {
            "Project text files are limited to 2 MiB"
        }
        val body = JSONObject().put("path", path).put("content", content).put("message", message)
        return request("PUT", listOf("v1", "projects", projectId, "files"), body)
            .optJSONObject("file")?.toNexusProjectFile()
            ?: throw IOException("Project file API did not return a revision")
    }

    fun uploadProjectBinaryFile(projectId: String, path: String, bytes: ByteArray,
                                contentType: String = "application/octet-stream",
                                message: String = "Uploaded from NEXUS Android"): NexusProjectFile {
        require(bytes.size <= 50 * 1024 * 1024) { "Project files are limited to 50 MiB per revision" }
        val url = base.newBuilder().addPathSegment("v1").addPathSegment("projects")
            .addPathSegment(projectId).addPathSegment("files").addPathSegment("binary")
            .addQueryParameter("path", path).addQueryParameter("message", message).build()
        val request = Request.Builder().url(url).header("Authorization", "Bearer $token")
            .header("Accept", "application/json")
            .put(bytes.toRequestBody(contentType.toMediaType())).build()
        http.newCall(request).execute().use { response ->
            val raw = response.body?.string().orEmpty()
            if (!response.isSuccessful) {
                val detail = try { JSONObject(raw).optString("detail") } catch (_: Exception) { "" }
                throw IOException(detail.ifBlank { "Binary upload returned HTTP ${response.code}" })
            }
            return try { JSONObject(raw).optJSONObject("file")?.toNexusProjectFile()
                ?: throw IOException("Binary upload returned no file revision") }
            catch (error: Exception) { if (error is IOException) throw error else throw IOException("Invalid binary upload response", error) }
        }
    }

    fun downloadProjectBinaryFile(projectId: String, path: String): ByteArray {
        val url = base.newBuilder().addPathSegment("v1").addPathSegment("projects")
            .addPathSegment(projectId).addPathSegment("files").addPathSegment("binary")
            .addQueryParameter("path", path).build()
        val request = Request.Builder().url(url).header("Authorization", "Bearer $token")
            .header("Accept", "application/octet-stream").get().build()
        http.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                val raw = response.body?.string().orEmpty()
                val detail = try { JSONObject(raw).optString("detail") } catch (_: Exception) { "" }
                throw IOException(detail.ifBlank { "Binary download returned HTTP ${response.code}" })
            }
            val body = response.body ?: throw IOException("Binary download body is empty")
            if (body.contentLength() > 50L * 1024 * 1024) throw IOException("Downloaded file exceeds 50 MiB")
            val output = java.io.ByteArrayOutputStream()
            body.byteStream().use { input ->
                val buffer = ByteArray(64 * 1024)
                while (true) {
                    val count = input.read(buffer)
                    if (count < 0) break
                    if (output.size() + count > 50 * 1024 * 1024) throw IOException("Downloaded file exceeds 50 MiB")
                    output.write(buffer, 0, count)
                }
            }
            return output.toByteArray()
        }
    }

    fun listProjectFileVersions(projectId: String, path: String): List<NexusProjectFile> {
        val items = request("GET", listOf("v1", "projects", projectId, "files", "versions"),
            query = mapOf("path" to path)).optJSONArray("versions") ?: JSONArray()
        return (0 until items.length()).mapNotNull { items.optJSONObject(it)?.toNexusProjectFile() }
    }

    fun restoreProjectFile(projectId: String, path: String, revision: Int): NexusProjectFile {
        val body = JSONObject().put("revision", revision).put("message", "Restored from Android")
        return request("POST", listOf("v1", "projects", projectId, "files", "restore"),
            body, mapOf("path" to path)).optJSONObject("file")?.toNexusProjectFile()
            ?: throw IOException("Project restore API did not return a revision")
    }

    private fun JSONObject.toNexusProjectFile() = NexusProjectFile(
        optString("path"), optInt("revision"), optLong("bytes"), optString("sha256"),
        optString("message"), optDouble("created_at"),
        optString("content_type", "text/plain; charset=utf-8"), optBoolean("is_binary")
    )

    private fun JSONObject.toNexusProject() = NexusProject(
        optString("id"), optString("name"), optString("description"), optString("context"),
        optString("status"), optInt("task_count")
    )

    fun listSkills(query: String = "", limit: Int = 100): List<NexusSkill> {
        val params = mutableMapOf("limit" to limit.coerceIn(1, 100).toString())
        if (query.isNotBlank()) params["q"] = query.take(200)
        val items = request("GET", listOf("v1", "skills"), query = params).optJSONArray("skills") ?: JSONArray()
        return (0 until items.length()).mapNotNull { index ->
            val item = items.optJSONObject(index) ?: return@mapNotNull null
            item.toNexusSkill()
        }
    }

    fun createSkill(name: String, description: String, instructions: String, category: String): NexusSkill {
        val body = JSONObject().put("name", name.trim()).put("description", description.trim())
            .put("instructions", instructions.trim()).put("category", category).put("enabled", true)
        val item = request("POST", listOf("v1", "skills"), body).optJSONObject("skill")
            ?: throw IOException("Skills API did not return a record")
        return item.toNexusSkill()
    }

    fun updateSkill(skill: NexusSkill): NexusSkill {
        val body = JSONObject().put("name", skill.name).put("description", skill.description)
            .put("instructions", skill.instructions).put("category", skill.category).put("enabled", skill.enabled)
        val item = request("PATCH", listOf("v1", "skills", skill.id), body).optJSONObject("skill")
            ?: throw IOException("Skills API did not return an updated record")
        return item.toNexusSkill()
    }

    fun deleteSkill(id: String) { request("DELETE", listOf("v1", "skills", id)) }

    private fun JSONObject.toNexusSkill() = NexusSkill(
        optString("id"), optString("name"), optString("description"),
        optString("instructions"), optString("category"), optBoolean("enabled")
    )

    fun suggestMemories(userMessages: List<String>): List<MemorySuggestion> {
        val messages = JSONArray()
        userMessages.takeLast(20).forEach { text ->
            if (text.isNotBlank()) messages.put(JSONObject().put("role", "user").put("content", text.take(4000)))
        }
        val items = request("POST", listOf("v1", "memories", "suggest"),
            JSONObject().put("messages", messages)).optJSONArray("suggestions") ?: JSONArray()
        return (0 until items.length()).mapNotNull { index ->
            val item = items.optJSONObject(index) ?: return@mapNotNull null
            val title = item.optString("title").trim()
            val content = item.optString("content").trim()
            val category = item.optString("category", "fact")
            if (title.isBlank() || content.isBlank()) null else MemorySuggestion(title, content, category)
        }
    }

    fun listAutomations(): List<NexusAutomation> {
        val items = request("GET", listOf("v1", "automations")).optJSONArray("automations") ?: JSONArray()
        return (0 until items.length()).mapNotNull { index ->
            val item = items.optJSONObject(index) ?: return@mapNotNull null
            val days = item.optJSONArray("weekdays") ?: JSONArray()
            NexusAutomation(
                item.optString("id"), item.optString("name"), item.optString("prompt"),
                item.optString("schedule_type"), item.optString("local_time"), item.optString("timezone"),
                (0 until days.length()).map { days.optInt(it) },
                if (item.isNull("run_at")) null else item.optDouble("run_at"),
                if (item.isNull("interval_minutes")) null else item.optInt("interval_minutes"),
                item.optBoolean("enabled"), if (item.isNull("next_run_at")) null else item.optDouble("next_run_at"),
                item.optString("project_id").takeIf { it.isNotBlank() && it != "null" },
                if (item.isNull("last_run_at")) null else item.optDouble("last_run_at"),
                item.optString("last_task_status").takeIf { it.isNotBlank() && it != "null" },
                item.optString("last_task_summary").takeIf { it.isNotBlank() && it != "null" }
            )
        }
    }

    fun listAutomationRuns(id: String, limit: Int = 30): List<NexusAutomationRun> {
        val items = request("GET", listOf("v1", "automations", id, "runs"),
            query = mapOf("limit" to limit.coerceIn(1, 100).toString())).optJSONArray("runs") ?: JSONArray()
        return (0 until items.length()).mapNotNull { index ->
            val item = items.optJSONObject(index) ?: return@mapNotNull null
            NexusAutomationRun(item.optString("id"), item.optString("task_id"),
                item.optDouble("scheduled_for"), item.optString("task_status"),
                item.optString("task_summary"), item.optString("task_error"))
        }
    }

    fun createAutomation(
        name: String, prompt: String, scheduleType: String, localTime: String,
        timezone: String, weekdays: List<Int>, intervalMinutes: Int?, projectId: String? = null, runAt: Double? = null
    ): NexusAutomation {
        val body = JSONObject().put("name", name.trim()).put("prompt", prompt.trim())
            .put("schedule_type", scheduleType).put("local_time", localTime)
            .put("timezone", timezone).put("weekdays", JSONArray(weekdays))
        if (intervalMinutes != null) body.put("interval_minutes", intervalMinutes)
        if (runAt != null) body.put("run_at", runAt)
        projectId?.let { body.put("project_id", it) }
        val item = request("POST", listOf("v1", "automations"), body).optJSONObject("automation")
            ?: throw IOException("Automation API did not return a record")
        return item.toAutomation()
    }

    fun updateAutomation(automation: NexusAutomation): NexusAutomation {
        val body = JSONObject().put("name", automation.name).put("prompt", automation.prompt)
            .put("schedule_type", automation.scheduleType).put("local_time", automation.localTime)
            .put("timezone", automation.timezone).put("weekdays", JSONArray(automation.weekdays))
            .put("enabled", automation.enabled)
        automation.runAt?.let { body.put("run_at", it) }
        automation.projectId?.let { body.put("project_id", it) }
        automation.intervalMinutes?.let { body.put("interval_minutes", it) }
        val item = request("PUT", listOf("v1", "automations", automation.id), body).optJSONObject("automation")
            ?: throw IOException("Automation API did not return an updated record")
        return item.toAutomation()
    }

    fun pauseAutomation(id: String) { request("POST", listOf("v1", "automations", id, "pause"), JSONObject()) }
    fun resumeAutomation(id: String) { request("POST", listOf("v1", "automations", id, "resume"), JSONObject()) }
    fun runAutomationNow(id: String): String =
        request("POST", listOf("v1", "automations", id, "run"), JSONObject()).optString("task_id")
    fun deleteAutomation(id: String) { request("DELETE", listOf("v1", "automations", id)) }

    private fun JSONObject.toAutomation(): NexusAutomation {
        val days = optJSONArray("weekdays") ?: JSONArray()
        return NexusAutomation(
            optString("id"), optString("name"), optString("prompt"), optString("schedule_type"),
            optString("local_time"), optString("timezone"),
            (0 until days.length()).map { days.optInt(it) },
            if (isNull("run_at")) null else optDouble("run_at"),
            if (isNull("interval_minutes")) null else optInt("interval_minutes"),
            optBoolean("enabled"), if (isNull("next_run_at")) null else optDouble("next_run_at"),
            optString("project_id").takeIf { it.isNotBlank() && it != "null" },
            if (isNull("last_run_at")) null else optDouble("last_run_at"),
            optString("last_task_status").takeIf { it.isNotBlank() && it != "null" },
            optString("last_task_summary").takeIf { it.isNotBlank() && it != "null" }
        )
    }

    fun listMemories(query: String = "", limit: Int = 100): List<NexusMemory> {
        val params = buildMap {
            put("limit", limit.coerceIn(1, 100).toString())
            if (query.isNotBlank()) put("q", query.take(200))
        }
        val items = request("GET", listOf("v1", "memories"), query = params)
            .optJSONArray("memories") ?: JSONArray()
        return (0 until items.length()).mapNotNull { index ->
            val item = items.optJSONObject(index) ?: return@mapNotNull null
            NexusMemory(item.optString("id"), item.optString("title"), item.optString("content"),
                item.optString("category", "general"), item.optBoolean("pinned"),
                item.optDouble("updated_at"))
        }
    }

    fun createMemory(title: String, content: String, category: String = "general", pinned: Boolean = false): NexusMemory {
        val obj = request("POST", listOf("v1", "memories"),
            JSONObject().put("title", title.trim()).put("content", content.trim())
                .put("category", category).put("pinned", pinned)).optJSONObject("memory")
            ?: throw IOException("Memory API did not return a record")
        return obj.toMemory()
    }

    fun updateMemory(memoryId: String, title: String, content: String, category: String, pinned: Boolean): NexusMemory {
        val obj = request("PATCH", listOf("v1", "memories", memoryId),
            JSONObject().put("title", title.trim()).put("content", content.trim())
                .put("category", category).put("pinned", pinned)).optJSONObject("memory")
            ?: throw IOException("Memory API did not return an updated record")
        return obj.toMemory()
    }

    fun deleteMemory(memoryId: String) {
        request("DELETE", listOf("v1", "memories", memoryId))
    }

    fun listApprovals(): List<AgentApproval> {
        val approvals = request("GET", listOf("v1", "approvals")).optJSONArray("approvals") ?: JSONArray()
        return (0 until approvals.length()).mapNotNull { index ->
            val item = approvals.optJSONObject(index) ?: return@mapNotNull null
            AgentApproval(item.optString("id"), item.optString("task_id"),
                item.optString("description"), item.optString("status"))
        }
    }

    fun decideApproval(approvalId: String, approved: Boolean, note: String = ""): String =
        request("POST", listOf("v1", "approvals", approvalId),
            JSONObject().put("approved", approved).put("note", note.take(1000)))["status"]?.toString().orEmpty()

    private fun request(
        method: String,
        segments: List<String>,
        body: JSONObject? = null,
        query: Map<String, String> = emptyMap()
    ): JSONObject {
        val builder = base.newBuilder()
        segments.forEach { segment -> builder.addPathSegment(segment) }
        query.forEach { (key, value) -> builder.addQueryParameter(key, value) }
        val requestBuilder = Request.Builder().url(builder.build())
            .header("Authorization", "Bearer $token")
            .header("Accept", "application/json")
        when (method) {
            "GET" -> requestBuilder.get()
            "POST" -> requestBuilder.post((body ?: JSONObject()).toString().toRequestBody(jsonType))
            "PUT" -> requestBuilder.put((body ?: JSONObject()).toString().toRequestBody(jsonType))
            "PATCH" -> requestBuilder.patch((body ?: JSONObject()).toString().toRequestBody(jsonType))
            "DELETE" -> requestBuilder.delete()
            else -> throw IllegalArgumentException("Unsupported HTTP method")
        }
        http.newCall(requestBuilder.build()).execute().use { response ->
            val raw = response.body?.string().orEmpty()
            if (!response.isSuccessful) {
                val detail = try { JSONObject(raw).optString("detail") } catch (_: Exception) { "" }
                throw IOException(detail.ifBlank { "Agent API returned HTTP ${response.code}" })
            }
            return try { JSONObject(raw) } catch (e: Exception) {
                throw IOException("Agent API returned invalid JSON", e)
            }
        }
    }

    private fun JSONObject.toMemory(): NexusMemory = NexusMemory(
        optString("id"), optString("title"), optString("content"),
        optString("category", "general"), optBoolean("pinned"), optDouble("updated_at")
    )

    private fun JSONArray?.toEventList(): List<AgentTaskEvent> {
        val array = this ?: return emptyList()
        return (0 until array.length()).mapNotNull { index ->
            val item = array.optJSONObject(index) ?: return@mapNotNull null
            AgentTaskEvent(item.optLong("id"), item.optDouble("created_at"),
                item.optString("kind"), item.optString("message"))
        }
    }

    private fun JSONArray?.toStepList(): List<AgentTaskStep> {
        val array = this ?: return emptyList()
        return (0 until array.length()).mapNotNull { index ->
            val item = array.optJSONObject(index) ?: return@mapNotNull null
            AgentTaskStep(item.optInt("step_index"), item.optString("title"), item.optString("action"),
                item.optString("status"), item.optString("error"))
        }
    }
}
