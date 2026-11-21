package ai.nexus.max

import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.HttpUrl.Companion.toHttpUrl
import org.json.JSONArray
import org.json.JSONObject
import java.io.IOException
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

data class AgentTaskEvent(val createdAt: Double, val kind: String, val message: String)
data class AgentTaskStep(val index: Int, val title: String, val action: String, val status: String, val error: String)

data class AgentApproval(val id: String, val taskId: String, val description: String, val status: String)

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

    fun createTask(prompt: String, maxSteps: Int = 8, idempotencyKey: String = UUID.randomUUID().toString()): String {
        require(prompt.trim().length >= 3) { "Task prompt is too short" }
        val body = JSONObject()
            .put("prompt", prompt.trim())
            .put("autonomy", "level3")
            .put("max_steps", maxSteps.coerceIn(1, 30))
            .put("idempotency_key", idempotencyKey)
        return request("POST", listOf("v1", "tasks"), body)["id"]?.toString()
            ?: throw IOException("Agent API did not return a task ID")
    }

    fun getTask(taskId: String): AgentTaskSnapshot {
        val obj = request("GET", listOf("v1", "tasks", taskId))
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

    fun listTasks(limit: Int = 30): List<AgentTaskSnapshot> {
        val tasks = request("GET", listOf("v1", "tasks"), query = mapOf("limit" to limit.coerceIn(1, 100).toString()))
            .optJSONArray("tasks") ?: JSONArray()
        return (0 until tasks.length()).mapNotNull { index ->
            val item = tasks.optJSONObject(index) ?: return@mapNotNull null
            AgentTaskSnapshot(item.optString("id"), item.optString("status"), item.optString("prompt"),
                item.optString("summary"), item.optString("error"), emptyList(), emptyList())
        }
    }

    fun cancelTask(taskId: String): String =
        request("POST", listOf("v1", "tasks", taskId, "cancel"), JSONObject())["status"]?.toString().orEmpty()

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
        segments.forEach(builder::addPathSegment)
        query.forEach { (key, value) -> builder.addQueryParameter(key, value) }
        val requestBuilder = Request.Builder().url(builder.build())
            .header("Authorization", "Bearer $token")
            .header("Accept", "application/json")
        when (method) {
            "GET" -> requestBuilder.get()
            "POST" -> requestBuilder.post((body ?: JSONObject()).toString().toRequestBody(jsonType))
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

    private fun JSONArray?.toEventList(): List<AgentTaskEvent> {
        val array = this ?: return emptyList()
        return (0 until array.length()).mapNotNull { index ->
            val item = array.optJSONObject(index) ?: return@mapNotNull null
            AgentTaskEvent(item.optDouble("created_at"), item.optString("kind"), item.optString("message"))
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
