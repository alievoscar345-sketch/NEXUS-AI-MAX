package ai.nexus.max

import android.app.Application
import android.app.Activity
import android.content.Intent
import android.speech.RecognizerIntent
import android.speech.tts.TextToSpeech
import android.os.Bundle
import android.net.Uri
import android.provider.OpenableColumns
import android.util.Base64
import java.io.File
import java.io.FileOutputStream
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.BasicTextField
import androidx.compose.material3.*
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Mic
import androidx.compose.material.icons.filled.VolumeUp
import androidx.compose.material.icons.filled.Stop
import androidx.compose.material.icons.filled.PushPin
import androidx.compose.material.icons.filled.Search
import androidx.compose.material.icons.filled.Share
import androidx.compose.material.icons.filled.MoreVert
import androidx.compose.runtime.*
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.AnnotatedString
import androidx.compose.ui.platform.LocalClipboardManager
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.compose.ui.window.Dialog
import androidx.compose.ui.window.DialogProperties
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import androidx.lifecycle.viewmodel.compose.viewModel
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONArray
import org.json.JSONObject
import java.util.UUID
import java.util.Locale
import java.util.concurrent.TimeUnit
import com.mikepenz.markdown.m3.Markdown
import com.mikepenz.markdown.compose.components.markdownComponents
import com.mikepenz.markdown.compose.elements.MarkdownHighlightedCodeBlock
import com.mikepenz.markdown.compose.elements.MarkdownHighlightedCodeFence
import org.intellij.markdown.ast.ASTNode

private val Ink = Color(0xFF050505)
private val BackgroundGlow = Color(0xFF26090E)
private val AccentRed = Color(0xFFD6293E)
private val Panel = Color(0xFF171717)
private val PanelRaised = Color(0xFF242222)
private val Violet = Color(0xFF87313A)
private val Blue = Color(0xFFB9B5B5)
private val MainText = Color(0xFFF0EEEE)
private val Muted = Color(0xFF969090)
private val AccentSoft = Color(0xFF5D252B)
private val DividerTone = Color(0xFF30282A)

data class ChatAttachment(
    val name: String,
    val mimeType: String,
    val localPath: String = "",
    val textContent: String = ""
)
data class ChatLine(
    val role: String,
    val content: String,
    val attachments: List<ChatAttachment> = emptyList(),
    val providerMessagesJson: String? = null
)
data class WebSearchAnswer(val text: String, val providerMessagesJson: String)
data class ChatSession(
    val id: String,
    val title: String,
    val updatedAt: Long,
    val lines: List<ChatLine>,
    val isPinned: Boolean = false
)
data class ApiSettings(
    val baseUrl: String = "https://anymodel.org/v1",
    val model: String = "claude-fable-5-2",
    val apiKey: String = "",
    val provider: String = "AnyModel",
    val openRouterModel: String = "",
    val openRouterApiKey: String = ""
)
data class OpenRouterModel(
    val id: String,
    val name: String,
    val description: String,
    val contextLength: Int,
    val promptPrice: String,
    val completionPrice: String,
    val isFree: Boolean
)

class NexusViewModel(application: Application) : AndroidViewModel(application) {
    private val prefs = application.getSharedPreferences("nexus_local", 0)
    var settings by mutableStateOf(
        ApiSettings(
            baseUrl = prefs.getString("base_url", "https://anymodel.org/v1") ?: "https://anymodel.org/v1",
            model = prefs.getString("model_id", "claude-fable-5-2") ?: "claude-fable-5-2",
            provider = prefs.getString("provider", "AnyModel") ?: "AnyModel",
            openRouterModel = prefs.getString("openrouter_model", "") ?: ""
        )
    )
        private set

    var chats by mutableStateOf(loadChats())
        private set
    var pendingAttachments by mutableStateOf<List<ChatAttachment>>(emptyList())
        private set
    var activeChatId by mutableStateOf(chats.firstOrNull()?.id)
        private set
    var busy by mutableStateOf(false)
        private set
    var error by mutableStateOf<String?>(null)
        private set
    var openRouterModels by mutableStateOf<List<OpenRouterModel>>(emptyList())
        private set
    var loadingOpenRouterModels by mutableStateOf(false)
        private set

    val activeChat: ChatSession?
        get() = chats.firstOrNull { it.id == activeChatId }

    private val client = OkHttpClient.Builder()
        .connectTimeout(25, TimeUnit.SECONDS)
        .readTimeout(120, TimeUnit.SECONDS)
        .build()

    init {
        if (chats.isEmpty()) createChat()
    }

    fun updateSettings(url: String, model: String, key: String) {
        val cleanUrl = url.trim().trimEnd('/')
        val cleanModel = model.trim()
        settings = settings.copy(baseUrl = cleanUrl, model = cleanModel, apiKey = key.trim())
        prefs.edit().putString("base_url", cleanUrl).putString("model_id", cleanModel).apply()
        error = null
    }

    fun updateProviderSettings(provider: String, openRouterModel: String, openRouterKey: String) {
        settings = settings.copy(provider = provider, openRouterModel = openRouterModel.trim(), openRouterApiKey = openRouterKey.trim())
        prefs.edit().putString("provider", provider).putString("openrouter_model", openRouterModel.trim()).apply()
        error = null
    }

    fun loadOpenRouterModels(apiKey: String) {
        if (apiKey.isBlank()) { error = "Введи OpenRouter API key перед загрузкой каталога."; return }
        if (loadingOpenRouterModels) return
        loadingOpenRouterModels = true
        viewModelScope.launch {
            try {
                val loaded = withContext(Dispatchers.IO) {
                    val request = Request.Builder()
                        .url("https://openrouter.ai/api/v1/models?endpoints=%2Fapi%2Fv1%2Fchat%2Fcompletions&output_modalities=all")
                        .header("Authorization", "Bearer " + apiKey.trim())
                        .header("Accept", "application/json")
                        .get().build()
                    client.newCall(request).execute().use { response ->
                        val raw = response.body?.string().orEmpty()
                        if (!response.isSuccessful) throw IllegalStateException("OpenRouter models API " + response.code + ": " + raw.take(250))
                        val data = JSONObject(raw).optJSONArray("data") ?: throw IllegalStateException("OpenRouter вернул каталог без списка data.")
                        (0 until data.length()).mapNotNull { index ->
                            val item = data.optJSONObject(index) ?: return@mapNotNull null
                            val id = item.optString("id").takeIf { it.isNotBlank() } ?: return@mapNotNull null
                            val pricing = item.optJSONObject("pricing") ?: JSONObject()
                            val prompt = pricing.optString("prompt", "unknown")
                            val completion = pricing.optString("completion", "unknown")
                            OpenRouterModel(id, item.optString("name", id), item.optString("description", ""), item.optInt("context_length", 0), prompt, completion, prompt.toDoubleOrNull() == 0.0 && completion.toDoubleOrNull() == 0.0)
                        }.sortedWith(compareByDescending<OpenRouterModel> { it.isFree }.thenBy { it.name.lowercase() })
                    }
                }
                openRouterModels = loaded
                if (settings.openRouterModel.isBlank() && loaded.isNotEmpty()) {
                    settings = settings.copy(openRouterModel = (loaded.firstOrNull { it.isFree } ?: loaded.first()).id)
                }
                error = if (loaded.isEmpty()) "OpenRouter вернул пустой каталог моделей." else null
            } catch (e: Exception) {
                error = e.message ?: "Не удалось загрузить модели OpenRouter."
            } finally { loadingOpenRouterModels = false }
        }
    }

    fun createChat() {
        val chat = ChatSession(UUID.randomUUID().toString(), "Новый диалог", System.currentTimeMillis(), emptyList())
        chats = (listOf(chat) + chats).sortedWith(compareByDescending<ChatSession> { it.isPinned }.thenByDescending { it.updatedAt })
        activeChatId = chat.id
        saveChats()
        error = null
    }

    fun selectChat(id: String) {
        activeChatId = id
        error = null
    }

    fun deleteChat(id: String) {
        chats = chats.filterNot { it.id == id }
        if (activeChatId == id) activeChatId = chats.firstOrNull()?.id
        if (chats.isEmpty()) createChat() else saveChats()
        error = null
    }

    fun togglePinChat(id: String) {
        chats = chats.map { if (it.id == id) it.copy(isPinned = !it.isPinned) else it }.sortedWith(compareByDescending<ChatSession> { it.isPinned }.thenByDescending { it.updatedAt })
        saveChats()
    }

    fun renameChat(id: String, title: String) {
        val clean = title.trim().take(60)
        if (clean.isEmpty()) return
        chats = chats.map { if (it.id == id) it.copy(title = clean, updatedAt = System.currentTimeMillis()) else it }
        saveChats()
    }

    fun addAttachment(uri: Uri) {
        viewModelScope.launch(Dispatchers.IO) {
            try {
                val resolver = getApplication<Application>().contentResolver
                val mime = resolver.getType(uri) ?: "application/octet-stream"
                val name = resolver.query(uri, arrayOf(OpenableColumns.DISPLAY_NAME), null, null, null)?.use { cursor ->
                    if (cursor.moveToFirst()) cursor.getString(0) else null
                } ?: "attachment"
                val attachment = if (mime.startsWith("image/")) {
                    val bytes = resolver.openInputStream(uri)?.use { it.readBytes() }
                        ?: throw IllegalStateException("Не удалось прочитать изображение.")
                    if (bytes.size > 8 * 1024 * 1024) throw IllegalStateException("Изображение слишком большое. Максимум 8 МБ.")
                    val directory = File(getApplication<Application>().filesDir, "nexus_attachments").apply { mkdirs() }
                    val file = File(directory, UUID.randomUUID().toString() + ".img")
                    FileOutputStream(file).use { it.write(bytes) }
                    ChatAttachment(name, mime, file.absolutePath, "")
                } else if (mime.startsWith("text/") || mime == "application/json" || mime == "application/xml") {
                    val bytes = resolver.openInputStream(uri)?.use { it.readBytes() }
                        ?: throw IllegalStateException("Не удалось прочитать документ.")
                    if (bytes.size > 1024 * 1024) throw IllegalStateException("Текстовый документ слишком большой. Максимум 1 МБ.")
                    ChatAttachment(name, mime, "", bytes.toString(Charsets.UTF_8))
                } else {
                    throw IllegalArgumentException("Поддерживаются изображения и текстовые файлы TXT, MD, CSV, JSON, XML.")
                }
                withContext(Dispatchers.Main) {
                    pendingAttachments = pendingAttachments + attachment
                    error = null
                }
            } catch (e: Exception) {
                withContext(Dispatchers.Main) { error = e.message ?: "Не удалось добавить файл." }
            }
        }
    }

    fun removePendingAttachment(index: Int) {
        pendingAttachments = pendingAttachments.filterIndexed { i, _ -> i != index }
    }

    fun showError(message: String) {
        error = message
    }

    fun send(text: String, useWebSearch: Boolean = false) {
        if (busy || (text.isBlank() && pendingAttachments.isEmpty())) return
        val cfg = settings
        val activeModel = if (cfg.provider == "OpenRouter") cfg.openRouterModel else cfg.model
        val activeKey = if (cfg.provider == "OpenRouter") cfg.openRouterApiKey else cfg.apiKey
        if (activeKey.isBlank() || activeModel.isBlank()) {
            error = if (cfg.provider == "OpenRouter") "Открой настройки и введи OpenRouter API key, затем выбери модель." else "Открой настройки API и введи ключ."
            return
        }
        if (cfg.provider == "AnyModel" && cfg.baseUrl.isBlank()) { error = "Укажи Base URL провайдера."; return }
        if (useWebSearch && cfg.provider == "OpenRouter") {
            error = "Встроенный поиск сейчас подключён только к AnyModel / Claude. Отключи поиск или переключись на AnyModel."
            return
        }
        if (activeChatId == null) createChat()
        val chatId = activeChatId ?: return
        val oldChat = chats.firstOrNull { it.id == chatId } ?: return
        val userLine = ChatLine("user", text.trim(), pendingAttachments)
        pendingAttachments = emptyList()
        val history = oldChat.lines + userLine
        val newTitle = if (oldChat.lines.isEmpty()) text.trim().replace("\n", " ").take(36).ifBlank { "Новый диалог" } else oldChat.title
        val placeholder = ChatLine("assistant", "")
        updateChat(chatId, oldChat.copy(title = newTitle, updatedAt = System.currentTimeMillis(), lines = history + placeholder))
        busy = true
        error = null
        var streamedAnswer = ""
        var providerMessagesJson: String? = null

        viewModelScope.launch {
            try {
                val answer = if (useWebSearch) {
                    val result = withContext(Dispatchers.IO) { requestWithWebSearch(cfg, history) }
                    providerMessagesJson = result.providerMessagesJson
                    result.text
                } else {
                    withContext(Dispatchers.IO) {
                    val messages = JSONArray().put(
                        JSONObject().put("role", "system").put("content", SYSTEM_PROMPT)
                    )
                    history.takeLast(24).forEach { line ->
                        val imageAttachments = line.attachments.filter { it.mimeType.startsWith("image/") }
                        val textAttachments = line.attachments.filter { it.textContent.isNotBlank() }
                        if (line.role == "user" && (imageAttachments.isNotEmpty() || textAttachments.isNotEmpty())) {
                            val parts = JSONArray()
                            val textPart = buildString {
                                append(line.content)
                                textAttachments.forEach { file ->
                                    append("\n\n[Прикреплённый файл: ")
                                    append(file.name)
                                    append("]\n")
                                    append(file.textContent)
                                }
                            }
                            if (textPart.isNotBlank()) parts.put(JSONObject().put("type", "text").put("text", textPart))
                            imageAttachments.forEach { image ->
                                val bytes = File(image.localPath).readBytes()
                                val encoded = Base64.encodeToString(bytes, Base64.NO_WRAP)
                                val dataUrl = "data:" + image.mimeType + ";base64," + encoded
                                parts.put(
                                    JSONObject().put("type", "image_url")
                                        .put("image_url", JSONObject().put("url", dataUrl))
                                )
                            }
                            messages.put(JSONObject().put("role", line.role).put("content", parts))
                        } else {
                            messages.put(JSONObject().put("role", line.role).put("content", line.content))
                        }
                    }
                    val body = JSONObject()
                        .put("model", activeModel)
                        .put("messages", messages)
                        .put("stream", true)
                    val request = Request.Builder()
                        .url(if (cfg.provider == "OpenRouter") "https://openrouter.ai/api/v1/chat/completions" else cfg.baseUrl + "/chat/completions")
                        .header("Authorization", "Bearer " + activeKey)
                        .header("HTTP-Referer", "https://github.com/alievoscar345-sketch/NEXUS-AI-MAX")
                        .header("X-OpenRouter-Title", "NEXUS AI MAX")
                        .header("Content-Type", "application/json")
                        .post(body.toString().toRequestBody("application/json".toMediaType()))
                        .build()
                    client.newCall(request).execute().use { response ->
                        if (!response.isSuccessful) {
                            val raw = response.body?.string().orEmpty()
                            throw IllegalStateException("API " + response.code + ": " + raw.take(350))
                        }
                        val reader = response.body?.charStream()?.buffered()
                            ?: throw IllegalStateException("Пустой ответ API")
                        reader.use { stream ->
                            while (true) {
                                val rawLine = stream.readLine() ?: break
                                if (!rawLine.startsWith("data:")) continue
                                val payload = rawLine.removePrefix("data:").trim()
                                if (payload == "[DONE]") break
                                if (payload.isBlank()) continue
                                val delta = try {
                                    JSONObject(payload).optJSONArray("choices")
                                        ?.optJSONObject(0)?.optJSONObject("delta")
                                        ?.opt("content")
                                } catch (_: Exception) { null }
                                val chunk = when (delta) {
                                    is String -> delta
                                    is JSONArray -> {
                                        val parts = StringBuilder()
                                        for (i in 0 until delta.length()) {
                                            val part = delta.optJSONObject(i)
                                            if (part != null) parts.append(part.optString("text", ""))
                                        }
                                        parts.toString()
                                    }
                                    else -> ""
                                }
                                if (chunk.isNotEmpty()) {
                                    streamedAnswer += chunk
                                    withContext(Dispatchers.Main) {
                                        updateChatLive(chatId, streamedAnswer)
                                    }
                                }
                            }
                        }
                        streamedAnswer
                    }
                }
                }
                if (answer.isBlank()) throw IllegalStateException("Модель вернула пустой ответ.")
                val latest = chats.firstOrNull { it.id == chatId }
                if (latest != null) updateChat(
                    chatId,
                    latest.copy(
                        lines = latest.lines.dropLast(1) + ChatLine("assistant", answer, providerMessagesJson = providerMessagesJson),
                        updatedAt = System.currentTimeMillis()
                    )
                )
            } catch (e: Exception) {
                val latest = chats.firstOrNull { it.id == chatId }
                if (latest != null) {
                    val lines = latest.lines.dropLast(1)
                    val finalLines = if (streamedAnswer.isBlank()) lines else lines + ChatLine("assistant", streamedAnswer)
                    updateChat(chatId, latest.copy(lines = finalLines, updatedAt = System.currentTimeMillis()))
                }
                error = e.message ?: "Ошибка запроса"
            } finally {
                busy = false
            }
        }
    }


    private fun requestWithWebSearch(cfg: ApiSettings, history: List<ChatLine>): WebSearchAnswer {
        val messages = JSONArray()
        history.takeLast(24).forEach { line ->
            if (line.role == "assistant" && !line.providerMessagesJson.isNullOrBlank()) {
                val preserved = JSONArray(line.providerMessagesJson)
                for (i in 0 until preserved.length()) {
                    messages.put(preserved.getJSONObject(i))
                }
                return@forEach
            }

            val imageAttachments = line.attachments.filter { it.mimeType.startsWith("image/") }
            val textAttachments = line.attachments.filter { it.textContent.isNotBlank() }
            if (line.role == "user" && (imageAttachments.isNotEmpty() || textAttachments.isNotEmpty())) {
                val parts = JSONArray()
                val textPart = buildString {
                    append(line.content)
                    textAttachments.forEach { file ->
                        append("\n\n[Прикреплённый файл: ")
                        append(file.name)
                        append("]\n")
                        append(file.textContent)
                    }
                }
                if (textPart.isNotBlank()) {
                    parts.put(JSONObject().put("type", "text").put("text", textPart))
                }
                imageAttachments.forEach { image ->
                    val bytes = File(image.localPath).readBytes()
                    val encoded = Base64.encodeToString(bytes, Base64.NO_WRAP)
                    parts.put(
                        JSONObject().put("type", "image")
                            .put("source", JSONObject()
                                .put("type", "base64")
                                .put("media_type", image.mimeType)
                                .put("data", encoded))
                    )
                }
                messages.put(JSONObject().put("role", "user").put("content", parts))
            } else {
                messages.put(JSONObject().put("role", line.role).put("content", line.content))
            }
        }

        val tools = JSONArray().put(
            JSONObject()
                .put("type", "web_search_20250305")
                .put("name", "web_search")
                .put("max_uses", 2)
        )
        val preservedAssistantMessages = JSONArray()

        for (attempt in 0 until 3) {
            val body = JSONObject()
                .put("model", cfg.model)
                .put("max_tokens", 4096)
                .put("system", SYSTEM_PROMPT)
                .put("messages", messages)
                .put("tools", tools)

            val request = Request.Builder()
                .url(cfg.baseUrl + "/messages")
                .header("x-api-key", cfg.apiKey)
                .header("anthropic-version", "2023-06-01")
                .header("Content-Type", "application/json")
                .post(body.toString().toRequestBody("application/json".toMediaType()))
                .build()

            val payloadText = client.newCall(request).execute().use { response ->
                val raw = response.body?.string().orEmpty()
                if (!response.isSuccessful) {
                    throw IllegalStateException("Поиск API " + response.code + ": " + raw.take(350))
                }
                raw
            }
            val payload = JSONObject(payloadText)
            val content = payload.optJSONArray("content")
                ?: throw IllegalStateException("Anthropic API вернул ответ без content.")
            val assistantMessage = JSONObject().put("role", "assistant").put("content", content)
            preservedAssistantMessages.put(assistantMessage)
            messages.put(assistantMessage)

            val stopReason = payload.optString("stop_reason")
            if (stopReason == "pause_turn") {
                if (attempt == 2) throw IllegalStateException("Поиск занял слишком много шагов. Попробуй уточнить запрос.")
                continue
            }
            if (stopReason == "tool_use") {
                throw IllegalStateException("Провайдер вернул неподдерживаемый клиентский инструмент вместо серверного веб-поиска.")
            }

            val answer = StringBuilder()
            val citations = linkedMapOf<String, String>()
            for (i in 0 until content.length()) {
                val block = content.optJSONObject(i) ?: continue
                if (block.optString("type") != "text") continue
                val text = block.optString("text")
                if (text.isNotBlank()) {
                    if (answer.isNotEmpty()) answer.append("\n\n")
                    answer.append(text)
                }
                val citationArray = block.optJSONArray("citations") ?: continue
                for (j in 0 until citationArray.length()) {
                    val citation = citationArray.optJSONObject(j) ?: continue
                    val sourceUrl = citation.optString("url")
                    if (sourceUrl.isBlank() || citations.containsKey(sourceUrl)) continue
                    val title = citation.optString("title").ifBlank { sourceUrl }
                    citations[sourceUrl] = title
                }
            }
            if (citations.isNotEmpty()) {
                answer.append("\n\n**Источники**\n")
                citations.forEach { (sourceUrl, title) ->
                    answer.append("- [")
                        .append(title.replace("]", "\\]"))
                        .append("](")
                        .append(sourceUrl)
                        .append(")\n")
                }
            }
            return WebSearchAnswer(answer.toString().trim(), preservedAssistantMessages.toString())
        }
        throw IllegalStateException("Не удалось завершить веб-поиск.")
    }

    private fun updateChatLive(id: String, answer: String) {
        val current = chats.firstOrNull { it.id == id } ?: return
        if (current.lines.isEmpty()) return
        val updatedLines = current.lines.dropLast(1) + ChatLine("assistant", answer)
        chats = chats.map {
            if (it.id == id) current.copy(lines = updatedLines, updatedAt = System.currentTimeMillis()) else it
        }
    }

    private fun updateChat(id: String, chat: ChatSession) {
        chats = chats.map { if (it.id == id) chat else it }.sortedWith(compareByDescending<ChatSession> { it.isPinned }.thenByDescending { it.updatedAt })
        saveChats()
    }

    private fun saveChats() {
        val array = JSONArray()
        chats.forEach { chat ->
            val lineArray = JSONArray()
            chat.lines.forEach { line ->
                val attachments = JSONArray()
                line.attachments.forEach { item ->
                    attachments.put(
                        JSONObject().put("name", item.name)
                            .put("mimeType", item.mimeType)
                            .put("localPath", item.localPath)
                            .put("textContent", item.textContent)
                    )
                }
                val lineObject = JSONObject().put("role", line.role).put("content", line.content).put("attachments", attachments)
                line.providerMessagesJson?.let { lineObject.put("providerMessagesJson", it) }
                lineArray.put(lineObject)
            }
            array.put(
                JSONObject()
                    .put("id", chat.id)
                    .put("title", chat.title)
                    .put("updatedAt", chat.updatedAt)
                    .put("isPinned", chat.isPinned)
                    .put("lines", lineArray)
            )
        }
        prefs.edit().putString("chats_json", array.toString()).apply()
    }

    private fun loadChats(): List<ChatSession> {
        return try {
            val array = JSONArray(prefs.getString("chats_json", "[]") ?: "[]")
            (0 until array.length()).map { i ->
                val obj = array.getJSONObject(i)
                val linesJson = obj.optJSONArray("lines") ?: JSONArray()
                val lines = (0 until linesJson.length()).map { j ->
                    val line = linesJson.getJSONObject(j)
                    val attachmentsJson = line.optJSONArray("attachments") ?: JSONArray()
                    val attachments = (0 until attachmentsJson.length()).map { k ->
                        val item = attachmentsJson.getJSONObject(k)
                        ChatAttachment(
                            item.optString("name"),
                            item.optString("mimeType"),
                            item.optString("localPath"),
                            item.optString("textContent")
                        )
                    }.filter { it.textContent.isNotBlank() || (it.localPath.isNotBlank() && File(it.localPath).exists()) }
                    ChatLine(
                        line.optString("role"),
                        line.optString("content"),
                        attachments,
                        line.optString("providerMessagesJson", "").takeIf { it.isNotBlank() }
                    )
                }
                ChatSession(
                    obj.optString("id", UUID.randomUUID().toString()),
                    obj.optString("title", "Диалог"),
                    obj.optLong("updatedAt", System.currentTimeMillis()),
                    lines,
                    obj.optBoolean("isPinned", false)
                )
            }.sortedWith(compareByDescending<ChatSession> { it.isPinned }.thenByDescending { it.updatedAt })
        } catch (_: Exception) {
            emptyList()
        }
    }

    companion object {
        private const val SYSTEM_PROMPT =
            "You are NEXUS AI MAX, a capable personal AI assistant. Respond in the user's language. Be rigorous, distinguish facts from uncertainty, and never claim to have browsed or executed an action unless a connected tool actually did so."
    }
}

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        androidx.core.view.WindowCompat.setDecorFitsSystemWindows(window, false)
        androidx.core.view.WindowCompat.getInsetsController(window, window.decorView).apply {
            isAppearanceLightStatusBars = false
            isAppearanceLightNavigationBars = false
        }
        setContent { NexusApp() }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun NexusApp(vm: NexusViewModel = viewModel()) {
    val clipboard = LocalClipboardManager.current
    var input by remember { mutableStateOf("") }
    var showHistory by remember { mutableStateOf(false) }
    var showSettings by remember { mutableStateOf(false) }
    var topMenuExpanded by remember { mutableStateOf(false) }
    var showChatSearch by remember { mutableStateOf(false) }
    var chatSearchQuery by remember { mutableStateOf("") }
    var renameTarget by remember { mutableStateOf<ChatSession?>(null) }
    var renameValue by remember { mutableStateOf("") }
    var url by remember { mutableStateOf(vm.settings.baseUrl) }
    var model by remember { mutableStateOf(vm.settings.model) }
    var key by remember { mutableStateOf("") }
    var selectedProvider by remember { mutableStateOf(vm.settings.provider) }
    var openRouterKey by remember { mutableStateOf("") }
    var selectedOpenRouterModel by remember { mutableStateOf(vm.settings.openRouterModel) }
    var freeModelsOnly by remember { mutableStateOf(false) }
    var modelSearch by remember { mutableStateOf("") }
    var webSearchEnabled by remember { mutableStateOf(false) }
    val imagePicker = rememberLauncherForActivityResult(ActivityResultContracts.GetContent()) { uri ->
        uri?.let { vm.addAttachment(it) }
    }
    val documentPicker = rememberLauncherForActivityResult(ActivityResultContracts.OpenDocument()) { uri ->
        uri?.let { vm.addAttachment(it) }
    }
    val context = LocalContext.current
    var ttsReady by remember { mutableStateOf(false) }
    val textToSpeech = remember(context) { TextToSpeech(context) { status ->
        ttsReady = status == TextToSpeech.SUCCESS
        if (status == TextToSpeech.SUCCESS) textToSpeech.language = Locale("ru")
    } }
    DisposableEffect(textToSpeech) { onDispose { textToSpeech.stop(); textToSpeech.shutdown() } }
    var speaking by remember { mutableStateOf(false) }
    val speechLauncher = rememberLauncherForActivityResult(ActivityResultContracts.StartActivityForResult()) { result ->
        if (result.resultCode == Activity.RESULT_OK) {
            val spoken = result.data?.getStringArrayListExtra(RecognizerIntent.EXTRA_RESULTS)?.firstOrNull()
            if (!spoken.isNullOrBlank()) input = listOf(input.trim(), spoken).filter { it.isNotBlank() }.joinToString(" ")
        }
    }

    MaterialTheme(colorScheme = darkColorScheme(
        primary = Violet,
        onPrimary = Color.White,
        background = Ink,
        surface = Panel,
        onSurface = MainText
    )) {
        Scaffold(
            containerColor = Ink,
            topBar = {
                Row(
                    Modifier.fillMaxWidth()
                        .background(Ink)
                        .windowInsetsPadding(WindowInsets.safeDrawing.only(WindowInsetsSides.Top))
                        .padding(horizontal = 16.dp, vertical = 10.dp),
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    TextButton(
                        onClick = { showHistory = true },
                        contentPadding = PaddingValues(horizontal = 8.dp, vertical = 8.dp)
                    ) {
                        Text("☰", color = MainText, fontSize = 23.sp)
                    }
                    Spacer(Modifier.width(8.dp))
                    Column(Modifier.weight(1f)) {
                        Text("NEXUS", color = MainText, fontWeight = FontWeight.Bold, fontSize = 17.sp, letterSpacing = 1.1.sp)
                        Text("AI MAX", color = Violet, fontSize = 10.sp, letterSpacing = 1.7.sp, fontWeight = FontWeight.SemiBold)
                    }
                    Box {
                        IconButton(onClick = { topMenuExpanded = true }) {
                            Icon(Icons.Filled.MoreVert, contentDescription = "Меню чата", tint = MainText)
                        }
                        DropdownMenu(expanded = topMenuExpanded, onDismissRequest = { topMenuExpanded = false }) {
                            DropdownMenuItem(text = { Text(if (vm.activeChat?.isPinned == true) "Открепить чат" else "Закрепить чат") }, leadingIcon = { Icon(Icons.Filled.PushPin, null, tint = AccentRed) }, onClick = { vm.activeChatId?.let { vm.togglePinChat(it) }; topMenuExpanded = false })
                            DropdownMenuItem(text = { Text("Поиск в чате") }, leadingIcon = { Icon(Icons.Filled.Search, null, tint = AccentRed) }, onClick = { chatSearchQuery = ""; showChatSearch = true; topMenuExpanded = false })
                            DropdownMenuItem(text = { Text("Поделиться") }, leadingIcon = { Icon(Icons.Filled.Share, null, tint = AccentRed) }, onClick = {
                                val chat = vm.activeChat
                                if (chat != null) {
                                    val transcript = buildString {
                                        append(chat.title).append("\n\n")
                                        chat.lines.forEach { line ->
                                            append(if (line.role == "user") "Вы" else "NEXUS").append(":\n")
                                            append(line.content).append("\n\n")
                                        }
                                    }
                                    val sendIntent = Intent(Intent.ACTION_SEND).apply {
                                        type = "text/plain"
                                        putExtra(Intent.EXTRA_SUBJECT, chat.title)
                                        putExtra(Intent.EXTRA_TEXT, transcript)
                                    }
                                    context.startActivity(Intent.createChooser(sendIntent, "Поделиться чатом"))
                                }
                                topMenuExpanded = false
                            })
                            DropdownMenuItem(text = { Text("Настройки") }, onClick = {
                                url = vm.settings.baseUrl
                                model = vm.settings.model
                                key = vm.settings.apiKey
                                selectedProvider = vm.settings.provider
                                openRouterKey = vm.settings.openRouterApiKey
                                selectedOpenRouterModel = vm.settings.openRouterModel
                                showSettings = true
                                topMenuExpanded = false
                            })
                        }
                    }
                }
            },
            bottomBar = {
                Column(Modifier.fillMaxWidth().background(Brush.verticalGradient(listOf(Ink, Color(0xFF120508), BackgroundGlow))).windowInsetsPadding(WindowInsets.safeDrawing.only(WindowInsetsSides.Bottom)).padding(horizontal = 12.dp, vertical = 8.dp)) {
                    if (vm.pendingAttachments.isNotEmpty()) {
                        Column(Modifier.fillMaxWidth().padding(bottom = 7.dp)) {
                            vm.pendingAttachments.forEachIndexed { index, attachment ->
                                Row(verticalAlignment = Alignment.CenterVertically) {
                                    Text(attachment.name, color = Blue, fontSize = 10.sp, modifier = Modifier.weight(1f), maxLines = 1)
                                    TextButton(onClick = { vm.removePendingAttachment(index) }, contentPadding = PaddingValues(horizontal = 5.dp, vertical = 0.dp)) {
                                        Text("Убрать", color = Color(0xFFC77B83), fontSize = 10.sp)
                                    }
                                }
                            }
                        }
                    }
                    Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.fillMaxWidth().padding(bottom = 5.dp)) {
                        TextButton(onClick = { imagePicker.launch("image/*") }, contentPadding = PaddingValues(horizontal = 7.dp, vertical = 3.dp)) {
                            Text("+ Фото", color = Blue, fontSize = 11.sp)
                        }
                        TextButton(onClick = {
                            documentPicker.launch(arrayOf("text/*", "application/json", "application/xml"))
                        }, contentPadding = PaddingValues(horizontal = 7.dp, vertical = 3.dp)) {
                            Text("+ Документ", color = Blue, fontSize = 11.sp)
                        }
                        TextButton(
                            onClick = { webSearchEnabled = !webSearchEnabled },
                            contentPadding = PaddingValues(horizontal = 7.dp, vertical = 3.dp)
                        ) {
                            Text(
                                if (webSearchEnabled) "Поиск: авто" else "Поиск: выкл",
                                color = if (webSearchEnabled) Violet else Muted,
                                fontSize = 11.sp
                            )
                        }
                    }
                    Row(
                        Modifier.fillMaxWidth().clip(RoundedCornerShape(28.dp))
                            .background(PanelRaised).padding(start = 7.dp, end = 7.dp, top = 7.dp, bottom = 7.dp),
                        verticalAlignment = Alignment.CenterVertically
                    ) {
                        IconButton(
                            onClick = {
                                val intent = Intent(RecognizerIntent.ACTION_RECOGNIZE_SPEECH).apply {
                                    putExtra(RecognizerIntent.EXTRA_LANGUAGE_MODEL, RecognizerIntent.LANGUAGE_MODEL_FREE_FORM)
                                    putExtra(RecognizerIntent.EXTRA_LANGUAGE, "ru-RU")
                                    putExtra(RecognizerIntent.EXTRA_PROMPT, "Говорите")
                                    putExtra(RecognizerIntent.EXTRA_MAX_RESULTS, 1)
                                }
                                try {
                                    speechLauncher.launch(intent)
                                } catch (_: Exception) {
                                    vm.showError("На устройстве нет доступного сервиса распознавания речи.")
                                }
                            },
                            modifier = Modifier.size(42.dp)
                        ) {
                            Icon(Icons.Filled.Mic, contentDescription = "Голосовой ввод", tint = AccentRed, modifier = Modifier.size(23.dp))
                        }
                        BasicTextField(
                            value = input,
                            onValueChange = { input = it },
                            modifier = Modifier.weight(1f).padding(vertical = 10.dp),
                            textStyle = androidx.compose.ui.text.TextStyle(color = MainText, fontSize = 15.sp, lineHeight = 21.sp),
                            maxLines = 6,
                            decorationBox = { inner ->
                                Box {
                                    if (input.isEmpty()) Text("Напишите сообщение…", color = Muted, fontSize = 14.sp)
                                    inner()
                                }
                            }
                        )
                        Button(
                            enabled = !vm.busy && (input.isNotBlank() || vm.pendingAttachments.isNotEmpty()),
                            onClick = { vm.send(input, webSearchEnabled); input = "" },
                            shape = RoundedCornerShape(16.dp),
                            contentPadding = PaddingValues(horizontal = 17.dp, vertical = 12.dp),
                            colors = ButtonDefaults.buttonColors(containerColor = Violet, disabledContainerColor = Color(0xFF3A292B))
                        ) {
                            Text(if (vm.busy) "…" else "↑", fontSize = 20.sp, fontWeight = FontWeight.Bold)
                        }
                    }
                    Text(
                        "NEXUS может допускать ошибки. Проверяйте важную информацию.",
                        color = Color(0xFF656779), fontSize = 9.sp,
                        modifier = Modifier.fillMaxWidth().padding(top = 7.dp),
                        textAlign = TextAlign.Center
                    )
                }
            }
        ) { padding ->
            Column(Modifier.fillMaxSize().background(Brush.verticalGradient(listOf(Ink, Ink, Color(0xFF100508), BackgroundGlow))).padding(padding)) {
                vm.error?.let { message ->
                    Surface(color = Color(0xFF321D20), shape = RoundedCornerShape(12.dp), modifier = Modifier.fillMaxWidth().padding(horizontal = 14.dp, vertical = 8.dp)) {
                        Text(message, color = Color(0xFFD69A9F), fontSize = 12.sp, modifier = Modifier.padding(12.dp))
                    }
                }
                val chat = vm.activeChat
                val lines = chat?.lines.orEmpty()
                if (lines.isEmpty()) {
                    Column(
                        Modifier.fillMaxSize().padding(horizontal = 25.dp),
                        verticalArrangement = Arrangement.Center,
                        horizontalAlignment = Alignment.Start
                    ) {
                        Box(
                            Modifier.size(54.dp).clip(RoundedCornerShape(18.dp))
                                .background(Brush.linearGradient(listOf(Violet.copy(alpha = 0.34f), AccentSoft.copy(alpha = 0.18f)))),
                            contentAlignment = Alignment.Center
                        ) {
                            Text("N", color = MainText, fontSize = 27.sp, fontWeight = FontWeight.Black)
                        }
                        Spacer(Modifier.height(22.dp))
                        Text("ДУМАЙ ШИРЕ.", color = Muted, fontSize = 11.sp, letterSpacing = 3.sp, fontWeight = FontWeight.Bold)
                        Text("Твоя идея.\\nСледующий уровень.", color = MainText, fontSize = 31.sp, lineHeight = 38.sp, fontWeight = FontWeight.SemiBold)
                        Spacer(Modifier.height(12.dp))
                        Text("NEXUS готов к работе. Сформулируй задачу, задай вопрос или начни исследование.", color = Muted, fontSize = 14.sp, lineHeight = 21.sp)
                        Spacer(Modifier.height(25.dp))
                    }
                } else {
                    LazyColumn(
                        modifier = Modifier.fillMaxSize(),
                        contentPadding = PaddingValues(start = 14.dp, end = 14.dp, top = 18.dp, bottom = 20.dp),
                        verticalArrangement = Arrangement.spacedBy(17.dp)
                    ) {
                        items(lines) { line ->
                            val isUser = line.role == "user"
                            Column(Modifier.fillMaxWidth(), horizontalAlignment = if (isUser) Alignment.End else Alignment.Start) {
                                if (isUser) Spacer(Modifier.height(2.dp))
                                Surface(
                                    color = if (isUser) PanelRaised else Color.Transparent,
                                    shape = RoundedCornerShape(22.dp),
                                    modifier = Modifier.widthIn(max = if (isUser) 340.dp else 520.dp)
                                ) {
                                    if (isUser) {
                                        Column(Modifier.padding(horizontal = 15.dp, vertical = 13.dp)) {
                                if (line.content.isNotBlank()) {
                                    Text(line.content, color = MainText, fontSize = 14.sp, lineHeight = 21.sp)
                                }
                                line.attachments.forEach { attachment ->
                                    Text("Вложение: " + attachment.name, color = Blue, fontSize = 11.sp, modifier = Modifier.padding(top = 5.dp))
                                }
                            }
                                    } else {
                                        Column(Modifier.fillMaxWidth().padding(horizontal = 15.dp, vertical = 13.dp)) {
                                            Markdown(
                                                line.content,
                                                components = markdownComponents(
                                                    codeFence = { model ->
                                                        val code = extractMarkdownCode(model.content, model.node, true)
                                                        Column(Modifier.fillMaxWidth()) {
                                                            Row(Modifier.fillMaxWidth(), verticalAlignment = Alignment.CenterVertically) {
                                                                Text("CODE", color = Muted, fontSize = 9.sp, modifier = Modifier.weight(1f))
                                                                TextButton(
                                                                    onClick = { clipboard.setText(AnnotatedString(code)) },
                                                                    contentPadding = PaddingValues(horizontal = 8.dp, vertical = 2.dp)
                                                                ) { Text("Копировать код", color = Blue, fontSize = 10.sp) }
                                                            }
                                                            MarkdownHighlightedCodeFence(content = model.content, node = model.node)
                                                        }
                                                    },
                                                    codeBlock = { model ->
                                                        val code = extractMarkdownCode(model.content, model.node, false)
                                                        Column(Modifier.fillMaxWidth()) {
                                                            Row(Modifier.fillMaxWidth(), verticalAlignment = Alignment.CenterVertically) {
                                                                Text("CODE", color = Muted, fontSize = 9.sp, modifier = Modifier.weight(1f))
                                                                TextButton(
                                                                    onClick = { clipboard.setText(AnnotatedString(code)) },
                                                                    contentPadding = PaddingValues(horizontal = 8.dp, vertical = 2.dp)
                                                                ) { Text("Копировать код", color = Blue, fontSize = 10.sp) }
                                                            }
                                                            MarkdownHighlightedCodeBlock(content = model.content, node = model.node)
                                                        }
                                                    }
                                                )
                                            )
                                        }
                                    }
                                }
                                if (!isUser) {
                                    Row(verticalAlignment = Alignment.CenterVertically) {
                                        IconButton(onClick = {
                                            if (speaking) { textToSpeech.stop(); speaking = false }
                                            else if (line.content.isNotBlank() && ttsReady) {
                                                textToSpeech.speak(line.content, TextToSpeech.QUEUE_FLUSH, null, "nexus_message")
                                                speaking = true
                                            } else if (!ttsReady) vm.showError("Синтез речи ещё не готов. Попробуй ещё раз.")
                                        }, modifier = Modifier.size(34.dp)) {
                                            Icon(if (speaking) Icons.Filled.Stop else Icons.Filled.VolumeUp, contentDescription = "Прослушать ответ", tint = AccentRed, modifier = Modifier.size(20.dp))
                                        }
                                        TextButton(onClick = { clipboard.setText(AnnotatedString(line.content)) }, contentPadding = PaddingValues(horizontal = 5.dp, vertical = 2.dp)) {
                                            Text("Копировать ответ", color = Muted, fontSize = 10.sp)
                                        }
                                    }
                                }
                            }
                        }
                        if (vm.busy) {
                            item {
                                Row(verticalAlignment = Alignment.CenterVertically) {
                                    CircularProgressIndicator(modifier = Modifier.size(15.dp), color = Violet, strokeWidth = 2.dp)
                                    Spacer(Modifier.width(10.dp))
                                    Text("NEXUS формирует ответ…", color = Muted, fontSize = 12.sp)
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if (showHistory) {
        Dialog(
            onDismissRequest = { showHistory = false },
            properties = DialogProperties(usePlatformDefaultWidth = false)
        ) {
            Box(Modifier.fillMaxSize(), contentAlignment = Alignment.CenterStart) {
                Surface(
                    modifier = Modifier.fillMaxHeight().width(316.dp),
                    color = Panel,
                    shape = RoundedCornerShape(topEnd = 22.dp, bottomEnd = 22.dp),
                    tonalElevation = 8.dp
                ) {
                    Column(
                        Modifier.fillMaxSize()
                            .windowInsetsPadding(WindowInsets.safeDrawing.only(WindowInsetsSides.Top + WindowInsetsSides.Bottom))
                            .padding(horizontal = 14.dp, vertical = 12.dp)
                    ) {
                        Row(verticalAlignment = Alignment.CenterVertically) {
                            Column(Modifier.weight(1f)) {
                                Text("NEXUS", color = MainText, fontWeight = FontWeight.Bold, fontSize = 19.sp, letterSpacing = 1.2.sp)
                                Text("AI MAX", color = Violet, fontSize = 10.sp, letterSpacing = 2.sp, fontWeight = FontWeight.Bold)
                            }
                            IconButton(onClick = { showHistory = false }) {
                                Text("×", color = Muted, fontSize = 25.sp)
                            }
                        }
                        Spacer(Modifier.height(18.dp))
                        Surface(
                            color = PanelRaised,
                            shape = RoundedCornerShape(14.dp),
                            modifier = Modifier.fillMaxWidth().clickable {
                                vm.createChat()
                                input = ""
                                showHistory = false
                            }
                        ) {
                            Row(Modifier.padding(horizontal = 13.dp, vertical = 12.dp), verticalAlignment = Alignment.CenterVertically) {
                                Box(
                                    Modifier.size(30.dp).clip(CircleShape).background(AccentRed),
                                    contentAlignment = Alignment.Center
                                ) {
                                    Text("+", color = Color.White, fontSize = 21.sp, fontWeight = FontWeight.Medium)
                                }
                                Spacer(Modifier.width(12.dp))
                                Text("Новый чат", color = MainText, fontSize = 14.sp, fontWeight = FontWeight.Medium)
                                Spacer(Modifier.weight(1f))
                                Text("↗", color = AccentRed, fontSize = 16.sp)
                            }
                        }
                        Spacer(Modifier.height(24.dp))
                        Text("ИСТОРИЯ", color = Muted, fontSize = 10.sp, fontWeight = FontWeight.Bold, letterSpacing = 1.5.sp, modifier = Modifier.padding(start = 8.dp, bottom = 9.dp))
                        if (vm.chats.isEmpty()) {
                            Text("Пока нет сохранённых диалогов.", color = Muted, fontSize = 12.sp, modifier = Modifier.padding(8.dp))
                        } else {
                            LazyColumn(modifier = Modifier.weight(1f), verticalArrangement = Arrangement.spacedBy(4.dp)) {
                                items(vm.chats) { chat ->
                                    Surface(
                                        color = if (chat.id == vm.activeChatId) AccentSoft.copy(alpha = 0.38f) else Color.Transparent,
                                        shape = RoundedCornerShape(12.dp),
                                        modifier = Modifier.fillMaxWidth()
                                    ) {
                                        Row(Modifier.padding(start = 11.dp, end = 5.dp, top = 8.dp, bottom = 8.dp), verticalAlignment = Alignment.CenterVertically) {
                                            Column(Modifier.weight(1f).clickable {
                                                vm.selectChat(chat.id)
                                                showHistory = false
                                            }) {
                                                Row(verticalAlignment = Alignment.CenterVertically) {
                                                    if (chat.isPinned) Icon(Icons.Filled.PushPin, "Закреплён", tint = AccentRed, modifier = Modifier.size(13.dp))
                                                    Text(chat.title, color = MainText, fontSize = 13.sp, maxLines = 1)
                                                }
                                                Text("${chat.lines.size} сообщений", color = Muted, fontSize = 10.sp)
                                            }
                                            var menuExpanded by remember(chat.id) { mutableStateOf(false) }
                                            Box {
                                                IconButton(onClick = { menuExpanded = true }) {
                                                    Text("⋯", color = Muted, fontSize = 20.sp)
                                                }
                                                DropdownMenu(expanded = menuExpanded, onDismissRequest = { menuExpanded = false }) {
                                                    DropdownMenuItem(
                                                        text = { Text("Переименовать", color = MainText) },
                                                        onClick = {
                                                            renameTarget = chat
                                                            renameValue = chat.title
                                                            menuExpanded = false
                                                        }
                                                    )
                                                    DropdownMenuItem(
                                                        text = { Text("Удалить", color = Color(0xFFE28B93)) },
                                                        onClick = {
                                                            vm.deleteChat(chat.id)
                                                            menuExpanded = false
                                                        }
                                                    )
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Divider(color = DividerTone, modifier = Modifier.padding(vertical = 10.dp))
                        Surface(
                            color = Color.Transparent,
                            modifier = Modifier.fillMaxWidth().clickable {
                                url = vm.settings.baseUrl
                                model = vm.settings.model
                                key = vm.settings.apiKey
                                selectedProvider = vm.settings.provider
                                openRouterKey = vm.settings.openRouterApiKey
                                selectedOpenRouterModel = vm.settings.openRouterModel
                                showSettings = true
                                showHistory = false
                            },
                            shape = RoundedCornerShape(12.dp)
                        ) {
                            Row(Modifier.padding(12.dp), verticalAlignment = Alignment.CenterVertically) {
                                Text("⚙", color = Muted, fontSize = 17.sp)
                                Spacer(Modifier.width(12.dp))
                                Text("Настройки", color = MainText, fontSize = 13.sp)
                            }
                        }
                    }
                }
                Box(Modifier.fillMaxSize().padding(start = 316.dp).clickable { showHistory = false })
            }
        }
    }

    if (showChatSearch) {
        val matches = vm.activeChat?.lines.orEmpty().withIndex().filter { chatSearchQuery.isNotBlank() && it.value.content.contains(chatSearchQuery, ignoreCase = true) }
        AlertDialog(
            onDismissRequest = { showChatSearch = false },
            containerColor = Panel,
            title = { Text("Поиск в чате", color = MainText) },
            text = {
                Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                    OutlinedTextField(chatSearchQuery, { chatSearchQuery = it }, singleLine = true, label = { Text("Найти в переписке") }, modifier = Modifier.fillMaxWidth())
                    if (chatSearchQuery.isNotBlank()) {
                        Text("Найдено: ${matches.size}", color = Muted, fontSize = 11.sp)
                        LazyColumn(Modifier.heightIn(max = 280.dp)) {
                            items(matches) { match ->
                                Column(Modifier.fillMaxWidth().clickable { clipboard.setText(AnnotatedString(match.value.content)); showChatSearch = false }.padding(vertical = 7.dp)) {
                                    Text(if (match.value.role == "user") "Вы" else "NEXUS", color = AccentRed, fontSize = 10.sp, fontWeight = FontWeight.Bold)
                                    Text(match.value.content.take(220), color = MainText, fontSize = 12.sp, maxLines = 4)
                                }
                                Divider(color = DividerTone)
                            }
                        }
                    }
                }
            },
            confirmButton = { TextButton(onClick = { showChatSearch = false }) { Text("Закрыть", color = AccentRed) } }
        )
    }

    if (showSettings) {
        AlertDialog(
            onDismissRequest = { showSettings = false },
            containerColor = Panel,
            title = { Text("Подключение API", color = MainText) },
            text = {
                Column(verticalArrangement = Arrangement.spacedBy(9.dp)) {
                    Text("ПРОВАЙДЕР", color = Muted, fontSize = 10.sp, fontWeight = FontWeight.Bold)
                    Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                        FilterChip(selected = selectedProvider == "AnyModel", onClick = { selectedProvider = "AnyModel" }, label = { Text("AnyModel") })
                        FilterChip(selected = selectedProvider == "OpenRouter", onClick = { selectedProvider = "OpenRouter" }, label = { Text("OpenRouter") })
                    }
                    if (selectedProvider == "AnyModel") {
                        OutlinedTextField(url, { url = it }, label = { Text("Base URL") }, singleLine = true)
                        OutlinedTextField(model, { model = it }, label = { Text("Model ID") }, singleLine = true)
                        OutlinedTextField(key, { key = it }, label = { Text("AnyModel API key") }, singleLine = true, visualTransformation = PasswordVisualTransformation())
                        Text("AnyModel остаётся прежним провайдером. Ключ хранится только в памяти и после перезапуска вводится заново.", color = Muted, fontSize = 10.sp, lineHeight = 14.sp)
                        Text("Встроенный поиск Claude работает только через AnyModel.", color = Muted, fontSize = 10.sp, lineHeight = 14.sp)
                    } else {
                        OutlinedTextField(openRouterKey, { openRouterKey = it }, label = { Text("OpenRouter API key") }, singleLine = true, visualTransformation = PasswordVisualTransformation())
                        Row(verticalAlignment = Alignment.CenterVertically) {
                            Button(onClick = { vm.loadOpenRouterModels(openRouterKey) }, enabled = !vm.loadingOpenRouterModels && openRouterKey.isNotBlank(), colors = ButtonDefaults.buttonColors(containerColor = Violet)) {
                                Text(if (vm.loadingOpenRouterModels) "Загрузка…" else "Загрузить каталог")
                            }
                            Spacer(Modifier.width(8.dp))
                            Text("${vm.openRouterModels.size} моделей", color = Muted, fontSize = 11.sp)
                        }
                        OutlinedTextField(modelSearch, { modelSearch = it }, label = { Text("Поиск модели") }, singleLine = true)
                        Row(verticalAlignment = Alignment.CenterVertically) {
                            Checkbox(checked = freeModelsOnly, onCheckedChange = { freeModelsOnly = it })
                            Text("Только бесплатные", color = MainText, fontSize = 12.sp)
                        }
                        val visibleModels = vm.openRouterModels.filter { (!freeModelsOnly || it.isFree) && (modelSearch.isBlank() || it.name.contains(modelSearch, true) || it.id.contains(modelSearch, true)) }
                        if (visibleModels.isEmpty()) {
                            Text(if (vm.openRouterModels.isEmpty()) "Нажми «Загрузить каталог», чтобы получить актуальный список из OpenRouter." else "Модели по этому фильтру не найдены.", color = Muted, fontSize = 11.sp)
                        } else {
                            Surface(color = PanelRaised, shape = RoundedCornerShape(12.dp)) {
                                LazyColumn(Modifier.fillMaxWidth().heightIn(max = 230.dp)) {
                                    items(visibleModels, key = { it.id }) { item ->
                                        Row(Modifier.fillMaxWidth().clickable { selectedOpenRouterModel = item.id }.padding(horizontal = 10.dp, vertical = 8.dp), verticalAlignment = Alignment.CenterVertically) {
                                            RadioButton(selected = (selectedOpenRouterModel.ifBlank { vm.settings.openRouterModel }) == item.id, onClick = { selectedOpenRouterModel = item.id })
                                            Column(Modifier.weight(1f)) {
                                                Text(item.name, color = MainText, fontSize = 12.sp, maxLines = 2)
                                                Text(item.id, color = Muted, fontSize = 9.sp, maxLines = 2)
                                                Text(if (item.isFree) "БЕСПЛАТНАЯ · контекст ${item.contextLength}" else "Платная · контекст ${item.contextLength}", color = if (item.isFree) Color(0xFF8FC6A0) else Muted, fontSize = 9.sp)
                                            }
                                        }
                                        Divider(color = DividerTone)
                                    }
                                }
                            }
                        }
                        Text("Бесплатность определяется по нулевой цене входных и выходных токенов в каталоге. У бесплатных моделей есть квоты и ограничения.", color = Muted, fontSize = 10.sp, lineHeight = 14.sp)
                        Text("Ключ хранится только в памяти приложения и будет потерян после перезапуска. Не используй этот режим для публичного распространения APK.", color = Muted, fontSize = 10.sp, lineHeight = 14.sp)
                    }
                }
            },
            confirmButton = {
                TextButton(onClick = {
                    vm.updateSettings(url, model, key)
                    vm.updateProviderSettings(selectedProvider, selectedOpenRouterModel.ifBlank { vm.settings.openRouterModel }, openRouterKey)
                    showSettings = false
                }) { Text("Сохранить", color = Violet) }
            },
            dismissButton = { TextButton(onClick = { showSettings = false }) { Text("Отмена", color = Muted) } }
        )
    }

    renameTarget?.let { target ->
        AlertDialog(
            onDismissRequest = { renameTarget = null },
            containerColor = Panel,
            title = { Text("Переименовать диалог", color = MainText) },
            text = { OutlinedTextField(renameValue, { renameValue = it }, singleLine = true, label = { Text("Название") }) },
            confirmButton = {
                TextButton(onClick = { vm.renameChat(target.id, renameValue); renameTarget = null }) { Text("Сохранить", color = Violet) }
            },
            dismissButton = { TextButton(onClick = { renameTarget = null }) { Text("Отмена", color = Muted) } }
        )
    }
}

@Composable
private fun SuggestionCard(title: String, subtitle: String, onClick: () -> Unit) {
    Surface(
        color = Panel,
        shape = RoundedCornerShape(14.dp),
        modifier = Modifier.fillMaxWidth().clickable(onClick = onClick)
    ) {
        Row(Modifier.padding(horizontal = 15.dp, vertical = 13.dp), verticalAlignment = Alignment.CenterVertically) {
            Column(Modifier.weight(1f)) {
                Text(title, color = MainText, fontSize = 13.sp, fontWeight = FontWeight.SemiBold)
                Text(subtitle, color = Muted, fontSize = 10.sp)
            }
            Text("↗", color = Violet, fontSize = 18.sp)
        }
    }
}


private fun extractMarkdownCode(content: String, node: ASTNode, fenced: Boolean): String {
    if (node.children.isEmpty()) return ""
    val start: Int
    val end: Int
    if (fenced) {
        if (node.children.size < 3) return ""
        start = node.children[2].startOffset
        val hasLanguage = node.children.size > 3 &&
            content.substring(node.children[1].startOffset, node.children[1].endOffset).isNotBlank()
        val minimumIndex = if (hasLanguage) 3 else 2
        end = node.children[(node.children.size - 2).coerceAtLeast(minimumIndex)].endOffset
    } else {
        start = node.children.first().startOffset
        end = node.children.last().endOffset
    }
    if (start < 0 || end < start || end > content.length) return ""
    return content.substring(start, end).replaceIndent()
}
