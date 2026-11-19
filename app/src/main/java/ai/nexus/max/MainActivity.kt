package ai.nexus.max

import android.app.Application
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
import androidx.compose.runtime.*
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
import java.util.concurrent.TimeUnit
import com.mikepenz.markdown.m3.Markdown
import com.mikepenz.markdown.compose.components.markdownComponents
import com.mikepenz.markdown.compose.elements.MarkdownHighlightedCodeBlock
import com.mikepenz.markdown.compose.elements.MarkdownHighlightedCodeFence
import org.intellij.markdown.ast.ASTNode

private val Ink = Color(0xFF0D0D0D)
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
    val attachments: List<ChatAttachment> = emptyList()
)
data class ChatSession(
    val id: String,
    val title: String,
    val updatedAt: Long,
    val lines: List<ChatLine>
)
data class ApiSettings(
    val baseUrl: String = "https://anymodel.org/v1",
    val model: String = "claude-fable-5-2",
    val apiKey: String = ""
)

class NexusViewModel(application: Application) : AndroidViewModel(application) {
    private val prefs = application.getSharedPreferences("nexus_local", 0)
    var settings by mutableStateOf(
        ApiSettings(
            baseUrl = prefs.getString("base_url", "https://anymodel.org/v1") ?: "https://anymodel.org/v1",
            model = prefs.getString("model_id", "claude-fable-5-2") ?: "claude-fable-5-2"
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
        settings = ApiSettings(cleanUrl, cleanModel, key.trim())
        prefs.edit().putString("base_url", cleanUrl).putString("model_id", cleanModel).apply()
        error = null
    }

    fun createChat() {
        val chat = ChatSession(UUID.randomUUID().toString(), "Новый диалог", System.currentTimeMillis(), emptyList())
        chats = listOf(chat) + chats
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

    fun send(text: String) {
        if (busy || (text.isBlank() && pendingAttachments.isEmpty())) return
        val cfg = settings
        if (cfg.apiKey.isBlank() || cfg.model.isBlank()) {
            error = "Открой настройки API и введи ключ."
            return
        }
        if (cfg.baseUrl.isBlank()) {
            error = "Укажи Base URL провайдера."
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

        viewModelScope.launch {
            try {
                val answer = withContext(Dispatchers.IO) {
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
                                    append("\\n\\n[Прикреплённый файл: ")
                                    append(file.name)
                                    append("]\\n")
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
                        .put("model", cfg.model)
                        .put("messages", messages)
                        .put("stream", true)
                    val request = Request.Builder()
                        .url(cfg.baseUrl + "/chat/completions")
                        .header("Authorization", "Bearer " + cfg.apiKey)
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
                if (answer.isBlank()) throw IllegalStateException("Модель вернула пустой ответ.")
                val latest = chats.firstOrNull { it.id == chatId }
                if (latest != null) updateChat(
                    chatId,
                    latest.copy(
                        lines = latest.lines.dropLast(1) + ChatLine("assistant", answer),
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

    private fun updateChatLive(id: String, answer: String) {
        val current = chats.firstOrNull { it.id == id } ?: return
        if (current.lines.isEmpty()) return
        val updatedLines = current.lines.dropLast(1) + ChatLine("assistant", answer)
        chats = chats.map {
            if (it.id == id) current.copy(lines = updatedLines, updatedAt = System.currentTimeMillis()) else it
        }
    }

    private fun updateChat(id: String, chat: ChatSession) {
        chats = chats.map { if (it.id == id) chat else it }.sortedByDescending { it.updatedAt }
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
                lineArray.put(JSONObject().put("role", line.role).put("content", line.content).put("attachments", attachments))
            }
            array.put(
                JSONObject()
                    .put("id", chat.id)
                    .put("title", chat.title)
                    .put("updatedAt", chat.updatedAt)
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
                    ChatLine(line.optString("role"), line.optString("content"), attachments)
                }
                ChatSession(
                    obj.optString("id", UUID.randomUUID().toString()),
                    obj.optString("title", "Диалог"),
                    obj.optLong("updatedAt", System.currentTimeMillis()),
                    lines
                )
            }.sortedByDescending { it.updatedAt }
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
    var renameTarget by remember { mutableStateOf<ChatSession?>(null) }
    var renameValue by remember { mutableStateOf("") }
    var url by remember { mutableStateOf(vm.settings.baseUrl) }
    var model by remember { mutableStateOf(vm.settings.model) }
    var key by remember { mutableStateOf("") }
    val imagePicker = rememberLauncherForActivityResult(ActivityResultContracts.GetContent()) { uri ->
        uri?.let { vm.addAttachment(it) }
    }
    val documentPicker = rememberLauncherForActivityResult(ActivityResultContracts.OpenDocument()) { uri ->
        uri?.let { vm.addAttachment(it) }
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
                        .background(Panel)
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
                    TextButton(
                        onClick = { vm.createChat(); input = "" },
                        contentPadding = PaddingValues(horizontal = 9.dp, vertical = 8.dp)
                    ) {
                        Text("+", color = MainText, fontSize = 25.sp)
                    }
                    TextButton(
                        onClick = {
                            url = vm.settings.baseUrl
                            model = vm.settings.model
                            key = vm.settings.apiKey
                            showSettings = true
                        },
                        contentPadding = PaddingValues(horizontal = 8.dp, vertical = 8.dp)
                    ) {
                        Text("⋯", color = MainText, fontSize = 24.sp)
                    }
                }
            },
            bottomBar = {
                Column(Modifier.fillMaxWidth().background(Panel).windowInsetsPadding(WindowInsets.safeDrawing.only(WindowInsetsSides.Bottom)).padding(horizontal = 14.dp, vertical = 9.dp)) {
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
                    }
                    Row(
                        Modifier.fillMaxWidth().clip(RoundedCornerShape(22.dp))
                            .background(PanelRaised).padding(start = 15.dp, end = 7.dp, top = 7.dp, bottom = 7.dp),
                        verticalAlignment = Alignment.CenterVertically
                    ) {
                        BasicTextField(
                            value = input,
                            onValueChange = { input = it },
                            modifier = Modifier.weight(1f).padding(vertical = 10.dp),
                            textStyle = androidx.compose.ui.text.TextStyle(color = MainText, fontSize = 15.sp),
                            decorationBox = { inner ->
                                Box {
                                    if (input.isEmpty()) Text("Напишите сообщение…", color = Muted, fontSize = 14.sp)
                                    inner()
                                }
                            }
                        )
                        Button(
                            enabled = !vm.busy && (input.isNotBlank() || vm.pendingAttachments.isNotEmpty()),
                            onClick = { vm.send(input); input = "" },
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
            Column(Modifier.fillMaxSize().padding(padding)) {
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
                        SuggestionCard("Исследовать тему", "Разбери вопрос на основе научных данных") { input = "Исследуй тему: " }
                        Spacer(Modifier.height(9.dp))
                        SuggestionCard("Составить план", "Преврати цель в последовательные действия") { input = "Помоги составить подробный план для цели: " }
                        Spacer(Modifier.height(9.dp))
                        SuggestionCard("Решить задачу", "Найди эффективное решение проблемы") { input = "Помоги решить задачу: " }
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
                                Text(if (isUser) "ВЫ" else "NEXUS", color = if (isUser) Blue else Violet, fontSize = 9.sp, fontWeight = FontWeight.Bold, letterSpacing = 1.5.sp, modifier = Modifier.padding(horizontal = 5.dp, vertical = 5.dp))
                                Surface(
                                    color = if (isUser) PanelRaised else Ink,
                                    shape = RoundedCornerShape(18.dp),
                                    modifier = Modifier.widthIn(max = 340.dp)
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
                                    TextButton(
                                        onClick = { clipboard.setText(AnnotatedString(line.content)) },
                                        contentPadding = PaddingValues(horizontal = 5.dp, vertical = 2.dp)
                                    ) {
                                        Text("Копировать ответ", color = Muted, fontSize = 10.sp)
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
        ModalBottomSheet(
            onDismissRequest = { showHistory = false },
            containerColor = Panel,
            contentColor = MainText,
            dragHandle = { BottomSheetDefaults.DragHandle(color = Muted) }
        ) {
            Column(Modifier.fillMaxWidth().padding(horizontal = 18.dp).padding(bottom = 28.dp)) {
                Row(verticalAlignment = Alignment.CenterVertically) {
                    Text("История диалогов", fontSize = 20.sp, fontWeight = FontWeight.Bold, modifier = Modifier.weight(1f))
                    TextButton(onClick = { vm.createChat(); showHistory = false }) { Text("+ Новый чат", color = Violet) }
                }
                Spacer(Modifier.height(12.dp))
                if (vm.chats.isEmpty()) {
                    Text("Пока нет сохранённых диалогов.", color = Muted, modifier = Modifier.padding(vertical = 20.dp))
                }
                vm.chats.forEach { chat ->
                    Surface(
                        color = if (chat.id == vm.activeChatId) Color(0xFF35272A) else PanelRaised,
                        shape = RoundedCornerShape(14.dp),
                        modifier = Modifier.fillMaxWidth().padding(vertical = 4.dp)
                    ) {
                        Column(Modifier.padding(12.dp)) {
                            Text(chat.title, color = MainText, fontSize = 14.sp, fontWeight = FontWeight.Medium, modifier = Modifier.fillMaxWidth().clickable {
                                vm.selectChat(chat.id); showHistory = false
                            })
                            Row(verticalAlignment = Alignment.CenterVertically) {
                                Text("${chat.lines.size} сообщений", color = Muted, fontSize = 10.sp, modifier = Modifier.weight(1f))
                                TextButton(onClick = {
                                    renameTarget = chat
                                    renameValue = chat.title
                                }, contentPadding = PaddingValues(horizontal = 7.dp)) { Text("Изменить", color = Blue, fontSize = 11.sp) }
                                TextButton(onClick = { vm.deleteChat(chat.id) }, contentPadding = PaddingValues(horizontal = 7.dp)) { Text("Удалить", color = Color(0xFFFF8798), fontSize = 11.sp) }
                            }
                        }
                    }
                }
            }
        }
    }

    if (showSettings) {
        AlertDialog(
            onDismissRequest = { showSettings = false },
            containerColor = Panel,
            title = { Text("Подключение API", color = MainText) },
            text = {
                Column(verticalArrangement = Arrangement.spacedBy(9.dp)) {
                    OutlinedTextField(url, { url = it }, label = { Text("Base URL") }, singleLine = true)
                    OutlinedTextField(model, { model = it }, label = { Text("Model ID") }, singleLine = true)
                    OutlinedTextField(key, { key = it }, label = { Text("API key") }, singleLine = true, visualTransformation = PasswordVisualTransformation())
                    Text("Ключ хранится только в памяти приложения и не сохраняется в истории или GitHub. После перезапуска его нужно ввести снова.", color = Muted, fontSize = 10.sp, lineHeight = 14.sp)
                }
            },
            confirmButton = {
                TextButton(onClick = { vm.updateSettings(url, model, key); showSettings = false }) { Text("Сохранить", color = Violet) }
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
