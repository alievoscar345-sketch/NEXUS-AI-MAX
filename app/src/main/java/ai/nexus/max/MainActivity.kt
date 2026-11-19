package ai.nexus.max

import android.app.Application
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
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

private val Ink = Color(0xFF090A10)
private val Panel = Color(0xFF12141E)
private val PanelRaised = Color(0xFF1A1D2A)
private val Violet = Color(0xFF8B78FF)
private val Blue = Color(0xFF62B5FF)
private val MainText = Color(0xFFF2F1FA)
private val Muted = Color(0xFF9294A7)

data class ChatLine(val role: String, val content: String)
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

    fun send(text: String) {
        if (busy || text.isBlank()) return
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
        val userLine = ChatLine("user", text.trim())
        val history = oldChat.lines + userLine
        val newTitle = if (oldChat.lines.isEmpty()) text.trim().replace("\n", " ").take(36).ifBlank { "Новый диалог" } else oldChat.title
        updateChat(chatId, oldChat.copy(title = newTitle, updatedAt = System.currentTimeMillis(), lines = history))
        busy = true
        error = null

        viewModelScope.launch {
            try {
                val answer = withContext(Dispatchers.IO) {
                    val messages = JSONArray().put(
                        JSONObject().put("role", "system").put("content", SYSTEM_PROMPT)
                    )
                    history.takeLast(24).forEach { line ->
                        messages.put(JSONObject().put("role", line.role).put("content", line.content))
                    }
                    val body = JSONObject()
                        .put("model", cfg.model)
                        .put("messages", messages)
                        .put("temperature", 0.35)
                    val request = Request.Builder()
                        .url(cfg.baseUrl + "/chat/completions")
                        .header("Authorization", "Bearer ${cfg.apiKey}")
                        .header("Content-Type", "application/json")
                        .post(body.toString().toRequestBody("application/json".toMediaType()))
                        .build()
                    client.newCall(request).execute().use { response ->
                        val raw = response.body?.string().orEmpty()
                        if (!response.isSuccessful) throw IllegalStateException("API ${response.code}: ${raw.take(350)}")
                        JSONObject(raw).getJSONArray("choices").getJSONObject(0)
                            .getJSONObject("message").getString("content")
                    }
                }
                val latest = chats.firstOrNull { it.id == chatId }
                if (latest != null) updateChat(
                    chatId,
                    latest.copy(lines = latest.lines + ChatLine("assistant", answer), updatedAt = System.currentTimeMillis())
                )
            } catch (e: Exception) {
                error = e.message ?: "Ошибка запроса"
            } finally {
                busy = false
            }
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
                lineArray.put(JSONObject().put("role", line.role).put("content", line.content))
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
                    ChatLine(line.optString("role"), line.optString("content"))
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
        setContent { NexusApp() }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun NexusApp(vm: NexusViewModel = viewModel()) {
    var input by remember { mutableStateOf("") }
    var showHistory by remember { mutableStateOf(false) }
    var showSettings by remember { mutableStateOf(false) }
    var renameTarget by remember { mutableStateOf<ChatSession?>(null) }
    var renameValue by remember { mutableStateOf("") }
    var url by remember { mutableStateOf(vm.settings.baseUrl) }
    var model by remember { mutableStateOf(vm.settings.model) }
    var key by remember { mutableStateOf("") }

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
                    Modifier.fillMaxWidth().background(Panel).padding(horizontal = 16.dp, vertical = 13.dp),
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    Box(
                        Modifier.size(39.dp).clip(RoundedCornerShape(13.dp))
                            .background(Brush.linearGradient(listOf(Violet, Blue))),
                        contentAlignment = Alignment.Center
                    ) {
                        Text("N", color = Color.White, fontWeight = FontWeight.Black, fontSize = 22.sp)
                    }
                    Spacer(Modifier.width(11.dp))
                    Column(Modifier.weight(1f)) {
                        Text("NEXUS", color = MainText, fontWeight = FontWeight.Bold, fontSize = 17.sp, letterSpacing = 2.sp)
                        Text("AI MAX  ·  PERSONAL INTELLIGENCE", color = Muted, fontSize = 8.sp, letterSpacing = 1.sp)
                    }
                    TextButton(onClick = { showHistory = true }) {
                        Text("ЧАТЫ", color = Blue, fontSize = 11.sp, fontWeight = FontWeight.Bold)
                    }
                    TextButton(onClick = { vm.createChat(); input = "" }) {
                        Text("+", color = Violet, fontSize = 24.sp)
                    }
                    TextButton(onClick = {
                        url = vm.settings.baseUrl
                        model = vm.settings.model
                        key = vm.settings.apiKey
                        showSettings = true
                    }) {
                        Text("API", color = Muted, fontSize = 11.sp)
                    }
                }
            },
            bottomBar = {
                Column(Modifier.fillMaxWidth().background(Panel).padding(horizontal = 12.dp, vertical = 10.dp)) {
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
                            enabled = !vm.busy && input.isNotBlank(),
                            onClick = { vm.send(input); input = "" },
                            shape = RoundedCornerShape(16.dp),
                            contentPadding = PaddingValues(horizontal = 17.dp, vertical = 12.dp),
                            colors = ButtonDefaults.buttonColors(containerColor = Violet, disabledContainerColor = Color(0xFF343044))
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
                    Surface(color = Color(0xFF3A202A), shape = RoundedCornerShape(12.dp), modifier = Modifier.fillMaxWidth().padding(horizontal = 14.dp, vertical = 8.dp)) {
                        Text(message, color = Color(0xFFFFA6B4), fontSize = 12.sp, modifier = Modifier.padding(12.dp))
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
                                .background(Brush.linearGradient(listOf(Violet.copy(alpha = 0.25f), Blue.copy(alpha = 0.15f)))),
                            contentAlignment = Alignment.Center
                        ) {
                            Text("✳", color = Violet, fontSize = 28.sp)
                        }
                        Spacer(Modifier.height(22.dp))
                        Text("ДУМАЙ ШИРЕ.", color = Muted, fontSize = 11.sp, letterSpacing = 3.sp, fontWeight = FontWeight.Bold)
                        Text("Твоя идея.
Следующий уровень.", color = MainText, fontSize = 31.sp, lineHeight = 38.sp, fontWeight = FontWeight.SemiBold)
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
                                    color = if (isUser) Color(0xFF24263A) else Panel,
                                    shape = RoundedCornerShape(18.dp),
                                    modifier = Modifier.widthIn(max = 340.dp)
                                ) {
                                    Text(line.content, color = MainText, fontSize = 14.sp, lineHeight = 21.sp, modifier = Modifier.padding(horizontal = 15.dp, vertical = 13.dp))
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
                        color = if (chat.id == vm.activeChatId) Color(0xFF28243F) else PanelRaised,
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
