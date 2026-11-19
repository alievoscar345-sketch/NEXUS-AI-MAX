package ai.nexus.max

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.BasicTextField
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.lifecycle.ViewModel
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
import java.util.concurrent.TimeUnit

data class ChatLine(val role: String, val content: String)
data class ApiSettings(val baseUrl: String = "https://anymodel.org/v1", val model: String = "cc/claude-fable-5-2", val apiKey: String = "")

class NexusViewModel : ViewModel() {
    var settings by mutableStateOf(ApiSettings()); private set
    var lines by mutableStateOf(listOf<ChatLine>()); private set
    var busy by mutableStateOf(false); private set
    var error by mutableStateOf<String?>(null); private set
    private val client = OkHttpClient.Builder().connectTimeout(25, TimeUnit.SECONDS).readTimeout(120, TimeUnit.SECONDS).build()

    fun updateSettings(url: String, model: String, key: String) {
        settings = ApiSettings(url.trim().trimEnd('/'), model.trim(), key.trim())
        error = null
    }

    fun send(text: String) {
        if (busy || text.isBlank()) return
        val cfg = settings
        if (cfg.apiKey.isBlank() || cfg.model.isBlank()) { error = "Открой API SETTINGS и введи API key."; return }
        val history = lines + ChatLine("user", text.trim())
        lines = history
        busy = true
        error = null
        viewModelScope.launch {
            try {
                val answer = withContext(Dispatchers.IO) {
                    val messages = JSONArray().put(JSONObject().put("role", "system").put("content", SYSTEM_PROMPT))
                    history.takeLast(20).forEach { messages.put(JSONObject().put("role", it.role).put("content", it.content)) }
                    val body = JSONObject().put("model", cfg.model).put("messages", messages).put("temperature", 0.35)
                    val request = Request.Builder().url(cfg.baseUrl + "/chat/completions")
                        .header("Authorization", "Bearer ${cfg.apiKey}")
                        .header("Content-Type", "application/json")
                        .post(body.toString().toRequestBody("application/json".toMediaType())).build()
                    client.newCall(request).execute().use { response ->
                        val raw = response.body?.string().orEmpty()
                        if (!response.isSuccessful) throw IllegalStateException("API ${response.code}: ${raw.take(450)}")
                        JSONObject(raw).getJSONArray("choices").getJSONObject(0).getJSONObject("message").getString("content")
                    }
                }
                lines = lines + ChatLine("assistant", answer)
            } catch (e: Exception) {
                error = e.message ?: "Ошибка запроса"
            } finally {
                busy = false
            }
        }
    }

    companion object {
        private const val SYSTEM_PROMPT = "You are NEXUS AI MAX, a capable personal AI assistant. Respond in the user's language. Be rigorous, distinguish facts from uncertainty, and never claim to have browsed or executed an action unless a connected tool actually did so. Request confirmation before consequential external actions."
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
    var showSettings by remember { mutableStateOf(false) }
    var url by remember { mutableStateOf("https://anymodel.org/v1") }
    var model by remember { mutableStateOf("cc/claude-fable-5-2") }
    var key by remember { mutableStateOf("") }

    MaterialTheme(colorScheme = darkColorScheme(primary = Color(0xFF9BB7FF), background = Color(0xFF0B1020), surface = Color(0xFF151D30))) {
        Scaffold(containerColor = Color(0xFF0B1020), topBar = {
            TopAppBar(title = { Column {
                Text("NEXUS AI MAX")
                Text("Claude Fable 5.2 · AnyModel", fontSize = 11.sp, color = Color.Gray)
            } }, actions = {
                TextButton(onClick = { url = vm.settings.baseUrl; model = vm.settings.model; key = vm.settings.apiKey; showSettings = true }) { Text("API SETTINGS") }
            }, colors = TopAppBarDefaults.topAppBarColors(containerColor = Color(0xFF151D30)))
        }, bottomBar = {
            Row(Modifier.fillMaxWidth().background(Color(0xFF151D30)).padding(10.dp), verticalAlignment = Alignment.CenterVertically) {
                BasicTextField(value = input, onValueChange = { input = it }, modifier = Modifier.weight(1f).background(Color(0xFF263149), RoundedCornerShape(20.dp)).padding(14.dp), textStyle = LocalTextStyle.current.copy(color = Color.White), decorationBox = { inner ->
                    Box { if (input.isEmpty()) Text("Задача для NEXUS…", color = Color.Gray); inner() }
                })
                Spacer(Modifier.width(8.dp))
                Button(enabled = !vm.busy && input.isNotBlank(), onClick = { vm.send(input); input = "" }) { Text("SEND") }
            }
        }) { padding ->
            Column(Modifier.fillMaxSize().padding(padding)) {
                vm.error?.let { Text(it, color = Color(0xFFFF8A80), modifier = Modifier.padding(12.dp)) }
                if (vm.lines.isEmpty()) {
                    Column(Modifier.fillMaxSize().padding(24.dp), verticalArrangement = Arrangement.Center, horizontalAlignment = Alignment.CenterHorizontally) {
                        Text("NEXUS", fontSize = 38.sp, color = Color(0xFF9BB7FF))
                        Text("Your personal AI system", color = Color.LightGray)
                        Spacer(Modifier.height(10.dp))
                        Text("Открой API SETTINGS, чтобы подключить Claude Fable 5.2.", color = Color.Gray)
                    }
                } else LazyColumn(Modifier.fillMaxSize(), contentPadding = PaddingValues(14.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) {
                    items(vm.lines) { line ->
                        Row(Modifier.fillMaxWidth(), horizontalArrangement = if (line.role == "user") Arrangement.End else Arrangement.Start) {
                            Surface(color = if (line.role == "user") Color(0xFF263D61) else Color(0xFF1B2639), shape = RoundedCornerShape(15.dp), modifier = Modifier.widthIn(max = 330.dp)) {
                                Text(line.content, color = Color.White, modifier = Modifier.padding(13.dp))
                            }
                        }
                    }
                    if (vm.busy) item { Text("NEXUS обрабатывает запрос…", color = Color.Gray) }
                }
            }
        }
    }

    if (showSettings) AlertDialog(onDismissRequest = { showSettings = false }, title = { Text("Подключение AnyModel API") }, text = {
        Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
            OutlinedTextField(url, { url = it }, label = { Text("Base URL") }, singleLine = true)
            OutlinedTextField(model, { model = it }, label = { Text("Model ID") }, singleLine = true)
            OutlinedTextField(key, { key = it }, label = { Text("API key") }, singleLine = true, visualTransformation = PasswordVisualTransformation())
            Text("Ключ скрыт на экране и хранится только в памяти приложения. После перезапуска его нужно ввести снова. Не отправляй ключ в чат и не публикуй в GitHub.", fontSize = 11.sp, color = Color.Gray)
        }
    }, confirmButton = {
        TextButton(onClick = { vm.updateSettings(url, model, key); showSettings = false }) { Text("Сохранить") }
    }, dismissButton = { TextButton(onClick = { showSettings = false }) { Text("Закрыть") } })
}
