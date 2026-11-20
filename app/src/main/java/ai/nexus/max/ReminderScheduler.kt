package ai.nexus.max

import android.app.AlarmManager
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import androidx.core.app.NotificationCompat
import org.json.JSONArray
import org.json.JSONObject
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.Locale

internal data class ParsedReminder(
    val task: String,
    val triggerAtMillis: Long,
    val zoneId: String
)

internal object ReminderParser {
    private val timeRegex = Regex("""(?<!\d)([01]?\d|2[0-3]):([0-5]\d)(?!\d)""")
    private val dateRegex = Regex("""\b(\d{4}-\d{2}-\d{2})\b""")

    fun parse(input: String, nowMillis: Long = System.currentTimeMillis(), deviceZone: ZoneId = ZoneId.systemDefault()): ParsedReminder? {
        if (!input.contains("напомни", ignoreCase = true)) return null
        val timeMatch = timeRegex.find(input) ?: return null
        val hour = timeMatch.groupValues[1].toInt()
        val minute = timeMatch.groupValues[2].toInt()
        val lower = input.lowercase(Locale("ru"))
        val zone = if (Regex("""по\s+(москве|мск|московскому\s+времени)""").containsMatchIn(lower)) ZoneId.of("Europe/Moscow") else deviceZone
        val today = Instant.ofEpochMilli(nowMillis).atZone(zone).toLocalDate()
        val dateMatch = dateRegex.find(input)
        val date = when {
            dateMatch != null -> LocalDate.parse(dateMatch.groupValues[1], DateTimeFormatter.ISO_LOCAL_DATE)
            lower.contains("послезавтра") -> today.plusDays(2)
            lower.contains("завтра") -> today.plusDays(1)
            else -> today
        }
        var target = date.atTime(LocalTime.of(hour, minute)).atZone(zone).toInstant().toEpochMilli()
        if (dateMatch == null && !lower.contains("завтра") && !lower.contains("послезавтра") && target <= nowMillis) {
            target = date.plusDays(1).atTime(LocalTime.of(hour, minute)).atZone(zone).toInstant().toEpochMilli()
        }
        if (target <= nowMillis) return null

        var task = input
            .replace(Regex("""(?i)напомни(?:\s+мне)?"""), "")
            .replace(timeRegex, "")
            .replace(dateRegex, "")
            .replace(Regex("""(?i)по\s+(?:москве|мск|московскому\s+времени)"""), "")
            .replace(Regex("""(?i)\b(сегодня|завтра|послезавтра)\b"""), "")
            .replace(Regex("""(?i)\b(?:в|на)\s*$"""), "")
            .replace(Regex("""(?i)^\s*(?:что|о\s+том,?\s+что)\s*"""), "")
            .replace(Regex("""\s+"""), " ")
            .trim(' ', ',', '.', ':', ';', '-', '—')
        if (task.isBlank()) return null
        return ParsedReminder(task, target, zone.id)
    }
}

internal object ReminderScheduler {
    private const val PREFS = "nexus_reminders"
    private const val KEY = "scheduled_json"
    private const val CHANNEL_ID = "nexus_reminders"
    const val CHAT_ID = "nexus-system-reminders"

    fun schedule(context: Context, id: String, task: String, triggerAtMillis: Long, zoneId: String) {
        val prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
        val array = JSONArray(prefs.getString(KEY, "[]") ?: "[]")
        val updated = JSONArray()
        for (i in 0 until array.length()) {
            val item = array.optJSONObject(i) ?: continue
            if (item.optString("id") != id) updated.put(item)
        }
        updated.put(JSONObject().put("id", id).put("task", task).put("triggerAt", triggerAtMillis).put("zoneId", zoneId))
        prefs.edit().putString(KEY, updated.toString()).apply()
        scheduleAlarm(context, id, triggerAtMillis)
    }

    fun scheduleAlarm(context: Context, id: String, triggerAtMillis: Long) {
        val alarmManager = context.getSystemService(Context.ALARM_SERVICE) as AlarmManager
        val intent = Intent(context, ReminderAlarmReceiver::class.java).putExtra("reminder_id", id)
        val pending = PendingIntent.getBroadcast(
            context, id.hashCode(), intent,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S && alarmManager.canScheduleExactAlarms()) {
            alarmManager.setExactAndAllowWhileIdle(AlarmManager.RTC_WAKEUP, triggerAtMillis, pending)
        } else {
            alarmManager.setAndAllowWhileIdle(AlarmManager.RTC_WAKEUP, triggerAtMillis, pending)
        }
    }

    fun restoreAlarms(context: Context) {
        val array = JSONArray(context.getSharedPreferences(PREFS, Context.MODE_PRIVATE).getString(KEY, "[]") ?: "[]")
        val now = System.currentTimeMillis()
        for (i in 0 until array.length()) {
            val item = array.optJSONObject(i) ?: continue
            val id = item.optString("id")
            val time = item.optLong("triggerAt")
            if (id.isNotBlank() && time > now) scheduleAlarm(context, id, time)
        }
    }

    fun deliver(context: Context, id: String) {
        val prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
        val array = JSONArray(prefs.getString(KEY, "[]") ?: "[]")
        var task: String? = null
        val remaining = JSONArray()
        for (i in 0 until array.length()) {
            val item = array.optJSONObject(i) ?: continue
            if (item.optString("id") == id) task = item.optString("task")
            else remaining.put(item)
        }
        prefs.edit().putString(KEY, remaining.toString()).apply()
        val reminder = task ?: return

        val chatPrefs = context.getSharedPreferences("nexus_local", Context.MODE_PRIVATE)
        val chats = JSONArray(chatPrefs.getString("chats_json", "[]") ?: "[]")
        val result = JSONArray()
        var found = false
        for (i in 0 until chats.length()) {
            val chat = chats.optJSONObject(i) ?: continue
            if (chat.optString("id") == CHAT_ID) {
                val lines = chat.optJSONArray("lines") ?: JSONArray()
                lines.put(JSONObject().put("role", "assistant").put("content", "Напоминание: $reminder").put("attachments", JSONArray()))
                chat.put("lines", lines)
                    .put("title", "Напоминания")
                    .put("isSystem", true)
                    .put("isPinned", false)
                    .put("hasUnread", true)
                    .put("updatedAt", System.currentTimeMillis())
                found = true
            }
            result.put(chat)
        }
        if (!found) {
            val lines = JSONArray().put(JSONObject().put("role", "assistant").put("content", "Напоминание: $reminder").put("attachments", JSONArray()))
            result.put(JSONObject().put("id", CHAT_ID).put("title", "Напоминания")
                .put("updatedAt", System.currentTimeMillis()).put("isPinned", false)
                .put("isSystem", true).put("hasUnread", true).put("lines", lines))
        }
        chatPrefs.edit().putString("chats_json", result.toString()).apply()
        showNotification(context, reminder)
    }

    private fun showNotification(context: Context, task: String) {
        val manager = context.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            manager.createNotificationChannel(NotificationChannel(CHANNEL_ID, "Напоминания NEXUS", NotificationManager.IMPORTANCE_HIGH))
        }
        if (Build.VERSION.SDK_INT >= 33 && context.checkSelfPermission(android.Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) return
        val openApp = PendingIntent.getActivity(
            context, 9001, Intent(context, MainActivity::class.java).addFlags(Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP),
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )
        manager.notify(task.hashCode(), NotificationCompat.Builder(context, CHANNEL_ID)
            .setSmallIcon(android.R.drawable.ic_dialog_info)
            .setContentTitle("NEXUS — Напоминание")
            .setContentText(task)
            .setStyle(NotificationCompat.BigTextStyle().bigText(task))
            .setPriority(NotificationCompat.PRIORITY_HIGH)
            .setAutoCancel(true)
            .setContentIntent(openApp)
            .build())
    }
}

class ReminderAlarmReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent) {
        val id = intent.getStringExtra("reminder_id") ?: return
        ReminderScheduler.deliver(context, id)
    }
}

class ReminderBootReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent) {
        if (intent.action == Intent.ACTION_BOOT_COMPLETED || intent.action == Intent.ACTION_MY_PACKAGE_REPLACED) {
            ReminderScheduler.restoreAlarms(context)
        }
    }
}
