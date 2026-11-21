package ai.nexus.max

import android.Manifest
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import androidx.core.app.NotificationCompat
import androidx.work.Constraints
import androidx.work.CoroutineWorker
import androidx.work.ExistingPeriodicWorkPolicy
import androidx.work.NetworkType
import androidx.work.PeriodicWorkRequestBuilder
import androidx.work.WorkManager
import androidx.work.WorkerParameters
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.IOException
import java.util.concurrent.TimeUnit

internal object AgentTaskNotifications {
    private const val WORK_NAME = "nexus-agent-task-notifications"

    fun schedule(context: Context) {
        val constraints = Constraints.Builder()
            .setRequiredNetworkType(NetworkType.CONNECTED)
            .build()
        val request = PeriodicWorkRequestBuilder<AgentTaskNotificationWorker>(15, TimeUnit.MINUTES)
            .setConstraints(constraints)
            .build()
        WorkManager.getInstance(context.applicationContext)
            .enqueueUniquePeriodicWork(WORK_NAME, ExistingPeriodicWorkPolicy.KEEP, request)
    }

    fun cancel(context: Context) {
        WorkManager.getInstance(context.applicationContext).cancelUniqueWork(WORK_NAME)
    }
}

class AgentTaskNotificationWorker(
    appContext: Context,
    params: WorkerParameters
) : CoroutineWorker(appContext, params) {

    override suspend fun doWork(): Result {
        val prefs = applicationContext.getSharedPreferences("nexus_local", Context.MODE_PRIVATE)
        val serverUrl = prefs.getString("agent_server_url", "").orEmpty()
        val token = SecureSecretStore.load(applicationContext)
        if (!serverUrl.startsWith("https://") || token.isBlank()) return Result.success()

        return try {
            val tasks = withContext(Dispatchers.IO) {
                AgentTaskClient(serverUrl, token).listTasks(100)
            }
            val finished = tasks.filter { it.status == "completed" || it.status == "failed" }
            val known = prefs.getStringSet("agent_notified_task_ids", emptySet())?.toMutableSet()
                ?: mutableSetOf()

            // First run establishes a baseline, so old historical tasks do not trigger a burst of alerts.
            if (!prefs.getBoolean("agent_notifications_initialized", false)) {
                known.addAll(finished.map { it.id })
                prefs.edit().putStringSet("agent_notified_task_ids", known)
                    .putBoolean("agent_notifications_initialized", true).apply()
                return Result.success()
            }

            finished.filterNot { it.id in known }.forEach { task ->
                if (showNotification(task)) known.add(task.id)
            }
            prefs.edit().putStringSet("agent_notified_task_ids", known).apply()
            Result.success()
        } catch (_: IOException) {
            Result.retry()
        } catch (_: Exception) {
            Result.retry()
        }
    }

    private fun showNotification(task: AgentTaskSnapshot): Boolean {
        if (Build.VERSION.SDK_INT >= 33 &&
            applicationContext.checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) return false

        val manager = applicationContext.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        if (!manager.areNotificationsEnabled()) return false
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            manager.createNotificationChannel(
                NotificationChannel(
                    "nexus_agent_updates",
                    "Результаты автономного агента",
                    NotificationManager.IMPORTANCE_DEFAULT
                )
            )
        }
        val openApp = PendingIntent.getActivity(
            applicationContext,
            task.id.hashCode(),
            Intent(applicationContext, MainActivity::class.java)
                .addFlags(Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP),
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )
        val completed = task.status == "completed"
        val detail = (if (completed) task.summary else task.error)
            .ifBlank { task.prompt }
            .take(500)
        manager.notify(
            task.id.hashCode(),
            NotificationCompat.Builder(applicationContext, "nexus_agent_updates")
                .setSmallIcon(android.R.drawable.ic_dialog_info)
                .setContentTitle(if (completed) "NEXUS: задача завершена" else "NEXUS: задача завершилась с ошибкой")
                .setContentText(detail)
                .setStyle(NotificationCompat.BigTextStyle().bigText(detail))
                .setAutoCancel(true)
                .setContentIntent(openApp)
                .build()
        )
        return true
    }
}
