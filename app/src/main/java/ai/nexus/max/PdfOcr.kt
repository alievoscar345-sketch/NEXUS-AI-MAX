package ai.nexus.max

import android.content.Context
import android.graphics.Bitmap
import android.graphics.pdf.PdfRenderer
import android.os.ParcelFileDescriptor
import com.googlecode.tesseract.android.TessBaseAPI
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileOutputStream
import java.security.MessageDigest
import java.util.concurrent.TimeUnit

private const val TESSDATA_COMMIT = "87416418657359cb625c412a48b6e1d6d41c29bd"
private const val MAX_OCR_PAGES = 30
private const val MAX_OCR_CHARS = 1_000_000

private data class OcrModel(val language: String, val size: Int, val gitBlobSha1: String)

private val OcrModels = listOf(
    OcrModel("rus", 3_861_738, "b146cb2263acbc6383f8e92ea0ce759537687bb8"),
    OcrModel("eng", 4_113_088, "bbef4675053b5b468cdb477053e28b1c698ba08e")
)

private val ocrHttpClient by lazy {
    OkHttpClient.Builder()
        .connectTimeout(20, TimeUnit.SECONDS)
        .readTimeout(90, TimeUnit.SECONDS)
        .callTimeout(120, TimeUnit.SECONDS)
        .build()
}

private fun gitBlobSha1(file: File): String {
    val digest = MessageDigest.getInstance("SHA-1")
    val size = file.length()
    digest.update("blob $size\u0000".toByteArray(Charsets.US_ASCII))
    file.inputStream().use { input ->
        val buffer = ByteArray(16 * 1024)
        while (true) {
            val count = input.read(buffer)
            if (count < 0) break
            digest.update(buffer, 0, count)
        }
    }
    return digest.digest().joinToString("") { "%02x".format(it) }
}

private fun ensureOcrModel(tessdataDir: File, model: OcrModel) {
    val target = File(tessdataDir, "${model.language}.traineddata")
    if (target.isFile && target.length() == model.size.toLong() && gitBlobSha1(target) == model.gitBlobSha1) return
    if (target.exists()) target.delete()
    val url = "https://raw.githubusercontent.com/tesseract-ocr/tessdata_fast/$TESSDATA_COMMIT/${model.language}.traineddata"
    val request = Request.Builder().url(url).get().build()
    val response = ocrHttpClient.newCall(request).execute()
    response.use {
        if (!it.isSuccessful) throw IllegalStateException("Не удалось загрузить языковую модель ${model.language} (HTTP ${it.code}).")
        val body = it.body ?: throw IllegalStateException("Пустой ответ сервера языковых моделей.")
        if (body.contentLength() > model.size) throw IllegalStateException("Размер языковой модели превышает ожидаемый.")
        val temp = File(tessdataDir, "${model.language}.traineddata.tmp")
        try {
            body.byteStream().use { input ->
                FileOutputStream(temp).use { output ->
                    val buffer = ByteArray(16 * 1024)
                    var total = 0
                    while (true) {
                        val count = input.read(buffer)
                        if (count < 0) break
                        total += count
                        if (total > model.size) throw IllegalStateException("Файл языковой модели слишком большой.")
                        output.write(buffer, 0, count)
                    }
                    output.fd.sync()
                    if (total != model.size) throw IllegalStateException("Загружена неполная языковая модель.")
                }
            }
            if (gitBlobSha1(temp) != model.gitBlobSha1) throw IllegalStateException("Контрольная сумма языковой модели не совпала.")
            if (!temp.renameTo(target)) throw IllegalStateException("Не удалось сохранить языковую модель.")
        } finally {
            if (temp.exists()) temp.delete()
        }
    }
}

private fun prepareOcrModels(context: Context): File {
    val root = File(context.filesDir, "nexus_tesseract")
    val tessdata = File(root, "tessdata")
    if (!tessdata.exists() && !tessdata.mkdirs()) throw IllegalStateException("Не удалось создать каталог OCR.")
    OcrModels.forEach { ensureOcrModel(tessdata, it) }
    return root
}

internal fun extractScannedPdfText(context: Context, pdfBytes: ByteArray): String {
    val tempPdf = File(context.cacheDir, "nexus_ocr_${System.nanoTime()}.pdf")
    FileOutputStream(tempPdf).use { it.write(pdfBytes) }
    try {
        val dataPath = prepareOcrModels(context)
        val descriptor = ParcelFileDescriptor.open(tempPdf, ParcelFileDescriptor.MODE_READ_ONLY)
        descriptor.use { pfd ->
            PdfRenderer(pfd).use { renderer ->
                if (renderer.pageCount > MAX_OCR_PAGES) {
                    throw IllegalArgumentException("OCR ограничен 30 страницами за один документ. Раздели скан на части.")
                }
                val tess = TessBaseAPI()
                if (!tess.init(dataPath.absolutePath, "rus+eng")) {
                    tess.recycle()
                    throw IllegalStateException("Не удалось инициализировать Tesseract с русской и английской моделями.")
                }
                try {
                    return buildString {
                        for (index in 0 until renderer.pageCount) {
                            if (length >= MAX_OCR_CHARS) break
                            renderer.openPage(index).use { page ->
                                val scale = minOf(2200f / page.width, 2200f / page.height, 3f)
                                val width = (page.width * scale).toInt().coerceAtMost(2200)
                                val height = (page.height * scale).toInt().coerceAtMost(2200)
                                val bitmap = Bitmap.createBitmap(width, height, Bitmap.Config.ARGB_8888)
                                try {
                                    page.render(bitmap, null, null, PdfRenderer.Page.RENDER_MODE_FOR_DISPLAY)
                                    tess.setImage(bitmap)
                                    val recognized = tess.getUTF8Text()?.trim().orEmpty()
                                    if (recognized.isNotBlank()) {
                                        if (isNotEmpty()) append("\n\n")
                                        append("--- Страница ")
                                        append(index + 1)
                                        append(" (OCR) ---\n")
                                        append(recognized.take((MAX_OCR_CHARS - length).coerceAtLeast(0)))
                                    }
                                } finally {
                                    bitmap.recycle()
                                }
                            }
                        }
                    }.take(MAX_OCR_CHARS)
                } finally {
                    tess.recycle()
                }
            }
        }
    } finally {
        tempPdf.delete()
    }
}
