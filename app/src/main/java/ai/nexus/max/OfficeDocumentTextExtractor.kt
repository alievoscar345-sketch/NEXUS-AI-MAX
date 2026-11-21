package ai.nexus.max

import org.kxml2.io.KXmlParser
import org.xmlpull.v1.XmlPullParser
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.zip.ZipInputStream

private const val MAX_OFFICE_UNCOMPRESSED_BYTES = 20 * 1024 * 1024
private const val MAX_OFFICE_ENTRY_BYTES = 10 * 1024 * 1024

private fun readOfficeArchive(bytes: ByteArray, accept: (String) -> Boolean): List<Pair<String, ByteArray>> {
    require(bytes.size <= 10 * 1024 * 1024) { "Файл Office слишком большой. Максимум 10 МБ." }
    val selected = mutableListOf<Pair<String, ByteArray>>()
    var total = 0
    ZipInputStream(ByteArrayInputStream(bytes)).use { zip ->
        var entryCount = 0
        while (true) {
            val entry = zip.nextEntry ?: break
            entryCount++
            require(entryCount <= 2000) { "В архиве Office слишком много элементов." }
            val output = ByteArrayOutputStream()
            val buffer = ByteArray(8192)
            var entryBytes = 0
            while (true) {
                val count = zip.read(buffer)
                if (count < 0) break
                entryBytes += count
                total += count
                require(entryBytes <= MAX_OFFICE_ENTRY_BYTES && total <= MAX_OFFICE_UNCOMPRESSED_BYTES) {
                    "Распакованное содержимое Office превышает безопасный лимит 20 МБ."
                }
                output.write(buffer, 0, count)
            }
            if (accept(entry.name)) selected += entry.name to output.toByteArray()
            zip.closeEntry()
        }
    }
    return selected
}

private fun officeParser(xml: ByteArray): XmlPullParser =
    KXmlParser().apply {
        setFeature(XmlPullParser.FEATURE_PROCESS_DOCDECL, false)
        setInput(ByteArrayInputStream(xml), "UTF-8")
    }

private fun localTag(parser: XmlPullParser): String =
    (parser.name ?: "").substringAfter(':')

private fun parseXlsxSharedStrings(xml: ByteArray): List<String> {
    val values = mutableListOf<String>()
    val parser = officeParser(xml)
    var current: StringBuilder? = null
    var event = parser.eventType
    while (event != XmlPullParser.END_DOCUMENT) {
        when (event) {
            XmlPullParser.START_TAG -> if (localTag(parser) == "si") current = StringBuilder()
            XmlPullParser.END_TAG -> if (localTag(parser) == "si") {
                values += current?.toString().orEmpty()
                current = null
            }
            XmlPullParser.TEXT -> if (current != null && localTag(parser) == "t") current?.append(parser.text)
        }
        if (event == XmlPullParser.START_TAG && localTag(parser) == "t" && current != null) {
            current?.append(parser.nextText())
        }
        event = parser.next()
    }
    return values
}

private fun parseXlsxSheet(xml: ByteArray, sharedStrings: List<String>): String {
    val parser = officeParser(xml)
    val output = StringBuilder()
    var cellType = ""
    var cellValue = StringBuilder()
    var inCell = false
    var event = parser.eventType
    while (event != XmlPullParser.END_DOCUMENT && output.length < 1_000_000) {
        when (event) {
            XmlPullParser.START_TAG -> when (localTag(parser)) {
                "c" -> {
                    inCell = true
                    cellType = parser.getAttributeValue(null, "t").orEmpty()
                    cellValue = StringBuilder()
                }
                "v", "t" -> if (inCell) cellValue.append(parser.nextText())
            }
            XmlPullParser.END_TAG -> when (localTag(parser)) {
                "c" -> {
                    val raw = cellValue.toString()
                    val value = if (cellType == "s") {
                        raw.toIntOrNull()?.let { sharedStrings.getOrNull(it) }.orEmpty()
                    } else raw
                    output.append(value).append('\t')
                    inCell = false
                }
                "row" -> {
                    while (output.isNotEmpty() && output.last() == '\t') output.setLength(output.length - 1)
                    output.append('\n')
                }
            }
        }
        event = parser.next()
    }
    return output.toString().trim()
}

internal fun extractXlsxText(bytes: ByteArray, maxChars: Int = 1_000_000): String {
    val entries = readOfficeArchive(bytes) { name ->
        name == "xl/sharedStrings.xml" || Regex("xl/worksheets/sheet[0-9]+\\.xml").matches(name)
    }
    val shared = entries.firstOrNull { it.first == "xl/sharedStrings.xml" }
        ?.let { parseXlsxSharedStrings(it.second) }.orEmpty()
    val sheets = entries.filter { Regex("xl/worksheets/sheet[0-9]+\\.xml").matches(it.first) }
        .sortedBy { it.first.substringAfterLast("sheet").substringBefore('.').toIntOrNull() ?: Int.MAX_VALUE }
    require(sheets.isNotEmpty()) { "В XLSX не найдены листы с данными." }
    return buildString {
        sheets.forEachIndexed { index, (name, xml) ->
            if (length >= maxChars) return@forEachIndexed
            append("[Лист ").append(index + 1).append(": ").append(name.substringAfterLast('/')).append("]\n")
            val remaining = (maxChars - length).coerceAtLeast(0)
            if (remaining > 0) append(parseXlsxSheet(xml, shared).take(remaining))
            append("\n")
        }
    }.take(maxChars).trim()
}

private fun parsePptxSlide(xml: ByteArray): String {
    val parser = officeParser(xml)
    val output = StringBuilder()
    var event = parser.eventType
    while (event != XmlPullParser.END_DOCUMENT && output.length < 1_000_000) {
        when (event) {
            XmlPullParser.START_TAG -> if (localTag(parser) == "t") output.append(parser.nextText())
            XmlPullParser.END_TAG -> when (localTag(parser)) {
                "p" -> output.append('\n')
                "br" -> output.append('\n')
            }
        }
        event = parser.next()
    }
    return output.toString().trim()
}

internal fun extractPptxText(bytes: ByteArray, maxChars: Int = 1_000_000): String {
    val slides = readOfficeArchive(bytes) { name ->
        Regex("ppt/slides/slide[0-9]+\\.xml").matches(name)
    }.sortedBy { it.first.substringAfterLast("slide").substringBefore('.').toIntOrNull() ?: Int.MAX_VALUE }
    require(slides.isNotEmpty()) { "В PPTX не найдены слайды." }
    return buildString {
        slides.forEachIndexed { index, (name, xml) ->
            if (length >= maxChars) return@forEachIndexed
            append("[Слайд ").append(index + 1).append("]\n")
            val remaining = (maxChars - length).coerceAtLeast(0)
            if (remaining > 0) append(parsePptxSlide(xml).take(remaining))
            append("\n\n")
        }
    }.take(maxChars).trim()
}
