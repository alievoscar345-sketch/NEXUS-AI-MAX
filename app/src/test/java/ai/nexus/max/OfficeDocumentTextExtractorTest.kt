package ai.nexus.max

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.ByteArrayOutputStream
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

class OfficeDocumentTextExtractorTest {
    private fun zip(vararg entries: Pair<String, String>): ByteArray {
        val output = ByteArrayOutputStream()
        ZipOutputStream(output).use { archive ->
            entries.forEach { (name, content) ->
                archive.putNextEntry(ZipEntry(name))
                archive.write(content.toByteArray(Charsets.UTF_8))
                archive.closeEntry()
            }
        }
        return output.toByteArray()
    }

    @Test
    fun extractsXlsxSharedAndInlineStringsInSheetOrder() {
        val bytes = zip(
            "xl/worksheets/sheet2.xml" to """<worksheet><sheetData><row><c t="inlineStr"><is><t>Second</t></is></c></row></sheetData></worksheet>""",
            "xl/sharedStrings.xml" to """<sst><si><t>Revenue</t></si><si><r><t>Q1</t></r><r><t> 2026</t></r></si></sst>""",
            "xl/worksheets/sheet1.xml" to """<worksheet><sheetData><row><c t="s"><v>0</v></c><c t="s"><v>1</v></c><c><v>125.5</v></c></row></sheetData></worksheet>"""
        )
        val text = extractXlsxText(bytes)
        assertTrue(text.indexOf("sheet1.xml") < text.indexOf("sheet2.xml"))
        assertTrue(text.contains("Revenue"))
        assertTrue(text.contains("Q1 2026"))
        assertTrue(text.contains("125.5"))
        assertTrue(text.contains("Second"))
    }

    @Test
    fun extractsPptxSlideTextAndKeepsSlideOrder() {
        val bytes = zip(
            "ppt/slides/slide2.xml" to """<p:sld xmlns:p="p" xmlns:a="a"><a:p><a:r><a:t>Later</a:t></a:r></a:p></p:sld>""",
            "ppt/slides/slide1.xml" to """<p:sld xmlns:p="p" xmlns:a="a"><a:p><a:r><a:t>Title</a:t></a:r></a:p><a:p><a:r><a:t>Body</a:t></a:r></a:p></p:sld>"""
        )
        val text = extractPptxText(bytes)
        assertTrue(text.indexOf("Title") < text.indexOf("Later"))
        assertTrue(text.contains("Body"))
    }

    @Test
    fun rejectsArchivesWithoutExpectedOfficeParts() {
        val bytes = zip("notes.txt" to "not an office document")
        var rejected = false
        try { extractXlsxText(bytes) } catch (_: IllegalArgumentException) { rejected = true }
        assertTrue(rejected)
    }

    @Test
    fun respectsOutputCharacterLimit() {
        val bytes = zip("ppt/slides/slide1.xml" to """<p:sld xmlns:p="p" xmlns:a="a"><a:p><a:r><a:t>LongText</a:t></a:r></a:p></p:sld>""")
        assertEquals(12, extractPptxText(bytes, maxChars = 12).length)
    }
}
