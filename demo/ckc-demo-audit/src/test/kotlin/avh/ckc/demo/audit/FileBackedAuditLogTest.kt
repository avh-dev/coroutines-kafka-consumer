package avh.ckc.demo.audit

import java.nio.file.Files
import kotlin.io.path.readLines
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class FileBackedAuditLogTest {
    @Test
    fun `writes published and processed audit records as compact tsv`() {
        val directory = Files.createTempDirectory("ckc-audit-test-")
        val config = FileAuditLogConfig(
            directory = directory,
            filePrefix = "audit",
            queueCapacity = 8,
            flushRecords = 2,
            flushIntervalMs = 10,
            fsyncIntervalMs = 0
        )

        FileBackedAuditLog(config).use { audit ->
            audit.published("order.events.v1", "order-1", 3, 42, 1_000)
            audit.processed("cauldron.events.v1", "cauldron-1", 4, 77, 2_000)
        }

        val lines = directory.resolve("audit-000000.tsv").readLines()
        assertEquals("# ckc-demo-audit-v1", lines[0])
        assertTrue(lines[1].matches(Regex("""P\t1\t3\t42\t1000\t\d+\torder-1""")))
        assertTrue(lines[2].matches(Regex("""C\t3\t4\t77\t2000\t\d+\tcauldron-1""")))
    }

    @Test
    fun `escapes tsv delimiters in keys`() {
        val directory = Files.createTempDirectory("ckc-audit-test-")
        val config = FileAuditLogConfig(
            directory = directory,
            filePrefix = "audit",
            flushIntervalMs = 10,
            fsyncIntervalMs = 0
        )

        FileBackedAuditLog(config).use { audit ->
            audit.published("batch.events.v1", "batch\t1\n2", 0, 1, 10)
        }

        val lines = directory.resolve("audit-000000.tsv").readLines()
        assertTrue(lines[1].endsWith("\tbatch 1 2"))
    }

    @Test
    fun `rotates audit segments`() {
        val directory = Files.createTempDirectory("ckc-audit-test-")
        val config = FileAuditLogConfig(
            directory = directory,
            filePrefix = "audit",
            flushRecords = 1,
            flushIntervalMs = 10,
            fsyncIntervalMs = 0,
            maxSegmentBytes = 60
        )

        FileBackedAuditLog(config).use { audit ->
            repeat(3) { index ->
                audit.published("order.events.v1", "order-$index", 0, index.toLong(), 10)
            }
        }

        assertTrue(directory.resolve("audit-000000.tsv").toFile().isFile)
        assertTrue(directory.resolve("audit-000001.tsv").toFile().isFile)
    }
}
