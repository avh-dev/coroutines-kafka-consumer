package avh.ckc.demo.audit

import kotlin.io.path.Path
import kotlin.test.Test
import kotlin.test.assertEquals

class FileAuditLogConfigTest {
    @Test
    fun `reads audit writer settings from environment`() {
        val config = FileAuditLogConfig.fromEnvironment(
            mapOf(
                "AUDIT_LOG_DIR" to "/tmp/audit",
                "AUDIT_LOG_FILE_PREFIX" to "processed-pod-1",
                "AUDIT_QUEUE_CAPACITY" to "17",
                "AUDIT_FLUSH_RECORDS" to "5",
                "AUDIT_FLUSH_INTERVAL_MS" to "25",
                "AUDIT_FSYNC_INTERVAL_MS" to "250",
                "AUDIT_MAX_SEGMENT_BYTES" to "4096"
            )
        )

        assertEquals(Path("/tmp/audit"), config.directory)
        assertEquals("processed-pod-1", config.filePrefix)
        assertEquals(17, config.queueCapacity)
        assertEquals(5, config.flushRecords)
        assertEquals(25, config.flushIntervalMs)
        assertEquals(250, config.fsyncIntervalMs)
        assertEquals(4096, config.maxSegmentBytes)
    }
}
