package avh.ckc.loadtest.kafka

import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import java.nio.file.Files
import kotlin.io.path.readLines
import kotlin.test.Test
import kotlin.test.assertTrue

class LoadTestAuditLogTest {
    @Test
    fun `writes producer acknowledgements to file-backed audit`() {
        val directory = Files.createTempDirectory("ckc-load-test-audit-")
        val audit = LoadTestAuditLog.fromEnvironment(
            mapOf(
                "AUDIT_LOG_DIR" to directory.toString(),
                "AUDIT_LOG_FILE_PREFIX" to "published-test",
                "AUDIT_FLUSH_INTERVAL_MS" to "10",
                "AUDIT_FSYNC_INTERVAL_MS" to "0"
            )
        )

        audit.use {
            it.published(
                RecordMetadata(
                    TopicPartition("order.events.v1", 2),
                    0,
                    123,
                    1_000,
                    0,
                    3,
                    12
                ),
                key = "order-1",
                eventType = "ORDER_CREATED"
            )
        }

        val lines = directory.resolve("published-test-000000.tsv").readLines()
        assertTrue(lines[1].matches(Regex("""P\t1\t2\t123\t1000\t\d+\torder-1""")))
    }
}
