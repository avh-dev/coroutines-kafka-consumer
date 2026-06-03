package avh.ckc.demo

import avh.ckc.demo.audit.AuditLineWriter
import avh.ckc.demo.audit.encodeAuditRecord
import avh.ckc.demo.config.DemoApplicationProperties
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoInteractions
import java.util.concurrent.CompletableFuture
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class AuditLogTest {
    @Test
    fun `encodes processed record as compact TCP audit payload`() {
        val encoded = encodeAuditRecord(
            type = "C",
            runId = "run-1",
            writerId = "demo-2",
            topic = DemoTopics.CAULDRON_EVENTS,
            partition = 4,
            offset = 77,
            kafkaTimestampMs = 2_000,
            messageKey = "cauldron\n1"
        )

        assertTrue(encoded.matches(Regex("""C\trun-1\tdemo-2\t3\t4\t77\t2000\t\d+\tcauldron 1""")))
    }

    @Test
    fun `sync processing waits for audit writer acknowledgement`() {
        val write = CompletableFuture<Unit>()
        val audit = auditLog(write)

        val processed = CompletableFuture.runAsync {
            audit.processed(DemoTopics.ORDER_EVENTS, "order-1", 0, 1, 1_000)
        }

        assertFalse(processed.isDone)
        write.complete(Unit)
        processed.join()
    }

    @Test
    fun `suspend processing waits for audit writer acknowledgement`() = runBlocking {
        val write = CompletableFuture<Unit>()
        val audit = auditLog(write)
        val record = ConsumerRecord(DemoTopics.ORDER_EVENTS, 0, 1, "order-1", "value")

        val processed = async {
            audit.processedSuspending(record)
        }

        yield()
        assertFalse(processed.isCompleted)
        write.complete(Unit)
        processed.await()
    }

    @Test
    fun `disabled audit does not call writer`() {
        val writer = mock(AuditLineWriter::class.java)
        val properties = DemoApplicationProperties(audit = DemoApplicationProperties.Audit(enabled = false))

        AuditLog(properties, writer, Unit).processed(DemoTopics.ORDER_EVENTS, "order-1", 0, 1, 1_000)

        verifyNoInteractions(writer)
    }

    @Test
    fun `close closes underlying writer`() {
        val writer = mock(AuditLineWriter::class.java)

        AuditLog(DemoApplicationProperties(), writer, Unit).close()

        verify(writer).close()
    }

    private fun auditLog(write: CompletableFuture<Unit>): AuditLog {
        val writer = mock(AuditLineWriter::class.java)
        doAnswer { write.join() }.`when`(writer).write(anyString())
        return AuditLog(DemoApplicationProperties(), writer, Unit)
    }
}
