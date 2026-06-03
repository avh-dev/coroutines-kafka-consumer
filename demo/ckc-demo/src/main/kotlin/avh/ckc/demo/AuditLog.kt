package avh.ckc.demo

import avh.ckc.demo.audit.AuditLineWriter
import avh.ckc.demo.audit.LazyAuditLineWriter
import avh.ckc.demo.audit.TcpAuditClient
import avh.ckc.demo.audit.encodeAuditRecord
import avh.ckc.demo.config.DemoApplicationProperties
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class AuditLog : AutoCloseable {
    private val enabled: Boolean
    private val runId: String
    private val writerId: String
    private val writer: AuditLineWriter

    @Autowired
    constructor(properties: DemoApplicationProperties) : this(
        properties = properties,
        writer = LazyAuditLineWriter { TcpAuditClient(properties.audit.host, properties.audit.port) }
    )

    internal constructor(
        properties: DemoApplicationProperties,
        writer: AuditLineWriter,
        unused: Unit = Unit
    ) {
        this.enabled = properties.audit.enabled
        this.runId = properties.audit.runId
        this.writerId = properties.audit.writerId
        this.writer = writer
    }

    suspend fun processedSuspending(record: ConsumerRecord<*, *>) {
        if (enabled) {
            writer.write(
                encodeAuditRecord(
                    type = "C",
                    runId = runId,
                    writerId = writerId,
                    topic = record.topic(),
                    messageKey = record.key()?.toString(),
                    partition = record.partition(),
                    offset = record.offset(),
                    kafkaTimestampMs = record.timestamp()
                )
            )
        }
    }

    fun processed(record: ConsumerRecord<*, *>) {
        if (enabled) {
            writer.write(
                encodeAuditRecord(
                    type = "C",
                    runId = runId,
                    writerId = writerId,
                    topic = record.topic(),
                    messageKey = record.key()?.toString(),
                    partition = record.partition(),
                    offset = record.offset(),
                    kafkaTimestampMs = record.timestamp()
                )
            )
        }
    }

    fun processed(topic: String, key: String?, partition: Int, offset: Long, kafkaTimestampMs: Long) {
        if (enabled) {
            writer.write(
                encodeAuditRecord(
                    type = "C",
                    runId = runId,
                    writerId = writerId,
                    topic = topic,
                    partition = partition,
                    offset = offset,
                    kafkaTimestampMs = kafkaTimestampMs,
                    messageKey = key
                )
            )
        }
    }

    override fun close() {
        writer.close()
    }
}
