package avh.ckc.demo

import avh.ckc.demo.audit.AuditLineWriter
import avh.ckc.demo.audit.LazyAuditLineWriter
import avh.ckc.demo.audit.TcpAuditClient
import avh.ckc.demo.audit.encodeAuditRecord
import avh.ckc.demo.config.DemoApplicationProperties
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.stereotype.Component

@Component
class AuditLog private constructor(
    properties: DemoApplicationProperties,
    private val writer: AuditLineWriter
) : AutoCloseable {
    private val enabled = properties.audit.enabled
    private val runId = properties.audit.runId
    private val writerId = properties.audit.writerId

    constructor(properties: DemoApplicationProperties) : this(
        properties = properties,
        writer = LazyAuditLineWriter { TcpAuditClient(properties.audit.host, properties.audit.port) }
    )

    internal constructor(
        properties: DemoApplicationProperties,
        writer: AuditLineWriter,
        unused: Unit = Unit
    ) : this(properties, writer)

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
