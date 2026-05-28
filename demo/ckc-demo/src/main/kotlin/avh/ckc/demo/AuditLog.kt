package avh.ckc.demo

import avh.ckc.demo.audit.FileAuditLogConfig
import avh.ckc.demo.audit.FileBackedAuditLog
import avh.ckc.demo.config.DemoApplicationProperties
import jakarta.annotation.PreDestroy
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.stereotype.Component
import kotlin.io.path.Path

@Component
class AuditLog(
    properties: DemoApplicationProperties
) {
    private val delegate: FileBackedAuditLog? =
        if (properties.audit.enabled) {
            FileBackedAuditLog(
                FileAuditLogConfig(
                    directory = Path(properties.audit.directory),
                    filePrefix = properties.audit.filePrefix.ifBlank { defaultFilePrefix() },
                    queueCapacity = properties.audit.queueCapacity,
                    flushRecords = properties.audit.flushRecords,
                    flushIntervalMs = properties.audit.flushIntervalMs,
                    fsyncIntervalMs = properties.audit.fsyncIntervalMs,
                    maxSegmentBytes = properties.audit.maxSegmentBytes
                ),
                threadName = "ckc-demo-audit-writer"
            )
        } else {
            null
        }

    fun processed(record: ConsumerRecord<*, *>) {
        processed(
            topic = record.topic(),
            key = record.key()?.toString().orEmpty(),
            partition = record.partition(),
            offset = record.offset(),
            kafkaTimestampMs = record.timestamp()
        )
    }

    fun processed(topic: String, key: String?, partition: Int, offset: Long, kafkaTimestampMs: Long) {
        delegate?.processed(
            topic = topic,
            key = key.orEmpty(),
            partition = partition,
            offset = offset,
            kafkaTimestampMs = kafkaTimestampMs
        )
    }

    @PreDestroy
    fun close() {
        delegate?.close()
    }
}

private fun defaultFilePrefix(): String =
    "processed-${System.getenv("HOSTNAME") ?: ProcessHandle.current().pid()}"
