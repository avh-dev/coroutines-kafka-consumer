package avh.ckc.loadtest.kafka

import avh.ckc.demo.audit.auditTopicId
import avh.ckc.loadtest.config.LoadTestConfig
import avh.ckc.loadtest.runtime.ShardContext
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.LoggerFactory

private const val AUDIT_TCP_LOGGER = "AUDIT_TCP"

class LoadTestAuditLog private constructor() {
    private val auditLogger = LoggerFactory.getLogger(AUDIT_TCP_LOGGER)

    fun published(metadata: RecordMetadata, key: String) {
        append(
            encodePublishedAuditRecord(
                topic = metadata.topic(),
                partition = metadata.partition(),
                offset = metadata.offset(),
                kafkaTimestampMs = metadata.timestamp(),
                key = key
            )
        )
    }

    fun generated(topic: String, key: String) {
        append(
            encodePublishedAuditRecord(
                topic = topic,
                partition = -1,
                offset = -1,
                kafkaTimestampMs = System.currentTimeMillis(),
                key = key
            )
        )
    }

    private fun append(record: String) {
        auditLogger.info(record)
    }

    companion object {
        fun fromConfig(config: LoadTestConfig, shardContext: ShardContext): LoadTestAuditLog =
            LoadTestAuditLog()
    }
}

internal fun encodePublishedAuditRecord(
    topic: String,
    partition: Int,
    offset: Long,
    kafkaTimestampMs: Long,
    key: String,
    auditTimestampMs: Long = System.currentTimeMillis()
): String =
    "P|${auditTopicId(topic)}|$partition|$offset|$kafkaTimestampMs|$auditTimestampMs|$key"
