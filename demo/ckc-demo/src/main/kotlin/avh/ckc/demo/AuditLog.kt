package avh.ckc.demo

import avh.ckc.demo.audit.auditTopicId
import avh.ckc.demo.config.DemoApplicationProperties
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory

private const val AUDIT_TCP_LOGGER = "AUDIT_TCP"
private val auditLogger = LoggerFactory.getLogger(AUDIT_TCP_LOGGER)

fun logProcessed(record: ConsumerRecord<*, *>, audit: DemoApplicationProperties.Audit) {
    logRecord(
        type = "C",
        topic = record.topic(),
        key = record.key()?.toString(),
        partition = record.partition(),
        offset = record.offset(),
        kafkaTimestampMs = record.timestamp(),
        audit = audit
    )
}

fun logFailed(record: ConsumerRecord<*, *>, audit: DemoApplicationProperties.Audit) {
    logRecord(
        type = "F",
        topic = record.topic(),
        key = record.key()?.toString(),
        partition = record.partition(),
        offset = record.offset(),
        kafkaTimestampMs = record.timestamp(),
        audit = audit
    )
}

fun logProcessed(
    topic: String,
    key: String?,
    partition: Int,
    offset: Long,
    kafkaTimestampMs: Long,
    audit: DemoApplicationProperties.Audit
) {
    logRecord(
        type = "C",
        topic = topic,
        key = key,
        partition = partition,
        offset = offset,
        kafkaTimestampMs = kafkaTimestampMs,
        audit = audit
    )
}

fun logFailed(
    topic: String,
    key: String?,
    partition: Int,
    offset: Long,
    kafkaTimestampMs: Long,
    audit: DemoApplicationProperties.Audit
) {
    logRecord(
        type = "F",
        topic = topic,
        key = key,
        partition = partition,
        offset = offset,
        kafkaTimestampMs = kafkaTimestampMs,
        audit = audit
    )
}

private fun logRecord(
    type: String,
    topic: String,
    key: String?,
    partition: Int,
    offset: Long,
    kafkaTimestampMs: Long,
    audit: DemoApplicationProperties.Audit
) {
    if (!audit.enabled) {
        return
    }
    auditLogger.info(encodeConsumerAuditRecord(type, topic, partition, offset, key))
}

internal fun encodeConsumerAuditRecord(
    type: String,
    topic: String,
    partition: Int,
    offset: Long,
    key: String?,
    auditTimestampMs: Long = System.currentTimeMillis()
): String =
    "$type|${auditTopicId(topic)}|$partition|$offset|$auditTimestampMs|${key.orEmpty()}"
