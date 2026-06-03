package avh.ckc.demo.audit

const val AUDIT_ORDER_EVENTS_TOPIC = "order.events.v1"
const val AUDIT_BATCH_EVENTS_TOPIC = "batch.events.v1"
const val AUDIT_CAULDRON_EVENTS_TOPIC = "cauldron.events.v1"

fun encodeAuditRecord(
    type: String,
    runId: String,
    writerId: String,
    topic: String,
    partition: Int,
    offset: Long,
    kafkaTimestampMs: Long,
    messageKey: String?,
    auditTimestampMs: Long = System.currentTimeMillis()
): String =
    "$type\t${sanitizeAuditComponent(runId, "local")}\t${sanitizeAuditComponent(writerId, "unknown")}\t" +
        "${auditTopicId(topic)}\t$partition\t$offset\t$kafkaTimestampMs\t$auditTimestampMs\t${sanitizeAuditKey(messageKey)}"

private fun auditTopicId(topic: String): Int =
    when (topic) {
        AUDIT_ORDER_EVENTS_TOPIC -> 1
        AUDIT_BATCH_EVENTS_TOPIC -> 2
        AUDIT_CAULDRON_EVENTS_TOPIC -> 3
        else -> error("No audit topic id configured for topic '$topic'")
    }

fun sanitizeAuditComponent(value: String?, fallback: String): String =
    value.orEmpty()
        .trim()
        .ifBlank { fallback }
        .replace('\t', '_')
        .replace('\r', '_')
        .replace('\n', '_')

fun sanitizeAuditKey(key: String?): String =
    key.orEmpty().replace('\t', ' ').replace('\r', ' ').replace('\n', ' ')
