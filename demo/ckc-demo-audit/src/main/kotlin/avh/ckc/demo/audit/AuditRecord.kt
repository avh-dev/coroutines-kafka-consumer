package avh.ckc.demo.audit

data class AuditRecord(
    val type: AuditEventType,
    val topic: String,
    val key: String,
    val partition: Int,
    val offset: Long,
    val kafkaTimestampMs: Long,
    val auditTimestampMs: Long
)
