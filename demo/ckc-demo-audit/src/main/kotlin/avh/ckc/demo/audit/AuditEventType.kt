package avh.ckc.demo.audit

enum class AuditEventType(val code: Char) {
    PUBLISHED('P'),
    PROCESSED('C')
}
