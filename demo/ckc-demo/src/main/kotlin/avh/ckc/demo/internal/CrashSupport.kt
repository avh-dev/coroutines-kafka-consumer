package avh.ckc.demo.internal

import ch.qos.logback.classic.LoggerContext
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

private const val AUDIT_TCP_LOGGER = "AUDIT_TCP"

fun interface JvmHalter {
    fun halt(status: Int)
}

fun interface AuditLogFlusher {
    fun flushAndStop()
}

@Component
class RuntimeJvmHalter : JvmHalter {
    override fun halt(status: Int) {
        Runtime.getRuntime().halt(status)
    }
}

@Component
class LogbackAuditLogFlusher : AuditLogFlusher {
    override fun flushAndStop() {
        val loggerFactory = LoggerFactory.getILoggerFactory()
        if (loggerFactory !is LoggerContext) {
            return
        }

        val auditLogger = loggerFactory.getLogger(AUDIT_TCP_LOGGER)
        val iterator = auditLogger.iteratorForAppenders()
        while (iterator.hasNext()) {
            iterator.next().stop()
        }
    }
}
