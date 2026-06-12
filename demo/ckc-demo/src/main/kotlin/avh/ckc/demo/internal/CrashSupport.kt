package avh.ckc.demo.internal

import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.logAuditShutdownMarker
import ch.qos.logback.classic.LoggerContext
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import org.springframework.stereotype.Component

private const val AUDIT_TCP_LOGGER = "AUDIT_TCP"
private const val AUDIT_SHUTDOWN_PHASE = Int.MIN_VALUE + 2

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
    private val logger = LoggerFactory.getLogger(javaClass)

    override fun flushAndStop() {
        val loggerFactory = LoggerFactory.getILoggerFactory()
        if (loggerFactory !is LoggerContext) {
            logger.warn("Skipping audit appender flush because logger factory is {}", loggerFactory.javaClass.name)
            return
        }

        val auditLogger = loggerFactory.getLogger(AUDIT_TCP_LOGGER)
        val iterator = auditLogger.iteratorForAppenders()
        while (iterator.hasNext()) {
            val appender = iterator.next()
            logger.info(
                "Stopping audit appender name={} class={} started={}",
                appender.name,
                appender.javaClass.name,
                appender.isStarted
            )
            appender.stop()
            logger.info("Stopped audit appender name={} started={}", appender.name, appender.isStarted)
        }
    }
}

@Component
class AuditShutdownLifecycle(
    private val auditLogFlusher: AuditLogFlusher,
    private val properties: DemoApplicationProperties
) : SmartLifecycle {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Volatile
    private var running = false

    override fun start() {
        running = true
    }

    override fun stop() {
        if (!running) {
            return
        }
        logger.info("Audit shutdown lifecycle stop started")
        if (properties.audit.enabled) {
            logAuditShutdownMarker("started", properties.audit)
            auditLogFlusher.flushAndStop()
        }
        running = false
        logger.info("Audit shutdown lifecycle stop finished")
    }

    override fun isRunning(): Boolean = running

    override fun isAutoStartup(): Boolean = true

    override fun getPhase(): Int = AUDIT_SHUTDOWN_PHASE
}
