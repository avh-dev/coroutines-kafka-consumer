package avh.ckc.spring

import org.springframework.context.ApplicationContext
import org.springframework.context.ConfigurableApplicationContext
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Logger

/**
 * Turns a managed CKC terminal failure into bounded Spring and JVM shutdown.
 *
 * The watchdog starts before Spring cleanup, so the entire shutdown procedure has one hard upper
 * bound. The context first gets a grace period and normal JVM exit is then requested. If either
 * cleanup or shutdown hooks exceed the total budget, [CkcTerminalFailureHaltHandler] terminates
 * the JVM forcibly.
 */
class CkcTerminalFailureCoordinator(
    private val closeContext: () -> Unit,
    private val exitHandler: CkcTerminalFailureExitHandler,
    private val haltHandler: CkcTerminalFailureHaltHandler,
    private val closeTimeoutMillis: Long = 30_000L,
    private val haltTimeoutMillis: Long = 35_000L
) {
    private val started = AtomicBoolean(false)

    init {
        require(closeTimeoutMillis > 0L) { "closeTimeoutMillis must be > 0" }
        require(haltTimeoutMillis > 0L) { "haltTimeoutMillis must be > 0" }
    }

    constructor(
        applicationContext: ApplicationContext,
        exitHandler: CkcTerminalFailureExitHandler,
        haltHandler: CkcTerminalFailureHaltHandler
    ) : this(
        closeContext = {
            val configurable = applicationContext as? ConfigurableApplicationContext
                ?: error("ApplicationContext is not configurable")
            configurable.close()
        },
        exitHandler = exitHandler,
        haltHandler = haltHandler
    )

    fun trigger() {
        if (!started.compareAndSet(false, true)) return
        val haltWatchdog = Thread {
            try {
                Thread.sleep(haltTimeoutMillis)
                haltHandler.halt(1)
            } catch (_: InterruptedException) {
                // System.exit returned (normally only in a test); forced halt is no longer needed.
            }
        }.apply {
            name = "ckc-terminal-failure-halt"
            isDaemon = true
            start()
        }
        Thread {
            try {
                val closeThread = Thread(closeContext).apply {
                    name = "ckc-terminal-context-close"
                    isDaemon = true
                    start()
                }
                closeThread.join(closeTimeoutMillis)
                if (closeThread.isAlive) {
                    logger.warning("Spring application context did not close within ${closeTimeoutMillis}ms")
                }
            } catch (error: Throwable) {
                logger.warning("Spring application context close failed: ${error.message}")
            } finally {
                exitHandler.exit(1)
                haltWatchdog.interrupt()
            }
        }.apply {
            name = "ckc-terminal-failure-exit"
            isDaemon = true
            start()
        }
    }

    private companion object {
        val logger: Logger = Logger.getLogger(CkcTerminalFailureCoordinator::class.java.name)
    }
}
