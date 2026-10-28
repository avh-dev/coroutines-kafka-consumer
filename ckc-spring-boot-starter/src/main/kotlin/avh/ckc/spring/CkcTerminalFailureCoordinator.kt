package avh.ckc.spring

import org.springframework.context.ApplicationContext
import org.springframework.context.ConfigurableApplicationContext
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Logger

/**
 * Turns a managed CKC terminal failure into bounded Spring and JVM shutdown.
 *
 * The context first gets a grace period. Normal JVM exit is then requested, while an independent
 * watchdog calls [CkcTerminalFailureHaltHandler] if a shutdown hook prevents that exit from completing.
 */
internal class CkcTerminalFailureCoordinator(
    private val closeContext: () -> Unit,
    private val exitHandler: CkcTerminalFailureExitHandler,
    private val haltHandler: CkcTerminalFailureHaltHandler,
    private val closeTimeoutMillis: Long = 30_000L,
    private val exitTimeoutMillis: Long = 5_000L
) {
    private val started = AtomicBoolean(false)

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
                val haltWatchdog = Thread {
                    try {
                        Thread.sleep(exitTimeoutMillis)
                        haltHandler.halt(1)
                    } catch (_: InterruptedException) {
                        // System.exit returned (normally only in a test); forced halt is no longer needed.
                    }
                }.apply {
                    name = "ckc-terminal-failure-halt"
                    isDaemon = true
                    start()
                }
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
