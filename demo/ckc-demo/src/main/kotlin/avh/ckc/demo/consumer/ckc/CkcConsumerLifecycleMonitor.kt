package avh.ckc.demo.consumer.ckc

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.CoroutineContext

internal data class MonitoredCkcConsumer(
    val name: String,
    val awaitReady: suspend () -> Unit,
    val awaitTermination: suspend () -> Unit
)

internal class CkcConsumerLifecycleMonitor(
    private val consumers: List<MonitoredCkcConsumer>,
    private val observeReadiness: Boolean,
    private val onReady: () -> Unit,
    private val onTerminalFailure: (Throwable) -> Unit,
    parentContext: CoroutineContext = Dispatchers.Default
) {
    private val scope = CoroutineScope(SupervisorJob() + parentContext)
    private val started = AtomicBoolean()
    private val stopping = AtomicBoolean()
    private val failed = AtomicBoolean()

    fun start() {
        check(started.compareAndSet(false, true)) { "Consumer lifecycle monitor is already started" }
        if (observeReadiness) {
            scope.launch {
                try {
                    coroutineScope {
                        consumers.map { consumer -> async { consumer.awaitReady() } }.awaitAll()
                    }
                    if (!stopping.get() && !failed.get()) onReady()
                } catch (failure: Throwable) {
                    reportFailure(failure)
                }
            }
        }
        consumers.forEach { consumer ->
            scope.launch {
                try {
                    consumer.awaitTermination()
                    reportFailure(IllegalStateException("CKC consumer '${consumer.name}' terminated unexpectedly"))
                } catch (failure: Throwable) {
                    reportFailure(failure)
                }
            }
        }
    }

    fun stop() {
        stopping.set(true)
        scope.cancel()
    }

    private fun reportFailure(failure: Throwable) {
        if (!stopping.get() && failed.compareAndSet(false, true)) {
            onTerminalFailure(failure)
        }
    }
}
