package avh.ckc.core.warmup

import avh.ckc.core.CoroutinesKafkaConsumer
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext

/**
 * Library-owned participant lifecycle. Coordinator performs the all-participant barrier between
 * drain and startMain; neither the application nor a demo endpoint owns a second consumer.
 */
internal class WarmupRecipientLifecycle<K, V>(
    private val createWarmup: () -> CoroutinesKafkaConsumer<K, V>,
    private val createMain: () -> CoroutinesKafkaConsumer<K, V>
) {
    private val mutex = Mutex()
    private val stopRequested = CompletableDeferred<Unit>()
    private var state = State.NEW
    private var warmup: CoroutinesKafkaConsumer<K, V>? = null
    private var main: CoroutinesKafkaConsumer<K, V>? = null

    suspend fun startWarmup() = mutex.withLock {
        check(state == State.NEW) { "Warm-up can only start once" }
        check(!stopRequested.isCompleted) { "Recipient is stopping" }
        state = State.WARMING
        try {
            val consumer = createWarmup().also { warmup = it }
            consumer.start()
            untilStopped { consumer.awaitReady() }
        } catch (error: Throwable) {
            state = State.FAILED
            throw error
        }
    }

    suspend fun drain(boundary: WarmupForwardingBoundary) = mutex.withLock {
        check(state == State.WARMING) { "Recipient is not warming" }
        check(!stopRequested.isCompleted) { "Recipient is stopping" }
        try {
            untilStopped { requireNotNull(warmup).drainWarmupAndStop(boundary) }
            state = State.DRAINED
        } catch (error: Throwable) {
            state = State.FAILED
            throw error
        }
    }

    suspend fun startMain() = mutex.withLock {
        check(state == State.DRAINED) { "Main subscription requires successful warm-up drain" }
        check(!stopRequested.isCompleted) { "Recipient is stopping" }
        try {
            createMain().also { main = it }.start()
            state = State.MAIN
        } catch (error: Throwable) {
            state = State.FAILED
            throw error
        }
    }

    suspend fun stop() {
        stopRequested.complete(Unit)
        withContext(NonCancellable) {
            mutex.withLock {
                if (state == State.STOPPED) return@withLock
                state = State.STOPPED
                var failure: Throwable? = null
                for (consumer in listOfNotNull(main, warmup)) {
                    try {
                        consumer.stop()
                    } catch (error: Throwable) {
                        if (failure == null) failure = error else failure.addSuppressed(error)
                    }
                }
                failure?.let { throw it }
            }
        }
    }

    private suspend fun untilStopped(action: suspend () -> Unit) = coroutineScope {
        val operation = async { action() }
        try {
            select<Unit> {
                stopRequested.onAwait { throw CancellationException("Warm-up recipient is stopping") }
                operation.onAwait { }
            }
        } finally {
            operation.cancel()
        }
    }

    private enum class State { NEW, WARMING, DRAINED, MAIN, FAILED, STOPPED }
}
