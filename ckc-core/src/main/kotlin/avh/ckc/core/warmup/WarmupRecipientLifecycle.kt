package avh.ckc.core.warmup

import avh.ckc.core.CoroutinesKafkaConsumer
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.time.Duration

internal interface WarmupOwnedConsumer {
    fun start()
    suspend fun awaitReady()
    suspend fun stop()
}

internal fun <K, V> CoroutinesKafkaConsumer<K, V>.asWarmupOwnedConsumer() = object : WarmupOwnedConsumer {
    override fun start() = startRuntime()
    override suspend fun awaitReady() = awaitRuntimeReady()
    override suspend fun stop() = stopRuntime()
}

/** Owns the local recipient -> drain -> main transition and never starts main after lifecycle cancellation. */
internal class WarmupRecipientLifecycle(
    private val createWarmup: () -> WarmupOwnedConsumer,
    private val main: WarmupOwnedConsumer,
    private val drainTracker: WarmupRecipientDrainTracker,
    private val outbox: WarmupCompletionOutbox,
    private val cleanupTimeout: Duration
) {
    private enum class State { NEW, WARMING, DRAINED, MAIN, FAILED, STOPPED }

    private val mutex = Mutex()
    private val startGate = Any()
    private val stopRequested = CompletableDeferred<Unit>()
    private var state = State.NEW
    private var warmup: WarmupOwnedConsumer? = null

    suspend fun startWarmup() = mutex.withLock {
        check(state == State.NEW) { "Warm-up recipient can only start once" }
        ensureRunning()
        try {
            createWarmup().also { warmup = it }.let(::startUnlessStopping)
            untilStopped { requireNotNull(warmup).awaitReady() }
            state = State.WARMING
        } catch (error: Throwable) {
            state = State.FAILED
            throw error
        }
    }

    suspend fun drain(finalOffsets: Collection<WarmupFinalOffset>) = mutex.withLock {
        check(state == State.WARMING) { "Warm-up recipient is not ready to drain" }
        ensureRunning()
        try {
            untilStopped { drainTracker.await(finalOffsets, cleanupTimeout) }
            untilStopped { requireNotNull(warmup).stop() }
            untilStopped { outbox.flush(cleanupTimeout) }
            outbox.stop()
            state = State.DRAINED
        } catch (error: Throwable) {
            state = State.FAILED
            throw error
        }
    }

    suspend fun startMain() = mutex.withLock {
        check(state == State.DRAINED) { "Warm-up recipient has not drained" }
        ensureRunning()
        try {
            startUnlessStopping(main)
            untilStopped { main.awaitReady() }
            state = State.MAIN
        } catch (error: Throwable) {
            runCatching { main.stop() }
            state = State.FAILED
            throw error
        }
    }

    suspend fun stop() {
        synchronized(startGate) { stopRequested.complete(Unit) }
        mutex.withLock {
            if (state == State.STOPPED) return
            val previousState = state
            state = State.STOPPED
            drainTracker.close()
            outbox.close()
            var failure: Throwable? = null
            for (consumer in listOfNotNull(warmup, main.takeIf { previousState == State.MAIN })) {
                try {
                    consumer.stop()
                } catch (error: Throwable) {
                    if (failure == null) failure = error else failure.addSuppressed(error)
                }
            }
            failure?.let { throw it }
        }
    }

    private fun ensureRunning() {
        check(!stopRequested.isCompleted) { "Warm-up recipient is stopping" }
    }

    private fun startUnlessStopping(consumer: WarmupOwnedConsumer) = synchronized(startGate) {
        ensureRunning()
        consumer.start()
    }

    private suspend fun untilStopped(block: suspend () -> Unit) = coroutineScope {
        val operation = async { block() }
        try {
            select<Unit> {
                stopRequested.onAwait { throw CancellationException("Warm-up recipient is stopping") }
                operation.onAwait { }
            }
        } finally {
            operation.cancel()
        }
    }
}
