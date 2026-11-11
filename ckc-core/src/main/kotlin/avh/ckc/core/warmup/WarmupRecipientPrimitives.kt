package avh.ckc.core.warmup

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.withTimeout
import org.apache.kafka.common.TopicPartition
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration

/** Tracks terminal recipient outcomes until every donor-declared tail offset has completed. */
internal class WarmupRecipientDrainTracker(private val sessionId: WarmupSessionId) {
    private val lock = Any()
    private val completed = mutableSetOf<Pair<TopicPartition, Long>>()
    private var boundary: Map<TopicPartition, Long?>? = null
    private var drained = CompletableDeferred<Unit>()
    private var closed = false

    fun onCompleted(result: WarmupRecordCompleted) = synchronized(lock) {
        if (closed || result.recordId.sessionId != sessionId) return
        completed += result.recordId.topicPartition to result.recordId.offset
        completeIfDrained()
    }

    suspend fun await(finalOffsets: Collection<WarmupFinalOffset>, timeout: Duration) {
        require(timeout.isPositive()) { "Warm-up drain timeout must be positive" }
        val signal = synchronized(lock) {
            check(!closed) { "Warm-up drain tracker is closed" }
            val newBoundary = finalOffsets.associate { it.topicPartition to it.offset }
            require(newBoundary.size == finalOffsets.size) { "Warm-up drain boundary contains duplicate partitions" }
            val current = boundary
            check(current == null || current == newBoundary) { "Warm-up drain boundary is already fixed" }
            boundary = newBoundary
            completeIfDrained()
            drained
        }
        withTimeout(timeout) { signal.await() }
    }

    fun close() {
        val signal = synchronized(lock) {
            if (closed) return
            closed = true
            completed.clear()
            drained
        }
        signal.completeExceptionally(CancellationException("Warm-up drain tracker closed"))
    }

    private fun completeIfDrained() {
        val expected = boundary ?: return
        if (expected.all { (partition, offset) -> offset == null || partition to offset in completed }) {
            drained.complete(Unit)
        }
    }
}

/** Serializes non-blocking recipient publications and provides an ACKed flush barrier for drain. */
internal class WarmupCompletionOutbox(
    scope: CoroutineScope,
    capacity: Int,
    private val send: suspend (WarmupRecordCompleted) -> Unit
) : WarmupCompletionPublisher, AutoCloseable {
    private sealed interface Item {
        data class Result(val completed: WarmupRecordCompleted) : Item
        data class Flush(val completion: CompletableDeferred<Unit>) : Item
    }

    private val channel = Channel<Item>(capacity.also {
        require(it > 0) { "Warm-up completion outbox capacity must be positive" }
    })
    private val failure = AtomicReference<Throwable?>()
    private val failureSignal = CompletableDeferred<Throwable>()
    private val job = scope.launch {
        for (item in channel) {
            when (item) {
                is Item.Result -> send(item.completed)
                is Item.Flush -> item.completion.complete(Unit)
            }
        }
    }.also { job ->
        job.invokeOnCompletion { error ->
            if (error != null && error !is CancellationException && failure.compareAndSet(null, error)) {
                failureSignal.complete(error)
                channel.close(error)
            }
        }
    }

    override fun publish(completed: WarmupRecordCompleted) {
        failure.get()?.let { throw IllegalStateException("Warm-up completion outbox failed", it) }
        check(channel.trySend(Item.Result(completed)).isSuccess) {
            "Warm-up completion outbox is full or closed"
        }
    }

    suspend fun flush(timeout: Duration) {
        require(timeout.isPositive()) { "Warm-up completion flush timeout must be positive" }
        val completion = CompletableDeferred<Unit>()
        withTimeout(timeout) {
            channel.send(Item.Flush(completion))
            select {
                completion.onAwait { }
                failureSignal.onAwait { throw it }
            }
        }
    }

    suspend fun stop() {
        channel.close()
        job.join()
        failure.get()?.let { throw it }
    }

    override fun close() {
        channel.close()
        job.cancel()
    }
}
