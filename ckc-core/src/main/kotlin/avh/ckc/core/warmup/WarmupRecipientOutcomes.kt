package avh.ckc.core.warmup

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.RecordDropReason
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration

internal fun interface WarmupCompletionPublisher {
    /** Enqueues a terminal recipient result without blocking the processing worker. */
    fun publish(completed: WarmupRecordCompleted)
}

/** Publishes recipient results only after the ordinary runtime records a terminal outcome. */
internal class WarmupRecipientOutcomeMetrics<K, V>(
    private val sessionId: WarmupSessionId,
    private val publisher: WarmupCompletionPublisher,
    delegate: ConsumerMetrics<K, V>
) : ConsumerMetrics<K, V> by delegate {
    private val delegate = delegate

    override fun onRecordProcessed(
        key: K?,
        value: V?,
        record: ConsumerRecord<K, V>,
        endToEndLatencyMillis: Long,
        durationNanos: Long,
        processedRemotely: Boolean
    ) {
        delegate.onRecordProcessed(
            key, value, record, endToEndLatencyMillis, durationNanos, processedRemotely
        )
        publish(record, WarmupRemoteOutcome.Succeeded)
    }

    override fun onRecordFailed(
        key: K?,
        value: V?,
        record: ConsumerRecord<K, V>,
        error: Throwable,
        durationNanos: Long
    ) {
        delegate.onRecordFailed(key, value, record, error, durationNanos)
        publish(record, WarmupRemoteOutcome.TerminalFailure)
    }

    override fun onRecordDropped(record: ConsumerRecord<K, V>, reason: RecordDropReason) {
        delegate.onRecordDropped(record, reason)
        publish(record, WarmupRemoteOutcome.Discarded(reason))
    }

    private fun publish(record: ConsumerRecord<K, V>, outcome: WarmupRemoteOutcome) {
        publisher.publish(
            WarmupRecordCompleted(
                WarmupRecordId(
                    sessionId,
                    TopicPartition(record.topic(), record.partition()),
                    record.offset()
                ),
                outcome
            )
        )
    }
}

/** Serializes recipient results and provides a transport-acknowledged drain barrier. */
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
