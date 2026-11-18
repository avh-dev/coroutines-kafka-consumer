package avh.ckc.core.warmup

import avh.ckc.core.isFatalJvmError
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.ConsumerPartitionStats
import avh.ckc.core.metrics.ConsumerRuntimeStats
import avh.ckc.core.metrics.LifecycleOutcome
import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.core.metrics.WarmupResultDeliveryFailureReason
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration

internal fun interface WarmupCompletionPublisher {
    /** Enqueues a terminal recipient result without blocking the processing worker. */
    fun publish(completed: WarmupRecordCompleted)
}

/**
 * Reserves result-delivery capacity before a record enters the recipient processing runtime.
 * Every successful [tryAcquire] must be followed exactly once by either terminal publication or
 * [release] when the processing runtime did not accept the record.
 */
internal interface WarmupResultAdmission {
    fun tryAcquire(record: ConsumerRecord<*, *>): Boolean
    fun release(record: ConsumerRecord<*, *>)
}

internal interface WarmupResultLane : WarmupCompletionPublisher, WarmupResultAdmission

internal interface WarmupResultDeliveryObserver {
    fun onStateChanged(outstanding: Int, queued: Int, awaitingAcknowledgement: Int, limited: Boolean) = Unit
    fun onAdmissionLimited() = Unit
    fun onFailure(reason: WarmupResultDeliveryFailureReason) = Unit
    fun onDrain(durationNanos: Long, outcome: LifecycleOutcome) = Unit
}

private object NoOpWarmupResultDeliveryObserver : WarmupResultDeliveryObserver

/** Serializes aggregate publication so a delayed callback cannot overwrite a newer gauge value. */
internal class WarmupResultDeliveryMetricsObserver<K, V>(
    private val metrics: ConsumerMetrics<K, V>
) {
    private data class State(val outstanding: Int, val queued: Int, val awaitingAck: Int, val limited: Boolean)

    private val lock = Any()
    private val stateByLane = mutableMapOf<String, State>()
    private var failureReported = false
    private var drainReported = false

    fun lane(laneId: String): WarmupResultDeliveryObserver = object : WarmupResultDeliveryObserver {
        override fun onStateChanged(
            outstanding: Int,
            queued: Int,
            awaitingAcknowledgement: Int,
            limited: Boolean
        ) = synchronized(lock) {
            stateByLane[laneId] = State(outstanding, queued, awaitingAcknowledgement, limited)
            metrics.onWarmupResultDeliveryChanged(
                stateByLane.values.sumOf(State::outstanding),
                stateByLane.values.sumOf(State::queued),
                stateByLane.values.sumOf(State::awaitingAck),
                stateByLane.values.count(State::limited)
            )
        }

        override fun onAdmissionLimited() = metrics.onWarmupResultAdmissionLimited()

        override fun onFailure(reason: WarmupResultDeliveryFailureReason) = synchronized(lock) {
            if (!failureReported) {
                failureReported = true
                metrics.onWarmupResultDeliveryFailure(reason)
            }
        }

        override fun onDrain(durationNanos: Long, outcome: LifecycleOutcome) = synchronized(lock) {
            if (!drainReported) {
                drainReported = true
                metrics.onWarmupResultDrain(durationNanos, outcome)
            }
        }
    }
}

/** Keeps user processing callbacks while isolating runtime-owned gauge and Kafka-client bindings. */
internal fun <K, V> warmupRecipientRuntimeMetrics(
    callbacks: ConsumerMetrics<K, V>,
    bindingId: String
): ConsumerMetrics<K, V> {
    val binding = callbacks.runtimeBinding(bindingId)
    if (binding === callbacks) return callbacks
    return object : ConsumerMetrics<K, V> by callbacks {
        override fun bindKafkaClientMetrics(pollLoopId: Int, consumer: Consumer<K, V>) =
            binding.bindKafkaClientMetrics(pollLoopId, consumer)

        override fun unbindKafkaClientMetrics(pollLoopId: Int) =
            binding.unbindKafkaClientMetrics(pollLoopId)

        override fun bindRuntimeMetrics(stats: ConsumerRuntimeStats) =
            binding.bindRuntimeMetrics(stats)

        override fun unbindRuntimeMetrics() = binding.unbindRuntimeMetrics()

        override fun bindPartitionMetrics(pollLoopId: Int, stats: ConsumerPartitionStats) =
            binding.bindPartitionMetrics(pollLoopId, stats)

        override fun unbindPartitionMetrics(pollLoopId: Int, stats: ConsumerPartitionStats) =
            binding.unbindPartitionMetrics(pollLoopId, stats)
    }
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

private data class WarmupResultCoordinate(val topic: String, val partition: Int, val offset: Long)

/**
 * Owns the recipient session's bounded terminal-result budget and acknowledgement barrier.
 *
 * Every accepted recipient record reserves one lane slot before it enters the processing runtime.
 * The reservation remains outstanding while the handler runs, while its terminal result is queued,
 * and while Kafka acknowledgement is pending. Independent lanes have separate budgets and share a
 * fixed worker set selected round-robin, so a hot mapping cannot consume another mapping's capacity.
 *
 * Publication is deliberately non-throwing: terminal callbacks have already recorded the handler's
 * actual outcome. Delivery failures are exposed through [awaitFailure] and close admission for the
 * affected recipient session without rewriting that business outcome.
 * The supplied sender and its transport remain owned by the surrounding protocol runtime.
 */
internal class WarmupCompletionOutbox(
    scope: CoroutineScope,
    private val laneCapacity: Int = DEFAULT_LANE_CAPACITY,
    senderConcurrency: Int = DEFAULT_SENDER_CONCURRENCY,
    private val send: suspend (WarmupRecordCompleted) -> Unit
) : AutoCloseable {
    private enum class DeliveryState { PROCESSING, QUEUED, AWAITING_ACK }

    private class Entry(
        val sequence: Long,
        val coordinate: WarmupResultCoordinate,
        val lane: LaneState,
        var state: DeliveryState = DeliveryState.PROCESSING,
        var completed: WarmupRecordCompleted? = null
    )

    private class LaneState(
        val observer: WarmupResultDeliveryObserver
    ) {
        val entries = linkedMapOf<WarmupResultCoordinate, Entry>()
        val queued = ArrayDeque<Entry>()
        var awaitingAcknowledgement = 0
        var limited = false
    }

    private data class Barrier(val sequence: Long, val completion: CompletableDeferred<Unit>)

    // All lane queues, reservations, barriers, cursors, and lifecycle flags are protected by lock.
    private val lock = Any()
    private val signal = Channel<Unit>(Channel.CONFLATED)
    private val lanes = linkedMapOf<String, LaneState>()
    private val barriers = mutableListOf<Barrier>()
    // Competing workers publish one failure without invoking observers or completing signals under lock.
    private val failure = AtomicReference<WarmupResultDeliveryException?>()
    private val failureSignal = CompletableDeferred<Throwable>()
    private var nextSequence = 0L
    private var nextLaneIndex = 0
    private var admissionClosed = false
    private var stopped = false
    private val workers: List<Job>

    init {
        require(laneCapacity > 0) { "Warm-up completion lane capacity must be positive" }
        require(senderConcurrency > 0) { "Warm-up completion sender concurrency must be positive" }
        workers = List(senderConcurrency) {
            scope.launch {
                workerLoop()
            }.also { worker ->
                worker.invokeOnCompletion { error ->
                    if (error != null && !isStopped()) {
                        if (!error.isFatalJvmError()) {
                            fail(
                                WarmupCompletionOutboxCancelledException(error),
                                WarmupResultDeliveryFailureReason.CANCELLED
                            )
                        }
                    }
                }
            }
        }
    }

    fun lane(
        laneId: String,
        observer: WarmupResultDeliveryObserver = NoOpWarmupResultDeliveryObserver
    ): WarmupResultLane {
        require(laneId.isNotBlank()) { "Warm-up result lane id must not be blank" }
        val state = synchronized(lock) {
            check(!admissionClosed) { "Warm-up completion outbox no longer accepts lanes" }
            check(laneId !in lanes) { "Warm-up result lane '$laneId' is already registered" }
            LaneState(observer).also { lanes[laneId] = it }
        }
        return object : WarmupResultLane {
            override fun tryAcquire(record: ConsumerRecord<*, *>): Boolean = acquire(state, record)
            override fun release(record: ConsumerRecord<*, *>) = release(state, record.coordinate())
            override fun publish(completed: WarmupRecordCompleted) = publish(state, completed)
        }
    }

    fun closeAdmission() {
        val snapshots = synchronized(lock) {
            admissionClosed = true
            lanes.values.forEach { it.limited = false }
            lanes.values.map(::snapshotLocked)
        }
        publishSnapshots(snapshots)
    }

    /** Waits only for work reserved before this call; later reservations do not extend the barrier. */
    suspend fun flush(timeout: Duration) {
        require(timeout.isPositive()) { "Warm-up completion flush timeout must be positive" }
        failure.get()?.let { throw it }
        val barrier = synchronized(lock) {
            failure.get()?.let { throw it }
            val target = nextSequence
            if (hasOutstandingAtOrBeforeLocked(target)) {
                Barrier(target, CompletableDeferred()).also(barriers::add)
            } else {
                null
            }
        } ?: return
        withTimeout(timeout) {
            select {
                barrier.completion.onAwait { }
                failureSignal.onAwait { throw it }
            }
        }
    }

    /** Closes admission, drains every previously accepted record, and then stops fixed send workers. */
    suspend fun stop(timeout: Duration) {
        require(timeout.isPositive()) { "Warm-up completion stop timeout must be positive" }
        val started = System.nanoTime()
        var outcome = LifecycleOutcome.FAILURE
        try {
            withTimeout(timeout) {
                closeAdmission()
                failure.get()?.let { failure ->
                    signal.close(failure)
                    workers.joinAll()
                    synchronized(lock) { stopped = true }
                    throw failure
                }
                flush(timeout)
                synchronized(lock) { stopped = true }
                signal.close()
                workers.joinAll()
                failure.get()?.let { throw it }
                outcome = LifecycleOutcome.SUCCESS
            }
        } catch (error: kotlinx.coroutines.TimeoutCancellationException) {
            outcome = LifecycleOutcome.TIMEOUT
            throw error
        } finally {
            distinctObservers().forEach { observer ->
                observe { observer.onDrain(System.nanoTime() - started, outcome) }
            }
        }
    }

    override fun close() {
        if (isStopped()) return
        closeAdmission()
        fail(
            WarmupCompletionOutboxCancelledException(CancellationException("Warm-up completion outbox closed")),
            WarmupResultDeliveryFailureReason.CANCELLED
        )
        workers.forEach { it.cancel() }
    }

    suspend fun awaitFailure(): Throwable = failureSignal.await()

    internal fun snapshot(): WarmupResultDeliverySnapshot = synchronized(lock) {
        WarmupResultDeliverySnapshot(
            outstanding = lanes.values.sumOf { it.entries.size },
            queued = lanes.values.sumOf { it.queued.size },
            awaitingAcknowledgement = lanes.values.sumOf { it.awaitingAcknowledgement },
            admissionLimitedLanes = lanes.values.count { it.limited }
        )
    }

    private fun acquire(lane: LaneState, record: ConsumerRecord<*, *>): Boolean {
        var limitedEpisode = false
        var accepted = false
        val snapshot = synchronized(lock) {
            val coordinate = record.coordinate()
            if (lane.entries.size >= laneCapacity && !admissionClosed && failure.get() == null) {
                if (!lane.limited) {
                    lane.limited = true
                    limitedEpisode = true
                }
                snapshotLocked(lane)
            } else if (admissionClosed || failure.get() != null) {
                snapshotLocked(lane)
            } else {
                check(coordinate !in lane.entries) { "Warm-up record $coordinate is already admitted" }
                lane.entries[coordinate] = Entry(++nextSequence, coordinate, lane)
                accepted = true
                snapshotLocked(lane)
            }
        }
        publishSnapshot(snapshot)
        if (limitedEpisode) observe { lane.observer.onAdmissionLimited() }
        return accepted
    }

    private fun release(lane: LaneState, coordinate: WarmupResultCoordinate) {
        val snapshot = synchronized(lock) {
            lane.entries[coordinate]?.takeIf { it.state == DeliveryState.PROCESSING }?.let {
                lane.entries.remove(coordinate)
                updateLimitedLocked(lane)
                completeBarriersLocked()
            }
            snapshotLocked(lane)
        }
        publishSnapshot(snapshot)
    }

    private fun publish(lane: LaneState, completed: WarmupRecordCompleted) {
        var internalFailure: WarmupResultDeliveryException? = null
        val snapshot = synchronized(lock) {
            val coordinate = completed.recordId.coordinate()
            val entry = lane.entries[coordinate]
            when {
                entry == null && failure.get() != null -> Unit
                entry == null -> internalFailure = WarmupResultDeliveryException(
                    "Terminal warm-up result $coordinate has no outstanding reservation"
                )
                entry.state != DeliveryState.PROCESSING -> internalFailure = WarmupResultDeliveryException(
                    "Terminal warm-up result $coordinate was published more than once"
                )
                else -> {
                    entry.completed = completed
                    entry.state = DeliveryState.QUEUED
                    lane.queued.addLast(entry)
                }
            }
            snapshotLocked(lane)
        }
        publishSnapshot(snapshot)
        if (internalFailure != null) {
            fail(requireNotNull(internalFailure), WarmupResultDeliveryFailureReason.INTERNAL)
        } else {
            signal.trySend(Unit)
        }
    }

    private suspend fun workerLoop() {
        while (currentCoroutineContext().isActive) {
            val entry = dequeue()
            if (entry == null) {
                if (isStopped() || failure.get() != null) return
                if (signal.receiveCatching().getOrNull() == null) return
                continue
            }
            try {
                send(requireNotNull(entry.completed))
            } catch (error: CancellationException) {
                fail(
                    WarmupCompletionOutboxCancelledException(error),
                    WarmupResultDeliveryFailureReason.CANCELLED
                )
                throw error
            } catch (error: Throwable) {
                if (error.isFatalJvmError()) throw error
                val reason = if (error is WarmupControlSendTimeoutException) {
                    WarmupResultDeliveryFailureReason.TIMEOUT
                } else {
                    WarmupResultDeliveryFailureReason.SEND_FAILURE
                }
                fail(WarmupResultDeliveryException("Warm-up result delivery failed", error), reason)
            } finally {
                acknowledgeFinished(entry)
            }
        }
    }

    private fun dequeue(): Entry? {
        var selected: Entry? = null
        var snapshot: LaneSnapshot? = null
        synchronized(lock) {
            if (failure.get() != null || lanes.isEmpty()) return null
            val candidates = lanes.values.toList()
            repeat(candidates.size) { offset ->
                val index = (nextLaneIndex + offset) % candidates.size
                val lane = candidates[index]
                val entry = lane.queued.removeFirstOrNull()
                if (entry != null) {
                    entry.state = DeliveryState.AWAITING_ACK
                    lane.awaitingAcknowledgement++
                    nextLaneIndex = (index + 1) % candidates.size
                    selected = entry
                    snapshot = snapshotLocked(lane)
                    return@synchronized
                }
            }
        }
        snapshot?.let(::publishSnapshot)
        if (snapshot().queued > 0) signal.trySend(Unit)
        return selected
    }

    private fun acknowledgeFinished(entry: Entry) {
        val snapshot = synchronized(lock) {
            entry.lane.awaitingAcknowledgement--
            entry.lane.entries.remove(entry.coordinate)
            updateLimitedLocked(entry.lane)
            completeBarriersLocked()
            snapshotLocked(entry.lane)
        }
        publishSnapshot(snapshot)
        if (snapshot().queued > 0) signal.trySend(Unit)
    }

    private fun fail(error: WarmupResultDeliveryException, reason: WarmupResultDeliveryFailureReason) {
        if (!failure.compareAndSet(null, error)) return
        val snapshots = synchronized(lock) {
            admissionClosed = true
            lanes.values.forEach { lane ->
                lane.limited = false
                lane.queued.clear()
                lane.entries.entries.removeIf { (_, entry) -> entry.state != DeliveryState.AWAITING_ACK }
                updateLimitedLocked(lane)
            }
            barriers.forEach { it.completion.completeExceptionally(error) }
            barriers.clear()
            lanes.values.map(::snapshotLocked)
        }
        failureSignal.complete(error)
        signal.close(error)
        publishSnapshots(snapshots)
        distinctObservers().forEach { observer -> observe { observer.onFailure(reason) } }
    }

    private fun hasOutstandingAtOrBeforeLocked(sequence: Long): Boolean =
        lanes.values.any { lane -> lane.entries.values.any { it.sequence <= sequence } }

    private fun completeBarriersLocked() {
        val iterator = barriers.iterator()
        while (iterator.hasNext()) {
            val barrier = iterator.next()
            if (!hasOutstandingAtOrBeforeLocked(barrier.sequence)) {
                barrier.completion.complete(Unit)
                iterator.remove()
            }
        }
    }

    private fun updateLimitedLocked(lane: LaneState) {
        if (lane.entries.size < laneCapacity && !admissionClosed && failure.get() == null) {
            lane.limited = false
        }
    }

    private fun snapshotLocked(lane: LaneState) = LaneSnapshot(
        lane.observer,
        lane.entries.size,
        lane.queued.size,
        lane.awaitingAcknowledgement,
        lane.limited
    )

    private fun publishSnapshots(snapshots: List<LaneSnapshot>) = snapshots.forEach(::publishSnapshot)

    private fun publishSnapshot(snapshot: LaneSnapshot) = observe {
        snapshot.observer.onStateChanged(
            snapshot.outstanding,
            snapshot.queued,
            snapshot.awaitingAcknowledgement,
            snapshot.limited
        )
    }

    private fun distinctObservers(): List<WarmupResultDeliveryObserver> =
        synchronized(lock) { lanes.values.map(LaneState::observer).distinct() }

    private fun isStopped(): Boolean = synchronized(lock) { stopped }

    private inline fun observe(block: () -> Unit) {
        try {
            block()
        } catch (_: Exception) {
            // Observability must not alter result delivery or session lifecycle.
        }
    }

    private data class LaneSnapshot(
        val observer: WarmupResultDeliveryObserver,
        val outstanding: Int,
        val queued: Int,
        val awaitingAcknowledgement: Int,
        val limited: Boolean
    )

    companion object {
        const val DEFAULT_LANE_CAPACITY = 1_024
        const val DEFAULT_SENDER_CONCURRENCY = 16
    }
}

internal data class WarmupResultDeliverySnapshot(
    val outstanding: Int,
    val queued: Int,
    val awaitingAcknowledgement: Int,
    val admissionLimitedLanes: Int
)

internal open class WarmupResultDeliveryException(message: String, cause: Throwable? = null) :
    IllegalStateException(message, cause)

internal class WarmupCompletionOutboxCancelledException(cause: Throwable) :
    WarmupResultDeliveryException("Warm-up completion outbox worker was cancelled before drain", cause)

private fun ConsumerRecord<*, *>.coordinate() =
    WarmupResultCoordinate(topic(), partition(), offset())

private fun WarmupRecordId.coordinate() =
    WarmupResultCoordinate(topicPartition.topic(), topicPartition.partition(), offset)
