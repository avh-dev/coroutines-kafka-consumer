package avh.ckc.core.warmup

import avh.ckc.core.isFatalJvmError
import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.core.processing.RecordProcessingOutcome
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration

internal fun interface WarmupDestinationResolver<K, V> {
    fun destinationFor(record: ConsumerRecord<K, V>): WarmupDestination?
}

/** One isolated donor session with its own quota, inflight drain, result tracker, and deadline. */
internal class WarmupDonorSession<K, V>(
    val sessionId: WarmupSessionId,
    val intervalBudget: Int,
    quotaInterval: Duration,
    initialQuota: Int = intervalBudget,
    quotaRampDuration: Duration = Duration.ZERO,
    private val delegationTimeout: Duration,
    private val destinationResolver: WarmupDestinationResolver<K, V>,
    private val sender: WarmupRecordSender<K, V>,
    private val observer: WarmupDataPlaneObserver<K, V> = noOpWarmupDataPlaneObserver(),
    private val supportsRemoteDiscard: (RecordDropReason) -> Boolean = { false },
    private val maxEarlyResults: Int = 1_024,
    earlyResultTtl: Duration = delegationTimeout,
    nanoTime: () -> Long = System::nanoTime
) : AutoCloseable {
    private enum class State { ADMITTED, ACTIVE, DRAINING, FAILED, CLOSED }

    private val log = LoggerFactory.getLogger(javaClass)
    private val quota = IntervalQuota(
        limit = intervalBudget,
        interval = quotaInterval,
        initialLimit = initialQuota,
        rampDuration = quotaRampDuration,
        nanoTime = nanoTime
    )
    private val lock = Any()
    private val state = AtomicReference(State.ADMITTED)
    private var inflight = 0
    private var drained = CompletableDeferred<Unit>().also { it.complete(Unit) }
    private val tracker = WarmupResultTracker(
        sessionId = sessionId,
        maxEarlyResults = maxEarlyResults,
        earlyResultTtl = earlyResultTtl,
        nanoTime = nanoTime,
        onPendingChanged = { pending -> observe { observer.onPendingChanged(sessionId, pending) } }
    )

    init {
        require(delegationTimeout.isPositive()) { "Warm-up delegation timeout must be > 0" }
    }

    fun activate(): Boolean = synchronized(lock) {
        state.compareAndSet(State.ADMITTED, State.ACTIVE)
    }

    fun tryReserve(record: ConsumerRecord<K, V>): WarmupDestination? {
        val destination = destinationResolver.destinationFor(record) ?: return null
        return synchronized(lock) {
            if (state.get() != State.ACTIVE || !quota.tryAcquire()) return@synchronized null
            if (inflight == 0) drained = CompletableDeferred()
            inflight++
            destination
        }
    }

    suspend fun processReserved(
        record: ConsumerRecord<K, V>,
        destination: WarmupDestination,
        processLocally: suspend () -> RecordProcessingOutcome
    ): RecordProcessingOutcome {
        try {
            var acknowledged = false
            val remote = try {
                withTimeout(delegationTimeout) {
                    val sendStarted = System.nanoTime()
                    val acknowledgement = sender.send(record, destination, sessionId)
                    acknowledged = true
                    val forwarded = WarmupForwardedEvent(
                        sessionId = sessionId,
                        sourceTopicPartition = TopicPartition(record.topic(), record.partition()),
                        sourceOffset = record.offset(),
                        destinationTopicPartition = acknowledgement.topicPartition,
                        destinationOffset = acknowledgement.offset
                    )
                    observe { observer.onForwarded(forwarded, System.nanoTime() - sendStarted) }
                    val recordId = WarmupRecordId(
                        sessionId = sessionId,
                        topicPartition = acknowledgement.topicPartition,
                        offset = acknowledgement.offset
                    )
                    val waitStarted = System.nanoTime()
                    val outcome = tracker.await(recordId, delegationTimeout)
                    observe { observer.onResult(record, outcome, System.nanoTime() - waitStarted) }
                    outcome
                }
            } catch (error: TimeoutCancellationException) {
                fail(WarmupDelegationFailedException("Warm-up delegation deadline expired", error))
                fallback(
                    record,
                    if (acknowledged) WarmupFallbackReason.RESULT_TIMEOUT else WarmupFallbackReason.SEND_FAILURE
                )
                return processLocally()
            } catch (error: WarmupResultTimeoutException) {
                fail(WarmupDelegationFailedException("Warm-up result deadline expired", error))
                fallback(record, WarmupFallbackReason.RESULT_TIMEOUT)
                return processLocally()
            } catch (error: CancellationException) {
                throw error
            } catch (error: Throwable) {
                if (error.isFatalJvmError()) throw error
                fail(error)
                fallback(
                    record,
                    if (acknowledged) WarmupFallbackReason.SESSION_FAILURE else WarmupFallbackReason.SEND_FAILURE
                )
                return processLocally()
            }

            return when (remote) {
                WarmupRemoteOutcome.Succeeded -> RecordProcessingOutcome.HandlerSucceededRemotely
                WarmupRemoteOutcome.TerminalFailure -> {
                    fallback(record, WarmupFallbackReason.REMOTE_TERMINAL_FAILURE)
                    processLocally()
                }
                is WarmupRemoteOutcome.Discarded -> {
                    if (supportsRemoteDiscard(remote.reason)) {
                        RecordProcessingOutcome.PolicyDiscarded(remote.reason)
                    } else {
                        fail(
                            InvalidWarmupRemoteOutcomeException(
                                "Remote discard ${remote.reason} is not supported by the source processing mode"
                            )
                        )
                        fallback(record, WarmupFallbackReason.SESSION_FAILURE)
                        processLocally()
                    }
                }
            }
        } finally {
            finishDelegation()
        }
    }

    fun accept(completed: WarmupRecordCompleted): WarmupResultTracker.Completion {
        val result = tracker.complete(completed)
        if (result == WarmupResultTracker.Completion.OVERFLOW) {
            fail(WarmupEarlyResultOverflowException(maxEarlyResults))
        }
        return result
    }

    fun stopDelegating() {
        synchronized(lock) {
            state.compareAndSet(State.ADMITTED, State.DRAINING)
            state.compareAndSet(State.ACTIVE, State.DRAINING)
            if (inflight == 0) drained.complete(Unit)
        }
    }

    suspend fun stopDelegatingAndAwaitInflight(timeout: Duration) {
        require(timeout.isPositive()) { "Warm-up in-flight drain timeout must be > 0" }
        val completion = synchronized(lock) {
            stopDelegating()
            drained
        }
        withTimeout(timeout) { completion.await() }
    }

    fun fail(error: Throwable) {
        val won = synchronized(lock) {
            val current = state.get()
            if (current == State.FAILED || current == State.CLOSED) false else {
                state.set(State.FAILED)
                if (inflight == 0) drained.complete(Unit)
                true
            }
        }
        if (won) tracker.fail(error)
    }

    fun pendingCount(): Int = tracker.pendingCount()

    fun isActive(): Boolean = state.get() == State.ACTIVE

    override fun close() {
        synchronized(lock) {
            state.set(State.CLOSED)
            if (inflight == 0) drained.complete(Unit)
        }
        tracker.close()
        (sender as? AutoCloseable)?.close()
    }

    private fun finishDelegation() = synchronized(lock) {
        check(inflight > 0) { "Warm-up delegation accounting underflow" }
        inflight--
        if (inflight == 0) drained.complete(Unit)
    }

    private fun fallback(record: ConsumerRecord<K, V>, reason: WarmupFallbackReason) {
        observe { observer.onFallback(record, reason) }
    }

    private inline fun observe(block: () -> Unit) {
        try {
            block()
        } catch (error: Exception) {
            log.warn("Warm-up data-plane observer failed for session {}", sessionId.value, error)
        }
    }
}

internal class WarmupDelegationFailedException(message: String, cause: Throwable) :
    IllegalStateException(message, cause)

internal class InvalidWarmupRemoteOutcomeException(message: String) : IllegalStateException(message)
