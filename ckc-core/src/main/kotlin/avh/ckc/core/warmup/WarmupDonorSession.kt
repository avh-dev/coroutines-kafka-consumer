package avh.ckc.core.warmup

import avh.ckc.core.isFatalJvmError
import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.core.metrics.WarmupFallbackReason
import avh.ckc.core.processing.RecordProcessingOutcome
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import kotlin.time.Duration

internal fun interface WarmupDestinationResolver<K, V> {
    fun destinationFor(record: ConsumerRecord<K, V>): WarmupDestination?
}

/**
 * One isolated donor session with its own quota, inflight drain, result tracker, and deadline.
 *
 * [sessionLock] protects [state], [inflight], and [inflightDrained]. Result waiters have a separate
 * lock inside [WarmupResultTracker]: `inflight` counts every delegation from successful admission
 * through its `finally`, while pending results count only acknowledged sends currently awaiting a
 * recipient outcome. A successful [tryBeginDelegation] must therefore have exactly one matching
 * [finishDelegation].
 *
 * `DRAINING` rejects new delegation but accepts results for existing work. `FAILED` also rejects new
 * work and releases existing waits through ownership-safe local fallback. `CLOSED` owns no inflight
 * work and accepts no result. The session owns [sender] and closes it only after the inflight count
 * reaches zero.
 */
internal class WarmupDonorSession<K, V>(
    override val sessionId: WarmupSessionId,
    val intervalBudget: Int,
    quotaInterval: Duration,
    initialQuota: Int = intervalBudget,
    quotaRampDuration: Duration = Duration.ZERO,
    quotaRampCurve: QuotaRampCurve = QuotaRampCurve.QUADRATIC,
    private val delegationTimeout: Duration,
    private val destinationResolver: WarmupDestinationResolver<K, V>,
    private val sender: WarmupRecordSender<K, V>,
    private val observer: WarmupDataPlaneObserver<K, V> = noOpWarmupDataPlaneObserver(),
    private val pendingLaneId: String = "default",
    private val supportsRemoteDiscard: (RecordDropReason) -> Boolean = { false },
    private val maxEarlyResults: Int = 1_024,
    earlyResultTtl: Duration = delegationTimeout,
    nanoTime: () -> Long = System::nanoTime
) : WarmupDonorSessionLifecycle {
    override val admissionBudgetPerSecond: Int = normalizedSessionBudget(intervalBudget, quotaInterval)
    private enum class State { ADMITTED, ACTIVE, DRAINING, FAILED, CLOSED }

    private val log = LoggerFactory.getLogger(javaClass)
    private val quota = IntervalQuota(
        limit = intervalBudget,
        interval = quotaInterval,
        initialLimit = initialQuota,
        rampDuration = quotaRampDuration,
        rampCurve = quotaRampCurve,
        nanoTime = nanoTime
    )
    private val sessionLock = Any()
    private val closeLock = Any()
    private var state = State.ADMITTED
    private var inflight = 0
    private var inflightDrained = CompletableDeferred<Unit>().also { it.complete(Unit) }
    private var trackerClosed = false
    private var senderClosed = false
    private val tracker = WarmupResultTracker(
        sessionId = sessionId,
        maxEarlyResults = maxEarlyResults,
        earlyResultTtl = earlyResultTtl,
        nanoTime = nanoTime,
        onPendingChanged = { pending ->
            observe { observer.onPendingChanged(WarmupPendingKey(sessionId, pendingLaneId), pending) }
        }
    )

    init {
        require(delegationTimeout.isPositive()) { "Warm-up delegation timeout must be > 0" }
    }

    override fun activate(): Boolean = synchronized(sessionLock) {
        if (state != State.ADMITTED) return@synchronized false
        state = State.ACTIVE
        true
    }

    fun tryBeginDelegation(record: ConsumerRecord<K, V>): WarmupDestination? {
        val destination = destinationResolver.destinationFor(record) ?: return null
        return synchronized(sessionLock) {
            if (state != State.ACTIVE || !quota.tryAcquire()) return@synchronized null
            if (inflight == 0) inflightDrained = CompletableDeferred()
            inflight++
            destination
        }
    }

    suspend fun processDelegatedRecord(
        record: ConsumerRecord<K, V>,
        destination: WarmupDestination,
        processLocally: suspend () -> RecordProcessingOutcome
    ): RecordProcessingOutcome {
        try {
            val remoteOutcome = try {
                sendAndAwaitResult(record, destination)
            } catch (error: CancellationException) {
                // External cancellation and ownership handoff must never become local fallback.
                throw error
            } catch (error: WarmupDelegationDeadlineException) {
                fail(error)
                reportFallback(
                    record,
                    if (error.acknowledged) {
                        WarmupFallbackReason.RESULT_TIMEOUT
                    } else {
                        WarmupFallbackReason.SEND_FAILURE
                    }
                )
                return processLocally()
            } catch (error: WarmupResultTimeoutException) {
                fail(WarmupDelegationFailedException("Warm-up result deadline expired", error))
                reportFallback(record, WarmupFallbackReason.RESULT_TIMEOUT)
                return processLocally()
            } catch (error: WarmupSendFailedException) {
                fail(error)
                reportFallback(record, WarmupFallbackReason.SEND_FAILURE)
                return processLocally()
            } catch (error: Throwable) {
                if (error.isFatalJvmError()) throw error
                fail(error)
                reportFallback(record, WarmupFallbackReason.SESSION_FAILURE)
                return processLocally()
            }

            return applyRemoteOutcome(record, remoteOutcome, processLocally)
        } finally {
            finishDelegation()
        }
    }

    private suspend fun sendAndAwaitResult(
        record: ConsumerRecord<K, V>,
        destination: WarmupDestination
    ): WarmupRemoteOutcome {
        var acknowledged = false
        return withTimeoutOrNull(delegationTimeout) {
            val sendStarted = System.nanoTime()
            val acknowledgement = try {
                sender.send(record, destination, sessionId)
            } catch (error: CancellationException) {
                throw error
            } catch (error: Throwable) {
                if (error.isFatalJvmError()) throw error
                throw WarmupSendFailedException(error)
            }
            acknowledged = true
            val forwarded = WarmupForwardedEvent(
                sessionId = sessionId,
                sourceTopicPartition = TopicPartition(record.topic(), record.partition()),
                sourceOffset = record.offset(),
                destinationTopicPartition = acknowledgement.topicPartition,
                destinationOffset = acknowledgement.offset
            )
            observe { observer.onForwarded(record, forwarded, System.nanoTime() - sendStarted) }
            val recordId = WarmupRecordId(
                sessionId = sessionId,
                topicPartition = acknowledgement.topicPartition,
                offset = acknowledgement.offset
            )
            val waitStarted = System.nanoTime()
            val outcome = tracker.await(recordId, delegationTimeout)
            observe { observer.onResult(record, outcome, System.nanoTime() - waitStarted) }
            outcome
        } ?: throw WarmupDelegationDeadlineException(acknowledged)
    }

    private suspend fun applyRemoteOutcome(
        record: ConsumerRecord<K, V>,
        remoteOutcome: WarmupRemoteOutcome,
        processLocally: suspend () -> RecordProcessingOutcome
    ): RecordProcessingOutcome = when (remoteOutcome) {
        WarmupRemoteOutcome.Succeeded -> RecordProcessingOutcome.HandlerSucceededRemotely
        WarmupRemoteOutcome.TerminalFailure -> {
            reportFallback(record, WarmupFallbackReason.REMOTE_TERMINAL_FAILURE)
            processLocally()
        }
        is WarmupRemoteOutcome.Discarded -> {
            if (supportsRemoteDiscard(remoteOutcome.reason)) {
                RecordProcessingOutcome.PolicyDiscarded(remoteOutcome.reason)
            } else {
                fail(
                    InvalidWarmupRemoteOutcomeException(
                        "Remote discard ${remoteOutcome.reason} is not supported by the source processing mode"
                    )
                )
                reportFallback(record, WarmupFallbackReason.SESSION_FAILURE)
                processLocally()
            }
        }
    }

    override fun acceptResult(completed: WarmupRecordCompleted): WarmupResultTracker.Completion {
        val result = tracker.complete(completed)
        if (result == WarmupResultTracker.Completion.OVERFLOW) {
            fail(WarmupEarlyResultOverflowException(maxEarlyResults))
        }
        return result
    }

    override fun stopDelegating() {
        synchronized(sessionLock) {
            if (state == State.ADMITTED || state == State.ACTIVE) state = State.DRAINING
            if (inflight == 0) inflightDrained.complete(Unit)
        }
    }

    override suspend fun stopDelegatingAndAwaitInflight(timeout: Duration) {
        require(timeout.isPositive()) { "Warm-up in-flight drain timeout must be > 0" }
        val completion = synchronized(sessionLock) {
            if (state == State.ADMITTED || state == State.ACTIVE) state = State.DRAINING
            if (inflight == 0) inflightDrained.complete(Unit)
            inflightDrained
        }
        val drained = withTimeoutOrNull(timeout) {
            completion.await()
            true
        }
        if (drained != true) {
            throw WarmupInflightDrainTimeoutException(timeout)
        }
    }

    /** Waits after a session failure until every ownership-safe fallback has left the pipeline. */
    override suspend fun awaitInflightCompletion() {
        val completion = synchronized(sessionLock) { inflightDrained }
        completion.await()
    }

    override fun fail(error: Throwable) {
        val won = synchronized(sessionLock) {
            if (state == State.FAILED || state == State.CLOSED) {
                false
            } else {
                state = State.FAILED
                if (inflight == 0) inflightDrained.complete(Unit)
                true
            }
        }
        if (won) tracker.fail(error)
    }

    fun pendingResultCount(): Int = tracker.pendingCount()

    fun isActive(): Boolean = synchronized(sessionLock) { state == State.ACTIVE }

    /**
     * Atomically prevents a not-yet-activated session from becoming active and verifies drain.
     * Once this succeeds, a caller may remove the session from its registry and close it.
     */
    override fun trySealForRemoval(): Boolean = synchronized(sessionLock) {
        if (state == State.ACTIVE || inflight != 0) return@synchronized false
        if (state == State.ADMITTED) state = State.DRAINING
        true
    }

    /** Closes an unactivated or stopped session. Active or inflight sessions are a caller error. */
    override fun close() = synchronized(closeLock) {
        synchronized(sessionLock) {
            if (state != State.CLOSED) {
                check(state != State.ACTIVE && inflight == 0) {
                    "Warm-up donor session ${sessionId.value} must stop and drain before close"
                }
                state = State.CLOSED
                inflightDrained.complete(Unit)
            }
        }
        if (!trackerClosed) {
            tracker.close()
            trackerClosed = true
        }
        if (!senderClosed) {
            (sender as? AutoCloseable)?.close()
            senderClosed = true
        }
    }

    private fun finishDelegation() = synchronized(sessionLock) {
        check(inflight > 0) { "Warm-up delegation accounting underflow" }
        inflight--
        if (inflight == 0) inflightDrained.complete(Unit)
    }

    private fun reportFallback(record: ConsumerRecord<K, V>, reason: WarmupFallbackReason) {
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

internal class WarmupInflightDrainTimeoutException(timeout: Duration) :
    IllegalStateException("Warm-up in-flight drain exceeded $timeout")

internal class WarmupDelegationDeadlineException(val acknowledged: Boolean) :
    IllegalStateException("Warm-up delegation deadline expired")

internal class WarmupSendFailedException(cause: Throwable) :
    IllegalStateException("Warm-up record send failed", cause)

internal class InvalidWarmupRemoteOutcomeException(message: String) : IllegalStateException(message)

private fun normalizedSessionBudget(intervalBudget: Int, quotaInterval: Duration): Int {
    require(intervalBudget > 0) { "Warm-up interval budget must be > 0" }
    require(quotaInterval.inWholeMilliseconds > 0) { "Warm-up quota interval must be at least 1 ms" }
    return Math.ceilDiv(
        Math.multiplyExact(intervalBudget.toLong(), 1_000L),
        quotaInterval.inWholeMilliseconds
    ).coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
}
