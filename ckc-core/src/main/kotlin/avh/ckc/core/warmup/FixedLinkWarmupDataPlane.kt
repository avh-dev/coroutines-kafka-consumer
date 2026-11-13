package avh.ckc.core.warmup

import avh.ckc.core.isFatalJvmError
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.WarmupFallbackReason
import avh.ckc.core.metrics.WarmupResultMetricOutcome
import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.core.processing.AdmittedRecordInterceptor
import avh.ckc.core.processing.RecordProcessingOutcome
import avh.ckc.core.processing.RecordProcessingOutcomeObserver
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration

fun interface WarmupDestinationResolver<K, V> {
    fun destinationFor(record: ConsumerRecord<K, V>): WarmupDestination?
}

internal class FixedLinkWarmupDataPlane<K, V>(
    val sessionId: WarmupSessionId,
    quota: Int,
    quotaInterval: Duration,
    initialQuota: Int = quota,
    quotaRampDuration: Duration = Duration.ZERO,
    private val delegationTimeout: Duration,
    private val destinationResolver: WarmupDestinationResolver<K, V>,
    private val sender: WarmupRecordSender<K, V>,
    private val forwardingObserver: WarmupForwardingObserver,
    private val metrics: ConsumerMetrics<K, V>,
    private val supportsRemoteDiscard: (RecordDropReason) -> Boolean = { false },
    private val maxEarlyResults: Int = 1_024,
    earlyResultTtl: Duration = delegationTimeout,
    nanoTime: () -> Long = System::nanoTime
) : AdmittedRecordInterceptor<K, V> {
    private val log = LoggerFactory.getLogger(javaClass)
    private val quota = IntervalQuota(
        limit = quota,
        interval = quotaInterval,
        initialLimit = initialQuota,
        rampDuration = quotaRampDuration,
        nanoTime = nanoTime
    )
    private val admissionLock = Any()
    private val active = AtomicBoolean(true)
    private val failure = AtomicReference<Throwable?>()
    private var delegationsInProgress = 0
    private var delegationsDrained = CompletableDeferred<Unit>()
    private val tracker = WarmupResultTracker(
        sessionId = sessionId,
        maxEarlyResults = maxEarlyResults,
        earlyResultTtl = earlyResultTtl,
        nanoTime = nanoTime,
        onPendingChanged = metrics::onWarmupPendingChanged
    )

    init {
        require(delegationTimeout.isPositive()) { "Warm-up delegation timeout must be > 0" }
    }

    override suspend fun process(
        record: ConsumerRecord<K, V>,
        processLocally: suspend () -> RecordProcessingOutcome
    ): RecordProcessingOutcome {
        val destination = destinationResolver.destinationFor(record)
        if (destination == null || !tryReserve(record)) {
            if (destination != null && failure.get() != null) {
                metrics.onWarmupFallback(record, WarmupFallbackReason.SESSION_FAILURE)
            }
            return processLocally()
        }
        return processReserved(record, destination, processLocally)
    }

    internal fun tryReserve(record: ConsumerRecord<K, V>): Boolean =
        destinationResolver.destinationFor(record) != null && tryBeginDelegation()

    internal suspend fun processReserved(
        record: ConsumerRecord<K, V>,
        processLocally: suspend () -> RecordProcessingOutcome
    ): RecordProcessingOutcome = processReserved(
        record,
        requireNotNull(destinationResolver.destinationFor(record)) { "Reserved record no longer has a destination" },
        processLocally
    )

    private suspend fun processReserved(
        record: ConsumerRecord<K, V>,
        destination: WarmupDestination,
        processLocally: suspend () -> RecordProcessingOutcome
    ): RecordProcessingOutcome {
        try {
            var acknowledged = false
            val remote = try {
                withTimeout(delegationTimeout) {
                    val sendStarted = System.nanoTime()
                    val acknowledgement = sender.send(record, destination)
                    acknowledged = true
                    val sendDuration = System.nanoTime() - sendStarted
                    metrics.onWarmupDelegated(record, sendDuration)
                    val forwardingEvent = WarmupForwardedEvent(
                        sessionId = sessionId,
                        sourceTopicPartition = TopicPartition(record.topic(), record.partition()),
                        sourceOffset = record.offset(),
                        destinationTopicPartition = acknowledgement.topicPartition,
                        destinationOffset = acknowledgement.offset
                    )
                    try {
                        forwardingObserver.onForwarded(forwardingEvent)
                    } catch (error: Exception) {
                        log.warn("Warm-up forwarding observer failed for {}", forwardingEvent, error)
                    }
                    val recordId = WarmupRecordId(
                        sessionId,
                        acknowledgement.topicPartition,
                        acknowledgement.offset
                    )
                    val waitStarted = System.nanoTime()
                    val outcome = tracker.await(recordId, delegationTimeout)
                    metrics.onWarmupResult(record, outcome.metricOutcome(), System.nanoTime() - waitStarted)
                    outcome
                }
            } catch (error: TimeoutCancellationException) {
                failSession(WarmupDelegationFailedException("Warm-up delegation deadline expired", error))
                metrics.onWarmupFallback(
                    record,
                    if (acknowledged) WarmupFallbackReason.RESULT_TIMEOUT else WarmupFallbackReason.SEND_FAILURE
                )
                return processLocally()
            } catch (error: WarmupResultTimeoutException) {
                failSession(WarmupDelegationFailedException("Warm-up result deadline expired", error))
                metrics.onWarmupFallback(record, WarmupFallbackReason.RESULT_TIMEOUT)
                return processLocally()
            } catch (error: CancellationException) {
                throw error
            } catch (error: Throwable) {
                if (error.isFatalJvmError()) throw error
                val reason = if (acknowledged) {
                    WarmupFallbackReason.SESSION_FAILURE
                } else {
                    WarmupFallbackReason.SEND_FAILURE
                }
                failSession(error)
                metrics.onWarmupFallback(record, reason)
                return processLocally()
            }

            return when (remote) {
                WarmupRemoteOutcome.Succeeded -> RecordProcessingOutcome.HandlerSucceeded
                WarmupRemoteOutcome.TerminalFailure -> {
                    metrics.onWarmupFallback(record, WarmupFallbackReason.REMOTE_TERMINAL_FAILURE)
                    processLocally()
                }
                is WarmupRemoteOutcome.Discarded -> {
                    if (!supportsRemoteDiscard(remote.reason)) {
                        val error = InvalidWarmupRemoteOutcomeException(
                            "Remote discard ${remote.reason} is not supported by the source processing mode"
                        )
                        failSession(error)
                        throw error
                    }
                    RecordProcessingOutcome.PolicyDiscarded(remote.reason)
                }
            }
        } finally {
            finishDelegation()
        }
    }

    fun accept(completed: WarmupRecordCompleted): WarmupResultTracker.Completion {
        val result = tracker.complete(completed)
        if (result == WarmupResultTracker.Completion.OVERFLOW) {
            failSession(WarmupEarlyResultOverflowException(maxEarlyResults))
        }
        return result
    }

    fun canDelegateRecord(record: ConsumerRecord<K, V>): Boolean =
        active.get() && failure.get() == null && destinationResolver.destinationFor(record) != null

    fun stopDelegating() {
        synchronized(admissionLock) {
            active.set(false)
            if (delegationsInProgress == 0) delegationsDrained.complete(Unit)
        }
    }

    suspend fun stopDelegatingAndAwaitInflight(timeout: Duration) {
        require(timeout.isPositive()) { "Warm-up in-flight drain timeout must be > 0" }
        val drain = synchronized(admissionLock) {
            active.set(false)
            if (delegationsInProgress == 0) delegationsDrained.complete(Unit)
            delegationsDrained
        }
        withTimeout(timeout) { drain.await() }
    }

    suspend fun failAndAwaitInflight(error: Throwable, timeout: Duration) {
        failSession(error)
        stopDelegatingAndAwaitInflight(timeout)
    }

    fun failDelegation(error: Throwable) = failSession(error)

    suspend fun awaitInflight(timeout: Duration) {
        require(timeout.isPositive()) { "Warm-up in-flight drain timeout must be > 0" }
        val drain = synchronized(admissionLock) { delegationsDrained }
        withTimeout(timeout) { drain.await() }
    }

    fun close() {
        stopDelegating()
        tracker.close()
        (sender as? AutoCloseable)?.close()
    }

    private fun failSession(error: Throwable) {
        if (failure.compareAndSet(null, error)) {
            stopDelegating()
            tracker.fail(error)
        }
    }

    private fun tryBeginDelegation(): Boolean = synchronized(admissionLock) {
        if (!active.get() || !quota.tryAcquire()) return false
        if (delegationsInProgress == 0 && delegationsDrained.isCompleted) {
            delegationsDrained = CompletableDeferred()
        }
        delegationsInProgress++
        true
    }

    private fun finishDelegation() = synchronized(admissionLock) {
        check(delegationsInProgress > 0) { "Warm-up delegation accounting underflow" }
        delegationsInProgress--
        if (!active.get() && delegationsInProgress == 0) delegationsDrained.complete(Unit)
    }

    private fun WarmupRemoteOutcome.metricOutcome(): WarmupResultMetricOutcome = when (this) {
        WarmupRemoteOutcome.Succeeded -> WarmupResultMetricOutcome.SUCCEEDED
        WarmupRemoteOutcome.TerminalFailure -> WarmupResultMetricOutcome.TERMINAL_FAILURE
        is WarmupRemoteOutcome.Discarded -> WarmupResultMetricOutcome.DISCARDED
    }
}

internal class WarmupDelegationFailedException(message: String, cause: Throwable) :
    IllegalStateException(message, cause)

internal class WarmupRecipientUnavailableException(message: String) : IllegalStateException(message)

internal class InvalidWarmupRemoteOutcomeException(message: String) : IllegalStateException(message)

internal class WarmupRecipientOutcomePublisher<K, V>(
    private val sessionId: WarmupSessionId,
    private val publisher: WarmupCompletionPublisher
) : RecordProcessingOutcomeObserver<K, V> {
    override fun onOutcome(record: ConsumerRecord<K, V>, outcome: RecordProcessingOutcome) {
        val remote = when (outcome) {
            RecordProcessingOutcome.HandlerSucceeded -> WarmupRemoteOutcome.Succeeded
            is RecordProcessingOutcome.HandlerFailed -> WarmupRemoteOutcome.TerminalFailure
            is RecordProcessingOutcome.PolicyDiscarded -> WarmupRemoteOutcome.Discarded(outcome.reason)
            RecordProcessingOutcome.Filtered -> return
            is RecordProcessingOutcome.HandedOff -> return
        }
        val completed = WarmupRecordCompleted(
                WarmupRecordId(
                    sessionId,
                    TopicPartition(record.topic(), record.partition()),
                    record.offset()
                ),
                remote
            )
        publisher.publish(completed)
    }
}
