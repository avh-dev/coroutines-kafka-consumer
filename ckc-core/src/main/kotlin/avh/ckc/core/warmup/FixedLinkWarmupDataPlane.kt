package avh.ckc.core.warmup

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.WarmupFallbackReason
import avh.ckc.core.metrics.WarmupResultMetricOutcome
import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.core.processing.AdmittedRecordInterceptor
import avh.ckc.core.processing.RecordProcessingOutcome
import avh.ckc.core.processing.RecordProcessingOutcomeObserver
import kotlinx.coroutines.CancellationException
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
    private val quota = IntervalQuota(quota, quotaInterval, nanoTime)
    private val active = AtomicBoolean(true)
    private val failure = AtomicReference<Throwable?>()
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
        if (destination == null || !active.get() || !quota.tryAcquire()) {
            if (destination != null && failure.get() != null) {
                metrics.onWarmupFallback(record, WarmupFallbackReason.SESSION_FAILURE)
            }
            return processLocally()
        }

        var acknowledged = false
        return try {
            val remote = withTimeout(delegationTimeout) {
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
                val recordId = WarmupRecordId(sessionId, acknowledgement.topicPartition, acknowledgement.offset)
                val waitStarted = System.nanoTime()
                val outcome = tracker.await(recordId, delegationTimeout)
                metrics.onWarmupResult(record, outcome.metricOutcome(), System.nanoTime() - waitStarted)
                outcome
            }
            when (remote) {
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
        } catch (error: TimeoutCancellationException) {
            failSession(WarmupDelegationFailedException("Warm-up delegation deadline expired", error))
            metrics.onWarmupFallback(
                record,
                if (acknowledged) WarmupFallbackReason.RESULT_TIMEOUT else WarmupFallbackReason.SEND_FAILURE
            )
            processLocally()
        } catch (error: WarmupResultTimeoutException) {
            failSession(WarmupDelegationFailedException("Warm-up result deadline expired", error))
            metrics.onWarmupFallback(record, WarmupFallbackReason.RESULT_TIMEOUT)
            processLocally()
        } catch (error: CancellationException) {
            throw error
        } catch (error: InvalidWarmupRemoteOutcomeException) {
            throw error
        } catch (error: Throwable) {
            val reason = if (acknowledged) {
                WarmupFallbackReason.SESSION_FAILURE
            } else {
                WarmupFallbackReason.SEND_FAILURE
            }
            failSession(error)
            metrics.onWarmupFallback(record, reason)
            processLocally()
        }
    }

    fun accept(completed: WarmupRecordCompleted): WarmupResultTracker.Completion {
        val result = tracker.complete(completed)
        if (result == WarmupResultTracker.Completion.OVERFLOW) {
            failSession(WarmupEarlyResultOverflowException(maxEarlyResults))
        }
        return result
    }

    fun stopDelegating() {
        active.set(false)
    }

    fun close() {
        active.set(false)
        tracker.close()
        (sender as? AutoCloseable)?.close()
    }

    private fun failSession(error: Throwable) {
        if (failure.compareAndSet(null, error)) {
            active.set(false)
            tracker.fail(error)
        }
    }

    private fun WarmupRemoteOutcome.metricOutcome(): WarmupResultMetricOutcome = when (this) {
        WarmupRemoteOutcome.Succeeded -> WarmupResultMetricOutcome.SUCCEEDED
        WarmupRemoteOutcome.TerminalFailure -> WarmupResultMetricOutcome.TERMINAL_FAILURE
        is WarmupRemoteOutcome.Discarded -> WarmupResultMetricOutcome.DISCARDED
    }
}

internal class WarmupDelegationFailedException(message: String, cause: Throwable) :
    IllegalStateException(message, cause)

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
            is RecordProcessingOutcome.HandedOff -> return
        }
        publisher.publish(
            WarmupRecordCompleted(
                WarmupRecordId(
                    sessionId,
                    TopicPartition(record.topic(), record.partition()),
                    record.offset()
                ),
                remote
            )
        )
    }
}
