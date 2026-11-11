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
    private val admissionLock = Any()
    private val active = AtomicBoolean(true)
    private val failure = AtomicReference<Throwable?>()
    private val finalOffsets = linkedMapOf<TopicPartition, Long?>()
    private var sendsInProgress = 0
    private var sendsDrained = CompletableDeferred<Unit>()
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
        if (destination == null || !tryBeginSend(destination)) {
            if (destination != null && failure.get() != null) {
                metrics.onWarmupFallback(record, WarmupFallbackReason.SESSION_FAILURE)
            }
            return processLocally()
        }

        var sendAccountingOpen = true
        fun finishSendAccounting() {
            if (sendAccountingOpen) {
                sendAccountingOpen = false
                finishSend()
            }
        }
        try {
            var acknowledged = false
            val remote = try {
                withTimeout(delegationTimeout) {
                    val sendStarted = System.nanoTime()
                    val acknowledgement = try {
                        sender.send(record, destination).also(::recordAcknowledgement)
                    } finally {
                        finishSendAccounting()
                    }
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
            finishSendAccounting()
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
        synchronized(admissionLock) {
            active.set(false)
            if (sendsInProgress == 0) sendsDrained.complete(Unit)
        }
    }

    suspend fun stopDelegatingAndAwaitSends(timeout: Duration): List<WarmupFinalOffset> {
        require(timeout.isPositive()) { "Warm-up send drain timeout must be > 0" }
        val drain = synchronized(admissionLock) {
            active.set(false)
            if (sendsInProgress == 0) sendsDrained.complete(Unit)
            sendsDrained
        }
        withTimeout(timeout) { drain.await() }
        return synchronized(admissionLock) {
            finalOffsets.map { (partition, offset) -> WarmupFinalOffset(partition, offset) }
        }
    }

    fun close() {
        stopDelegating()
        tracker.close()
        (sender as? AutoCloseable)?.close()
    }

    private fun failSession(error: Throwable) {
        if (failure.compareAndSet(null, error)) {
            active.set(false)
            tracker.fail(error)
        }
    }

    private fun tryBeginSend(destination: WarmupDestination): Boolean = synchronized(admissionLock) {
        if (!active.get() || !quota.tryAcquire()) return false
        val topicPartition = TopicPartition(destination.topic, destination.partition)
        finalOffsets.putIfAbsent(topicPartition, null)
        if (sendsInProgress == 0 && sendsDrained.isCompleted) sendsDrained = CompletableDeferred()
        sendsInProgress++
        true
    }

    private fun recordAcknowledgement(acknowledgement: WarmupSendAcknowledgement) = synchronized(admissionLock) {
        val current = finalOffsets[acknowledgement.topicPartition]
        if (current == null || acknowledgement.offset > current) {
            finalOffsets[acknowledgement.topicPartition] = acknowledgement.offset
        }
    }

    private fun finishSend() = synchronized(admissionLock) {
        check(sendsInProgress > 0) { "Warm-up send accounting underflow" }
        sendsInProgress--
        if (!active.get() && sendsInProgress == 0) sendsDrained.complete(Unit)
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
    private val onPublished: (WarmupRecordCompleted) -> Unit = {},
    private val publisher: WarmupCompletionPublisher
) : RecordProcessingOutcomeObserver<K, V> {
    override fun onOutcome(record: ConsumerRecord<K, V>, outcome: RecordProcessingOutcome) {
        val remote = when (outcome) {
            RecordProcessingOutcome.HandlerSucceeded -> WarmupRemoteOutcome.Succeeded
            is RecordProcessingOutcome.HandlerFailed -> WarmupRemoteOutcome.TerminalFailure
            is RecordProcessingOutcome.PolicyDiscarded -> WarmupRemoteOutcome.Discarded(outcome.reason)
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
        onPublished(completed)
    }
}
