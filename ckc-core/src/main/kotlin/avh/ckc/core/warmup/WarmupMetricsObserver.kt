package avh.ckc.core.warmup

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.WarmupFallbackReason
import avh.ckc.core.metrics.WarmupResultMetricOutcome
import org.apache.kafka.clients.consumer.ConsumerRecord

/** Adapts isolated donor-session observations to one consumer-level metrics callback. */
internal class WarmupMetricsObserver<K, V>(
    private val metrics: ConsumerMetrics<K, V>
) : WarmupDataPlaneObserver<K, V> {
    private val pendingLock = Any()
    private val pendingBySession = mutableMapOf<WarmupSessionId, Int>()

    override fun onForwarded(
        record: ConsumerRecord<K, V>,
        event: WarmupForwardedEvent,
        durationNanos: Long
    ) = metrics.onWarmupDelegated(record, durationNanos)

    override fun onResult(
        record: ConsumerRecord<K, V>,
        outcome: WarmupRemoteOutcome,
        durationNanos: Long
    ) = metrics.onWarmupResult(record, outcome.metricOutcome(), durationNanos)

    override fun onFallback(record: ConsumerRecord<K, V>, reason: WarmupFallbackReason) =
        metrics.onWarmupFallback(record, reason)

    override fun onPendingChanged(sessionId: WarmupSessionId, pending: Int) {
        val aggregate = synchronized(pendingLock) {
            if (pending > 0) pendingBySession[sessionId] = pending else pendingBySession.remove(sessionId)
            pendingBySession.values.sum()
        }
        metrics.onWarmupPendingChanged(aggregate)
    }

    private fun WarmupRemoteOutcome.metricOutcome(): WarmupResultMetricOutcome = when (this) {
        WarmupRemoteOutcome.Succeeded -> WarmupResultMetricOutcome.SUCCEEDED
        WarmupRemoteOutcome.TerminalFailure -> WarmupResultMetricOutcome.TERMINAL_FAILURE
        is WarmupRemoteOutcome.Discarded -> WarmupResultMetricOutcome.DISCARDED
    }
}
