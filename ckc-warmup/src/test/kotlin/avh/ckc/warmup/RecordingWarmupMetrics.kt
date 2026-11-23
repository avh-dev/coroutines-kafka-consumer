package avh.ckc.warmup

import avh.ckc.core.RecordingMetrics
import java.util.concurrent.CopyOnWriteArrayList
import org.apache.kafka.clients.consumer.ConsumerRecord

data class WarmupResultCall<K, V>(
    val record: ConsumerRecord<K, V>,
    val outcome: WarmupResultMetricOutcome,
    val durationNanos: Long
)

internal class RecordingWarmupMetrics<K, V> : RecordingMetrics<K, V>(), WarmupMetrics<K, V> {
    val warmupDelegated = CopyOnWriteArrayList<Pair<ConsumerRecord<K, V>, Long>>()
    val warmupResults = CopyOnWriteArrayList<WarmupResultCall<K, V>>()
    val warmupFallbacks = CopyOnWriteArrayList<Pair<ConsumerRecord<K, V>, WarmupFallbackReason>>()
    val warmupPending = CopyOnWriteArrayList<Int>()

    override fun onWarmupDelegated(record: ConsumerRecord<K, V>, sendDurationNanos: Long) {
        warmupDelegated += record to sendDurationNanos
    }

    override fun onWarmupResult(
        record: ConsumerRecord<K, V>,
        outcome: WarmupResultMetricOutcome,
        waitDurationNanos: Long
    ) {
        warmupResults += WarmupResultCall(record, outcome, waitDurationNanos)
    }

    override fun onWarmupFallback(record: ConsumerRecord<K, V>, reason: WarmupFallbackReason) {
        warmupFallbacks += record to reason
    }

    override fun onWarmupPendingChanged(pending: Int) {
        warmupPending += pending
    }
}
