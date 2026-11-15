package avh.ckc.core.warmup

import avh.ckc.core.RecordingMetrics
import avh.ckc.core.metrics.WarmupFallbackReason
import avh.ckc.core.metrics.WarmupResultMetricOutcome
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class WarmupMetricsObserverTest {
    @Test
    fun `observer routes results and aggregates pending across donor sessions`() {
        val metrics = RecordingMetrics<String, String>()
        val observer = WarmupMetricsObserver(metrics)
        val record = ConsumerRecord("orders", 2, 9, "key", "value")
        val forwarded = WarmupForwardedEvent(
            WarmupSessionId("first"),
            TopicPartition("orders", 2),
            9,
            TopicPartition("warmup-orders", 0),
            15
        )

        observer.onForwarded(record, forwarded, 11)
        observer.onResult(record, WarmupRemoteOutcome.Succeeded, 12)
        observer.onResult(record, WarmupRemoteOutcome.TerminalFailure, 13)
        observer.onResult(
            record,
            WarmupRemoteOutcome.Discarded(avh.ckc.core.metrics.RecordDropReason.QUEUE_OVERFLOW),
            14
        )
        observer.onFallback(record, WarmupFallbackReason.RESULT_TIMEOUT)
        observer.onPendingChanged(WarmupSessionId("first"), 2)
        observer.onPendingChanged(WarmupSessionId("second"), 3)
        observer.onPendingChanged(WarmupSessionId("first"), 0)

        assertEquals(listOf(record to 11L), metrics.warmupDelegated)
        assertEquals(
            listOf(
                WarmupResultMetricOutcome.SUCCEEDED,
                WarmupResultMetricOutcome.TERMINAL_FAILURE,
                WarmupResultMetricOutcome.DISCARDED
            ),
            metrics.warmupResults.map { it.outcome }
        )
        assertEquals(listOf(record to WarmupFallbackReason.RESULT_TIMEOUT), metrics.warmupFallbacks)
        assertEquals(listOf(2, 5, 3), metrics.warmupPending)
    }
}
