package avh.ckc.warmup

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.LifecycleOutcome
import avh.ckc.warmup.micrometer.micrometerWarmupMetrics
import org.apache.kafka.clients.consumer.ConsumerRecord

/** Observes delegation and recipient result delivery for one warm-up participant. */
interface WarmupMetrics<K, V> {
    /** Records one producer-acknowledged delegation without tagging its session or offset. */
    fun onWarmupDelegated(record: ConsumerRecord<K, V>, sendDurationNanos: Long) = Unit

    /** Records one explicit recipient result and the wait after producer acknowledgement. */
    fun onWarmupResult(
        record: ConsumerRecord<K, V>,
        outcome: WarmupResultMetricOutcome,
        waitDurationNanos: Long
    ) = Unit

    fun onWarmupFallback(record: ConsumerRecord<K, V>, reason: WarmupFallbackReason) = Unit

    /** Reports aggregate pending remote results across this donor's independent sessions. */
    fun onWarmupPendingChanged(pending: Int) = Unit

    /** Reports the bounded recipient result-delivery pipeline across this consumer's mappings. */
    fun onWarmupResultDeliveryChanged(
        outstanding: Int,
        queued: Int,
        awaitingAcknowledgement: Int,
        admissionLimitedMappings: Int
    ) = Unit

    /** Records the start of one capacity-limited admission episode for a recipient mapping. */
    fun onWarmupResultAdmissionLimited() = Unit

    /** Records a terminal result-delivery failure without changing the handler's business outcome. */
    fun onWarmupResultDeliveryFailure(reason: WarmupResultDeliveryFailureReason) = Unit

    /** Records one result-delivery drain attempt. */
    fun onWarmupResultDrain(durationNanos: Long, outcome: LifecycleOutcome) = Unit
}

enum class WarmupResultMetricOutcome {
    SUCCEEDED,
    TERMINAL_FAILURE,
    DISCARDED
}

enum class WarmupFallbackReason {
    SEND_FAILURE,
    RESULT_TIMEOUT,
    SESSION_FAILURE,
    REMOTE_TERMINAL_FAILURE
}

enum class WarmupResultDeliveryFailureReason {
    SEND_FAILURE,
    TIMEOUT,
    CANCELLED,
    INTERNAL
}

private object NoopWarmupMetrics : WarmupMetrics<Any?, Any?>

@Suppress("UNCHECKED_CAST")
internal fun <K, V> defaultWarmupMetrics(metrics: ConsumerMetrics<K, V>): WarmupMetrics<K, V> =
    (metrics as? WarmupMetrics<K, V>) ?: micrometerWarmupMetrics(metrics)
        ?: NoopWarmupMetrics as WarmupMetrics<K, V>
