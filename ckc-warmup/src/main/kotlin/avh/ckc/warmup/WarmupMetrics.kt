package avh.ckc.warmup

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.LifecycleOutcome
import avh.ckc.warmup.micrometer.micrometerWarmupMetrics
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Observes delegation and recipient result delivery for one warm-up participant. All methods
 * default to no-op. Callbacks run inline on donor processing, poll, recipient send, or shutdown
 * paths and can overlap. Keep them thread-safe and non-blocking. Callback exceptions of type
 * [Exception] are isolated by the warm-up runtime.
 */
interface WarmupMetrics<K, V> {
    /**
     * Records one append confirmed by a producer acknowledgement or a matching early recipient
     * result. [sendDurationNanos] measures the donor's time waiting for that confirmation.
     */
    fun onWarmupDelegated(record: ConsumerRecord<K, V>, sendDurationNanos: Long) = Unit

    /** Records an explicit recipient result and [waitDurationNanos] since the append was confirmed. */
    fun onWarmupResult(
        record: ConsumerRecord<K, V>,
        outcome: WarmupResultMetricOutcome,
        waitDurationNanos: Long
    ) = Unit

    /** Reports an admitted delegation that returned to the ownership-checked local path. */
    fun onWarmupFallback(record: ConsumerRecord<K, V>, reason: WarmupFallbackReason) = Unit

    /** Publishes the current count of pending remote results across this participant's donor sessions. */
    fun onWarmupPendingChanged(pending: Int) = Unit

    /**
     * Publishes current recipient result-delivery counts across this participant's mappings.
     * [outstanding] includes reserved records still in processing and both send stages. [queued]
     * counts ready results waiting to send. [awaitingAcknowledgement] counts submitted results
     * waiting for Kafka. [admissionLimitedMappings] counts mappings under capacity pressure.
     */
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

    /** Reports elapsed [durationNanos] and [outcome] for the observed result-delivery drain. */
    fun onWarmupResultDrain(durationNanos: Long, outcome: LifecycleOutcome) = Unit
}

/** Recipient processing result observed by the donor. */
enum class WarmupResultMetricOutcome {
    /** The recipient handler succeeded. */
    SUCCEEDED,
    /** The recipient exhausted its handler processing path. */
    TERMINAL_FAILURE,
    /** A recipient processing policy discarded the record. */
    DISCARDED
}

/** Reason a delegation returned to the donor's local processing path. */
enum class WarmupFallbackReason {
    /** The donor could not confirm the forwarding append. */
    SEND_FAILURE,
    /** The forwarding append was confirmed but the recipient result missed its deadline. */
    RESULT_TIMEOUT,
    /** The donor session failed before it could use a recipient result. */
    SESSION_FAILURE,
    /** The recipient reported terminal processing failure. */
    REMOTE_TERMINAL_FAILURE,
    /** The recipient discarded a record that the donor's policy requires it to handle. */
    REMOTE_DISCARD_REJECTED
}

/** Reason the recipient could not deliver outstanding results to the donor. */
enum class WarmupResultDeliveryFailureReason {
    /** Kafka rejected or failed the result send. */
    SEND_FAILURE,
    /** A result send or flush exceeded its delivery budget. */
    TIMEOUT,
    /** The result pipeline was cancelled while it still held results. */
    CANCELLED,
    /** The result pipeline failed an internal check. */
    INTERNAL
}

/** Default metrics sink when the participant supplies no compatible metrics binding. */
private object NoopWarmupMetrics : WarmupMetrics<Any?, Any?>

@Suppress("UNCHECKED_CAST")
internal fun <K, V> defaultWarmupMetrics(metrics: ConsumerMetrics<K, V>): WarmupMetrics<K, V> =
    (metrics as? WarmupMetrics<K, V>) ?: micrometerWarmupMetrics(metrics)
        ?: NoopWarmupMetrics as WarmupMetrics<K, V>
