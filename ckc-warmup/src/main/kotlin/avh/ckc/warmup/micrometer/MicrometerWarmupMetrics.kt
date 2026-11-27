package avh.ckc.warmup.micrometer

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.LifecycleOutcome
import avh.ckc.core.metrics.micrometer.MicrometerConsumerMeterBinding
import avh.ckc.warmup.WarmupFallbackReason
import avh.ckc.warmup.WarmupMetrics
import avh.ckc.warmup.WarmupResultDeliveryFailureReason
import avh.ckc.warmup.WarmupResultMetricOutcome
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.consumer.ConsumerRecord

/** Maps warm-up callbacks to meters under the participant consumer binding. */
internal class MicrometerWarmupMetrics<K, V>(
    private val binding: MicrometerConsumerMeterBinding
) : WarmupMetrics<K, V> {
    private val pendingWarmupResults = binding.extensionGaugeValue(WARMUP_PENDING)
    private val warmupResultOutstanding = binding.extensionGaugeValue(WARMUP_RESULT_DELIVERY_OUTSTANDING)
    private val warmupResultQueued = binding.extensionGaugeValue(WARMUP_RESULT_DELIVERY_QUEUED)
    private val warmupResultAwaitingAck = binding.extensionGaugeValue(WARMUP_RESULT_DELIVERY_AWAITING_ACK)
    private val warmupResultLimitedMappings = binding.extensionGaugeValue(WARMUP_RESULT_DELIVERY_LIMITED_MAPPINGS)

    override fun onWarmupDelegated(record: ConsumerRecord<K, V>, sendDurationNanos: Long) {
        val topic = "topic" to record.topic()
        binding.extensionCounter(WARMUP_DELEGATED, topic).increment()
        binding.extensionTimer(WARMUP_SEND_DURATION, topic).record(sendDurationNanos, TimeUnit.NANOSECONDS)
    }

    override fun onWarmupResult(
        record: ConsumerRecord<K, V>,
        outcome: WarmupResultMetricOutcome,
        waitDurationNanos: Long
    ) {
        val topic = "topic" to record.topic()
        val result = "outcome" to outcome.name.lowercase()
        binding.extensionCounter(WARMUP_RESULT, topic, result).increment()
        binding.extensionTimer(WARMUP_RESULT_WAIT_DURATION, topic, result).record(waitDurationNanos, TimeUnit.NANOSECONDS)
    }

    override fun onWarmupFallback(record: ConsumerRecord<K, V>, reason: WarmupFallbackReason) {
        binding.extensionCounter(
            WARMUP_FALLBACK,
            "topic" to record.topic(),
            "reason" to reason.name.lowercase()
        ).increment()
    }

    override fun onWarmupPendingChanged(pending: Int) {
        pendingWarmupResults.set(pending.coerceAtLeast(0))
    }

    override fun onWarmupResultDeliveryChanged(
        outstanding: Int,
        queued: Int,
        awaitingAcknowledgement: Int,
        admissionLimitedMappings: Int
    ) {
        warmupResultOutstanding.set(outstanding.coerceAtLeast(0))
        warmupResultQueued.set(queued.coerceAtLeast(0))
        warmupResultAwaitingAck.set(awaitingAcknowledgement.coerceAtLeast(0))
        warmupResultLimitedMappings.set(admissionLimitedMappings.coerceAtLeast(0))
    }

    override fun onWarmupResultAdmissionLimited() {
        binding.extensionCounter(WARMUP_RESULT_ADMISSION_LIMITED).increment()
    }

    override fun onWarmupResultDeliveryFailure(reason: WarmupResultDeliveryFailureReason) {
        binding.extensionCounter(WARMUP_RESULT_DELIVERY_FAILURE, "reason" to reason.name.lowercase()).increment()
    }

    override fun onWarmupResultDrain(durationNanos: Long, outcome: LifecycleOutcome) {
        binding.extensionTimer(WARMUP_RESULT_DRAIN_DURATION, "outcome" to outcome.name.lowercase())
            .record(durationNanos, TimeUnit.NANOSECONDS)
    }

    companion object {
        fun <K, V> bind(metrics: ConsumerMetrics<K, V>): WarmupMetrics<K, V>? =
            metrics.unwrap(MicrometerConsumerMeterBinding::class.java)?.let { MicrometerWarmupMetrics(it) }
    }
}

private const val WARMUP_DELEGATED = "warmup.delegated"
private const val WARMUP_SEND_DURATION = "warmup.send.duration"
private const val WARMUP_RESULT = "warmup.result"
private const val WARMUP_RESULT_WAIT_DURATION = "warmup.result.wait.duration"
private const val WARMUP_FALLBACK = "warmup.fallback"
private const val WARMUP_PENDING = "warmup.pending"
private const val WARMUP_RESULT_DELIVERY_OUTSTANDING = "warmup.result.delivery.outstanding"
private const val WARMUP_RESULT_DELIVERY_QUEUED = "warmup.result.delivery.queued"
private const val WARMUP_RESULT_DELIVERY_AWAITING_ACK = "warmup.result.delivery.awaiting.ack"
private const val WARMUP_RESULT_DELIVERY_LIMITED_MAPPINGS = "warmup.result.delivery.limited.mappings"
private const val WARMUP_RESULT_ADMISSION_LIMITED = "warmup.result.admission.limited"
private const val WARMUP_RESULT_DELIVERY_FAILURE = "warmup.result.delivery.failure"
private const val WARMUP_RESULT_DRAIN_DURATION = "warmup.result.drain.duration"
