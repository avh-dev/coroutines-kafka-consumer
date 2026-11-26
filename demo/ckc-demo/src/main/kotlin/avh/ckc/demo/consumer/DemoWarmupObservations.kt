package avh.ckc.demo.consumer

import avh.ckc.warmup.TrafficWarmupForwardedEvent
import avh.ckc.warmup.TrafficWarmupLifecycleEvent
import avh.ckc.warmup.TrafficWarmupLifecyclePhase
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import java.util.UUID
import org.slf4j.LoggerFactory

internal fun recordWarmupLifecycle(event: TrafficWarmupLifecycleEvent, meterRegistry: MeterRegistry) {
    warmupLog.info(
        "Warm-up lifecycle: phase={}, participantId={}, sessionId={}, reasonCode={}, recoveryDurationMillis={}",
        event.phase,
        event.participantId,
        event.sessionId,
        event.reasonCode,
        event.recoveryDurationMillis
    )
    if (event.phase != TrafficWarmupLifecyclePhase.RECIPIENT_FINISHED) return

    val outcome = when (event.reasonCode) {
        null -> "success"
        "incomplete_donor_coverage", "participant_stopped" -> "skipped"
        "donor_acceptance_timeout", "donor_completion_timeout", "donor_liveness_timeout",
        "participant_start_timeout", "metadata_timeout",
        "consumer_preparation_timeout", "result_delivery_timeout",
        "recipient_cleanup_timeout" -> "timeout"
        else -> "error"
    }
    // One immutable sample per recipient attempt, including restarts of the same pod.
    val attempt = event.sessionId ?: UUID.randomUUID().toString()
    Gauge.builder("demo.warmup.recipient.records", event) { (it.processedRecords ?: 0).toDouble() }
        .tags("outcome", outcome, "attempt", attempt)
        .strongReference(true)
        .register(meterRegistry)
    event.durationMillis?.let {
        Gauge.builder("demo.warmup.recipient.duration.seconds", event) { (it.durationMillis ?: 0) / 1000.0 }
            .tags("outcome", outcome, "attempt", attempt)
            .strongReference(true)
            .register(meterRegistry)
    }
}

internal fun logWarmupForwarded(event: TrafficWarmupForwardedEvent) {
    warmupLog.info(
        "Warm-up record forwarded: sessionId={}, source={}-{}@{}, destination={}-{}@{}",
        event.sessionId,
        event.sourceTopic,
        event.sourcePartition,
        event.sourceOffset,
        event.destinationTopic,
        event.destinationPartition,
        event.destinationOffset
    )
}

private val warmupLog = LoggerFactory.getLogger("avh.ckc.demo.consumer.DemoWarmupObservations")
