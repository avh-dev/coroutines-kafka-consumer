package avh.ckc.warmup.micrometer

import avh.ckc.core.metrics.LifecycleOutcome
import avh.ckc.micrometer.MicrometerConsumerMetricsSchema
import avh.ckc.micrometer.micrometerConsumerMetrics
import avh.ckc.warmup.WarmupFallbackReason
import avh.ckc.warmup.WarmupResultDeliveryFailureReason
import avh.ckc.warmup.WarmupResultMetricOutcome
import avh.ckc.warmup.defaultWarmupMetrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class MicrometerWarmupMetricsTest {
    @Test
    fun `reused consumer metrics binding updates the registered warmup gauges`() {
        val registry = SimpleMeterRegistry()
        val baseMetrics = micrometerConsumerMetrics<String, String>(
            MicrometerConsumerMetricsSchema(registry, metricPrefix = "test")
        )
        val first = defaultWarmupMetrics(baseMetrics)
        first.onWarmupPendingChanged(1)
        first.onWarmupResultDeliveryChanged(3, 2, 1, 1)

        val second = defaultWarmupMetrics(baseMetrics)
        second.onWarmupPendingChanged(4)
        second.onWarmupResultDeliveryChanged(9, 4, 5, 2)

        assertEquals(4.0, registry.get("test.ckc.warmup.pending").gauge().value())
        assertEquals(9.0, registry.get("test.ckc.warmup.result.delivery.outstanding").gauge().value())
        assertEquals(4.0, registry.get("test.ckc.warmup.result.delivery.queued").gauge().value())
        assertEquals(5.0, registry.get("test.ckc.warmup.result.delivery.awaiting.ack").gauge().value())
        assertEquals(2.0, registry.get("test.ckc.warmup.result.delivery.limited.mappings").gauge().value())
    }

    @Test
    fun `warmup metrics expose transport outcomes fallback and aggregate pending`() {
        val registry = SimpleMeterRegistry()
        val baseMetrics = micrometerConsumerMetrics<String, String>(
            MicrometerConsumerMetricsSchema(registry, metricPrefix = "test")
        ) { consumerId = "orders-consumer" }
        assertTrue(registry.meters.none { ".warmup." in it.id.name })
        val metrics = defaultWarmupMetrics(baseMetrics)
        val record = ConsumerRecord("orders", 0, 0L, "key", "value")

        metrics.onWarmupDelegated(record, 10)
        metrics.onWarmupResult(record, WarmupResultMetricOutcome.SUCCEEDED, 20)
        metrics.onWarmupFallback(record, WarmupFallbackReason.RESULT_TIMEOUT)
        metrics.onWarmupPendingChanged(4)
        metrics.onWarmupResultDeliveryChanged(9, 4, 5, 1)
        metrics.onWarmupResultAdmissionLimited()
        metrics.onWarmupResultDeliveryFailure(WarmupResultDeliveryFailureReason.TIMEOUT)
        metrics.onWarmupResultDrain(30, LifecycleOutcome.SUCCESS)

        assertEquals(1.0, registry.get("test.ckc.warmup.delegated").counter().count())
        assertEquals(1L, registry.get("test.ckc.warmup.send.duration").timer().count())
        assertEquals(1.0, registry.get("test.ckc.warmup.result").tag("outcome", "succeeded").counter().count())
        assertEquals(1L, registry.get("test.ckc.warmup.result.wait.duration").timer().count())
        assertEquals(
            1.0,
            registry.get("test.ckc.warmup.fallback").tag("reason", "result_timeout").counter().count()
        )
        assertEquals(4.0, registry.get("test.ckc.warmup.pending").tag("consumer_id", "orders-consumer").gauge().value())
        assertEquals(9.0, registry.get("test.ckc.warmup.result.delivery.outstanding").gauge().value())
        assertEquals(4.0, registry.get("test.ckc.warmup.result.delivery.queued").gauge().value())
        assertEquals(5.0, registry.get("test.ckc.warmup.result.delivery.awaiting.ack").gauge().value())
        assertEquals(1.0, registry.get("test.ckc.warmup.result.delivery.limited.mappings").gauge().value())
        assertEquals(1.0, registry.get("test.ckc.warmup.result.admission.limited").counter().count())
        assertEquals(
            1.0,
            registry.get("test.ckc.warmup.result.delivery.failure").tag("reason", "timeout").counter().count()
        )
        assertEquals(
            1L,
            registry.get("test.ckc.warmup.result.drain.duration").tag("outcome", "success").timer().count()
        )
    }
}
