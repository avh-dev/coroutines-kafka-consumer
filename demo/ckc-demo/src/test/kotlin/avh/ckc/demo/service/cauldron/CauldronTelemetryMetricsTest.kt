package avh.ckc.demo.service.cauldron

import avh.ckc.demo.model.EtaContext
import avh.ckc.demo.sampleTelemetryEvent
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class CauldronTelemetryMetricsTest {
    private val meterRegistry = SimpleMeterRegistry()
    private val metrics = CauldronTelemetryMetrics(meterRegistry)

    @Test
    fun `records forward event time gap from previous eta context`() {
        metrics.recordEventGap(
            telemetryEvent = sampleTelemetryEvent().toBuilder()
                .setMetadata(
                    sampleTelemetryEvent().metadata.toBuilder()
                        .setOccurredAt("2026-03-25T10:15:31.245Z")
                        .build()
                )
                .build(),
            previous = etaContext(updatedAt = "2026-03-25T10:15:16.245Z")
        )

        val timer = meterRegistry.find("ckc.demo.cauldron.telemetry.event.gap").timer()

        assertNotNull(timer)
        assertEquals(1L, timer.count())
        assertEquals(15_000.0, timer.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS))
    }

    @Test
    fun `records out of order telemetry instead of negative gap`() {
        metrics.recordEventGap(
            telemetryEvent = sampleTelemetryEvent().toBuilder()
                .setMetadata(
                    sampleTelemetryEvent().metadata.toBuilder()
                        .setOccurredAt("2026-03-25T10:15:10.000Z")
                        .build()
                )
                .build(),
            previous = etaContext(updatedAt = "2026-03-25T10:15:16.245Z")
        )

        val counter = meterRegistry.find("ckc.demo.cauldron.telemetry.event.out.of.order").counter()

        assertNotNull(counter)
        assertEquals(1.0, counter.count())
        assertEquals(0, meterRegistry.find("ckc.demo.cauldron.telemetry.event.gap").meters().size)
    }

    @Test
    fun `prometheus metric name matches grafana query`() {
        val prometheusRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        CauldronTelemetryMetrics(prometheusRegistry).recordEventGap(
            telemetryEvent = sampleTelemetryEvent().toBuilder()
                .setMetadata(
                    sampleTelemetryEvent().metadata.toBuilder()
                        .setOccurredAt("2026-03-25T10:15:31.245Z")
                        .build()
                )
                .build(),
            previous = etaContext(updatedAt = "2026-03-25T10:15:16.245Z")
        )

        val scrape = prometheusRegistry.scrape()

        assertTrue(scrape.contains("ckc_demo_cauldron_telemetry_event_gap_seconds_count"))
        assertTrue(scrape.contains("ckc_demo_cauldron_telemetry_event_gap_seconds_sum"))
        assertTrue(scrape.contains("ckc_demo_cauldron_telemetry_event_gap_seconds_max"))
        assertFalse(scrape.contains("ckc_demo_cauldron_telemetry_event_gap_count"))
    }

    private fun etaContext(updatedAt: String): EtaContext =
        EtaContext(
            batchId = "batch-healing-001",
            previousTemperatureC = 91.4,
            previousDensitySg = 1.18,
            previousBubbleRateHz = 8.7,
            previousMagicalEtaUnits = 120.0,
            previousModelRequestId = "eta-1",
            updatedAt = updatedAt
        )
}
