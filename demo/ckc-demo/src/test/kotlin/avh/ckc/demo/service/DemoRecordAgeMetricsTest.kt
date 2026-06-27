package avh.ckc.demo.service

import avh.ckc.demo.sampleTelemetryEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import java.util.Optional
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class DemoRecordAgeMetricsTest {
    @Test
    fun `records age without publishing process duration`() {
        val meterRegistry = SimpleMeterRegistry()
        val metrics = DemoRecordAgeMetrics(meterRegistry)
        val record = telemetryRecord()

        metrics.onProcessed("cauldron_events", record)

        val summary = meterRegistry.find("ckc.record.age")
            .tag("consumer_id", "cauldron_events")
            .tag("topic", "cauldron.events.v1")
            .tag("event_type", "CAULDRON_TELEMETRY")
            .tag("error", "none")
            .summary()

        assertNotNull(summary)
        assertEquals(1L, summary.count())
        assertNull(meterRegistry.find("ckc.record.process.duration").timer())
    }

    @Test
    fun `prometheus record age metric name matches grafana query`() {
        val prometheusRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        val metrics = DemoRecordAgeMetrics(prometheusRegistry)
        val record = telemetryRecord()

        metrics.onProcessed("cauldron_events", record)

        val scrape = prometheusRegistry.scrape()

        assertTrue(scrape.contains("ckc_record_age_count"))
        assertTrue(scrape.contains("ckc_record_age_sum"))
        assertFalse(scrape.contains("ckc_record_age_milliseconds"))
    }

    private fun telemetryRecord(): ConsumerRecord<String, CauldronTelemetryEvent> =
        ConsumerRecord(
            "cauldron.events.v1",
            0,
            1L,
            System.currentTimeMillis() - 1_500,
            TimestampType.CREATE_TIME,
            0,
            0,
            "cauldron-1",
            sampleTelemetryEvent(),
            RecordHeaders(),
            Optional.empty()
        )
}
