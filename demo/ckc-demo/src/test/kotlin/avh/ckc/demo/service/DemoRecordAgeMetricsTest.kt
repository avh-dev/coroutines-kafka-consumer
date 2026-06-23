package avh.ckc.demo.service

import avh.ckc.demo.sampleTelemetryEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import java.util.Optional
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class DemoRecordAgeMetricsTest {
    @Test
    fun `records age without publishing process duration`() {
        val meterRegistry = SimpleMeterRegistry()
        val metrics = DemoRecordAgeMetrics(meterRegistry)
        val record: ConsumerRecord<String, CauldronTelemetryEvent> = ConsumerRecord(
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
}
