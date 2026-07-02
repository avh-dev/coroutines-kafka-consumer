package avh.ckc.demo.service

import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.micrometer.MicrometerConsumerMetrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class DemoRecordMetricsTest {
    @Test
    fun `records freshness first stale drops with CKC record metric tags`() {
        val meterRegistry = SimpleMeterRegistry()
        val metrics = MicrometerConsumerMetrics(meterRegistry)
            .forConsumer<String, String>(consumerId = "cauldron_events")
        val recordMetrics = DemoRecordMetrics()
        val context = DemoConsumerRecordContext(
            key = "cauldron-1",
            topic = "cauldron.events.v1",
            partition = 0,
            offset = 42,
            timestamp = 1_000
        )

        recordMetrics.onDropped(metrics, context, "payload", RecordDropReason.STALE_AGE)

        val counter = meterRegistry.find("ckc.record.dropped")
            .tag("topic", "cauldron.events.v1")
            .tag("reason", "stale_age")
            .counter()
        assertNotNull(counter)
        assertEquals(1.0, counter.count())
    }
}
