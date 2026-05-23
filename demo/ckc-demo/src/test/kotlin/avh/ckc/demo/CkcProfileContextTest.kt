package avh.ckc.demo

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.demo.proto.CauldronTelemetryEvent
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

@SpringBootTest(
    properties = [
        "demo.kafka.enabled=false"
    ]
)
@ActiveProfiles("ckc")
class CkcProfileContextTest(
    @Autowired private val meterRegistry: MeterRegistry,
    @Autowired
    @Qualifier("consumerMetrics")
    private val consumerMetrics: ConsumerMetrics<String, CauldronTelemetryEvent>
) {
    @Test
    fun contextLoads() {
    }

    @Test
    fun `consumer profile info metric identifies ckc implementation`() {
        val gauge = meterRegistry.find("ckc.demo.consumer.profile.info")
            .tag("consumer_impl", "ckc")
            .tag("spring_profile", "ckc")
            .gauge()

        assertNotNull(gauge)
        assertEquals(1.0, gauge.value())
    }

    @Test
    fun `record metrics do not include implementation tag`() {
        consumerMetrics.onRecordProcessed(
            key = "key",
            value = sampleTelemetryEvent(),
            record = ConsumerRecord("cauldron.events.v1", 0, 0L, "key".toByteArray(), ByteArray(0)),
            recordAgeMillis = 10,
            durationNanos = 1_000_000
        )

        val counter = meterRegistry.find("ckc.record.processed")
            .tag("consumer_id", "cauldron_events")
            .tag("topic", "cauldron.events.v1")
            .tag("event_type", "CAULDRON_TELEMETRY")
            .counter()

        assertNotNull(counter)
        assertNull(
            meterRegistry.find("ckc.record.processed")
                .tag("consumer_impl", "ckc")
                .counter()
        )
    }
}
