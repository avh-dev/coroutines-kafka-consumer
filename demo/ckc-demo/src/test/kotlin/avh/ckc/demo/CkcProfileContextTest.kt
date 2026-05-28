package avh.ckc.demo

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.demo.ml.eta.SuspendArcaneEtaModelClient
import avh.ckc.demo.ml.eta.SyncArcaneEtaModelClient
import avh.ckc.demo.ml.flavour.SuspendOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.SyncOrderFlavourModelClient
import avh.ckc.demo.proto.CauldronTelemetryEvent
import com.linecorp.armeria.client.WebClient
import io.ktor.client.HttpClient
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import org.springframework.boot.actuate.autoconfigure.metrics.MetricsProperties
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.test.context.ActiveProfiles
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@SpringBootTest(
    properties = [
        "demo.kafka.enabled=false"
    ]
)
@ActiveProfiles("ckc")
class CkcProfileContextTest(
    @Autowired private val applicationContext: ApplicationContext,
    @Autowired private val meterRegistry: MeterRegistry,
    @Autowired private val metricsProperties: MetricsProperties,
    @Autowired
    @Qualifier("consumerMetrics")
    private val consumerMetrics: ConsumerMetrics<String, CauldronTelemetryEvent>
) {
    @Test
    fun contextLoads() {
    }

    @Test
    fun `ckc profile creates only suspend model clients`() {
        assertTrue(applicationContext.getBeansOfType(HttpClient::class.java).isNotEmpty())
        assertFalse(applicationContext.getBeansOfType(WebClient::class.java).isNotEmpty())
        assertTrue(applicationContext.getBeansOfType(SuspendArcaneEtaModelClient::class.java).isNotEmpty())
        assertTrue(applicationContext.getBeansOfType(SuspendOrderFlavourModelClient::class.java).isNotEmpty())
        assertFalse(applicationContext.getBeansOfType(SyncArcaneEtaModelClient::class.java).isNotEmpty())
        assertFalse(applicationContext.getBeansOfType(SyncOrderFlavourModelClient::class.java).isNotEmpty())
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
    fun `model call timer publishes percentile histogram buckets`() {
        assertEquals(
            true,
            metricsProperties.distribution.percentilesHistogram["ckc.demo.model.call.duration"]
        )
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
