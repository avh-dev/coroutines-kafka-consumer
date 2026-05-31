package avh.ckc.demo

import avh.ckc.demo.service.DemoRecordMetrics
import avh.ckc.demo.ml.eta.SuspendArcaneEtaModelClient
import avh.ckc.demo.ml.eta.SyncArcaneEtaModelClient
import avh.ckc.demo.ml.flavour.SuspendOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.SyncOrderFlavourModelClient
import avh.ckc.micrometer.MicrometerConsumerMetrics
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.boot.actuate.autoconfigure.metrics.MetricsProperties
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@SpringBootTest(
    properties = [
        "demo.kafka.enabled=false"
    ]
)
@ActiveProfiles("confluent-parallel")
class ConfluentParallelProfileContextTest(
    @Autowired private val applicationContext: ApplicationContext,
    @Autowired private val meterRegistry: MeterRegistry,
    @Autowired private val metricsProperties: MetricsProperties
) {
    @Test
    fun contextLoads() {
    }

    @Test
    fun `confluent parallel profile creates only sync model clients`() {
        assertFalse(applicationContext.getBeansOfType(SuspendArcaneEtaModelClient::class.java).isNotEmpty())
        assertFalse(applicationContext.getBeansOfType(SuspendOrderFlavourModelClient::class.java).isNotEmpty())
        assertTrue(applicationContext.getBeansOfType(SyncArcaneEtaModelClient::class.java).isNotEmpty())
        assertTrue(applicationContext.getBeansOfType(SyncOrderFlavourModelClient::class.java).isNotEmpty())
    }

    @Test
    fun `consumer profile info metric identifies confluent parallel implementation`() {
        val gauge = meterRegistry.find("ckc.demo.consumer.profile.info")
            .tag("consumer_impl", "confluent_parallel")
            .tag("spring_profile", "confluent-parallel")
            .gauge()

        assertNotNull(gauge)
        assertEquals(1.0, gauge.value())
    }

    @Test
    fun `confluent parallel profile does not publish CKC record metrics`() {
        assertFalse(applicationContext.getBeansOfType(DemoRecordMetrics::class.java).isNotEmpty())
        assertFalse(applicationContext.getBeansOfType(MicrometerConsumerMetrics::class.java).isNotEmpty())
    }

    @Test
    fun `confluent parallel processing timer publishes percentile histogram buckets`() {
        assertEquals(
            true,
            metricsProperties.distribution.percentilesHistogram["pc.user.function.processing.time"]
        )
    }
}
