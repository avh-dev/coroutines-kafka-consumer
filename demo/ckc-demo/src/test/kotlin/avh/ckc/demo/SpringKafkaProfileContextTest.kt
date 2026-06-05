package avh.ckc.demo

import avh.ckc.core.ProcessingMode
import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.consumer.springkafka.SpringKafkaProfileConfiguration
import avh.ckc.demo.ml.eta.SuspendArcaneEtaModelClient
import avh.ckc.demo.ml.eta.SyncArcaneEtaModelClient
import avh.ckc.demo.ml.flavour.SuspendOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.SyncOrderFlavourModelClient
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.test.context.ActiveProfiles
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@SpringBootTest(
    properties = [
        "demo.kafka.enabled=false",
        "SERVER_PORT=0"
    ]
)
@ActiveProfiles("spring-kafka")
class SpringKafkaProfileContextTest(
    @Autowired private val applicationContext: ApplicationContext,
    @Autowired private val meterRegistry: MeterRegistry
) {
    @Test
    fun contextLoads() {
    }

    @Test
    fun `spring kafka profile creates only sync model clients`() {
        assertFalse(applicationContext.getBeansOfType(SuspendArcaneEtaModelClient::class.java).isNotEmpty())
        assertFalse(applicationContext.getBeansOfType(SuspendOrderFlavourModelClient::class.java).isNotEmpty())
        assertTrue(applicationContext.getBeansOfType(SyncArcaneEtaModelClient::class.java).isNotEmpty())
        assertTrue(applicationContext.getBeansOfType(SyncOrderFlavourModelClient::class.java).isNotEmpty())
    }

    @Test
    fun `consumer profile info metric identifies spring kafka implementation`() {
        val gauge = meterRegistry.find("ckc.demo.consumer.profile.info")
            .tag("consumer_impl", "spring_kafka")
            .tag("spring_profile", "spring-kafka")
            .gauge()

        assertNotNull(gauge)
        assertEquals(1.0, gauge.value())
    }

    @Test
    fun `spring kafka lifecycle listeners use time based offset commits`() {
        val properties = DemoApplicationProperties().apply {
            kafka.consumer.commitIntervalMs = 1_234
            consumers.order.processingMode = ProcessingMode.AT_LEAST_ONCE_ORDERED_BY_PARTITION
            consumers.batch.processingMode = ProcessingMode.AT_LEAST_ONCE_ORDERED_BY_PARTITION
        }
        val configuration = SpringKafkaProfileConfiguration()

        val orderConsumerFactory = configuration.orderOrderConsumerFactory(properties)
        val orderContainerFactory = configuration.orderLifecycleListenerContainerFactory(orderConsumerFactory, properties)
        val batchConsumerFactory = configuration.batchOrderConsumerFactory(properties)
        val batchContainerFactory = configuration.batchLifecycleListenerContainerFactory(batchConsumerFactory, properties)
        val telemetryConsumerFactory = configuration.cauldronTelemetryConsumerFactory(properties)

        assertEquals(ContainerProperties.AckMode.TIME, orderContainerFactory.containerProperties.ackMode)
        assertEquals(1_234, orderContainerFactory.containerProperties.ackTime)
        assertEquals(ContainerProperties.AckMode.TIME, batchContainerFactory.containerProperties.ackMode)
        assertEquals(1_234, batchContainerFactory.containerProperties.ackTime)
        assertEquals(false, orderConsumerFactory.config()[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG])
        assertEquals(false, batchConsumerFactory.config()[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG])
        assertEquals(true, telemetryConsumerFactory.config()[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG])
        assertEquals(1_234, telemetryConsumerFactory.config()[ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG])
    }

    private fun ConsumerFactory<*, *>.config(): Map<String, Any> =
        (this as DefaultKafkaConsumerFactory<*, *>).configurationProperties
}
