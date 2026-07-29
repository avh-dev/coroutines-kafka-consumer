package avh.ckc.demo

import avh.ckc.core.ProcessingMode
import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.consumer.springkafkathreadpool.SpringKafkaThreadPoolProfileConfiguration
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
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.util.ReflectionTestUtils
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@SpringBootTest(
    properties = [
        "demo.kafka.enabled=false",
        "SERVER_PORT=0",
        "spring.autoconfigure.exclude=com.linecorp.armeria.spring.ArmeriaAutoConfiguration," +
                "com.linecorp.armeria.spring.actuate.ArmeriaSpringActuatorAutoConfiguration"
    ]
)
@ActiveProfiles("spring-kafka-thread-pool")
class SpringKafkaThreadPoolProfileContextTest(
    @Autowired private val applicationContext: ApplicationContext,
    @Autowired private val meterRegistry: MeterRegistry
) {
    @Test
    fun contextLoads() {
    }

    @Test
    fun `thread pool profile creates only sync model clients`() {
        assertFalse(applicationContext.getBeansOfType(SuspendArcaneEtaModelClient::class.java).isNotEmpty())
        assertFalse(applicationContext.getBeansOfType(SuspendOrderFlavourModelClient::class.java).isNotEmpty())
        assertTrue(applicationContext.getBeansOfType(SyncArcaneEtaModelClient::class.java).isNotEmpty())
        assertTrue(applicationContext.getBeansOfType(SyncOrderFlavourModelClient::class.java).isNotEmpty())
    }

    @Test
    fun `consumer profile info metric identifies spring kafka thread pool implementation`() {
        val gauge = meterRegistry.find("ckc.demo.consumer.profile.info")
            .tag("consumer_impl", "spring_kafka_thread_pool")
            .tag("profile", "spring-kafka-thread-pool")
            .tag("spring_profile", "spring-kafka-thread-pool")
            .gauge()

        assertNotNull(gauge)
        assertEquals(1.0, gauge.value())
    }

    @Test
    fun `thread pool listener factories use batch listener admission and container commits`() {
        val properties = DemoApplicationProperties().apply {
            kafka.consumer.commitIntervalMs = 1_234
            consumers.order.processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
            consumers.batch.processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
            consumers.telemetry.processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST
        }
        val configuration = SpringKafkaThreadPoolProfileConfiguration()

        val orderConsumerFactory = configuration.springKafkaThreadPoolOrderConsumerFactory(properties)
        val orderContainerFactory = configuration.springKafkaThreadPoolOrderListenerContainerFactory(
            orderConsumerFactory,
            properties
        )
        val batchConsumerFactory = configuration.springKafkaThreadPoolBatchConsumerFactory(properties)
        val batchContainerFactory = configuration.springKafkaThreadPoolBatchListenerContainerFactory(
            batchConsumerFactory,
            properties
        )
        val telemetryConsumerFactory = configuration.springKafkaThreadPoolTelemetryConsumerFactory(properties)
        val telemetryContainerFactory = configuration.springKafkaThreadPoolTelemetryListenerContainerFactory(
            telemetryConsumerFactory,
            properties
        )

        assertEquals(true, orderContainerFactory.isBatchListener)
        assertEquals(true, batchContainerFactory.isBatchListener)
        assertEquals(true, telemetryContainerFactory.isBatchListener)
        assertEquals(ContainerProperties.AckMode.TIME, orderContainerFactory.containerProperties.ackMode)
        assertEquals(1_234, orderContainerFactory.containerProperties.ackTime)
        assertEquals(ContainerProperties.AckMode.TIME, batchContainerFactory.containerProperties.ackMode)
        assertEquals(1_234, batchContainerFactory.containerProperties.ackTime)
        assertEquals(false, orderConsumerFactory.config()[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG])
        assertEquals(false, batchConsumerFactory.config()[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG])
        assertEquals(true, telemetryConsumerFactory.config()[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG])
        assertEquals("ckc-demo", orderConsumerFactory.config()[ConsumerConfig.GROUP_ID_CONFIG])
        assertEquals("ckc-demo", batchConsumerFactory.config()[ConsumerConfig.GROUP_ID_CONFIG])
        assertEquals("ckc-demo", telemetryConsumerFactory.config()[ConsumerConfig.GROUP_ID_CONFIG])
        assertEquals(1_234, telemetryConsumerFactory.config()[ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG])
        assertThreadPoolAdmissionRecovery(orderContainerFactory)
        assertThreadPoolAdmissionRecovery(batchContainerFactory)
        assertThreadPoolAdmissionRecovery(telemetryContainerFactory)
    }

    private fun ConsumerFactory<*, *>.config(): Map<String, Any> =
        (this as DefaultKafkaConsumerFactory<*, *>).configurationProperties

    private fun assertThreadPoolAdmissionRecovery(factory: Any) {
        val containerFactory = factory as org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory<*, *>
        val errorHandler = ReflectionTestUtils.getField(containerFactory, "commonErrorHandler")

        assertIs<DefaultErrorHandler>(errorHandler)
        assertEquals(true, errorHandler.isAckAfterHandle)
        assertEquals(true, containerFactory.containerProperties.isStopImmediate)
        assertEquals(5_000L, containerFactory.containerProperties.shutdownTimeout)
    }
}
