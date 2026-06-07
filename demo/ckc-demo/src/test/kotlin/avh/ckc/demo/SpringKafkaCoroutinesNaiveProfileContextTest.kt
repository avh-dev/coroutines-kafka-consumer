package avh.ckc.demo

import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.consumer.springkafkacoroutinesnaive.SpringKafkaCoroutinesNaiveProfileConfiguration
import avh.ckc.demo.ml.eta.ArmeriaSuspendArcaneEtaModelClient
import avh.ckc.demo.ml.eta.SuspendArcaneEtaModelClient
import avh.ckc.demo.ml.eta.SyncArcaneEtaModelClient
import avh.ckc.demo.ml.flavour.ArmeriaSuspendOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.SuspendOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.SyncOrderFlavourModelClient
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.test.context.ActiveProfiles
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@SpringBootTest(
    properties = [
        "demo.kafka.enabled=false",
        "SERVER_PORT=0"
    ]
)
@ActiveProfiles("spring-kafka-coroutines-naive")
class SpringKafkaCoroutinesNaiveProfileContextTest(
    @Autowired private val applicationContext: ApplicationContext,
    @Autowired private val meterRegistry: MeterRegistry,
    @Autowired
    @Qualifier("springKafkaCoroutinesNaiveWorkerDispatcher")
    private val workerDispatcher: ExecutorCoroutineDispatcher
) {
    @Test
    fun contextLoads() {
    }

    @Test
    fun `naive profile creates only suspend model clients`() {
        assertIs<ArmeriaSuspendArcaneEtaModelClient>(
            applicationContext.getBean(SuspendArcaneEtaModelClient::class.java)
        )
        assertIs<ArmeriaSuspendOrderFlavourModelClient>(
            applicationContext.getBean(SuspendOrderFlavourModelClient::class.java)
        )
        assertFalse(applicationContext.getBeansOfType(SyncArcaneEtaModelClient::class.java).isNotEmpty())
        assertFalse(applicationContext.getBeansOfType(SyncOrderFlavourModelClient::class.java).isNotEmpty())
    }

    @Test
    fun `consumer profile info metric identifies naive spring kafka coroutine implementation`() {
        val gauge = meterRegistry.find("ckc.demo.consumer.profile.info")
            .tag("consumer_impl", "spring_kafka_coroutines_naive")
            .tag("spring_profile", "spring-kafka-coroutines-naive")
            .gauge()

        assertNotNull(gauge)
        assertEquals(1.0, gauge.value())
    }

    @Test
    fun `naive profile creates shared named worker dispatcher`() {
        val threadName = runBlocking {
            withContext(workerDispatcher) {
                Thread.currentThread().name
            }
        }

        assertTrue(threadName.startsWith("spring-kafka-coroutines-naive-worker-"))
    }

    @Test
    fun `naive listener factories use batch listener admission and container commits`() {
        val properties = DemoApplicationProperties().apply {
            kafka.consumer.commitIntervalMs = 1_234
        }
        val configuration = SpringKafkaCoroutinesNaiveProfileConfiguration()

        val orderConsumerFactory = configuration.springKafkaCoroutinesNaiveOrderConsumerFactory(properties)
        val orderContainerFactory = configuration.springKafkaCoroutinesNaiveOrderListenerContainerFactory(
            orderConsumerFactory,
            properties
        )
        val batchConsumerFactory = configuration.springKafkaCoroutinesNaiveBatchConsumerFactory(properties)
        val batchContainerFactory = configuration.springKafkaCoroutinesNaiveBatchListenerContainerFactory(
            batchConsumerFactory,
            properties
        )
        val telemetryConsumerFactory = configuration.springKafkaCoroutinesNaiveTelemetryConsumerFactory(properties)
        val telemetryContainerFactory = configuration.springKafkaCoroutinesNaiveTelemetryListenerContainerFactory(
            telemetryConsumerFactory,
            properties
        )

        assertEquals(true, orderContainerFactory.isBatchListener)
        assertEquals(true, batchContainerFactory.isBatchListener)
        assertEquals(true, telemetryContainerFactory.isBatchListener)
        assertEquals(false, orderConsumerFactory.config()[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG])
        assertEquals(false, batchConsumerFactory.config()[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG])
        assertEquals(false, telemetryConsumerFactory.config()[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG])
        assertEquals(1_234, telemetryConsumerFactory.config()[ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG])
    }

    private fun ConsumerFactory<*, *>.config(): Map<String, Any> =
        (this as DefaultKafkaConsumerFactory<*, *>).configurationProperties
}
