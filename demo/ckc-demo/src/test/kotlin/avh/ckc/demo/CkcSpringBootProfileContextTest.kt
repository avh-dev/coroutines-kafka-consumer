package avh.ckc.demo

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.demo.ml.eta.ArmeriaSuspendArcaneEtaModelClient
import avh.ckc.demo.ml.eta.SuspendArcaneEtaModelClient
import avh.ckc.demo.ml.eta.SyncArcaneEtaModelClient
import avh.ckc.demo.ml.flavour.ArmeriaSuspendOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.SuspendOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.SyncOrderFlavourModelClient
import avh.ckc.spring.CkcConsumerProperties
import avh.ckc.spring.CkcConsumerRegistry
import com.linecorp.armeria.client.WebClient
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.test.context.ActiveProfiles
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@SpringBootTest(
    properties = [
        "demo.kafka.enabled=false",
        "SERVER_PORT=0",
        "KAFKA_ENABLED=false",
        "spring.autoconfigure.exclude=com.linecorp.armeria.spring.ArmeriaAutoConfiguration," +
                "com.linecorp.armeria.spring.actuate.ArmeriaSpringActuatorAutoConfiguration"
    ]
)
@ActiveProfiles("ckc-spring-boot")
class CkcSpringBootProfileContextTest(
    @Autowired private val applicationContext: ApplicationContext,
    @Autowired private val meterRegistry: MeterRegistry,
    @Autowired private val consumerRegistry: CkcConsumerRegistry
) {
    @Test
    fun contextLoads() {
    }

    @Test
    fun `ckc spring boot profile registers configured starter consumers`() {
        assertEquals(
            setOf("order-events", "batch-events", "cauldron-events"),
            consumerRegistry.consumerNames
        )
        consumerRegistry.consumerNames.forEach { consumerName ->
            assertFalse(consumerRegistry.isRunning(consumerName))
        }
    }

    @Test
    fun `ckc spring boot profile uses suspend model clients`() {
        assertTrue(applicationContext.getBeansOfType(WebClient::class.java).isNotEmpty())
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
    fun `ckc spring boot profile does not create old ckc worker dispatcher`() {
        assertTrue(applicationContext.getBeansOfType(ExecutorCoroutineDispatcher::class.java).isEmpty())
    }

    @Test
    fun `consumer profile info metric identifies ckc spring boot profile`() {
        val gauge = meterRegistry.find("ckc.demo.consumer.profile.info")
            .tag("consumer_impl", "ckc")
            .tag("spring_profile", "ckc-spring-boot")
            .gauge()

        assertNotNull(gauge)
        assertEquals(1.0, gauge.value())
    }

    @Test
    fun `ckc spring boot profile uses custom metrics for audit wrapping`() {
        val ckcProperties = applicationContext.getBean(CkcConsumerProperties::class.java)

        assertEquals(CkcConsumerProperties.MetricsImplementation.CUSTOM, ckcProperties.metrics.implementation)
        assertEquals(
            setOf("orderConsumerMetrics", "batchConsumerMetrics", "cauldronConsumerMetrics"),
            applicationContext.getBeansOfType(ConsumerMetrics::class.java).keys
        )
    }
}
