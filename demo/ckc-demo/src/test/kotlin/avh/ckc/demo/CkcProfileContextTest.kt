package avh.ckc.demo

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.demo.ml.eta.ArmeriaSuspendArcaneEtaModelClient
import avh.ckc.demo.ml.eta.SuspendArcaneEtaModelClient
import avh.ckc.demo.ml.eta.SyncArcaneEtaModelClient
import avh.ckc.demo.ml.flavour.ArmeriaSuspendOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.SuspendOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.SyncOrderFlavourModelClient
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.api.OrderHttpService
import com.linecorp.armeria.client.WebClient
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.test.context.ActiveProfiles
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@SpringBootTest(
    properties = [
        "demo.kafka.enabled=false",
        "SERVER_PORT=0"
    ]
)
@ActiveProfiles("ckc")
class CkcProfileContextTest(
    @Autowired private val applicationContext: ApplicationContext,
    @Autowired private val meterRegistry: MeterRegistry,
    @Autowired
    @Qualifier("consumerMetrics")
    private val consumerMetrics: ConsumerMetrics<String, CauldronTelemetryEvent>,
    @Autowired
    @Qualifier("ckcWorkerDispatcher")
    private val workerDispatcher: ExecutorCoroutineDispatcher
) {
    @Test
    fun contextLoads() {
    }

    @Test
    fun `ckc profile creates only suspend model clients`() {
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
    fun `consumer profile info metric identifies ckc implementation`() {
        val gauge = meterRegistry.find("ckc.demo.consumer.profile.info")
            .tag("consumer_impl", "ckc")
            .tag("spring_profile", "ckc")
            .gauge()

        assertNotNull(gauge)
        assertEquals(1.0, gauge.value())
    }

    @Test
    fun `ckc profile creates shared named worker dispatcher`() {
        assertEquals(1, applicationContext.getBeansOfType(ExecutorCoroutineDispatcher::class.java).size)
        val threadName = runBlocking {
            withContext(workerDispatcher) {
                Thread.currentThread().name
            }
        }

        assertTrue(threadName.startsWith("ckc-worker-"))
    }

    @Test
    fun `record metrics do not include implementation tag`() {
        consumerMetrics.onRecordProcessed(
            key = "key",
            value = sampleTelemetryEvent(),
            record = ConsumerRecord("cauldron.events.v1", 0, 0L, "key", sampleTelemetryEvent()),
            recordAgeMillis = 10,
            durationNanos = 1_000_000
        )

        val counter = meterRegistry.find("ckc.record.processed")
            .tag("consumer_id", "cauldron_events")
            .tag("topic", "cauldron.events.v1")
            .tag("event_type", "CAULDRON_TELEMETRY")
            .counter()
        val age = meterRegistry.find("ckc.record.age")
            .tag("consumer_id", "cauldron_events")
            .tag("topic", "cauldron.events.v1")
            .tag("event_type", "CAULDRON_TELEMETRY")
            .tag("error", "none")
            .summary()

        assertNotNull(counter)
        assertNotNull(age)
        assertNull(
            meterRegistry.find("ckc.record.processed")
                .tag("consumer_impl", "ckc")
                .counter()
        )
    }

    @Test
    fun `consumer profile does not create order API service`() {
        assertTrue(applicationContext.getBeansOfType(OrderHttpService::class.java).isEmpty())
    }
}
