package avh.ckc.demo

import avh.ckc.demo.ml.eta.SuspendArcaneEtaModelClient
import avh.ckc.demo.ml.eta.SyncArcaneEtaModelClient
import avh.ckc.demo.ml.flavour.SuspendOrderFlavourModelClient
import avh.ckc.demo.ml.flavour.SyncOrderFlavourModelClient
import avh.ckc.demo.service.DemoRecordAgeMetrics
import avh.ckc.demo.service.DemoRecordMetrics
import avh.ckc.micrometer.MicrometerConsumerMetrics
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
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
@ActiveProfiles("confluent-parallel-reactor")
class ConfluentParallelReactorProfileContextTest(
    @Autowired private val applicationContext: ApplicationContext,
    @Autowired private val meterRegistry: MeterRegistry,
    @Autowired
    @Qualifier("confluentParallelReactorWorkerDispatcher")
    private val workerDispatcher: ExecutorCoroutineDispatcher
) {
    @Test
    fun contextLoads() {
    }

    @Test
    fun `confluent parallel reactor profile creates only suspend model clients`() {
        assertTrue(applicationContext.getBeansOfType(SuspendArcaneEtaModelClient::class.java).isNotEmpty())
        assertTrue(applicationContext.getBeansOfType(SuspendOrderFlavourModelClient::class.java).isNotEmpty())
        assertFalse(applicationContext.getBeansOfType(SyncArcaneEtaModelClient::class.java).isNotEmpty())
        assertFalse(applicationContext.getBeansOfType(SyncOrderFlavourModelClient::class.java).isNotEmpty())
    }

    @Test
    fun `consumer profile info metric identifies confluent parallel reactor implementation`() {
        val gauge = meterRegistry.find("ckc.demo.consumer.profile.info")
            .tag("consumer_impl", "confluent_parallel")
            .tag("spring_profile", "confluent-parallel-reactor")
            .gauge()

        assertNotNull(gauge)
        assertEquals(1.0, gauge.value())
    }

    @Test
    fun `confluent parallel reactor profile publishes only CKC-style record age metrics`() {
        assertTrue(applicationContext.getBeansOfType(DemoRecordAgeMetrics::class.java).isNotEmpty())
        assertFalse(applicationContext.getBeansOfType(DemoRecordMetrics::class.java).isNotEmpty())
        assertFalse(applicationContext.getBeansOfType(MicrometerConsumerMetrics::class.java).isNotEmpty())
    }

    @Test
    fun `confluent parallel reactor profile creates shared named worker dispatcher`() {
        assertEquals(1, applicationContext.getBeansOfType(ExecutorCoroutineDispatcher::class.java).size)
        val threadName = runBlocking {
            withContext(workerDispatcher) {
                Thread.currentThread().name
            }
        }

        assertTrue(threadName.startsWith("pc-reactor-worker-"))
    }
}
