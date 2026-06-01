package avh.ckc.demo

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.micrometer.MicrometerConsumerMetrics
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.test.context.ActiveProfiles
import kotlin.test.assertFalse
import kotlin.test.assertSame

@SpringBootTest(
    properties = [
        "demo.kafka.enabled=false",
        "demo.consumers.metrics-implementation=NOOP"
    ]
)
@ActiveProfiles("ckc")
class CkcNoopMetricsProfileContextTest(
    @Autowired private val applicationContext: ApplicationContext,
    @Autowired
    @Qualifier("consumerMetrics")
    private val consumerMetrics: ConsumerMetrics<String, CauldronTelemetryEvent>
) {
    @Test
    fun `noop metrics implementation does not create Micrometer adapter`() {
        assertFalse(applicationContext.getBeansOfType(MicrometerConsumerMetrics::class.java).isNotEmpty())
        assertSame<Any>(ConsumerMetrics.NOOP, consumerMetrics)
    }
}
