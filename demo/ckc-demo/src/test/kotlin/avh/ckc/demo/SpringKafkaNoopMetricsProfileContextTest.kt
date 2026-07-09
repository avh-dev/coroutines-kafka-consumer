package avh.ckc.demo

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.micrometer.MicrometerConsumerMetricsSchema
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
        "SERVER_PORT=0",
        "demo.consumers.metrics-implementation=NOOP",
        "spring.autoconfigure.exclude=com.linecorp.armeria.spring.ArmeriaAutoConfiguration," +
                "com.linecorp.armeria.spring.actuate.ArmeriaSpringActuatorAutoConfiguration"
    ]
)
@ActiveProfiles("spring-kafka")
class SpringKafkaNoopMetricsProfileContextTest(
    @Autowired private val applicationContext: ApplicationContext,
    @Autowired
    @Qualifier("springKafkaConsumerMetrics")
    private val consumerMetrics: ConsumerMetrics<String, CauldronTelemetryEvent>
) {
    @Test
    fun `noop metrics implementation does not create Micrometer schema`() {
        assertFalse(applicationContext.getBeansOfType(MicrometerConsumerMetricsSchema::class.java).isNotEmpty())
        assertSame<Any>(ConsumerMetrics.NOOP, consumerMetrics)
    }
}
