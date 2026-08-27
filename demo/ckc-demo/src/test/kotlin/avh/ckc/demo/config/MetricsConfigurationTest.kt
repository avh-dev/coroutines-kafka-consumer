package avh.ckc.demo.config

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.springframework.mock.env.MockEnvironment
import kotlin.test.Test
import kotlin.test.assertNotNull

class MetricsConfigurationTest {
    @Test
    fun `experiment target name replaces generated timeline profile`() {
        val registry = SimpleMeterRegistry()
        val environment = MockEnvironment().apply { setActiveProfiles("spring-kafka-thread-pool") }
        val properties = DemoApplicationProperties(experimentTargetName = "spring+thread-pool.many-consumers.linger50")

        MetricsConfiguration().consumerProfileInfoMetric(registry, environment, properties)

        assertNotNull(
            registry.find("ckc.demo.consumer.profile.info")
                .tag("profile", "spring+thread-pool.many-consumers.linger50")
                .tag("spring_profile", "spring-kafka-thread-pool")
                .gauge()
        )
    }
}
