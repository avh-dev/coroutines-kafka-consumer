package avh.ckc.demo.consumer

import avh.ckc.demo.config.DemoApplicationProperties
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class KafkaConsumerClientMetricsTest {
    @Test
    fun `spring consumer factory installs client metrics listener only when enabled`() {
        val registry = SimpleMeterRegistry()

        val enabledFactory = kafkaConsumerFactoryWithClientMetrics<String, String>(
            consumerProperties = emptyMap(),
            meterRegistry = registry,
            consumerId = "orders",
            enabled = true
        ) as DefaultKafkaConsumerFactory<String, String>
        val disabledFactory = kafkaConsumerFactoryWithClientMetrics<String, String>(
            consumerProperties = emptyMap(),
            meterRegistry = registry,
            consumerId = "orders",
            enabled = false
        ) as DefaultKafkaConsumerFactory<String, String>

        assertTrue(enabledFactory.listeners.isNotEmpty())
        assertTrue(disabledFactory.listeners.isEmpty())
    }

    @Test
    fun `noop metrics implementation disables kafka client metrics`() {
        val properties = DemoApplicationProperties()
        assertTrue(properties.kafkaClientMetricsEnabled)

        properties.consumers.metricsImplementation = DemoApplicationProperties.MetricsImplementation.NOOP

        assertFalse(properties.kafkaClientMetricsEnabled)
    }
}
