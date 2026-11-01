package avh.ckc.demo.consumer.ckc

import avh.ckc.core.ProcessingMode
import avh.ckc.demo.DemoTopics
import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.serialization.OrderLifecycleEventSerializer
import org.apache.kafka.clients.producer.ProducerConfig
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

class AutomaticTrafficWarmupPlanTest {
    @Test
    fun `automatic mode creates independent participants sharing one context`() {
        val properties = automaticProperties().also {
            it.kafka.bootstrapServers = "main:9092"
            it.trafficWarmup.kafkaBootstrapServers = "warmup:9093"
            it.trafficWarmup.initialFraction = 0.02
            it.trafficWarmup.targetFraction = 0.25
            it.trafficWarmup.rampUpSeconds = 45
            it.trafficWarmup.holdSeconds = 15
        }

        val plan = requireNotNull(AutomaticTrafficWarmupPlan.from(properties))
        val order = requireNotNull(plan.order)
        val batch = requireNotNull(plan.batch)

        assertSame(order.context, batch.context)
        assertNull(plan.telemetry)
        assertEquals("order_events", order.consumerId)
        assertEquals(DemoTopics.ORDER_EVENTS, order.sourceTopic)
        assertEquals(DemoTopics.TRAFFIC_WARMUP_ORDER_EVENTS, order.warmupTopic)
        assertEquals("warmup:9093", order.warmupConnectionProperties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG])
        assertEquals(
            OrderLifecycleEventSerializer::class.java,
            order.forwardingProducerProperties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG]
        )
        assertEquals(0.02, order.forwardingProfile.initialFraction)
        assertEquals(0.25, order.forwardingProfile.targetFraction)
        assertEquals(45.seconds, order.forwardingProfile.rampUpDuration)
        assertEquals(15.seconds, order.forwardingProfile.holdDuration)
    }

    @Test
    fun `non automatic modes do not create a managed plan`() {
        DemoApplicationProperties.TrafficWarmupRole.entries
            .filterNot { it == DemoApplicationProperties.TrafficWarmupRole.AUTOMATIC }
            .forEach { role ->
                val properties = DemoApplicationProperties().also { it.trafficWarmup.role = role }
                assertNull(AutomaticTrafficWarmupPlan.from(properties), "role=$role")
            }
    }

    @Test
    fun `enabled participant must use unordered processing`() {
        val properties = automaticProperties().also {
            it.trafficWarmup.telemetry.enabled = true
            it.consumers.telemetry.processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST
        }

        val error = assertFailsWith<IllegalArgumentException> {
            AutomaticTrafficWarmupPlan.from(properties)
        }

        assertTrue(error.message.orEmpty().contains("cauldron_events must use AT_LEAST_ONCE_NO_ORDERING"))
    }

    @Test
    fun `custom topic requires known audit mapping only when audit is enabled`() {
        val properties = automaticProperties().also {
            it.trafficWarmup.order.topic = "custom.warmup.orders"
        }

        val error = assertFailsWith<IllegalArgumentException> {
            AutomaticTrafficWarmupPlan.from(properties)
        }
        assertEquals(
            "No audit topic ID is configured for traffic warm-up topic 'custom.warmup.orders'",
            error.message
        )

        properties.audit.enabled = false
        val plan = requireNotNull(AutomaticTrafficWarmupPlan.from(properties))
        assertEquals("custom.warmup.orders", plan.order?.warmupTopic)
    }

    private fun automaticProperties() = DemoApplicationProperties().also {
        it.trafficWarmup.role = DemoApplicationProperties.TrafficWarmupRole.AUTOMATIC
        it.consumers.order.processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
        it.consumers.batch.processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
    }
}
