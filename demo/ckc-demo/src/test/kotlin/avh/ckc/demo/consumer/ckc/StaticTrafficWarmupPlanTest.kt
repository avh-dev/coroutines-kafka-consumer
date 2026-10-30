package avh.ckc.demo.consumer.ckc

import avh.ckc.core.ProcessingMode
import avh.ckc.demo.DemoTopics
import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.serialization.OrderLifecycleEventSerializer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class StaticTrafficWarmupPlanTest {
    @Test
    fun `disabled plan keeps all consumers on their main topics`() {
        val plan = StaticTrafficWarmupPlan.from(DemoApplicationProperties())

        assertMain(plan.order, DemoTopics.ORDER_EVENTS)
        assertMain(plan.batch, DemoTopics.BATCH_EVENTS)
        assertMain(plan.telemetry, DemoTopics.CAULDRON_EVENTS)
    }

    @Test
    fun `donor plan adds typed forwarding only to enabled unordered participants`() {
        val properties = warmupProperties(DemoApplicationProperties.TrafficWarmupRole.DONOR)
        properties.kafka.bootstrapServers = "main:9092"
        properties.trafficWarmup.kafkaBootstrapServers = "warmup:9093"
        properties.trafficWarmup.batch.enabled = false

        val plan = StaticTrafficWarmupPlan.from(properties)

        val forwarding = plan.order.route.forwardingConfiguration!!
        assertEquals("order_events", forwarding.consumerId.value)
        assertEquals("static-session", forwarding.sessionId.value)
        assertEquals(DemoTopics.ORDER_EVENTS, forwarding.assignments.single().sourceTopic)
        assertEquals(DemoTopics.TRAFFIC_WARMUP_ORDER_EVENTS, forwarding.assignments.single().warmupTopic)
        assertEquals("warmup:9093", forwarding.producerProperties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG])
        assertEquals(
            OrderLifecycleEventSerializer::class.java,
            forwarding.producerProperties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG]
        )
        assertFalse(plan.order.recipient)
        assertMain(plan.batch, DemoTopics.BATCH_EVENTS)
        assertNull(plan.batch.route.forwardingConfiguration)
    }

    @Test
    fun `recipient plan explicitly assigns each enabled warm-up partition without forwarding`() {
        val properties = warmupProperties(DemoApplicationProperties.TrafficWarmupRole.RECIPIENT)
        properties.trafficWarmup.order.partition = 3
        properties.trafficWarmup.batch.enabled = false

        val plan = StaticTrafficWarmupPlan.from(properties)

        assertEquals(
            TopicPartition(DemoTopics.TRAFFIC_WARMUP_ORDER_EVENTS, 3),
            plan.order.route.assignedPartition
        )
        assertNull(plan.order.route.mainTopic)
        assertNull(plan.order.route.forwardingConfiguration)
        assertTrue(plan.order.recipient)
        assertMain(plan.batch, DemoTopics.BATCH_EVENTS)
        assertFalse(plan.batch.recipient)
    }

    @Test
    fun `enabled participant must use unordered processing`() {
        val properties = warmupProperties(DemoApplicationProperties.TrafficWarmupRole.DONOR)
        properties.trafficWarmup.telemetry.enabled = true
        properties.consumers.telemetry.processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST

        val error = assertFailsWith<IllegalArgumentException> {
            StaticTrafficWarmupPlan.from(properties)
        }

        assertTrue(error.message.orEmpty().contains("cauldron_events must use AT_LEAST_ONCE_NO_ORDERING"))
    }

    @Test
    fun `enabled participants cannot share one warm-up partition`() {
        val properties = warmupProperties(DemoApplicationProperties.TrafficWarmupRole.RECIPIENT)
        properties.trafficWarmup.batch.topic = properties.trafficWarmup.order.topic

        val error = assertFailsWith<IllegalArgumentException> {
            StaticTrafficWarmupPlan.from(properties)
        }

        assertTrue(error.message.orEmpty().contains("Traffic warm-up partitions must be exclusive"))
    }

    @Test
    fun `custom warm-up topic is rejected before startup when audit is enabled`() {
        val properties = warmupProperties(DemoApplicationProperties.TrafficWarmupRole.DONOR)
        properties.trafficWarmup.order.topic = "custom.warmup.orders"

        val error = assertFailsWith<IllegalArgumentException> {
            StaticTrafficWarmupPlan.from(properties)
        }

        assertEquals(
            "No audit topic ID is configured for traffic warm-up topic 'custom.warmup.orders'",
            error.message
        )
    }

    @Test
    fun `custom warm-up topic remains available when audit is disabled`() {
        val properties = warmupProperties(DemoApplicationProperties.TrafficWarmupRole.DONOR)
        properties.audit.enabled = false
        properties.trafficWarmup.order.topic = "custom.warmup.orders"

        val plan = StaticTrafficWarmupPlan.from(properties)

        assertEquals(
            "custom.warmup.orders",
            plan.order.route.forwardingConfiguration!!.assignments.single().warmupTopic
        )
    }

    @Test
    fun `audit separator is rejected in session and consumer identifiers`() {
        val sessionProperties = warmupProperties(DemoApplicationProperties.TrafficWarmupRole.DONOR)
        sessionProperties.trafficWarmup.sessionId = "run|1"
        val sessionError = assertFailsWith<IllegalArgumentException> {
            StaticTrafficWarmupPlan.from(sessionProperties)
        }
        assertEquals("demo.traffic-warmup.session-id must not contain '|'", sessionError.message)

        val consumerProperties = warmupProperties(DemoApplicationProperties.TrafficWarmupRole.DONOR)
        consumerProperties.trafficWarmup.order.consumerId = "orders|primary"
        val consumerError = assertFailsWith<IllegalArgumentException> {
            StaticTrafficWarmupPlan.from(consumerProperties)
        }
        assertEquals("Traffic warm-up consumer ID must not contain '|'", consumerError.message)
    }

    private fun warmupProperties(
        role: DemoApplicationProperties.TrafficWarmupRole
    ): DemoApplicationProperties = DemoApplicationProperties().also {
        it.trafficWarmup.role = role
        it.trafficWarmup.sessionId = "static-session"
        it.consumers.order.processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
        it.consumers.batch.processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
    }

    private fun assertMain(participant: StaticTrafficWarmupParticipantPlan, topic: String) {
        assertEquals(topic, participant.route.mainTopic)
        assertNull(participant.route.assignedPartition)
        assertFalse(participant.recipient)
    }
}
