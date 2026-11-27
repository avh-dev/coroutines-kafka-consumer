package avh.ckc.warmup

import avh.ckc.core.ConsumerContainer
import avh.ckc.core.consumerContainer
import avh.ckc.core.coroutinesKafkaConsumer
import avh.ckc.warmup.broker.BrokerLifecycleEvent
import avh.ckc.warmup.broker.KafkaBrokerLifecycleLog
import avh.ckc.warmup.protocol.WarmupLivenessProbe
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.DescribeClusterResult
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
internal class WarmupSubscriptionValidationIntegrationTest : WarmupContextKafkaFixture() {
    @Test
    fun `same cluster rejects forwarding into the sender's own subscription`() = runBlocking {
        val orders = unique("orders")
        verifyColdRoutes(orders, orders)
    }

    @Test
    fun `cluster ID detects a neighboring main subscription without a mapping for that topic`() = runBlocking {
        val batches = unique("batches")
        val aliasConnection = mapOf("bootstrap.servers" to "${kafka.bootstrapServers},127.0.0.1:1")
        verifyColdRoutes(unique("orders"), batches, aliasConnection, neighborTopic = batches)
    }

    @Test
    fun `same topic name on a different cluster allows forwarding`() = runBlocking {
        KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"))
            .withEnv("CLUSTER_ID", Uuid.randomUuid().toString()).use { destinationKafka ->
                destinationKafka.start()
                val destination = mapOf("bootstrap.servers" to destinationKafka.bootstrapServers)
                val orders = unique("orders")
                val control = unique("control")
                val namespace = unique("separate-cluster")
                val group = unique("orders-group")
                createTopics(orders, control)
                createTopicsAt(destination, orders)

                val donorActive = CompletableDeferred<Unit>()
                val donorContext = faultInjectionContext(namespace, control, send = { message ->
                    send(message).also {
                        if (message is WarmupLivenessProbe && message.reply) donorActive.complete(Unit)
                    }
                })
                val donorHandled = CopyOnWriteArrayList<String>()
                val recipientHandled = CopyOnWriteArrayList<String>()
                val forwarded = CopyOnWriteArrayList<TrafficWarmupForwardedEvent>()
                val donor = stringConsumer(
                    donorContext, "orders", orders, orders, group, donorHandled,
                    warmupProperties = destination, forwarded = forwarded
                )
                val recipient = stringConsumer(
                    context(namespace, control, warmupDuration = 8.seconds),
                    "orders", orders, orders, group, recipientHandled, warmupProperties = destination
                )
                try {
                    donor.start()
                    withTimeout(15.seconds) { donor.awaitReady() }
                    recipient.start()
                    withTimeout(15.seconds) { donorActive.await() }

                    val record = produce(orders, "key", "value", StringSerializer())
                    awaitFor { recipientHandled == listOf("value") }
                    withTimeout(20.seconds) { recipient.awaitReady() }
                    assertEquals(listOf(record.offset()), forwarded.map { it.sourceOffset })
                    assertTrue(donorHandled.isEmpty())
                    assertEquals(1L, endOffset(kafkaConnection(), orders))
                    assertEquals(1L, endOffset(destination, orders))
                } finally {
                    consumerContainer(recipient, donor).stop()
                }
            }
    }

    @Test
    fun `potential overlap with unresolved cluster identity starts cold`() = runBlocking {
        val orders = unique("orders")
        verifyColdRoutes(
            orders, orders, mapOf("bootstrap.servers" to "127.0.0.1:1"),
            expectedReason = "warmup_cluster_identity_unavailable"
        )
    }

    @Test
    fun `stopping one participant during cluster lookup preserves another participant's donor services`() = runBlocking {
        KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"))
            .withEnv("CLUSTER_ID", Uuid.randomUuid().toString()).use { destinationKafka ->
                destinationKafka.start()
                val destination = mapOf("bootstrap.servers" to destinationKafka.bootstrapServers)
                val clusterId = Admin.create(destination).use { admin ->
                    admin.describeCluster().clusterId().get(5, TimeUnit.SECONDS)
                }
                val lookupRequested = CompletableDeferred<Unit>()
                val lookupResult = KafkaFutureImpl<String>()
                val delayedDescription = mock<DescribeClusterResult>()
                whenever(delayedDescription.clusterId()).thenAnswer {
                    lookupRequested.complete(Unit)
                    lookupResult
                }
                val delayedAdmin = mock<Admin>()
                whenever(delayedAdmin.describeCluster()).thenReturn(delayedDescription)

                val orders = unique("orders")
                val batches = unique("batches")
                val warmup = unique("warmup-batches")
                val control = unique("control")
                val lifecycle = unique("broker-lifecycle")
                val namespace = unique("stop-during-cluster-lookup")
                val group = unique("batches-group")
                createTopics(orders, batches, warmup, control, lifecycle)
                createTopicsAt(destination, orders)

                val donorActive = CompletableDeferred<Unit>()
                val donorContext = faultInjectionContext(
                    namespace, control,
                    configure = {
                        preparationTimeout = 10.seconds
                        brokerLifecycle {
                            topic = lifecycle
                            kafkaProperties = kafkaConnection()
                        }
                    },
                    subscriptionAdminFactory = { properties ->
                        if (properties["bootstrap.servers"] == destinationKafka.bootstrapServers) delayedAdmin
                        else Admin.create(properties)
                    },
                    send = { message ->
                        send(message).also {
                            if (message is WarmupLivenessProbe && message.reply) donorActive.complete(Unit)
                        }
                    }
                )
                val stopped = stringConsumer(
                    donorContext, "orders", orders, orders, unique("orders-group"), CopyOnWriteArrayList(),
                    warmupProperties = destination
                )
                val donorHandled = CopyOnWriteArrayList<String>()
                val forwarded = CopyOnWriteArrayList<TrafficWarmupForwardedEvent>()
                val donor = stringConsumer(
                    donorContext, "batches", batches, warmup, group, donorHandled, forwarded = forwarded
                )
                val recipientHandled = CopyOnWriteArrayList<String>()
                val recipient = stringConsumer(
                    context(namespace, control, warmupDuration = 8.seconds, brokerLifecycleTopic = lifecycle),
                    "batches", batches, warmup, group, recipientHandled
                )

                startBrokerWitness(kafka.bootstrapServers, lifecycle, unique("epoch")).use {
                    try {
                        stopped.start()
                        donor.start()
                        withTimeout(5.seconds) { lookupRequested.await() }
                        withTimeout(5.seconds) { stopped.stop() }
                        assertFalse(lookupResult.isDone)
                        lookupResult.complete(clusterId)
                        withTimeout(15.seconds) { donor.awaitReady() }
                        KafkaBrokerLifecycleLog(lifecycle, kafkaConnection(), 3_000).use { observer ->
                            observer.seek(0)
                            awaitLifecycle(observer) { it is BrokerLifecycleEvent.JoinConfirmed }
                        }

                        recipient.start()
                        withTimeout(15.seconds) { donorActive.await() }
                        val original = produce(batches, "key", "value", StringSerializer())
                        awaitFor { recipientHandled == listOf("value") }
                        withTimeout(20.seconds) { recipient.awaitReady() }

                        assertEquals(listOf(original.offset()), forwarded.map { it.sourceOffset })
                        assertTrue(donorHandled.isEmpty())
                    } finally {
                        lookupResult.complete(clusterId)
                        consumerContainer(recipient, donor, stopped).stop()
                    }
                }
            }
    }

    private suspend fun verifyColdRoutes(
        source: String,
        destination: String,
        destinationConnection: Map<String, Any?> = kafkaConnection(),
        neighborTopic: String? = null,
        expectedReason: String = "warmup_subscription_overlap"
    ) {
        val control = unique("control")
        val neighborSource = unique("neighbor-source")
        val neighborDestination = unique("neighbor-warmup")
        createTopics(*setOf(source, destination, control, neighborSource, neighborDestination).toTypedArray())
        val namespace = unique("conflicting-route")
        val orderGroup = unique("orders-group")
        val neighborGroup = unique("neighbor-group")
        val events = CopyOnWriteArrayList<TrafficWarmupLifecycleEvent>()
        val forwarded = CopyOnWriteArrayList<TrafficWarmupForwardedEvent>()
        val handled = CopyOnWriteArrayList<String>()
        val readyToPublish = CompletableDeferred<Unit>()
        val donorContext = faultInjectionContext(namespace, control, configure = {
            onLifecycleEvent(events::add)
        }, send = { message ->
            send(message).also {
                if (message is WarmupLivenessProbe && message.reply) readyToPublish.complete(Unit)
            }
        })
        val recipientContext = context(namespace, control, warmupDuration = 8.seconds, lifecycleObserver = { event ->
            events += event
            if (event.phase == TrafficWarmupLifecyclePhase.COLD_FALLBACK) readyToPublish.complete(Unit)
        })

        fun application(context: TrafficWarmupContext): ConsumerContainer {
            val orders = stringConsumer(
                context, "orders", source, destination, orderGroup, handled,
                warmupProperties = destinationConnection,
                forwarded = forwarded
            )
            if (neighborTopic == null) return consumerContainer(orders)

            val neighbor = coroutinesKafkaConsumer<String, String>(
                consumerProperties(neighborGroup, StringDeserializer::class.java)
            ) {
                topics(neighborSource, neighborTopic)
                handle {}
                trafficWarmup(context, "neighbor") {
                    mapTopic(neighborSource, neighborDestination)
                    warmupConsumerProperties = warmupConsumerConnection()
                    forwardingProducerProperties = forwardingConnection(StringSerializer::class.java)
                    onForwarded(forwarded::add)
                }
            }
            return consumerContainer(orders, neighbor)
        }

        val donor = application(donorContext)
        val recipient = application(recipientContext)
        try {
            donor.start()
            withTimeout(15.seconds) { donor.awaitReady() }
            recipient.start()
            withTimeout(15.seconds) { readyToPublish.await() }

            produce(source, "key", "value", StringSerializer())
            withTimeout(20.seconds) { recipient.awaitReady() }
            awaitFor { handled.isNotEmpty() }
            assertEquals(listOf("value"), handled.toList())
            assertEquals(
                listOf(expectedReason, expectedReason),
                events.filter { it.phase == TrafficWarmupLifecyclePhase.COLD_FALLBACK }.map { it.reasonCode }
            )
            assertTrue(events.none { it.phase == TrafficWarmupLifecyclePhase.DISCOVERING })
            assertTrue(forwarded.isEmpty())
            assertEquals(if (source == destination) 1L else 0L, endOffset(kafkaConnection(), destination))
        } finally {
            try {
                recipient.stop()
            } finally {
                donor.stop()
            }
        }
    }

    private fun endOffset(connection: Map<String, Any?>, topic: String): Long = Admin.create(connection).use { admin ->
        val partition = TopicPartition(topic, 0)
        admin.listOffsets(mapOf(partition to OffsetSpec.latest())).all().get(5, TimeUnit.SECONDS)
            .getValue(partition).offset()
    }
}
