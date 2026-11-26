package avh.ckc.spring

import avh.ckc.warmup.TrafficWarmupLifecyclePhase
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.assertj.AssertableApplicationContext
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

@Testcontainers(disabledWithoutDocker = true)
class CkcWarmupKafkaTest {
    @Test
    fun `shared context warms both consumers and Spring publishes actual forwarding coordinates`() {
        val topics = createTopics()
        val donor = WarmupTestEvents()
        val recipient = WarmupTestEvents()
        kafkaRunner(topics, donor, "donor")
            .withPropertyValues("ckc.metrics.implementation=CUSTOM")
            .run { donorContext ->
                assertColdStartup(donorContext, donor)
                kafkaRunner(topics, recipient, "recipient")
                    .withPropertyValues("ckc.consumers.batches.auto-startup=false")
                    .run { recipientContext ->
                        val registry = recipientContext.getBean(CkcConsumerRegistry::class.java)
                        assertThat(registry.isRunning("orders")).isTrue()
                        assertThat(registry.isRunning("batches")).isFalse()
                        assertThat(warmupState(recipientContext, "orders")["mainReady"]).isEqualTo(false)
                        registry.start("batches")
                        await { recipient.lifecycle.any { it.event.phase == TrafficWarmupLifecyclePhase.WARMING } }
                        assertThat(warmupState(recipientContext, "orders")["phase"]).isEqualTo("WARMING")
                        assertThat(warmupState(recipientContext, "batches")["phase"]).isEqualTo("WARMING")

                        val destinations = mapOf(topics.warmupOrders to topics.orders, topics.warmupBatches to topics.batches)
                        val sourceOffsets = mutableMapOf<Pair<String, String>, Long>()
                        withProducer { producer ->
                            runBlocking {
                                withTimeout(8_000) {
                                    var batch = 0
                                    while (!recipient.processed.map { it.topic() }.containsAll(destinations.keys)) {
                                        destinations.values.forEach { source ->
                                            val value = "delegated-$batch"
                                            sourceOffsets[source to value] = produce(producer, source, value)
                                        }
                                        batch++
                                        delay(150)
                                    }
                                }
                            }
                        }

                        assertMainReady(recipientContext, recipient, "orders", "batches")
                        val finished = recipient.lifecycle.single {
                            it.event.phase == TrafficWarmupLifecyclePhase.RECIPIENT_FINISHED
                        }
                        assertThat(finished.contextName).isEqualTo("shared")
                        assertThat(finished.event.reasonCode).isNull()
                        assertThat(finished.event.processedRecords).isGreaterThanOrEqualTo(2)
                        assertThat(recipient.lifecycle.map { it.event.phase })
                            .doesNotContain(TrafficWarmupLifecyclePhase.COLD_FALLBACK)
                        destinations.forEach { (destination, source) ->
                            val processed = recipient.processed.first { it.topic() == destination }
                            val forwarded = donor.forwarded.single {
                                it.event.destinationTopic == destination && it.event.destinationOffset == processed.offset()
                            }
                            assertThat(forwarded.contextName).isEqualTo("shared")
                            assertThat(forwarded.consumerName)
                                .isEqualTo(if (source == topics.orders) "orders" else "batches")
                            assertThat(forwarded.event.sessionId).isEqualTo(finished.event.sessionId)
                            assertThat(forwarded.event.sourceTopic).isEqualTo(source)
                            assertThat(forwarded.event.sourcePartition).isZero()
                            assertThat(forwarded.event.sourceOffset).isEqualTo(sourceOffsets[source to processed.value()])
                            assertThat(forwarded.event.destinationPartition).isEqualTo(processed.partition())

                            val meters = donorContext.getBean(SimpleMeterRegistry::class.java)
                            assertThat(meters.get("custom.ckc.warmup.delegated")
                                .tags("consumer_id", forwarded.consumerName, "topic", source).counter().count())
                                .isGreaterThanOrEqualTo(1.0)
                        }
                    }
            }
    }

    @Test
    fun `late manual start joins the main group after the shared participant deadline`() {
        val topics = createTopics()
        val events = WarmupTestEvents()
        kafkaRunner(topics, events, "late")
            .withPropertyValues(
                "ckc.consumers.batches.auto-startup=false",
                "ckc.warmup.contexts.shared.participant-start-timeout=250ms"
            )
            .run { context ->
                assertMainReady(context, events, "orders")
                assertThat(warmupState(context, "batches")["phase"]).isEqualTo("REGISTERED")
                assertThat(events.lifecycle.map { it.event.reasonCode }).contains("participant_start_timeout")
                context.getBean(CkcConsumerRegistry::class.java).start("batches")
                assertMainReady(context, events, "orders", "batches")
                assertThat(events.lifecycle.map { it.event.phase }).doesNotContain(TrafficWarmupLifecyclePhase.WARMING)
            }
    }

    @Test
    fun `stopping a participant during warming releases it and lets the other start cold`() {
        val topics = createTopics()
        val donor = WarmupTestEvents()
        val recipient = WarmupTestEvents()
        val exited = AtomicInteger()
        kafkaRunner(topics, donor, "donor").run { donorContext ->
            assertColdStartup(donorContext, donor)
            kafkaRunner(topics, recipient, "stopping")
                .withPropertyValues("ckc.warmup.contexts.shared.warmup-duration=30s")
                .withBean(CkcTerminalFailureExitHandler::class.java, { CkcTerminalFailureExitHandler { exited.incrementAndGet() } })
                .withBean(CkcTerminalFailureHaltHandler::class.java, { CkcTerminalFailureHaltHandler { exited.incrementAndGet() } })
                .run { context ->
                    val registry = context.getBean(CkcConsumerRegistry::class.java)
                    await { recipient.lifecycle.any { it.event.phase == TrafficWarmupLifecyclePhase.WARMING } }
                    registry.stop("orders")

                    assertThat(registry.isRunning("orders")).isFalse()
                    assertThat(warmupState(context, "orders"))
                        .isEqualTo(mapOf("context" to "shared", "phase" to "PARTICIPANT_STOPPED", "mainReady" to false))
                    assertThat(recipient.lifecycle.none {
                        it.event.participantId == "orders" && it.event.phase == TrafficWarmupLifecyclePhase.MAIN_READY
                    }).isTrue()
                    assertThat(recipient.lifecycle.map { it.event.reasonCode }).contains("participant_stopped")
                    assertMainReady(context, recipient, "batches")
                    assertThat(exited.get()).isZero()
                }
        }
    }

    @Test
    fun `disabled warmup consumes main records without looking up the warmup connection`() {
        val topics = createTopics()
        val events = WarmupTestEvents()
        kafkaRunner(topics, events, "disabled")
            .withPropertyValues("ckc.warmup.enabled=false", "ckc.warmup.contexts.shared.control-cluster=unknown")
            .run { context ->
                assertThat(context).hasNotFailed()
                withProducer { producer -> produce(producer, topics.orders, "main") }
                await { events.processed.any { it.topic() == topics.orders && it.value() == "main" } }
                assertThat(context.consumerDetails("orders")).doesNotContainKey("warmup")
                assertThat(events.lifecycle).isEmpty()
                assertThat(events.forwarded).isEmpty()
            }
    }

    private fun assertColdStartup(context: AssertableApplicationContext, events: WarmupTestEvents) {
        assertMainReady(context, events, "orders", "batches")
        assertThat(events.lifecycle.map { it.event.phase }).contains(TrafficWarmupLifecyclePhase.COLD_FALLBACK)
        assertThat(events.lifecycle.map { it.event.reasonCode }).contains("incomplete_donor_coverage")
    }

    private fun assertMainReady(
        context: AssertableApplicationContext,
        events: WarmupTestEvents,
        vararg participants: String
    ) {
        assertThat(context).hasNotFailed()
        await {
            val ready = events.lifecycle.filter { it.event.phase == TrafficWarmupLifecyclePhase.MAIN_READY }
            participants.all { name -> ready.any { it.contextName == "shared" && it.event.participantId == name } }
        }
        participants.forEach {
            assertThat(warmupState(context, it))
                .isEqualTo(mapOf("context" to "shared", "phase" to "MAIN_READY", "mainReady" to true))
        }
    }

    private fun warmupState(context: AssertableApplicationContext, name: String): Map<*, *> =
        context.consumerDetails(name)["warmup"] as Map<*, *>

    private fun kafkaRunner(
        topics: WarmupTestTopics,
        events: WarmupTestEvents,
        instance: String
    ): ApplicationContextRunner =
        warmupTestRunner(events)
            .withPropertyValues(*topics.properties(true))
            .withPropertyValues(
                "ckc.clusters.main.kafka-properties[bootstrap.servers]=${main.bootstrapServers}",
                "ckc.clusters.control.kafka-properties[bootstrap.servers]=${control.bootstrapServers}",
                "ckc.clusters.destination.kafka-properties[bootstrap.servers]=${destination.bootstrapServers}",
                "ckc.warmup.contexts.shared.instance-id=$instance"
            )

    private fun createTopics(): WarmupTestTopics {
        val topics = WarmupTestTopics("starter-${UUID.randomUUID()}")
        createTopics(main, topics.orders, topics.batches)
        createTopics(control, topics.control)
        createTopics(destination, topics.warmupOrders, topics.warmupBatches)
        return topics
    }

    private fun createTopics(kafka: KafkaContainer, vararg topics: String) {
        Admin.create(mapOf("bootstrap.servers" to kafka.bootstrapServers)).use { admin ->
            admin.createTopics(topics.map { NewTopic(it, 1, 1.toShort()) }).all().get(10, TimeUnit.SECONDS)
        }
    }

    private fun <T> withProducer(block: (KafkaProducer<String, String>) -> T): T =
        KafkaProducer(
            mapOf(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to main.bootstrapServers),
            StringSerializer(),
            StringSerializer()
        ).use(block)

    private fun produce(producer: KafkaProducer<String, String>, topic: String, value: String): Long =
        producer.send(ProducerRecord(topic, "key", value)).get(5, TimeUnit.SECONDS).offset()

    private fun await(condition: () -> Boolean) = runBlocking {
        withTimeout(30_000) {
            while (!condition()) delay(25)
        }
    }

    companion object {
        private val image = DockerImageName.parse("apache/kafka-native:3.8.0")

        @Container
        @JvmStatic
        val main = KafkaContainer(image)

        @Container
        @JvmStatic
        val control = KafkaContainer(image)

        @Container
        @JvmStatic
        val destination = KafkaContainer(image)
    }
}
