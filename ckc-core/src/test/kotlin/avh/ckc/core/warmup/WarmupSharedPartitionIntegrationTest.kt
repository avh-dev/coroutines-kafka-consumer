package avh.ckc.core.warmup

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.CoroutinesKafkaConsumerBuilder
import avh.ckc.core.ProcessingMode
import avh.ckc.core.RecordingMetrics
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class WarmupSharedPartitionIntegrationTest {
    private val openTopologies = CopyOnWriteArrayList<Topology>()

    @AfterEach
    fun closeOpenTopologies() = runBlocking {
        openTopologies.toList().asReversed().forEach { it.close() }
    }

    @Test
    fun `one donor warms one recipient and records one remote source outcome`() = runBlocking {
        val topology = topology(maxSessions = 2)
        val session = topology.startRecipient("recipient")
        topology.startDonor()

        topology.produceSource("one")
        topology.awaitSourceOffset(1)

        assertEquals(listOf("one"), session.handled.toList())
        assertTrue(topology.localHandled.isEmpty())
        assertEquals(1, topology.donorMetrics.warmupDelegated.size)
        assertEquals(1, topology.donorMetrics.warmupResults.size)
        assertEquals(1, topology.donorMetrics.processed.size)
        assertTrue(topology.donorMetrics.processed.single().processedRemotely)
        assertTrue(topology.donorMetrics.consumerFailures.isEmpty())
        assertTrue(session.metrics.consumerFailures.isEmpty())
        topology.close()
    }

    @Test
    fun `one donor warms two recipients and admits another while warmup is active`() = runBlocking {
        val topology = topology(maxSessions = 3)
        val first = topology.startRecipient("first")
        topology.startDonor()
        topology.produceSource("before-join")
        topology.awaitSourceOffset(1)

        val second = topology.startRecipient("second")
        repeat(6) { topology.produceSource("after-join-$it") }
        topology.awaitSourceOffset(7)

        assertEquals(listOf("before-join"), first.handled.take(1))
        assertEquals(3, second.handled.size)
        assertEquals(4, first.handled.size)
        assertEquals(
            (0..5).map { "after-join-$it" }.toSet(),
            (first.handled.drop(1) + second.handled).toSet()
        )
        assertTrue(topology.localHandled.isEmpty())
        assertTrue(topology.donorMetrics.consumerFailures.isEmpty())
        topology.close()
    }

    @Test
    fun `several donors share one destination partition without cross session handling`() = runBlocking {
        val warmupTopic = unique("warmup-shared")
        createTopic(warmupTopic)
        val first = topology(warmupTopic = warmupTopic)
        val second = topology(warmupTopic = warmupTopic)
        val firstRecipient = first.startRecipient("first-recipient")
        val secondRecipient = second.startRecipient("second-recipient")
        first.startDonor()
        second.startDonor()

        first.produceSource("from-first")
        second.produceSource("from-second")
        first.awaitSourceOffset(1)
        second.awaitSourceOffset(1)

        assertEquals(listOf("from-first"), firstRecipient.handled.toList())
        assertEquals(listOf("from-second"), secondRecipient.handled.toList())
        assertTrue(first.localHandled.isEmpty())
        assertTrue(second.localHandled.isEmpty())
        first.close()
        second.close()
    }

    @Test
    fun `late old session record never reaches a new recipient handler`() = runBlocking {
        val topology = topology(maxSessions = 1)
        val old = topology.startRecipient("old")
        topology.startDonor()
        topology.produceSource("old-owned")
        topology.awaitSourceOffset(1)
        topology.closeSession(old)

        val current = topology.startRecipient("current")
        produceWarmup(topology.warmupTopic, old.sessionId, "late-old")
        topology.produceSource("current-owned")
        topology.awaitSourceOffset(2)
        delay(250)

        assertEquals(listOf("old-owned"), old.handled.toList())
        assertEquals(listOf("current-owned"), current.handled.toList())
        assertTrue(current.metrics.dropped.isEmpty())
        assertTrue(current.metrics.failed.isEmpty())
        topology.close()
    }

    @Test
    fun `closing a session with inflight leaves its sibling and donor operational`() = runBlocking {
        val topology = topology(maxSessions = 2, delegationTimeout = 5.seconds)
        val releaseFirst = CompletableDeferred<Unit>()
        val firstStarted = CompletableDeferred<Unit>()
        try {
            val first = topology.startRecipient("first") { value ->
                if (value == "blocked") {
                    firstStarted.complete(Unit)
                    releaseFirst.await()
                }
            }
            val second = topology.startRecipient("second")
            topology.startDonor()
            topology.produceSource("blocked")
            withTimeout(10_000) { firstStarted.await() }

            topology.plane.stopDelegating(first.sessionId)
            val close = async { topology.closeSession(first) }
            topology.produceSource("sibling")
            awaitFor { second.handled.takeIf { "sibling" in it } }
            releaseFirst.complete(Unit)
            withTimeout(10_000) { close.await() }
            topology.awaitSourceOffset(2)
            topology.produceSource("after-close")
            topology.awaitSourceOffset(3)

            assertTrue(second.handled.containsAll(listOf("sibling", "after-close")))
            assertFalse(topology.donor.stateSnapshot().failed)
            assertTrue(topology.donorMetrics.consumerFailures.isEmpty())
        } finally {
            releaseFirst.complete(Unit)
            topology.close()
        }
    }

    @Test
    fun `recipient loss falls back locally and releases donor capacity`() = runBlocking {
        val topology = topology(maxSessions = 1, delegationTimeout = 350.milliseconds)
        val lost = topology.startRecipient("lost")
        topology.startDonor()
        lost.prepared.stop()
        lost.stopped = true

        topology.produceSource("fallback")
        topology.awaitSourceOffset(1)
        assertEquals(listOf("fallback"), topology.localHandled.toList())
        assertEquals(1, topology.donorMetrics.warmupFallbacks.size)
        topology.closeSession(lost)

        val replacement = topology.startRecipient("replacement")
        topology.produceSource("remote-again")
        topology.awaitSourceOffset(2)
        assertEquals(listOf("remote-again"), replacement.handled.toList())
        assertFalse(topology.donor.stateSnapshot().failed)
        topology.close()
    }

    @Test
    fun `donor session failure falls back bounded while sibling keeps processing`() = runBlocking {
        val topology = topology(maxSessions = 2, delegationTimeout = 5.seconds)
        val resultBlocked = CompletableDeferred<Unit>()
        try {
            val failedRecipient = topology.startRecipient("failed") { value ->
                if (value == "fail-session") resultBlocked.await()
            }
            val sibling = topology.startRecipient("sibling")
            topology.startDonor()
            topology.produceSource("fail-session")
            awaitFor { failedRecipient.handled.takeIf { "fail-session" in it } }

            topology.plane.fail(failedRecipient.sessionId, IllegalStateException("capability lost"))
            topology.awaitSourceOffset(1)
            topology.produceSource("sibling-continues")
            topology.awaitSourceOffset(2)

            assertEquals(listOf("fail-session"), topology.localHandled.toList())
            assertTrue("sibling-continues" in sibling.handled)
            assertFalse(topology.donor.stateSnapshot().failed)
        } finally {
            resultBlocked.complete(Unit)
            topology.close()
        }
    }

    @Test
    fun `namespace restart warms again without replay or quarantine state`() = runBlocking {
        val warmupTopic = unique("warmup-restart")
        val sourceTopic = unique("source-restart")
        val donorGroup = unique("donor-restart-group")
        createTopic(warmupTopic)
        createTopic(sourceTopic)
        val first = topology(sourceTopic = sourceTopic, warmupTopic = warmupTopic, donorGroup = donorGroup)
        val firstRecipient = first.startRecipient("before-restart")
        first.startDonor()
        first.produceSource("before-restart")
        first.awaitSourceOffset(1)
        assertEquals(listOf("before-restart"), firstRecipient.handled.toList())
        first.close()

        val second = topology(sourceTopic = sourceTopic, warmupTopic = warmupTopic, donorGroup = donorGroup)
        val secondRecipient = second.startRecipient("after-restart")
        second.startDonor()
        second.produceSource("after-restart")
        second.awaitSourceOffset(2)

        assertEquals(listOf("after-restart"), secondRecipient.handled.toList())
        assertTrue(second.localHandled.isEmpty())
        second.close()
    }

    @Test
    fun `one invalid destination metadata cold falls back the complete recipient context`() = runBlocking {
        val validTopic = unique("warmup-valid")
        val invalidTopic = unique("warmup-invalid")
        createTopic(validTopic)
        createTopic(invalidTopic, partitions = 2)
        val participant = WarmupParticipantId("participant")
        val valid = mapping(participant, unique("source-valid"), validTopic)
        val invalid = mapping(participant, unique("source-invalid"), invalidTopic)
        val now = System.currentTimeMillis()
        val recipientId = WarmupInstanceId("recipient")
        val donorId = WarmupInstanceId("donor")
        val request = WarmupRequested(
            WarmupControlNamespace("global5-v6-integration"), WarmupSessionId("metadata-session"),
            recipientId, listOf(valid, invalid), now + 10_000, 10, 10, "configuration", now + 20_000
        )
        val prepared = CopyOnWriteArrayList<WarmupDestinationStart>()
        val discovery = WarmupRecipientDiscovery(
            request,
            KafkaWarmupDestinationMetadata(
                mapOf(participant to mapOf("bootstrap.servers" to kafka.bootstrapServers)),
                5.seconds
            ),
            WarmupRecipientConsumerPreparation { _, start ->
                prepared += start
                error("consumer preparation must not start after incomplete metadata")
            },
            5.seconds
        )
        val offeredMappings = listOf(valid, invalid).map {
            WarmupDonorMapping(it, setOf(WarmupSourcePartition(0, 1)))
        }
        assertTrue(discovery.onOffer(DonorOffered(
            request.namespace, request.sessionId, donorId, recipientId, offeredMappings,
            request.configurationFingerprint, request.expiresAtEpochMillis
        )))
        val acceptance = assertInstanceOf(
            WarmupRecipientSelection.Accepted::class.java,
            discovery.selectDonors()
        ).messages.single()
        assertTrue(discovery.onDonorAccepted(DonorAccepted(
            acceptance.namespace, acceptance.sessionId, donorId, recipientId, acceptance.mappings,
            acceptance.intervalBudget, acceptance.initialQuota, acceptance.configurationFingerprint,
            acceptance.expiresAtEpochMillis
        )))

        val result = discovery.prepareConsumers()

        assertInstanceOf(WarmupRecipientPreparationResult.ColdFallback::class.java, result)
        assertEquals(WarmupRecipientDiscovery.State.COLD_FALLBACK, discovery.state())
        assertTrue(prepared.isEmpty())
    }

    private fun topology(
        sourceTopic: String = unique("source"),
        warmupTopic: String = unique("warmup"),
        maxSessions: Int = 4,
        delegationTimeout: kotlin.time.Duration = 3.seconds,
        donorGroup: String = unique("donor-group")
    ): Topology {
        createTopicIfMissing(sourceTopic)
        createTopicIfMissing(warmupTopic)
        return Topology(sourceTopic, warmupTopic, maxSessions, delegationTimeout, donorGroup)
            .also(openTopologies::add)
    }

    private inner class Topology(
        val sourceTopic: String,
        val warmupTopic: String,
        maxSessions: Int,
        private val delegationTimeout: kotlin.time.Duration,
        private val donorGroup: String
    ) {
        val plane = MultiSessionWarmupDataPlane<String, String>(maxSessions, maxSessions * 1_000)
        val donorMetrics = RecordingMetrics<String, String>()
        val localHandled = CopyOnWriteArrayList<String>()
        private val participant = WarmupParticipantId("participant-${UUID.randomUUID()}")
        private val sessions = CopyOnWriteArrayList<Recipient>()
        private var closed = false
        val donor: CoroutinesKafkaConsumer<String, String> = CoroutinesKafkaConsumerBuilder<String, String>()
            .apply {
                processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
                workerConcurrency = 4
                workChannelCapacity = 32
                commitIntervalMs = 100
                commitRecordsThreshold = 1
                metrics = donorMetrics
                topics(sourceTopic)
                handle { localHandled += it.value() }
            }
            .buildWarmupDonor(consumerProperties(donorGroup), plane)

        suspend fun startDonor() {
            donor.start()
            withTimeout(10_000) { donor.awaitReady() }
        }

        suspend fun startRecipient(
            name: String,
            beforeResult: suspend (String) -> Unit = {}
        ): Recipient {
            val sessionId = WarmupSessionId("$name-${UUID.randomUUID()}")
            val handled = CopyOnWriteArrayList<String>()
            val metrics = RecordingMetrics<String, String>()
            val mapping = mapping(participant, sourceTopic, warmupTopic)
            val startOffset = endOffset(warmupTopic)
            val preparation = KafkaWarmupRecipientConsumerPreparation(
                mapOf(
                    participant to warmupRecipientParticipantRuntime<String, String>(
                        consumerProperties(unique("ignored-recipient-group")),
                        completionPublisher = {
                            WarmupCompletionPublisher { completed -> plane.acceptResult(completed) }
                        }
                    ) {
                        processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
                        workerConcurrency = 2
                        workChannelCapacity = 16
                        commitIntervalMs = 100
                        commitRecordsThreshold = 1
                        this.metrics = metrics
                        handle { record ->
                            val value = record.value()
                            handled += value
                            beforeResult(value)
                        }
                    }
                )
            )
            val prepared = preparation.prepare(
                sessionId,
                WarmupDestinationStart(WarmupPartitionAssignment(mapping), startOffset)
            )
            prepared.start()
            withTimeout(10_000) { prepared.awaitReady() }
            val session = WarmupDonorSession(
                sessionId = sessionId,
                intervalBudget = 1_000,
                quotaInterval = 1.seconds,
                initialQuota = 1_000,
                delegationTimeout = delegationTimeout,
                destinationResolver = WarmupDestinationResolver { record ->
                    WarmupDestination(warmupTopic).takeIf { record.topic() == sourceTopic }
                },
                sender = KafkaWarmupRecordSender(producerProperties()),
                observer = WarmupMetricsObserver(donorMetrics)
            )
            assertEquals(MultiSessionWarmupDataPlane.Admission.ACCEPTED, plane.admit(session))
            assertTrue(plane.activate(sessionId))
            return Recipient(sessionId, prepared, handled, metrics).also(sessions::add)
        }

        fun produceSource(value: String) = produce(sourceTopic, value)

        suspend fun awaitSourceOffset(offset: Long) {
            awaitFor(timeoutMillis = 15_000) {
                committedOffset(donorGroup, sourceTopic)?.takeIf { it.offset() == offset }
            }
        }

        suspend fun closeSession(recipient: Recipient) {
            plane.stopDelegatingAndAwaitInflight(recipient.sessionId, 10.seconds)
            assertTrue(plane.removeDrainedSession(recipient.sessionId))
            if (!recipient.stopped) {
                recipient.prepared.stop()
                recipient.stopped = true
            }
            sessions.remove(recipient)
        }

        suspend fun close() {
            if (closed) return
            var failure: Throwable? = null
            sessions.toList().forEach { recipient ->
                try {
                    closeSession(recipient)
                } catch (error: Throwable) {
                    failure = failure.append(error)
                    if (!recipient.stopped) {
                        try {
                            recipient.prepared.stop()
                        } catch (stopError: Throwable) {
                            failure = failure.append(stopError)
                        }
                    }
                }
            }
            try {
                donor.stop()
            } catch (error: Throwable) {
                failure = failure.append(error)
            }
            if (failure == null) {
                closed = true
                openTopologies.remove(this)
            }
            failure?.let { throw it }
        }

        private fun Throwable?.append(error: Throwable): Throwable = this?.also {
            it.addSuppressed(error)
        } ?: error
    }

    private data class Recipient(
        val sessionId: WarmupSessionId,
        val prepared: PreparedWarmupRecipientConsumer,
        val handled: CopyOnWriteArrayList<String>,
        val metrics: RecordingMetrics<String, String>,
        var stopped: Boolean = false
    )

    private fun mapping(
        participant: WarmupParticipantId,
        sourceTopic: String,
        warmupTopic: String
    ) = WarmupTopicMapping(
        participant, sourceTopic, warmupTopic,
        "mapping-${sourceTopic.hashCode()}-${warmupTopic.hashCode()}"
    )

    private fun consumerProperties(groupId: String): Map<String, Any?> = mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers,
        ConsumerConfig.GROUP_ID_CONFIG to groupId,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java
    )

    private fun producerProperties(): Map<String, Any?> = mapOf(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        ProducerConfig.ACKS_CONFIG to "all"
    )

    private fun createTopicIfMissing(topic: String) {
        AdminClient.create(mapOf("bootstrap.servers" to kafka.bootstrapServers)).use { admin ->
            if (topic !in admin.listTopics().names().get()) {
                admin.createTopics(listOf(NewTopic(topic, 1, 1))).all().get()
            }
        }
    }

    private fun createTopic(topic: String, partitions: Int = 1) {
        AdminClient.create(mapOf("bootstrap.servers" to kafka.bootstrapServers)).use { admin ->
            admin.createTopics(listOf(NewTopic(topic, partitions, 1))).all().get()
        }
    }

    private fun endOffset(topic: String): Long {
        val partition = TopicPartition(topic, 0)
        AdminClient.create(mapOf("bootstrap.servers" to kafka.bootstrapServers)).use { admin ->
            return admin.listOffsets(mapOf(partition to OffsetSpec.latest()))
                .all().get().getValue(partition).offset()
        }
    }

    private fun committedOffset(groupId: String, topic: String): OffsetAndMetadata? {
        val partition = TopicPartition(topic, 0)
        KafkaConsumer<String, String>(consumerProperties(groupId)).use { consumer ->
            return consumer.committed(setOf(partition))[partition]
        }
    }

    private fun produce(topic: String, value: String) {
        KafkaProducer<String, String>(producerProperties()).use { producer ->
            producer.send(ProducerRecord(topic, 0, value, value)).get()
        }
    }

    private fun produceWarmup(topic: String, sessionId: WarmupSessionId, value: String) {
        KafkaProducer<String, String>(producerProperties()).use { producer ->
            val record = ProducerRecord<String, String>(topic, 0, value, value)
            record.headers().add(RecordHeader(WARMUP_SESSION_HEADER, sessionId.value.toByteArray()))
            producer.send(record).get()
        }
    }

    private suspend fun <T : Any> awaitFor(
        timeoutMillis: Long = 10_000,
        block: () -> T?
    ): T = withTimeout(timeoutMillis) {
        while (true) {
            block()?.let { return@withTimeout it }
            delay(20)
        }
        @Suppress("UNREACHABLE_CODE")
        error("unreachable")
    }

    private fun unique(prefix: String) = "$prefix-${UUID.randomUUID()}"

    companion object {
        @Container
        @JvmStatic
        val kafka = KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"))
    }
}
