package avh.ckc.warmup

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.CoroutinesKafkaConsumerBuilder
import avh.ckc.core.ProcessingMode
import avh.ckc.warmup.protocol.ControlRecordCompleted
import avh.ckc.warmup.protocol.DonorAccepted
import avh.ckc.warmup.protocol.DonorClosed
import avh.ckc.warmup.protocol.DonorOffered
import avh.ckc.warmup.protocol.SessionAccepted
import avh.ckc.warmup.protocol.SessionEnded
import avh.ckc.warmup.protocol.WARMUP_SESSION_HEADER
import avh.ckc.warmup.protocol.WarmupCompletionRequested
import avh.ckc.warmup.protocol.WarmupControlMessage
import avh.ckc.warmup.protocol.WarmupControlNamespace
import avh.ckc.warmup.protocol.WarmupControlRouter
import avh.ckc.warmup.protocol.WarmupDonorMapping
import avh.ckc.warmup.protocol.WarmupInstanceId
import avh.ckc.warmup.protocol.WarmupMappingQuota
import avh.ckc.warmup.protocol.WarmupParticipantId
import avh.ckc.warmup.protocol.WarmupPartitionAssignment
import avh.ckc.warmup.protocol.WarmupQuotaProfile
import avh.ckc.warmup.protocol.WarmupRequested
import avh.ckc.warmup.protocol.WarmupSessionEndState
import avh.ckc.warmup.protocol.WarmupSessionId
import avh.ckc.warmup.protocol.WarmupSourcePartition
import avh.ckc.warmup.protocol.WarmupTopicMapping
import avh.ckc.warmup.protocol.normalizedAdmissionBudget
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
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
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class WarmupSharedPartitionIntegrationTest {
    private val openTopologies = CopyOnWriteArrayList<Topology>()
    private val openProtocolPairs = CopyOnWriteArrayList<ProtocolPair>()

    @AfterEach
    fun closeOpenTopologies() = runBlocking {
        openProtocolPairs.toList().asReversed().forEach { it.close() }
        openTopologies.toList().asReversed().forEach { it.close() }
    }

    @Test
    fun `warmup recipient independently assigns partition zero at captured offset and filters sessions`() = runBlocking {
        val topic = "warmup-recipient-${UUID.randomUUID()}"
        val participant = WarmupParticipantId("orders")
        val session = WarmupSessionId("recipient-session")
        val mapping = WarmupTopicMapping(participant, "orders", topic, "mapping-fingerprint")
        createTopic(topic)
        produceWarmup(topic, session, "before-captured-start")
        val capturedStart = KafkaWarmupDestinationMetadata(
            mapOf(participant to mapOf("bootstrap.servers" to kafka.bootstrapServers)),
            5_000.milliseconds
        ).endOffsets(mapping).getValue(0)
        val handled = CopyOnWriteArrayList<String>()
        val preparation = KafkaWarmupRecipientConsumerPreparation(
            mapOf(
                participant to warmupRecipientParticipantRuntime<String, String>(
                    consumerProperties("warmup-${UUID.randomUUID()}")
                ) {
                    processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
                    handle { handled += it.value() }
                }
            )
        )
        val prepared = preparation.prepare(
            session,
            WarmupDestinationStart(WarmupPartitionAssignment(mapping), capturedStart)
        )

        produceWarmup(topic, WarmupSessionId("foreign-session"), "foreign")
        produceWarmup(topic, session, "owned")
        try {
            prepared.start()
            withTimeout(15_000) { prepared.awaitReady() }
            awaitFor(timeoutMillis = 15_000) { handled.takeIf { it.size == 1 } }
            delay(300)
            assertEquals(listOf("owned"), handled.toList())
        } finally {
            prepared.stop()
        }
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
    fun `donor pending metric aggregates two simultaneously waiting sessions and drains to zero`() = runBlocking {
        val topology = topology(maxSessions = 2, delegationTimeout = 5.seconds)
        val release = CompletableDeferred<Unit>()
        val first = topology.startRecipient("first-pending") { release.await() }
        val second = topology.startRecipient("second-pending") { release.await() }
        topology.startDonor()

        topology.produceSource("first-wait")
        topology.produceSource("second-wait")
        awaitFor { (first.handled.size + second.handled.size).takeIf { it == 2 } }
        awaitFor { topology.donorMetrics.warmupPending.lastOrNull()?.takeIf { it == 2 } }

        release.complete(Unit)
        topology.awaitSourceOffset(2)
        awaitFor { topology.donorMetrics.warmupPending.lastOrNull()?.takeIf { it == 0 } }

        assertEquals(2, topology.donorMetrics.warmupPending.max())
        assertEquals(0, topology.donorMetrics.warmupPending.last())
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
    fun `kafka control lifecycle discovers activates reports and closes a warmup session`() = runBlocking {
        val pair = protocolPair()
        pair.start()

        val discovery = pair.discover()
        assertInstanceOf(WarmupDiscoveryResult.Ready::class.java, discovery)
        pair.produceSource("protocol-remote")
        pair.awaitSourceOffset(1)
        pair.outbox.flush(5.seconds)
        assertEquals(WarmupCompletionResult.Completed, pair.complete())

        assertEquals(listOf("protocol-remote"), pair.recipientHandled.toList())
        assertTrue(pair.localHandled.isEmpty())
        assertTrue(pair.donorMessages.any { it is WarmupCompletionRequested })
        assertTrue(pair.recipientMessages.any { it is DonorClosed })
        assertTrue(pair.donorMessages.any { it is ControlRecordCompleted })
        assertEquals(0, pair.plane.admittedSessionCount())
        assertTrue(pair.donorMetrics.processed.single().processedRemotely)
        assertFalse(pair.donor.stateSnapshot().failed)
        pair.close()
    }

    @Test
    fun `restarted kafka control namespace starts at tail and warms a new session`() = runBlocking {
        val sourceTopic = unique("protocol-source-restart")
        val warmupTopic = unique("protocol-warmup-restart")
        val controlTopic = unique("protocol-control-restart")
        val namespace = WarmupControlNamespace(unique("global5-v6-restart"))
        val donorGroup = unique("protocol-donor-group")
        createTopic(sourceTopic)
        createTopic(warmupTopic)
        createTopic(controlTopic)

        val first = protocolPair(sourceTopic, warmupTopic, controlTopic, namespace, donorGroup)
        first.start()
        assertInstanceOf(WarmupDiscoveryResult.Ready::class.java, first.discover())
        first.produceSource("before-restart")
        first.awaitSourceOffset(1)
        first.outbox.flush(5.seconds)
        assertEquals(WarmupCompletionResult.Completed, first.complete())
        val firstSession = first.sessionId
        first.close()

        val second = protocolPair(sourceTopic, warmupTopic, controlTopic, namespace, donorGroup)
        second.start()
        assertInstanceOf(WarmupDiscoveryResult.Ready::class.java, second.discover())
        second.produceSource("after-restart")
        second.awaitSourceOffset(2)
        second.outbox.flush(5.seconds)
        assertEquals(WarmupCompletionResult.Completed, second.complete())

        assertEquals(listOf("after-restart"), second.recipientHandled.toList())
        assertTrue(second.recipientMessages.none { it.sessionId == firstSession })
        assertTrue(second.donorMessages.none { it.sessionId == firstSession })
        assertFalse(second.donor.stateSnapshot().failed)
        second.close()
    }

    @Test
    fun `protocol metadata fallback cold starts the main recipient consumer`() = runBlocking {
        val pair = protocolPair(warmupPartitions = 2)
        pair.start()

        val fallback = pair.discover()

        assertInstanceOf(WarmupDiscoveryResult.ColdFallback::class.java, fallback)
        awaitFor { pair.donorActivation.state(pair.sessionId)?.takeIf { it == WarmupDonorActivation.State.CLOSED } }
        val coldHandled = CopyOnWriteArrayList<String>()
        val cold = pair.coldMainConsumer(coldHandled)
        try {
            cold.start()
            withTimeout(10_000) { cold.awaitReady() }
            pair.produceSource("cold-main")
            awaitFor { coldHandled.takeIf { "cold-main" in it } }

            assertEquals(listOf("cold-main"), coldHandled.toList())
            assertTrue(pair.recipientMessages.any {
                it is SessionEnded && it.state == WarmupSessionEndState.COLD_FALLBACK
            })
            assertEquals(0, pair.plane.admittedSessionCount())
            assertFalse(cold.stateSnapshot().failed)
        } finally {
            cold.stop()
            pair.close()
        }
    }

    @Test
    fun `recreated data plane uses stable source offset and a new recipient session`() = runBlocking {
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
    fun `real metadata validation prevents all preparation when one destination is incompatible`() = runBlocking {
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
            recipientId, listOf(valid, invalid), now + 10_000,
            normalizedAdmissionBudget(listOf(valid, invalid)), "configuration", now + 20_000
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
            acceptance.admissionBudgetPerSecond, acceptance.configurationFingerprint,
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
        createTopic(sourceTopic)
        createTopic(warmupTopic)
        return Topology(sourceTopic, warmupTopic, maxSessions, delegationTimeout, donorGroup)
            .also(openTopologies::add)
    }

    private fun protocolPair(
        sourceTopic: String = unique("protocol-source"),
        warmupTopic: String = unique("protocol-warmup"),
        controlTopic: String = unique("protocol-control"),
        namespace: WarmupControlNamespace = WarmupControlNamespace(unique("global5-v6")),
        donorGroup: String = unique("protocol-donor-group"),
        warmupPartitions: Int = 1
    ): ProtocolPair {
        createTopic(sourceTopic)
        createTopic(warmupTopic, warmupPartitions)
        createTopic(controlTopic)
        return ProtocolPair(sourceTopic, warmupTopic, controlTopic, namespace, donorGroup)
            .also(openProtocolPairs::add)
    }

    private inner class ProtocolPair(
        val sourceTopic: String,
        val warmupTopic: String,
        private val controlTopic: String,
        private val namespace: WarmupControlNamespace,
        private val donorGroup: String
    ) {
        val sessionId = WarmupSessionId.random()
        private val donorId = WarmupInstanceId.random()
        private val recipientId = WarmupInstanceId.random()
        private val participant = WarmupParticipantId("participant-${UUID.randomUUID()}")
        private val mapping = mapping(participant, sourceTopic, warmupTopic)
        private val configurationFingerprint = "configuration-${UUID.randomUUID()}"
        private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val plane = MultiSessionWarmupDataPlane<String, String>(2, 2_000)
        val donorMetrics = RecordingWarmupMetrics<String, String>()
        private val donorObserver = WarmupMetricsObserver(donorMetrics)
        val localHandled = CopyOnWriteArrayList<String>()
        val recipientHandled = CopyOnWriteArrayList<String>()
        private val recipientMetrics = RecordingWarmupMetrics<String, String>()
        val donorMessages = CopyOnWriteArrayList<WarmupControlMessage>()
        val recipientMessages = CopyOnWriteArrayList<WarmupControlMessage>()
        private val donorRouter = WarmupControlRouter(donorId, namespace)
        private val recipientRouter = WarmupControlRouter(recipientId, namespace)
        private val donorTap = donorRouter.register { donorMessages += it.message }
        private val recipientTap = recipientRouter.register { recipientMessages += it.message }
        val donor = CoroutinesKafkaConsumerBuilder<String, String>()
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
            .runtimeFactory().subscribed(consumerProperties(donorGroup), plane)
        private lateinit var donorTransport: WarmupControlTransport
        private lateinit var recipientTransport: WarmupControlTransport
        val donorActivation: WarmupDonorActivation<WarmupDonorSession<String, String>>
        private val recipientActivation: WarmupDonorActivation<WarmupDonorSession<String, String>>
        private val donorRuntime: WarmupDiscoveryRuntime<WarmupDonorSession<String, String>>
        private val recipientRuntime: WarmupDiscoveryRuntime<WarmupDonorSession<String, String>>
        private lateinit var request: WarmupRequested
        private lateinit var recipientDiscovery: WarmupRecipientDiscovery
        val outbox: WarmupCompletionOutbox
        private val resultLane: WarmupResultLane
        private var donorControlStartAttempted = false
        private var recipientControlStartAttempted = false
        private var completed = false
        private var closed = false

        init {
            donorActivation = WarmupDonorActivation(
                localInstanceId = donorId,
                namespace = namespace,
                supportedMappings = listOf(mapping),
                dataPlane = plane,
                eligibility = MainConsumerWarmupDonorEligibility(mapOf(participant to donor)),
                preparation = WarmupDonorSessionPreparation { acceptance -> donorSession(acceptance) },
                scope = scope,
                onPreparationFailed = { donorTransport.send(it) },
                onDonorClosed = { donorTransport.send(it) },
                drainTimeout = 3.seconds
            )
            val recipientPlane = MultiSessionWarmupDataPlane<String, String>(1, 1)
            recipientActivation = WarmupDonorActivation(
                localInstanceId = recipientId,
                namespace = namespace,
                supportedMappings = emptyList(),
                dataPlane = recipientPlane,
                eligibility = WarmupDonorEligibility { null },
                preparation = WarmupDonorSessionPreparation { error("Recipient is not a donor") },
                scope = scope,
                onPreparationFailed = { recipientTransport.send(it) },
                onDonorClosed = { recipientTransport.send(it) }
            )
            donorTransport = controlTransport(donorId, namespace, controlTopic, donorRouter)
            recipientTransport = controlTransport(recipientId, namespace, controlTopic, recipientRouter)
            donorRuntime = WarmupDiscoveryRuntime(
                donorId,
                namespace,
                donorRouter,
                donorTransport,
                StatelessWarmupDonorDiscovery(
                    donorId,
                    namespace,
                    listOf(mapping),
                    MainConsumerWarmupDonorEligibility(mapOf(participant to donor))
                ),
                donorActivation,
                150.milliseconds,
                5.seconds,
                5.seconds,
                scope = scope
            )
            recipientRuntime = WarmupDiscoveryRuntime(
                recipientId,
                namespace,
                recipientRouter,
                recipientTransport,
                StatelessWarmupDonorDiscovery(recipientId, namespace, emptyList()) { null },
                recipientActivation,
                150.milliseconds,
                5.seconds,
                5.seconds,
                scope = scope
            )
            outbox = WarmupCompletionOutbox(scope, 64) { completed ->
                recipientTransport.send(
                    ControlRecordCompleted(
                        namespace,
                        sessionId,
                        recipientId,
                        donorId,
                        completed,
                        configurationFingerprint,
                        request.expiresAtEpochMillis
                    )
                )
            }
            resultLane = outbox.lane(mapping.mappingFingerprint)
        }

        suspend fun start() {
            donor.start()
            withTimeout(10_000) { donor.awaitReady() }
            donorControlStartAttempted = true
            check(donorRuntime.startControl())
            recipientControlStartAttempted = true
            check(recipientRuntime.startControl())
        }

        suspend fun discover(): WarmupDiscoveryResult {
            check(!::recipientDiscovery.isInitialized) { "Protocol discovery already started" }
            val now = System.currentTimeMillis()
            request = WarmupRequested(
                namespace = namespace,
                sessionId = sessionId,
                senderInstanceId = recipientId,
                mappings = listOf(mapping),
                offerDeadlineEpochMillis = now + 5_000,
                admissionBudgetPerSecond = normalizedAdmissionBudget(listOf(mapping)),
                configurationFingerprint = configurationFingerprint,
                expiresAtEpochMillis = now + 30_000
            )
            val preparation = KafkaWarmupRecipientConsumerPreparation(
                mapOf(
                    participant to warmupRecipientParticipantRuntime<String, String>(
                        consumerProperties(unique("protocol-recipient-group")),
                        completionPublisher = { resultLane },
                        resultAdmission = { resultLane }
                    ) {
                        processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
                        workerConcurrency = 2
                        workChannelCapacity = 16
                        commitIntervalMs = 100
                        commitRecordsThreshold = 1
                        metrics = recipientMetrics
                        handle { recipientHandled += it.value() }
                    }
                )
            )
            recipientDiscovery = WarmupRecipientDiscovery(
                request,
                KafkaWarmupDestinationMetadata(
                    mapOf(participant to mapOf("bootstrap.servers" to kafka.bootstrapServers)),
                    5.seconds
                ),
                preparation,
                5.seconds
            )
            return recipientRuntime.discoverAndPrepare(recipientDiscovery)
        }

        suspend fun complete(): WarmupCompletionResult {
            val result = recipientRuntime.completeWarmup(recipientDiscovery, finishSends = { result ->
                if (result == WarmupCompletionResult.Completed ||
                    result is WarmupCompletionResult.CompletedWithDiagnostics
                ) {
                    outbox.stop(5.seconds)
                } else {
                    outbox.abort(5.seconds)
                }
                recipientRuntime.stopRecipientProbes()
                result
            })
            if (result is WarmupCompletionResult.Completed ||
                result is WarmupCompletionResult.CompletedWithDiagnostics
            ) {
                completed = true
            }
            return result
        }

        fun produceSource(value: String) = produce(sourceTopic, value)

        suspend fun awaitSourceOffset(offset: Long) {
            awaitFor(timeoutMillis = 15_000) {
                committedOffset(donorGroup, sourceTopic)?.takeIf { it.offset() == offset }
            }
        }

        fun coldMainConsumer(handled: MutableList<String>): CoroutinesKafkaConsumer<String, String> =
            avh.ckc.core.coroutinesKafkaConsumer<String, String>(consumerProperties(unique("cold-recipient-group"))) {
                    processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
                    commitIntervalMs = 100
                    commitRecordsThreshold = 1
                    topics(sourceTopic)
                    handle { handled += it.value() }
                }

        suspend fun close() {
            if (closed) return
            val cleanupErrors = mutableListOf<Throwable>()

            suspend fun cleanup(operation: String, action: suspend () -> Unit) {
                try {
                    action()
                } catch (error: Throwable) {
                    cleanupErrors += IllegalStateException("ProtocolPair cleanup failed: $operation", error)
                }
            }

            if (::recipientDiscovery.isInitialized && !completed) {
                cleanup("recipient discovery") {
                    when (val result = recipientDiscovery.terminate()) {
                        WarmupRecipientCleanupResult.Complete -> Unit
                        is WarmupRecipientCleanupResult.CompleteWithDiagnostics -> error(
                            "Recipient cleanup completed with diagnostics: ${result.diagnostics.joinToString()}"
                        )
                        is WarmupRecipientCleanupResult.Incomplete -> error(
                            "Recipient cleanup is incomplete: ${result.pendingAssignments}; " +
                                result.diagnostics.joinToString()
                        )
                    }
                }
            }
            cleanup("completion outbox") { outbox.stop(5.seconds) }
            if (recipientControlStartAttempted) {
                cleanup("recipient control transport") { recipientRuntime.stop() }
            }
            if (donorControlStartAttempted) {
                cleanup("donor control transport") { donorRuntime.stop() }
            }
            cleanup("recipient runtime registration") { recipientRuntime.close() }
            cleanup("donor runtime registration") { donorRuntime.close() }
            cleanup("donor message tap") { donorTap.close() }
            cleanup("recipient message tap") { recipientTap.close() }
            cleanup("main donor consumer") { donor.stop() }
            scope.cancel()
            closed = true
            openProtocolPairs.remove(this)
            if (cleanupErrors.isNotEmpty()) {
                throw IllegalStateException("ProtocolPair cleanup had ${cleanupErrors.size} failure(s)").apply {
                    cleanupErrors.forEach(::addSuppressed)
                }
            }
        }

        private fun donorSession(acceptance: SessionAccepted): WarmupDonorSession<String, String> {
            val destinations = acceptance.mappings.associate {
                it.mapping.sourceTopic to WarmupDestination(it.mapping.warmupTopic)
            }
            return WarmupDonorSession(
                sessionId = acceptance.sessionId,
                quotaProfile = acceptance.mappings.single().mapping.quota.profile,
                quotaInterval = acceptance.mappings.single().mapping.quota.quotaIntervalMillis.milliseconds,
                delegationTimeout = 5.seconds,
                destinationResolver = WarmupDestinationResolver { record -> destinations[record.topic()] },
                sender = KafkaWarmupRecordSender(producerProperties()),
                observer = donorObserver
            )
        }
    }

    private fun controlTransport(
        instanceId: WarmupInstanceId,
        namespace: WarmupControlNamespace,
        topic: String,
        router: WarmupControlRouter
    ) = WarmupControlTransport(
        WarmupControlTransportConfiguration(
            instanceId,
            namespace,
            topic,
            producerProperties = mapOf("bootstrap.servers" to kafka.bootstrapServers),
            consumerProperties = mapOf("bootstrap.servers" to kafka.bootstrapServers),
            pollTimeout = 20.milliseconds,
            sendTimeout = 5.seconds,
            metadataTimeout = 5.seconds
        ),
        router
    )

    private inner class Topology(
        val sourceTopic: String,
        val warmupTopic: String,
        maxSessions: Int,
        private val delegationTimeout: kotlin.time.Duration,
        private val donorGroup: String
    ) {
        val plane = MultiSessionWarmupDataPlane<String, String>(maxSessions, maxSessions * 1_000)
        val donorMetrics = RecordingWarmupMetrics<String, String>()
        private val donorObserver = WarmupMetricsObserver(donorMetrics)
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
            .runtimeFactory().subscribed(consumerProperties(donorGroup), plane)

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
            val metrics = RecordingWarmupMetrics<String, String>()
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
            val recipient = Recipient(sessionId, prepared, handled, metrics)
            sessions += recipient
            try {
                prepared.start()
                withTimeout(10_000) { prepared.awaitReady() }
            } catch (error: Throwable) {
                try {
                    prepared.stop()
                    recipient.stopped = true
                    sessions.remove(recipient)
                } catch (stopError: Throwable) {
                    error.addSuppressed(stopError)
                }
                throw error
            }
            val session = WarmupDonorSession(
                sessionId = sessionId,
                quotaProfile = WarmupQuotaProfile.constant(1_000),
                quotaInterval = 1.seconds,
                delegationTimeout = delegationTimeout,
                destinationResolver = WarmupDestinationResolver { record ->
                    WarmupDestination(warmupTopic).takeIf { record.topic() == sourceTopic }
                },
                sender = KafkaWarmupRecordSender(producerProperties()),
                observer = donorObserver
            )
            try {
                assertEquals(DonorSessionAdmission.ACCEPTED, plane.admit(session))
                assertTrue(plane.activate(sessionId))
            } catch (error: Throwable) {
                plane.stopDelegating(sessionId)
                runCatching { plane.removeDrainedSession(sessionId) }
                try {
                    prepared.stop()
                    recipient.stopped = true
                    sessions.remove(recipient)
                } catch (stopError: Throwable) {
                    error.addSuppressed(stopError)
                }
                throw error
            }
            return recipient
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
        val metrics: RecordingWarmupMetrics<String, String>,
        var stopped: Boolean = false
    )

    private fun mapping(
        participant: WarmupParticipantId,
        sourceTopic: String,
        warmupTopic: String
    ) = WarmupTopicMapping(
        participant, sourceTopic, warmupTopic,
        "mapping-${sourceTopic.hashCode()}-${warmupTopic.hashCode()}",
        WarmupMappingQuota(1_000)
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

    private fun createTopic(topic: String, partitions: Int = 1) {
        AdminClient.create(mapOf("bootstrap.servers" to kafka.bootstrapServers)).use { admin ->
            try {
                admin.createTopics(listOf(NewTopic(topic, partitions, 1))).all().get(10, TimeUnit.SECONDS)
            } catch (error: ExecutionException) {
                if (error.cause !is TopicExistsException) throw error
            }
            // CreateTopics may return before the broker can answer requests for the new partitions.
            val offsets = (0 until partitions).associate { TopicPartition(topic, it) to OffsetSpec.latest() }
            val started = TimeSource.Monotonic.markNow()
            while (true) {
                try {
                    admin.listOffsets(offsets).all().get(5, TimeUnit.SECONDS)
                    return
                } catch (error: ExecutionException) {
                    if (error.cause !is RetriableException || started.elapsedNow() >= 5.seconds) throw error
                    Thread.sleep(50)
                }
            }
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
