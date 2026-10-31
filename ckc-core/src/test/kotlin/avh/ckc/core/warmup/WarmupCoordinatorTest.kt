package avh.ckc.core.warmup

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.mockito.kotlin.mock
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class WarmupCoordinatorTest {
    @Test
    fun `first instance falls back to main when no donor exists`() = runBlocking {
        val bus = ControlBus()
        val participant = TestParticipant("orders", "orders", "warmup-orders")
        val coordinator = coordinator("first", bus).also { it.register(participant) }

        try {
            assertEquals(WarmupStartupResult.NO_DONOR_FALLBACK, coordinator.start())
            assertEquals(WarmupCoordinatorState.MAIN, coordinator.state())
            assertEquals(listOf("main"), participant.events)
        } finally {
            coordinator.stop()
        }
    }

    @Test
    fun `automatic session warms multiple participants drains all before starting mains and releases donor`() = runBlocking {
        val bus = ControlBus()
        val lifecycleEvents = CopyOnWriteArrayList<String>()
        val donorParticipants = listOf(
            TestParticipant("orders", "orders", "warmup-orders"),
            TestParticipant("payments", "payments", "warmup-payments")
        )
        val recipientParticipants = listOf(
            TestParticipant("orders", "orders", "warmup-orders", lifecycleEvents),
            TestParticipant("payments", "payments", "warmup-payments", lifecycleEvents)
        )
        val donor = coordinator("donor", bus, load = 0.9).also { coordinator ->
            donorParticipants.forEach(coordinator::register)
        }
        val recipient = coordinator("recipient", bus).also { coordinator ->
            recipientParticipants.forEach(coordinator::register)
        }

        try {
            assertEquals(WarmupStartupResult.NO_DONOR_FALLBACK, donor.start())
            assertEquals(WarmupStartupResult.WARMED, withTimeout(2_000) { recipient.start() })

            assertEquals(WarmupCoordinatorState.MAIN, donor.state())
            assertEquals(WarmupCoordinatorState.MAIN, recipient.state())
            donorParticipants.forEach {
                assertEquals(listOf("main", "reserved", "forwarding", "boundary", "released"), it.events)
            }
            recipientParticipants.forEach {
                assertEquals(listOf("recipient", "drained", "main-after-drain"), it.events)
            }
            val firstMain = recipientParticipants.minOf { lifecycleEvents.indexOf("${it.id}:main-after-drain") }
            val lastDrain = recipientParticipants.maxOf { lifecycleEvents.indexOf("${it.id}:drained") }
            assertTrue(lastDrain < firstMain, "Every recipient must drain before any main subscription starts")
        } finally {
            recipient.stop()
            donor.stop()
        }
    }

    @Test
    fun `duplicate control messages do not repeat participant lifecycle actions`() = runBlocking {
        val bus = ControlBus(duplicateMessages = true)
        val donorParticipant = TestParticipant("orders", "orders", "warmup-orders")
        val recipientParticipant = TestParticipant("orders", "orders", "warmup-orders")
        val donor = coordinator("donor", bus).also { it.register(donorParticipant) }
        val recipient = coordinator("recipient", bus).also { it.register(recipientParticipant) }

        try {
            donor.start()
            assertEquals(WarmupStartupResult.WARMED, withTimeout(2_000) { recipient.start() })
            assertEquals(1, donorParticipant.events.count { it == "forwarding" })
            assertEquals(1, donorParticipant.events.count { it == "boundary" })
            assertEquals(1, recipientParticipant.events.count { it == "recipient" })
            assertEquals(1, recipientParticipant.events.count { it == "drained" })
        } finally {
            recipient.stop()
            donor.stop()
        }
    }

    @Test
    fun `globally shared lease permits only one simultaneous warm-up assignment`() = runBlocking {
        val bus = ControlBus()
        val donors = (1..2).map { number ->
            coordinator("donor-$number", bus).also {
                it.register(TestParticipant("orders", "orders", "warmup-orders", hold = 300.milliseconds))
            }
        }
        val recipients = (1..2).map { number ->
            coordinator("recipient-$number", bus).also {
                it.register(TestParticipant("orders", "orders", "warmup-orders", hold = 300.milliseconds))
            }
        }

        try {
            assertTrue(
                donors.map { donor -> async { donor.start() } }.awaitAll()
                    .all { it == WarmupStartupResult.NO_DONOR_FALLBACK }
            )
            val results = recipients.map { recipient -> async { recipient.start() } }.awaitAll()

            assertEquals(1, results.count { it == WarmupStartupResult.WARMED })
            assertEquals(1, results.count { it == WarmupStartupResult.NO_DONOR_FALLBACK })
            assertEquals(1, bus.leases.maximumActive)
        } finally {
            recipients.forEach { it.stop() }
            donors.forEach { it.stop() }
        }
    }

    @Test
    fun `failure starting main during fallback fails startup instead of hanging`() = runBlocking {
        val bus = ControlBus()
        val failure = IllegalStateException("main failed")
        val coordinator = coordinator("recipient", bus).also {
            it.register(TestParticipant("orders", "orders", "warmup-orders", mainFailure = failure))
        }

        try {
            val thrown = assertFailsWith<IllegalStateException> {
                withTimeout(2_000) { coordinator.start() }
            }
            assertEquals(failure.message, thrown.message)
            assertEquals(WarmupCoordinatorState.FAILED, coordinator.state())
        } finally {
            coordinator.stop()
        }
    }

    @Test
    fun `timer send failure falls back before forwarding`() = runBlocking {
        val bus = ControlBus(failSend = { it is DonorSelected })
        val donor = coordinator("donor", bus).also {
            it.register(TestParticipant("orders", "orders", "warmup-orders"))
        }
        val recipientParticipant = TestParticipant("orders", "orders", "warmup-orders")
        val recipient = coordinator("recipient", bus).also { it.register(recipientParticipant) }

        try {
            donor.start()
            assertEquals(
                WarmupStartupResult.NO_DONOR_FALLBACK,
                withTimeout(2_000) { recipient.start() }
            )
            assertEquals(WarmupCoordinatorState.MAIN, recipient.state())
            assertEquals(listOf("fallback-prepared", "main"), recipientParticipant.events)
        } finally {
            recipient.stop()
            donor.stop()
        }
    }

    @Test
    fun `control readiness timeout performs cold-start fallback`() = runBlocking {
        val bus = ControlBus()
        val channel = TestChannel(WarmupControlMessageHandler { }, bus, readyOnStart = false)
        val participant = TestParticipant("orders", "orders", "warmup-orders")
        val coordinator = coordinator("recipient", bus, stageTimeout = 30.milliseconds) { channel }
            .also { it.register(participant) }

        try {
            assertEquals(WarmupStartupResult.NO_DONOR_FALLBACK, withTimeout(2_000) { coordinator.start() })
            assertEquals(listOf("fallback-prepared", "main"), participant.events)
        } finally {
            coordinator.stop()
        }
    }

    @Test
    fun `recipient startup failure falls back and releases donor reservation`() = runBlocking {
        val bus = ControlBus()
        val donorParticipant = TestParticipant("orders", "orders", "warmup-orders")
        val donor = coordinator("donor", bus, load = 0.9).also { it.register(donorParticipant) }
        val failure = IllegalStateException("recipient failed")
        val recipientParticipant = TestParticipant(
            "orders", "orders", "warmup-orders", recipientFailure = failure
        )
        val recipient = coordinator("recipient", bus, load = 0.1).also { it.register(recipientParticipant) }
        val nextRecipient = coordinator("next-recipient", bus, load = 0.0).also {
            it.register(TestParticipant("orders", "orders", "warmup-orders"))
        }

        try {
            donor.start()
            assertEquals(WarmupStartupResult.NO_DONOR_FALLBACK, withTimeout(2_000) { recipient.start() })
            withTimeout(2_000) {
                while (donor.state() != WarmupCoordinatorState.MAIN || bus.leases.active != 0) delay(5)
            }
            assertTrue("fallback-prepared" in recipientParticipant.events)
            assertEquals(WarmupStartupResult.WARMED, withTimeout(2_000) { nextRecipient.start() })
            assertEquals(2, donorParticipant.events.count { it == "reserved" })
            assertEquals(1, donorParticipant.events.count { it == "reservation-released" })
        } finally {
            nextRecipient.stop()
            recipient.stop()
            donor.stop()
        }
    }

    @Test
    fun `transport failure cannot be overwritten by delayed fallback main startup`() = runBlocking {
        val bus = ControlBus()
        val mainGate = CompletableDeferred<Unit>()
        val participant = TestParticipant("orders", "orders", "warmup-orders", mainGate = mainGate)
        lateinit var channel: TestChannel
        val coordinator = coordinator("recipient", bus) { handler ->
            TestChannel(handler, bus).also { channel = it }
        }.also { it.register(participant) }

        try {
            val startupScope = CoroutineScope(Dispatchers.Default)
            val startup = startupScope.async { coordinator.start() }
            withTimeout(2_000) {
                while ("main-starting" !in participant.events) delay(5)
            }
            val failure = IllegalStateException("control failed")
            channel.fail(failure)
            withTimeout(2_000) {
                while (coordinator.state() != WarmupCoordinatorState.FAILED) delay(5)
            }
            mainGate.complete(Unit)

            assertFailsWith<IllegalStateException> { withTimeout(2_000) { startup.await() } }
            assertEquals(WarmupCoordinatorState.FAILED, coordinator.state())
        } finally {
            mainGate.complete(Unit)
            coordinator.stop()
        }
    }

    @Test
    fun `owned consumer terminal failure is exposed by coordinator`() = runBlocking {
        val bus = ControlBus()
        val participant = TestParticipant("orders", "orders", "warmup-orders")
        val coordinator = coordinator("first", bus).also { it.register(participant) }

        try {
            coordinator.start()
            val cause = IllegalStateException("consumer failed")
            coordinator.onParticipantFailure(participant.configuration.consumerId, cause)

            val thrown = assertFailsWith<IllegalStateException> {
                withTimeout(2_000) { coordinator.awaitTermination() }
            }
            assertEquals(cause.message, thrown.cause?.cause?.message ?: thrown.cause?.message)
            assertEquals(WarmupCoordinatorState.FAILED, coordinator.state())
        } finally {
            coordinator.stop()
        }
    }

    private fun coordinator(
        instance: String,
        bus: ControlBus,
        load: Double = 0.5,
        stageTimeout: kotlin.time.Duration = 1.seconds,
        channelFactory: (WarmupControlMessageHandler) -> WarmupControlChannel = bus::channel
    ): WarmupCoordinator =
        WarmupCoordinator(
            configuration = WarmupCoordinatorConfiguration(
                instanceId = WarmupInstanceId(instance),
                assignmentLeaseProvider = bus.leases,
                donorSearchTimeout = 20.milliseconds,
                stageTimeout = stageTimeout,
                donorLoadScore = { load }
            ),
            parentContext = Dispatchers.Default,
            channelFactory = channelFactory
        )

    private class ControlBus(
        private val duplicateMessages: Boolean = false,
        private val failSend: (WarmupControlMessage) -> Boolean = { false }
    ) {
        private val channels = CopyOnWriteArrayList<TestChannel>()
        val leases = TestLeaseProvider()

        fun channel(handler: WarmupControlMessageHandler): WarmupControlChannel =
            TestChannel(handler, this).also(channels::add)

        suspend fun publish(message: WarmupControlMessage): RecordMetadata {
            if (failSend(message)) throw IllegalStateException("send failed for ${message.javaClass.simpleName}")
            val deliveries = if (duplicateMessages) 2 else 1
            repeat(deliveries) {
                channels.filter { it.active }.forEach { channel -> channel.handler.onMessage(message) }
            }
            return mock()
        }
    }

    private class TestChannel(
        val handler: WarmupControlMessageHandler,
        private val bus: ControlBus,
        private val readyOnStart: Boolean = true
    ) : WarmupControlChannel {
        private val ready = CompletableDeferred<Unit>()
        private val termination = CompletableDeferred<Unit>()
        @Volatile var active = false

        override fun start() {
            active = true
            if (readyOnStart) ready.complete(Unit)
        }

        override suspend fun awaitReady() = ready.await()
        override suspend fun send(message: WarmupControlMessage): RecordMetadata = bus.publish(message)
        override suspend fun awaitTermination() = termination.await()
        override suspend fun stop() {
            active = false
            termination.complete(Unit)
        }

        fun fail(error: Throwable) {
            active = false
            termination.completeExceptionally(error)
        }
    }

    private class TestLeaseProvider : WarmupAssignmentLeaseProvider {
        private val lock = Any()
        private var owner: WarmupSessionId? = null
        var active: Int = 0
            private set
        var maximumActive: Int = 0
            private set

        override suspend fun tryAcquire(
            sessionId: WarmupSessionId,
            donorInstanceId: WarmupInstanceId,
            recipientInstanceId: WarmupInstanceId,
            participants: List<WarmupParticipantConfiguration>
        ): WarmupAssignmentLease? = synchronized(lock) {
            if (owner != null) return@synchronized null
            owner = sessionId
            active++
            maximumActive = maxOf(maximumActive, active)
            val released = AtomicBoolean(false)
            val assignments = participants.flatMap { participant ->
                participant.topicMappings.map { mapping ->
                    WarmupPartitionAssignment(
                        participant.consumerId,
                        mapping.sourceTopic,
                        mapping.warmupTopic,
                        0
                    )
                }
            }
            object : WarmupAssignmentLease {
                override val assignments = assignments
                override suspend fun release() {
                    if (!released.compareAndSet(false, true)) return
                    synchronized(lock) {
                        check(owner == sessionId)
                        owner = null
                        active--
                    }
                }
            }
        }
    }

    private class TestParticipant(
        val id: String,
        sourceTopic: String,
        warmupTopic: String,
        private val lifecycleEvents: MutableList<String> = CopyOnWriteArrayList(),
        hold: kotlin.time.Duration = 0.milliseconds,
        private val mainFailure: Throwable? = null,
        private val recipientFailure: Throwable? = null,
        private val mainGate: CompletableDeferred<Unit>? = null
    ) : CoordinatorParticipant {
        override val configuration = WarmupParticipantConfiguration(
            WarmupConsumerId(id),
            listOf(WarmupTopicMapping(sourceTopic, warmupTopic)),
            WarmupForwardingProfile(rampUpDuration = 0.milliseconds, holdDuration = hold)
        )
        val events = CopyOnWriteArrayList<String>()
        private lateinit var assignment: List<WarmupPartitionAssignment>

        override suspend fun startMain() {
            if (mainGate != null) {
                events += "main-starting"
                mainGate.await()
            }
            mainFailure?.let { throw it }
            events += "main"
        }

        override fun capability(requested: WarmupParticipantConfiguration) = WarmupParticipantCapability(
            configuration.consumerId,
            requested.topicMappings.map {
                WarmupTopicCapability(it.sourceTopic, compatible = true, ownedPartitions = setOf(0), observedMessages = 1)
            }
        )

        override suspend fun reserveDonor(assignment: List<WarmupPartitionAssignment>) {
            this.assignment = assignment
            events += "reserved"
        }

        override suspend fun releaseDonorReservation() { events += "reservation-released" }

        override suspend fun activateDonor(sessionId: WarmupSessionId) { events += "forwarding" }

        override suspend fun completeForwarding(): WarmupForwardingBoundary {
            events += "boundary"
            return WarmupForwardingBoundary(assignment.associate { it.topicPartition to 4L })
        }

        override suspend fun finishDonor(sessionId: WarmupSessionId) { events += "released" }

        override suspend fun startRecipient(assignment: List<WarmupPartitionAssignment>) {
            this.assignment = assignment
            recipientFailure?.let { throw it }
            events += "recipient"
        }

        override suspend fun drainRecipient(boundary: WarmupForwardingBoundary) {
            events += "drained"
            lifecycleEvents += "$id:drained"
        }

        override suspend fun startMainAfterDrain() {
            events += "main-after-drain"
            lifecycleEvents += "$id:main-after-drain"
        }

        override suspend fun prepareFallback() { events += "fallback-prepared" }

        override suspend fun stop() { events += "stopped" }
    }
}
