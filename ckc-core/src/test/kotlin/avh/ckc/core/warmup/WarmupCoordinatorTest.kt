package avh.ckc.core.warmup

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.mockito.kotlin.mock
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.Test
import kotlin.test.assertEquals
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

    private fun coordinator(instance: String, bus: ControlBus, load: Double = 0.5): WarmupCoordinator =
        WarmupCoordinator(
            configuration = WarmupCoordinatorConfiguration(
                instanceId = WarmupInstanceId(instance),
                donorSearchTimeout = 20.milliseconds,
                stageTimeout = 1.seconds,
                donorLoadScore = { load }
            ),
            parentContext = Dispatchers.Default,
            channelFactory = bus::channel
        )

    private class ControlBus(private val duplicateMessages: Boolean = false) {
        private val channels = CopyOnWriteArrayList<TestChannel>()

        fun channel(handler: WarmupControlMessageHandler): WarmupControlChannel =
            TestChannel(handler, this).also(channels::add)

        suspend fun publish(message: WarmupControlMessage): RecordMetadata {
            val deliveries = if (duplicateMessages) 2 else 1
            repeat(deliveries) {
                channels.filter { it.active }.forEach { channel -> channel.handler.onMessage(message) }
            }
            return mock()
        }
    }

    private class TestChannel(
        val handler: WarmupControlMessageHandler,
        private val bus: ControlBus
    ) : WarmupControlChannel {
        private val ready = CompletableDeferred<Unit>()
        @Volatile var active = false

        override fun start() {
            active = true
            ready.complete(Unit)
        }

        override suspend fun awaitReady() = ready.await()
        override suspend fun send(message: WarmupControlMessage): RecordMetadata = bus.publish(message)
        override suspend fun awaitTermination() = CompletableDeferred<Unit>().await()
        override suspend fun stop() { active = false }
    }

    private class TestParticipant(
        val id: String,
        sourceTopic: String,
        warmupTopic: String,
        private val lifecycleEvents: MutableList<String> = CopyOnWriteArrayList()
    ) : CoordinatorParticipant {
        override val configuration = WarmupParticipantConfiguration(
            WarmupConsumerId(id),
            listOf(WarmupTopicMapping(sourceTopic, warmupTopic)),
            WarmupForwardingProfile(rampUpDuration = 0.milliseconds, holdDuration = 0.milliseconds)
        )
        val events = CopyOnWriteArrayList<String>()
        private lateinit var assignment: List<WarmupPartitionAssignment>

        override suspend fun startMain() { events += "main" }

        override fun capability(requested: WarmupParticipantConfiguration) = WarmupParticipantCapability(
            configuration.consumerId,
            requested.topicMappings.map {
                WarmupTopicCapability(it.sourceTopic, compatible = true, ownedPartitions = setOf(0), observedMessages = 1)
            }
        )

        override fun reserveDonor(assignment: List<WarmupPartitionAssignment>) {
            this.assignment = assignment
            events += "reserved"
        }

        override suspend fun activateDonor(sessionId: WarmupSessionId) { events += "forwarding" }

        override suspend fun completeForwarding(): WarmupForwardingBoundary {
            events += "boundary"
            return WarmupForwardingBoundary(assignment.associate { it.topicPartition to 4L })
        }

        override suspend fun finishDonor(sessionId: WarmupSessionId) { events += "released" }

        override suspend fun startRecipient(assignment: List<WarmupPartitionAssignment>) {
            this.assignment = assignment
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

        override suspend fun stop() { events += "stopped" }
    }
}
