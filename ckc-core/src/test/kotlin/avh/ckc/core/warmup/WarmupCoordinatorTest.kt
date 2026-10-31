package avh.ckc.core.warmup

import kotlinx.coroutines.CompletableDeferred
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
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds

class WarmupCoordinatorTest {
    @Test
    fun `active coordinator owns assignment and ambiguous decision send still completes from journal`() = runBlocking {
        val bus = ControlBus(throwAfterPublish = { it is AssignmentProposed })
        val donorParticipant = TestParticipant()
        val recipientParticipant = TestParticipant()
        val donor = coordinator("donor", bus).also { it.register(donorParticipant) }
        val recipient = coordinator("recipient", bus).also { it.register(recipientParticipant) }
        try {
            assertEquals(WarmupStartupResult.NO_DONOR_FALLBACK, donor.start())
            assertEquals(
                WarmupStartupResult.WARMED,
                withTimeout(3_000) { recipient.start() },
                (bus.messages.map { it::class.simpleName }.toString() + " view=" + recipient.controlPlaneView())
            )
            assertEquals(2, bus.messages.count { it is AssignmentProposed })
            assertEquals(listOf("fallback-prepared", "main", "reserved", "forwarding", "boundary", "released"), donorParticipant.events)
            assertEquals(listOf("recipient", "drained", "main-after-drain"), recipientParticipant.events)
        } finally {
            recipient.stop(); donor.stop()
        }
    }

    @Test
    fun `two recipients cannot acquire the same journal reserved partition`() = runBlocking {
        val bus = ControlBus()
        val donor = coordinator("donor", bus).also { it.register(TestParticipant(hold = 100.milliseconds)) }
        val recipients = listOf("one", "two").map { id ->
            coordinator("recipient-$id", bus).also { it.register(TestParticipant(hold = 100.milliseconds)) }
        }
        try {
            donor.start()
            val results = recipients.map { async { it.start() } }.awaitAll()
            assertEquals(1, results.count { it == WarmupStartupResult.WARMED })
            assertEquals(1, results.count { it == WarmupStartupResult.NO_DONOR_FALLBACK })
            val assignments = bus.messages.filterIsInstance<AssignmentProposed>()
            assertTrue(assignments.isNotEmpty())
        } finally {
            recipients.forEach { it.stop() }; donor.stop()
        }
    }

    @Test
    fun `recipient startup failure performs cleanup and cold starts main before forwarding`() = runBlocking {
        val bus = ControlBus()
        val donorParticipant = TestParticipant()
        val recipientParticipant = TestParticipant(recipientFailure = IllegalStateException("recipient failed"))
        val donor = coordinator("donor", bus).also { it.register(donorParticipant) }
        val recipient = coordinator("recipient", bus).also { it.register(recipientParticipant) }
        try {
            donor.start()
            assertEquals(WarmupStartupResult.NO_DONOR_FALLBACK, withTimeout(3_000) { recipient.start() })
            assertEquals(listOf("fallback-prepared", "main"), recipientParticipant.events)
            assertTrue("forwarding" !in donorParticipant.events)
            delay(200)
            assertTrue(
                "reservation-released" in donorParticipant.events,
                "events=${donorParticipant.events}, messages=${bus.messages.map { it::class.simpleName }}"
            )
        } finally {
            recipient.stop(); donor.stop()
        }
    }

    private fun coordinator(id: String, bus: ControlBus) = WarmupCoordinator(
        WarmupCoordinatorConfiguration(
            WarmupInstanceId(id), donorSearchTimeout = 20.milliseconds,
            stageTimeout = 120.milliseconds, heartbeatInterval = 20.milliseconds,
            electionTimeout = 80.milliseconds
        ), Dispatchers.Default, bus::channel
    )

    private class ControlBus(private val throwAfterPublish: (WarmupControlMessage) -> Boolean = { false }) {
        private val channels = CopyOnWriteArrayList<TestChannel>()
        val messages = CopyOnWriteArrayList<WarmupControlMessage>()
        private var offset = 0L

        fun channel(handler: WarmupControlMessageHandler): WarmupControlChannel = synchronized(this) {
            TestChannel(handler, this).also(channels::add)
        }

        @Synchronized fun endOffset(): Long = offset

        suspend fun publish(message: WarmupControlMessage): RecordMetadata {
            val record = synchronized(this) {
                messages += message
                WarmupControlRecord(offset++, message)
            }
            channels.filter(TestChannel::active).forEach { it.handler.onMessage(record) }
            if (throwAfterPublish(message)) throw IllegalStateException("ambiguous send")
            return mock()
        }
    }

    private class TestChannel(
        val handler: WarmupControlMessageHandler,
        private val bus: ControlBus
    ) : WarmupControlChannel {
        private val ready = CompletableDeferred<Unit>()
        private val terminated = CompletableDeferred<Unit>()
        private var startOffset = -1L
        @Volatile var active = false
        override fun start() { startOffset = bus.endOffset(); active = true; ready.complete(Unit) }
        override suspend fun awaitReady() = ready.await()
        override fun logPosition() = WarmupControlLogPosition(TopicPartition("control", 0), 0, startOffset)
        override suspend fun send(message: WarmupControlMessage) = bus.publish(message)
        override suspend fun awaitTermination() = terminated.await()
        override suspend fun stop() { active = false; terminated.complete(Unit) }
    }

    private class TestParticipant(
        private val hold: kotlin.time.Duration = 0.milliseconds,
        private val recipientFailure: Throwable? = null
    ) : CoordinatorParticipant {
        override val configuration = WarmupParticipantConfiguration(
            WarmupConsumerId("orders"), listOf(WarmupTopicMapping("orders", "warmup-orders")),
            WarmupForwardingProfile(1.0, 1.0, 0.milliseconds, hold)
        )
        val events = CopyOnWriteArrayList<String>()
        private lateinit var assignment: List<WarmupPartitionAssignment>
        override suspend fun startMain() { events += "main" }
        override fun capability(requested: WarmupParticipantConfiguration) = WarmupParticipantCapability(
            configuration.consumerId, listOf(WarmupTopicCapability("orders", true, setOf(0), 10))
        )
        override suspend fun reserveDonor(assignment: List<WarmupPartitionAssignment>) {
            this.assignment = assignment; events += "reserved"
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
        override suspend fun drainRecipient(boundary: WarmupForwardingBoundary) { events += "drained" }
        override suspend fun startMainAfterDrain() { events += "main-after-drain" }
        override suspend fun prepareFallback(knownFailure: Throwable) { events += "fallback-prepared" }
        override suspend fun stop() = Unit
    }
}
