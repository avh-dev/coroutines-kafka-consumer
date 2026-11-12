package avh.ckc.core.warmup

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.time.Duration.Companion.milliseconds

class TrafficWarmupContextTest {
    private val local = WarmupInstanceId("recipient")
    private val donor = WarmupInstanceId("donor")
    private val consumerId = WarmupConsumerId("orders")
    private val mapping = WarmupTopicMapping(consumerId, "orders", "warmup-orders")

    @Test
    fun `complete remote session drains recipient before main starts`() = runBlocking {
        val participant = FakeParticipant(consumerId, listOf(mapping))
        lateinit var channel: FakeControlChannel
        val context = context { handler ->
            FakeControlChannel(handler) { message, control ->
                when (message) {
                    is WarmupRequested -> control.deliver(
                        DonorOffered(
                            message.sessionId,
                            donor,
                            local,
                            listOf(WarmupDonorMapping(mapping, setOf(0))),
                            message.configurationFingerprint,
                            message.expiresAtEpochMillis
                        )
                    )
                    is SessionAccepted -> control.deliver(
                        DonorAccepted(
                            message.sessionId,
                            donor,
                            local,
                            message.configurationFingerprint,
                            message.expiresAtEpochMillis
                        )
                    )
                    is RecipientReady -> control.deliver(
                        ForwardingStopped(
                            message.sessionId,
                            donor,
                            local,
                            listOf(WarmupFinalOffset(TopicPartition("warmup-orders", 0), null)),
                            "budget elapsed",
                            message.configurationFingerprint,
                            message.expiresAtEpochMillis
                        )
                    )
                    else -> Unit
                }
            }.also { channel = it }
        }
        context.register(participant)

        context.start()
        context.start()
        withTimeout(2_000) { context.awaitReady() }

        assertEquals(listOf("recipient-start", "recipient-drain", "recipient-main"), participant.events)
        assertTrue(channel.sent.any { it is SessionEnded && it.state == WarmupSessionEndState.COMPLETED })
        context.stop()
    }

    @Test
    fun `missing donor performs bounded cold start`() = runBlocking {
        val participant = FakeParticipant(consumerId, listOf(mapping))
        val context = context { handler -> FakeControlChannel(handler) }
        context.register(participant)

        context.start()
        withTimeout(2_000) { context.awaitReady() }

        assertEquals(listOf("main"), participant.events)
        context.stop()
    }

    @Test
    fun `unready control transport falls back to main within readiness deadline`() = runBlocking {
        val participant = FakeParticipant(consumerId, listOf(mapping))
        val neverReady = CompletableDeferred<Unit>()
        val context = context { handler -> FakeControlChannel(handler, readiness = neverReady) }
        context.register(participant)

        context.start()
        withTimeout(2_000) { context.awaitReady() }

        assertEquals(listOf("main"), participant.events)
        context.stop()
    }

    @Test
    fun `application stop while discovering never starts main`() = runBlocking {
        val participant = FakeParticipant(consumerId, listOf(mapping))
        lateinit var channel: FakeControlChannel
        val context = context { handler -> FakeControlChannel(handler).also { channel = it } }
        context.register(participant)
        context.start()
        withTimeout(2_000) { channel.firstRequest.await() }

        context.stop()

        assertFalse(participant.mainStarted)
        assertFalse(participant.recipientMainStarted)
    }

    @Test
    fun `cold started context remains available as donor for a later pod`() = runBlocking {
        val participant = DonorParticipant(consumerId, listOf(mapping))
        lateinit var channel: FakeControlChannel
        val context = context { handler -> FakeControlChannel(handler).also { channel = it } }
        context.register(participant)
        context.start()
        withTimeout(2_000) { context.awaitReady() }
        val localRequest = channel.sent.filterIsInstance<WarmupRequested>().single()
        val session = WarmupSessionId("later-recipient")
        val laterRecipient = WarmupInstanceId("later-recipient")
        val expires = System.currentTimeMillis() + 2_000

        channel.deliver(
            WarmupRequested(
                session,
                laterRecipient,
                listOf(mapping),
                System.currentTimeMillis() + 100,
                localRequest.configurationFingerprint,
                expires
            )
        )
        withTimeout(2_000) {
            while (channel.sent.none { it is DonorOffered && it.sessionId == session }) kotlinx.coroutines.yield()
        }
        val assignment = WarmupPartitionAssignment(mapping, 0)
        channel.deliver(
            SessionAccepted(
                session,
                laterRecipient,
                local,
                listOf(assignment),
                System.currentTimeMillis() + 20,
                localRequest.configurationFingerprint,
                expires
            )
        )
        withTimeout(2_000) {
            while (channel.sent.none { it is DonorAccepted && it.sessionId == session }) kotlinx.coroutines.yield()
        }
        channel.deliver(
            RecipientReady(
                session,
                laterRecipient,
                local,
                listOf(assignment),
                localRequest.configurationFingerprint,
                expires
            )
        )
        withTimeout(2_000) {
            while (channel.sent.none { it is ForwardingStopped && it.sessionId == session }) kotlinx.coroutines.yield()
        }

        assertEquals(listOf("prepare", "activate", "stop-sends"), participant.donorEvents)
        context.stop()
    }

    @Test
    fun `remote outcomes are routed only to the donor that owns their destination`() = runBlocking {
        val mappings = listOf(
            WarmupTopicMapping(WarmupConsumerId("orders"), "orders", "warmup-orders"),
            WarmupTopicMapping(WarmupConsumerId("batches"), "batches", "warmup-batches"),
            WarmupTopicMapping(WarmupConsumerId("cauldrons"), "cauldrons", "warmup-cauldrons")
        )
        val participants = mappings.map { mapping -> DonorParticipant(mapping.consumerId, listOf(mapping)) }
        lateinit var channel: FakeControlChannel
        val context = context { handler -> FakeControlChannel(handler).also { channel = it } }
        participants.forEach(context::register)
        context.start()
        withTimeout(2_000) { context.awaitReady() }
        val localRequest = channel.sent.filterIsInstance<WarmupRequested>().single()
        val session = WarmupSessionId("multi-consumer-recipient")
        val recipient = WarmupInstanceId("multi-consumer-recipient")
        val expires = System.currentTimeMillis() + 3_000

        channel.deliver(
            WarmupRequested(
                session,
                recipient,
                mappings,
                System.currentTimeMillis() + 100,
                localRequest.configurationFingerprint,
                expires
            )
        )
        withTimeout(2_000) {
            while (channel.sent.none { it is DonorOffered && it.sessionId == session }) kotlinx.coroutines.yield()
        }
        val assignments = mappings.map { mapping -> WarmupPartitionAssignment(mapping, 0) }
        channel.deliver(
            SessionAccepted(
                session,
                recipient,
                local,
                assignments,
                System.currentTimeMillis() + 1_000,
                localRequest.configurationFingerprint,
                expires
            )
        )
        withTimeout(2_000) {
            while (participants.any { "prepare" !in it.donorEvents }) kotlinx.coroutines.yield()
        }
        channel.deliver(
            RecipientReady(
                session,
                recipient,
                local,
                assignments,
                localRequest.configurationFingerprint,
                expires
            )
        )
        withTimeout(2_000) {
            while (participants.any { "activate" !in it.donorEvents }) kotlinx.coroutines.yield()
        }

        assignments.forEachIndexed { index, assignment ->
            channel.deliver(
                ControlRecordCompleted(
                    session,
                    recipient,
                    local,
                    WarmupRecordCompleted(
                        WarmupRecordId(session, assignment.destination, index.toLong()),
                        WarmupRemoteOutcome.Succeeded
                    ),
                    localRequest.configurationFingerprint,
                    expires
                )
            )
        }
        channel.deliver(
            ControlRecordCompleted(
                session,
                recipient,
                local,
                WarmupRecordCompleted(
                    WarmupRecordId(session, TopicPartition("unknown-warmup-topic", 0), 0),
                    WarmupRemoteOutcome.Succeeded
                ),
                localRequest.configurationFingerprint,
                expires
            )
        )

        participants.zip(assignments).forEachIndexed { index, (participant, assignment) ->
            assertEquals(
                listOf(WarmupRecordId(session, assignment.destination, index.toLong())),
                participant.accepted
            )
        }
        context.stop()
    }

    @Test
    fun `cancelled stop caller does not cancel shared context cleanup`() = runBlocking {
        val stopEntered = CompletableDeferred<Unit>()
        val releaseStop = CompletableDeferred<Unit>()
        val participant = BlockingStopParticipant(consumerId, listOf(mapping), stopEntered, releaseStop)
        val context = context { handler -> FakeControlChannel(handler) }
        context.register(participant)
        context.start()
        withTimeout(2_000) { context.awaitReady() }

        val firstStop = async { runCatching { context.stop() } }
        withTimeout(2_000) { stopEntered.await() }
        firstStop.cancelAndJoin()
        val repeatedStop = async { context.stop() }
        assertFalse(repeatedStop.isCompleted)

        releaseStop.complete(Unit)
        withTimeout(2_000) { repeatedStop.await() }
        assertEquals(1, participant.stopCalls)
    }

    @Test
    fun `main failure is preserved when cleanup reports the same error`() = runBlocking {
        val failure = IllegalStateException("main failed")
        val participant = FailingParticipant(consumerId, listOf(mapping), failure)
        val context = context { handler -> FakeControlChannel(handler) }
        context.register(participant)
        context.start()
        withTimeout(2_000) { context.awaitReady() }

        val terminal = withTimeout(2_000) { runCatching { context.awaitTermination() }.exceptionOrNull() }
        val stopFailure = runCatching { context.stop() }.exceptionOrNull()

        assertSame(failure, terminal)
        assertSame(failure, stopFailure)
    }

    private fun context(
        factory: (WarmupControlMessageHandler) -> WarmupControlChannel
    ): TrafficWarmupContext {
        val configuration = TrafficWarmupContextConfiguration().apply {
            instanceId = local
            controlTopic = "control"
            offerWindow = 20.milliseconds
            negotiationTimeout = 100.milliseconds
            warmupDuration = 20.milliseconds
            delegationTimeout = 100.milliseconds
            cleanupTimeout = 100.milliseconds
            controlReadinessTimeout = 100.milliseconds
            parentContext = Dispatchers.Default
        }
        return TrafficWarmupContext(configuration, factory)
    }

    private class FakeControlChannel(
        private val handler: WarmupControlMessageHandler,
        private val readiness: CompletableDeferred<Unit>? = null,
        private val response: suspend (WarmupControlMessage, FakeControlChannel) -> Unit = { _, _ -> }
    ) : WarmupControlChannel {
        private val offset = AtomicLong()
        val sent = CopyOnWriteArrayList<WarmupControlMessage>()
        val firstRequest = CompletableDeferred<Unit>()
        private var started = false

        override fun start() { started = true }
        override suspend fun awaitReady() {
            check(started)
            readiness?.await()
        }
        override fun logPosition() = WarmupControlLogPosition(TopicPartition("control", 0), 0, 0, 0)

        override suspend fun send(message: WarmupControlMessage): RecordMetadata {
            sent += message
            deliver(message)
            if (message is WarmupRequested) firstRequest.complete(Unit)
            response(message, this)
            return mock()
        }

        suspend fun deliver(message: WarmupControlMessage) {
            handler.onMessage(WarmupControlRecord(offset.getAndIncrement(), message))
        }

        override suspend fun awaitTermination() = Unit
        override suspend fun stop() = Unit
    }

    private class FakeParticipant(
        override val consumerId: WarmupConsumerId,
        override val mappings: List<WarmupTopicMapping>
    ) : RegisteredWarmupParticipant {
        val events = mutableListOf<String>()
        var mainStarted = false
        var recipientMainStarted = false

        override fun donorMappings(requested: Collection<WarmupTopicMapping>) = emptyList<WarmupDonorMapping>()
        override fun prepareDonor(sessionId: WarmupSessionId, assignments: List<WarmupPartitionAssignment>) =
            error("recipient test must not prepare a donor")

        override fun prepareRecipient(
            sessionId: WarmupSessionId,
            assignments: List<WarmupPartitionAssignment>,
            donorByDestination: Map<TopicPartition, WarmupInstanceId>,
            sendCompletion: suspend (WarmupInstanceId, WarmupRecordCompleted) -> Unit
        ) = object : PreparedWarmupRecipient {
            override val assignments = assignments
            override suspend fun start() { events += "recipient-start" }
            override suspend fun drain(finalOffsets: Collection<WarmupFinalOffset>) { events += "recipient-drain" }
            override suspend fun startMain() {
                recipientMainStarted = true
                events += "recipient-main"
            }
            override suspend fun stop() = Unit
        }

        override suspend fun startMain() {
            mainStarted = true
            events += "main"
        }
        override suspend fun stop() = Unit
        override suspend fun awaitTermination() = CompletableDeferred<Unit>().await()
    }

    private class DonorParticipant(
        override val consumerId: WarmupConsumerId,
        override val mappings: List<WarmupTopicMapping>
    ) : RegisteredWarmupParticipant {
        var mainStarted = false
        val donorEvents = CopyOnWriteArrayList<String>()
        val accepted = CopyOnWriteArrayList<WarmupRecordId>()

        override fun donorMappings(requested: Collection<WarmupTopicMapping>): List<WarmupDonorMapping> =
            if (mainStarted && mappings.single() in requested) {
                listOf(WarmupDonorMapping(mappings.single(), setOf(0)))
            } else {
                emptyList()
            }

        override fun prepareDonor(
            sessionId: WarmupSessionId,
            assignments: List<WarmupPartitionAssignment>
        ): PreparedWarmupDonor {
            donorEvents += "prepare"
            return object : PreparedWarmupDonor {
                override val sessionId = sessionId
                override val assignments = assignments
                override fun activate() { donorEvents += "activate" }
                override fun accept(completed: WarmupRecordCompleted): WarmupResultTracker.Completion {
                    accepted += completed.recordId
                    return WarmupResultTracker.Completion.DELIVERED
                }
                override suspend fun stopAndAwaitSends(timeout: kotlin.time.Duration): List<WarmupFinalOffset> {
                    donorEvents += "stop-sends"
                    return assignments.map { WarmupFinalOffset(it.destination, null) }
                }
                override fun finish() { donorEvents += "finish" }
            }
        }

        override fun prepareRecipient(
            sessionId: WarmupSessionId,
            assignments: List<WarmupPartitionAssignment>,
            donorByDestination: Map<TopicPartition, WarmupInstanceId>,
            sendCompletion: suspend (WarmupInstanceId, WarmupRecordCompleted) -> Unit
        ): PreparedWarmupRecipient = error("not expected")

        override suspend fun startMain() { mainStarted = true }
        override suspend fun stop() = Unit
        override suspend fun awaitTermination() = CompletableDeferred<Unit>().await()
    }

    private class BlockingStopParticipant(
        override val consumerId: WarmupConsumerId,
        override val mappings: List<WarmupTopicMapping>,
        private val stopEntered: CompletableDeferred<Unit>,
        private val releaseStop: CompletableDeferred<Unit>
    ) : RegisteredWarmupParticipant {
        var stopCalls = 0
        override fun donorMappings(requested: Collection<WarmupTopicMapping>) = emptyList<WarmupDonorMapping>()
        override fun prepareDonor(sessionId: WarmupSessionId, assignments: List<WarmupPartitionAssignment>) =
            error("not expected")
        override fun prepareRecipient(
            sessionId: WarmupSessionId,
            assignments: List<WarmupPartitionAssignment>,
            donorByDestination: Map<TopicPartition, WarmupInstanceId>,
            sendCompletion: suspend (WarmupInstanceId, WarmupRecordCompleted) -> Unit
        ): PreparedWarmupRecipient = error("not expected")
        override suspend fun startMain() = Unit
        override suspend fun stop() {
            stopCalls++
            stopEntered.complete(Unit)
            releaseStop.await()
        }
        override suspend fun awaitTermination() = CompletableDeferred<Unit>().await()
    }

    private class FailingParticipant(
        override val consumerId: WarmupConsumerId,
        override val mappings: List<WarmupTopicMapping>,
        private val failure: Throwable
    ) : RegisteredWarmupParticipant {
        override fun donorMappings(requested: Collection<WarmupTopicMapping>) = emptyList<WarmupDonorMapping>()
        override fun prepareDonor(sessionId: WarmupSessionId, assignments: List<WarmupPartitionAssignment>) =
            error("not expected")
        override fun prepareRecipient(
            sessionId: WarmupSessionId,
            assignments: List<WarmupPartitionAssignment>,
            donorByDestination: Map<TopicPartition, WarmupInstanceId>,
            sendCompletion: suspend (WarmupInstanceId, WarmupRecordCompleted) -> Unit
        ): PreparedWarmupRecipient = error("not expected")
        override suspend fun startMain() = Unit
        override suspend fun stop(): Unit = throw failure
        override suspend fun awaitTermination(): Unit = throw failure
    }
}
