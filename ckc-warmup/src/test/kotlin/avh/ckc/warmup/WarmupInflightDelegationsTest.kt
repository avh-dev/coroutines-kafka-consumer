package avh.ckc.warmup

import avh.ckc.core.metrics.RecordHandoffReason
import avh.ckc.core.processing.RecordProcessingOutcome
import avh.ckc.warmup.protocol.WarmupMappingQuota
import avh.ckc.warmup.protocol.WarmupParticipantId
import avh.ckc.warmup.protocol.WarmupRecordCompleted
import avh.ckc.warmup.protocol.WarmupRecordId
import avh.ckc.warmup.protocol.WarmupRemoteOutcome
import avh.ckc.warmup.protocol.WarmupSessionId
import avh.ckc.warmup.protocol.WarmupTopicMapping
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

@OptIn(ExperimentalCoroutinesApi::class)
class WarmupInflightDelegationsTest {
    @Test
    fun `both ACK and result waits occupy capacity and a draining session releases it`() = runTest {
        val capacity = Semaphore(2)
        val plane = WarmupParticipantDataPlane<String, String>()
        val first = ControlledMapping(capacity, "first", plane = plane)
        val second = ControlledMapping(capacity, "second", plane = plane)

        val firstRecord = async { first.process(0) }
        val firstSend = first.sends.receive()
        val secondRecord = async { first.process(1) }
        val secondSend = second.sends.receive()
        assertEquals(0, first.session.pendingResultCount() + second.session.pendingResultCount())
        assertEquals(0, capacity.availablePermits)
        assertEquals(RecordProcessingOutcome.HandlerSucceeded, first.process(2))
        assertTrue(first.sends.tryReceive().isFailure)
        assertTrue(second.sends.tryReceive().isFailure)

        firstSend.acknowledge()
        secondSend.acknowledge()
        runCurrent()
        assertEquals(2, first.session.pendingResultCount() + second.session.pendingResultCount())
        assertEquals(0, capacity.availablePermits)
        assertEquals(RecordProcessingOutcome.HandlerSucceeded, first.process(3))

        first.complete(firstSend)
        assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, firstRecord.await())
        val drainingRecord = async { first.process(4) }
        val drainingSend = first.sends.receive()
        first.lane.stopDelegating()
        assertFalse(first.lane.trySealForRemoval())
        second.complete(secondSend)
        assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, secondRecord.await())

        val next = async { first.process(5) }
        val nextSend = second.sends.receive()
        nextSend.acknowledge()
        second.complete(nextSend)
        assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, next.await())
        val whileDraining = async { first.process(6) }
        val whileDrainingSend = second.sends.receive()
        assertTrue(first.sends.tryReceive().isFailure)
        drainingSend.acknowledge()
        first.complete(drainingSend)
        assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, drainingRecord.await())
        assertTrue(first.lane.trySealForRemoval())
        first.lane.close()

        val afterDrain = async { second.process(7) }
        val afterDrainSend = second.sends.receive()
        assertEquals(0, capacity.availablePermits)
        listOf(whileDrainingSend, afterDrainSend).forEach { send ->
            send.acknowledge()
            second.complete(send)
        }
        assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, whileDraining.await())
        assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, afterDrain.await())
        assertEquals(2, capacity.availablePermits)
    }

    @Test
    fun `participants in different sessions race for the last place without spending the losing quota`() =
        runBlocking {
            withTimeout(10.seconds) {
                val capacity = Semaphore(2)
                val first = ControlledMapping(capacity, "first", source = "orders", quota = 2)
                val second = ControlledMapping(capacity, "second", source = "batches", quota = 1)
                val occupied = async { first.process(0) }
                val heldSend = first.sends.receive()
                val ready = Channel<Unit>(2)
                val start = CompletableDeferred<Unit>()
                val refused = Channel<ControlledMapping>(2)
                val contenders = listOf(first, second).associateWith { mapping ->
                    async(Dispatchers.Default) {
                        ready.send(Unit)
                        start.await()
                        mapping.process(1) {
                            refused.send(mapping)
                            RecordProcessingOutcome.HandlerSucceeded
                        }
                    }
                }
                repeat(2) { ready.receive() }
                start.complete(Unit)
                val loser = refused.receive()
                val winner = contenders.keys.single { it != loser }
                val winningSend = winner.sends.receive()
                assertEquals(RecordProcessingOutcome.HandlerSucceeded, contenders.getValue(loser).await())
                assertEquals(0, capacity.availablePermits)
                assertTrue(loser.sends.tryReceive().isFailure)

                winningSend.acknowledge()
                winner.complete(winningSend)
                assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, contenders.getValue(winner).await())
                val retry = async { loser.process(2) }
                val retrySend = loser.sends.receive()
                assertEquals(0, capacity.availablePermits)
                assertEquals(WarmupResultTracker.Completion.DUPLICATE, winner.complete(winningSend))
                assertEquals(0, capacity.availablePermits)

                heldSend.acknowledge()
                first.complete(heldSend)
                retrySend.acknowledge()
                loser.complete(retrySend)
                assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, occupied.await())
                assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, retry.await())
                assertEquals(2, capacity.availablePermits)
            }
        }

    @Test
    fun `send failure and result timeout retain capacity through fallback and handoff`() = runTest {
        val capacity = Semaphore(2)
        val failing = ControlledMapping(capacity, "send-failure")
        val timingOut = ControlledMapping(capacity, "result-timeout")
        val healthy = ControlledMapping(capacity, "healthy")
        val finishFallback = CompletableDeferred<Unit>()
        val handedOff = RecordProcessingOutcome.HandedOff(RecordHandoffReason.ASSIGNMENT_ENDED)
        val sendFailure = async {
            failing.process(0) {
                finishFallback.await()
                RecordProcessingOutcome.HandlerSucceeded
            }
        }
        val timeout = async {
            timingOut.process(0) {
                finishFallback.await()
                handedOff
            }
        }
        failing.sends.receive().ack.completeExceptionally(IllegalStateException("send failed"))
        val timedOutSend = timingOut.sends.receive()
        timedOutSend.acknowledge()
        runCurrent()
        assertEquals(1, timingOut.session.pendingResultCount())

        advanceTimeBy(10.seconds.inWholeMilliseconds)
        runCurrent()
        assertFalse(failing.session.isActive())
        assertFalse(timingOut.session.isActive())
        assertEquals(0, capacity.availablePermits)
        assertEquals(RecordProcessingOutcome.HandlerSucceeded, healthy.process(0))
        assertTrue(healthy.sends.tryReceive().isFailure)
        assertEquals(WarmupResultTracker.Completion.IGNORED, timingOut.complete(timedOutSend))
        assertEquals(0, capacity.availablePermits)

        finishFallback.complete(Unit)
        assertEquals(RecordProcessingOutcome.HandlerSucceeded, sendFailure.await())
        assertEquals(handedOff, timeout.await())
        assertEquals(2, capacity.availablePermits)
        assertEquals(WarmupResultTracker.Completion.IGNORED, timingOut.complete(timedOutSend))
        assertEquals(2, capacity.availablePermits)
    }

    @Test
    fun `cancellation during either wait returns capacity exactly once`() = runTest {
        val capacity = Semaphore(2)
        val mapping = ControlledMapping(capacity, "cancelled")
        val awaitingAck = async { mapping.process(0) { error("Cancelled record ran locally") } }
        val ackSend = mapping.sends.receive()
        val awaitingResult = async { mapping.process(1) { error("Cancelled record ran locally") } }
        val resultSend = mapping.sends.receive()
        resultSend.acknowledge()
        runCurrent()
        assertEquals(1, mapping.session.pendingResultCount())
        assertEquals(0, capacity.availablePermits)

        awaitingAck.cancelAndJoin()
        assertEquals(1, capacity.availablePermits)
        ackSend.acknowledge()
        awaitingResult.cancelAndJoin()
        assertEquals(2, capacity.availablePermits)
        mapping.complete(resultSend)
        mapping.complete(resultSend)
        assertEquals(2, capacity.availablePermits)
        assertTrue(mapping.session.isActive())
    }

    private class ControlledMapping(
        capacity: Semaphore,
        id: String,
        private val source: String = "source",
        private val plane: WarmupParticipantDataPlane<String, String> = WarmupParticipantDataPlane(),
        quota: Int = 10
    ) {
        private val sessionId = WarmupSessionId(id)
        private val mapping = WarmupTopicMapping(
            WarmupParticipantId(source), source, "warmup-$source", "fingerprint-$source", WarmupMappingQuota(quota)
        )
        val sends = Channel<PendingSend>(Channel.UNLIMITED)
        val session = WarmupDonorSession(
            sessionId = sessionId,
            quotaProfile = mapping.quota.profile,
            quotaInterval = 1.seconds,
            delegationTimeout = 10.seconds,
            destinationResolver = WarmupDestinationResolver<String, String> { WarmupDestination(mapping.warmupTopic) },
            sender = WarmupRecordSender { record, destination, sessionId ->
                val recordId = WarmupRecordId(sessionId, TopicPartition(destination.topic, 0), record.offset())
                val send = PendingSend(recordId)
                sends.trySend(send).getOrThrow()
                send.ack.await()
            },
            nanoTime = { 0L },
            inflightDelegations = capacity
        )
        val lane = WarmupParticipantDonorLane(sessionId, plane, mapOf(mapping to session)).also {
            check(it.activate())
        }

        suspend fun process(
            offset: Long,
            processLocally: suspend () -> RecordProcessingOutcome = { RecordProcessingOutcome.HandlerSucceeded }
        ) = plane.process(ConsumerRecord(source, 0, offset, "key", "value"), processLocally)

        fun complete(send: PendingSend) =
            lane.acceptResult(WarmupRecordCompleted(send.recordId, WarmupRemoteOutcome.Succeeded))
    }

    private class PendingSend(val recordId: WarmupRecordId) {
        val ack = CompletableDeferred<WarmupSendAcknowledgement>()

        fun acknowledge() {
            ack.complete(WarmupSendAcknowledgement(recordId.topicPartition, recordId.offset))
        }
    }
}
