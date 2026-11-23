package avh.ckc.core.warmup

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.apache.kafka.clients.producer.RecordMetadata
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalCoroutinesApi::class)
class WarmupDonorLivenessTest {
    private val recipient = WarmupInstanceId("recipient")
    private val firstDonor = WarmupInstanceId("first-donor")
    private val secondDonor = WarmupInstanceId("second-donor")
    private val request = WarmupRequested(
        WarmupControlNamespace("test"), WarmupSessionId("current"), recipient,
        listOf(WarmupTopicMapping(WarmupParticipantId("orders"), "orders", "warmup-orders", "mapping")),
        10_000, 10, "context", 20_000
    )

    @Test
    fun `idle donors survive a brief outage and one missing donor ends the session`() = runTest {
        lateinit var liveness: WarmupDonorLiveness
        var silentDonor: WarmupInstanceId? = null
        var controlAvailable = true
        val channel = probeChannel { probe ->
            if (controlAvailable && probe.targetInstanceId != silentDonor) liveness.onReply(probe.asReply())
        }
        liveness = WarmupDonorLiveness(request, setOf(firstDonor, secondDonor), channel, 9.seconds, 3.seconds,
            testScheduler.timeSource)
        val timeout = async { liveness.awaitTimeout() }

        advanceTimeBy(10.seconds)
        assertFalse(timeout.isCompleted)
        controlAvailable = false
        advanceTimeBy(3.seconds)
        controlAvailable = true
        advanceTimeBy(4.seconds)
        assertFalse(timeout.isCompleted)

        silentDonor = secondDonor
        advanceTimeBy(8.seconds)
        runCurrent()
        assertTrue(timeout.isCompleted)
        timeout.await()
    }

    @Test
    fun `a hung send and replayed or foreign replies cannot extend the deadline`() = runTest {
        lateinit var liveness: WarmupDonorLiveness
        var firstReply: WarmupLivenessProbe? = null
        val channel = probeChannel { probe ->
            val reply = probe.asReply()
            if (firstReply == null) {
                firstReply = reply
                liveness.onReply(reply)
            } else {
                liveness.onReply(requireNotNull(firstReply))
                liveness.onReply(reply.copy(sessionId = WarmupSessionId("old-session")))
                liveness.onReply(reply.copy(senderInstanceId = secondDonor))
                liveness.onReply(reply.copy(configurationFingerprint = "other-context"))
                awaitCancellation()
            }
        }
        liveness = WarmupDonorLiveness(request, setOf(firstDonor), channel, 9.seconds, 3.seconds,
            testScheduler.timeSource)
        val timeout = async { liveness.awaitTimeout() }

        advanceTimeBy(8.seconds)
        assertFalse(timeout.isCompleted)
        advanceTimeBy(2.seconds)
        runCurrent()
        assertTrue(timeout.isCompleted)
        timeout.await()
    }

    private fun WarmupLivenessProbe.asReply() =
        copy(senderInstanceId = targetInstanceId, targetInstanceId = senderInstanceId, reply = true)

    private fun probeChannel(onProbe: suspend (WarmupLivenessProbe) -> Unit) = object : WarmupControlChannel {
        override fun start() = Unit
        override suspend fun awaitReady() = Unit
        override fun initialOffsetBoundary(): Long = -1
        override suspend fun awaitTermination() = Unit
        override suspend fun stop() = Unit
        override suspend fun send(message: WarmupControlMessage): RecordMetadata {
            onProbe(message as WarmupLivenessProbe)
            return mock()
        }
    }
}
