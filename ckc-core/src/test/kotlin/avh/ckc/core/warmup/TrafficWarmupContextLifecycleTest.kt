package avh.ckc.core.warmup

import avh.ckc.core.CoroutinesKafkaConsumer
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import kotlin.time.Duration.Companion.seconds

class TrafficWarmupContextLifecycleTest {
    @Test
    fun `stop owns and closes a launched main runtime before delayed readiness`() = runBlocking {
        val fallbackReasons = mutableListOf<String?>()
        val context = TrafficWarmupContext(TrafficWarmupContextConfiguration().apply {
            instanceId = "lifecycle-test"
            namespace = "global5-v6-lifecycle-test"
            controlTopic = "warmup-control"
            participantStartTimeout = 1.seconds
            onLifecycleEvent { event ->
                if (event.phase == TrafficWarmupLifecyclePhase.COLD_FALLBACK) {
                    fallbackReasons += event.reasonCode
                }
            }
        })
        val first = DelayedMainParticipant("first", "source-first", "shared-warmup")
        val second = DelayedMainParticipant("second", "source-second", "shared-warmup")
        val firstLifecycle = context.register(first)
        val secondLifecycle = context.register(second)

        firstLifecycle.start()
        secondLifecycle.start()
        withTimeout(5.seconds) {
            first.launched.await()
            second.launched.await()
        }
        second.allowReadiness.complete(Unit)
        withTimeout(5.seconds) { secondLifecycle.awaitReady() }

        withTimeout(5.seconds) { firstLifecycle.stop() }

        assertTrue(first.stopped.isCompleted)
        assertFalse(first.allowReadiness.isCompleted)
        assertTrue(runCatching { firstLifecycle.awaitReady() }.exceptionOrNull() is CancellationException)
        assertEquals(listOf("protocol_initialization_failed"), fallbackReasons)

        secondLifecycle.stop()
    }

    private class DelayedMainParticipant(
        id: String,
        sourceTopic: String,
        warmupTopic: String
    ) : RegisteredTrafficWarmupParticipant {
        override val participantId = WarmupParticipantId(id)
        override val mappings = listOf(
            WarmupTopicMapping(participantId, sourceTopic, warmupTopic, "$id-fingerprint")
        )
        override val warmupConsumerProperties: Map<String, Any?> = emptyMap()
        override val mainConsumer: CoroutinesKafkaConsumer<*, *> = mock<CoroutinesKafkaConsumer<Any?, Any?>>()
        val launched = CompletableDeferred<Unit>()
        val allowReadiness = CompletableDeferred<Unit>()
        val stopped = CompletableDeferred<Unit>()

        override fun prepareDonorLane(acceptance: SessionAccepted): WarmupDonorSessionLifecycle =
            error("Protocol initialization must fail before donor preparation")

        override suspend fun prepareRecipient(
            sessionId: WarmupSessionId,
            start: WarmupDestinationStart,
            completionPublisher: WarmupCompletionPublisher
        ): PreparedWarmupRecipientConsumer = error("Protocol initialization must fail before recipient preparation")

        override fun startMainRuntime() {
            launched.complete(Unit)
        }

        override suspend fun awaitMainReady() {
            allowReadiness.await()
        }

        override suspend fun stopMain() {
            stopped.complete(Unit)
        }

        override suspend fun awaitMainTermination() = Unit
    }
}
