package avh.ckc.warmup

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.warmup.dataplane.WarmupCompletionOutbox
import avh.ckc.warmup.donor.WarmupDonorQuotaPhases
import avh.ckc.warmup.donor.WarmupDonorSessionLifecycle
import avh.ckc.warmup.kafka.WarmupControlChannel
import avh.ckc.warmup.protocol.SessionAccepted
import avh.ckc.warmup.protocol.WarmupParticipantId
import avh.ckc.warmup.protocol.WarmupSessionId
import avh.ckc.warmup.protocol.WarmupTopicMapping
import avh.ckc.warmup.recipient.PreparedWarmupRecipientConsumer
import avh.ckc.warmup.recipient.WarmupDestinationStart
import avh.ckc.warmup.runtime.RegisteredTrafficWarmupParticipant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

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

    @Test
    fun `terminal main stop error records completed participant cleanup`() = runBlocking {
        val context = fallbackContext("terminal-stop")
        val terminalFailure = IllegalStateException("terminal consumer failure")
        val first = DelayedMainParticipant(
            "first",
            "source-first",
            "shared-warmup",
            mutableListOf(StopAction(terminalFailure, runtimeReleased = true))
        )
        val second = DelayedMainParticipant("second", "source-second", "shared-warmup")
        val firstLifecycle = context.register(first)
        val secondLifecycle = context.register(second)
        startAndRelease(firstLifecycle, secondLifecycle, first, second)

        val error = runCatching { firstLifecycle.stop() }.exceptionOrNull()

        assertEquals(terminalFailure, error)
        assertTrue(firstLifecycle.isStopComplete())
        assertEquals(1, first.stopCalls)
        val repeated = runCatching { firstLifecycle.stop() }.exceptionOrNull()
        assertEquals(terminalFailure.message, requireNotNull(repeated).message)
        assertEquals(1, first.stopCalls)
        secondLifecycle.stop()
    }

    @Test
    fun `incomplete main stop remains retryable until runtime is released`() = runBlocking {
        val context = fallbackContext("retry-stop")
        val firstFailure = IllegalStateException("cleanup incomplete")
        val first = DelayedMainParticipant(
            "first",
            "source-first",
            "shared-warmup",
            mutableListOf(
                StopAction(firstFailure, runtimeReleased = false),
                StopAction(error = null, runtimeReleased = true)
            )
        )
        val second = DelayedMainParticipant("second", "source-second", "shared-warmup")
        val firstLifecycle = context.register(first)
        val secondLifecycle = context.register(second)
        startAndRelease(firstLifecycle, secondLifecycle, first, second)

        assertEquals(firstFailure, runCatching { firstLifecycle.stop() }.exceptionOrNull())
        assertFalse(firstLifecycle.isStopComplete())

        firstLifecycle.stop()

        assertTrue(firstLifecycle.isStopComplete())
        assertEquals(2, first.stopCalls)
        secondLifecycle.stop()
    }

    @Test
    fun `stopping a neighbor before the recipient workflow starts releases the surviving main runtime`() = runBlocking {
        val firstWaiting = CompletableDeferred<Unit>()
        val releaseObserver = CountDownLatch(1)
        val fallback = CompletableDeferred<String?>()
        val first = DelayedMainParticipant("first", "source-first", "warmup-first")
        val second = DelayedMainParticipant("second", "source-second", "warmup-second")
        val context = TrafficWarmupContext(TrafficWarmupContextConfiguration().apply {
            namespace = "stop-before-recipient-start"
            controlTopic = "warmup-control"
            onLifecycleEvent { event ->
                if (event.phase == TrafficWarmupLifecyclePhase.WAITING_FOR_PARTICIPANTS &&
                    event.participantId == first.participantId.value
                ) {
                    firstWaiting.complete(Unit)
                    check(releaseObserver.await(10, TimeUnit.SECONDS)) { "Observer was not released" }
                }
                if (event.phase == TrafficWarmupLifecyclePhase.COLD_FALLBACK) {
                    fallback.complete(event.reasonCode)
                }
            }
        }) { _, _, _, _ -> mock<WarmupControlChannel>() }
        val firstLifecycle = context.register(first)
        val secondLifecycle = context.register(second)
        val firstStart = async(Dispatchers.Default) { firstLifecycle.start() }

        try {
            withTimeout(5.seconds) { firstWaiting.await() }
            secondLifecycle.start()
            withTimeout(5.seconds) { secondLifecycle.stop() }
            releaseObserver.countDown()
            withTimeout(5.seconds) {
                firstStart.await()
                first.allowReadiness.complete(Unit)
                firstLifecycle.awaitReady()
                assertEquals("participant_stopped", fallback.await())
            }
            assertEquals(1, first.startCalls)
            assertFalse(second.launched.isCompleted)
        } finally {
            releaseObserver.countDown()
            try {
                secondLifecycle.stop()
            } finally {
                firstLifecycle.stop()
            }
        }
    }

    private fun fallbackContext(id: String): TrafficWarmupContext =
        TrafficWarmupContext(TrafficWarmupContextConfiguration().apply {
            instanceId = id
            namespace = "global5-v6-$id"
            controlTopic = "warmup-control"
            participantStartTimeout = 1.seconds
        })

    private suspend fun startAndRelease(
        firstLifecycle: avh.ckc.core.extension.ManagedConsumerLifecycle,
        secondLifecycle: avh.ckc.core.extension.ManagedConsumerLifecycle,
        first: DelayedMainParticipant,
        second: DelayedMainParticipant
    ) {
        firstLifecycle.start()
        secondLifecycle.start()
        withTimeout(5.seconds) {
            first.launched.await()
            second.launched.await()
        }
        first.allowReadiness.complete(Unit)
        second.allowReadiness.complete(Unit)
        withTimeout(5.seconds) {
            firstLifecycle.awaitReady()
            secondLifecycle.awaitReady()
        }
    }

    private data class StopAction(
        val error: Throwable?,
        val runtimeReleased: Boolean
    )

    private class DelayedMainParticipant(
        id: String,
        sourceTopic: String,
        warmupTopic: String,
        private val stopActions: MutableList<StopAction> = mutableListOf(
            StopAction(error = null, runtimeReleased = true)
        )
    ) : RegisteredTrafficWarmupParticipant {
        override val participantId = WarmupParticipantId(id)
        override val mappings = listOf(
            WarmupTopicMapping(participantId, sourceTopic, warmupTopic, "$id-fingerprint")
        )
        override val warmupConsumerProperties: Map<String, Any?> = emptyMap()
        override val mainTopics = listOf(mappings.single().sourceTopic)
        override val mainProperties: Map<String, Any?> = emptyMap()
        override val forwardingProducerProperties: Map<String, Any?> = emptyMap()
        override val mainConsumer: CoroutinesKafkaConsumer<*, *> = mock<CoroutinesKafkaConsumer<Any?, Any?>>()
        val launched = CompletableDeferred<Unit>()
        val allowReadiness = CompletableDeferred<Unit>()
        val stopped = CompletableDeferred<Unit>()
        var startCalls = 0
            private set
        var stopCalls = 0
            private set
        private var runtimeReleased = false

        init {
            val runtimeControl = mock<avh.ckc.core.extension.ConsumerRuntimeControl>()
            whenever(runtimeControl.isShutdownComplete()).thenAnswer { runtimeReleased }
            whenever(mainConsumer.runtimeControl).thenReturn(runtimeControl)
        }

        override fun prepareDonorLane(
            acceptance: SessionAccepted,
            quotaPhases: WarmupDonorQuotaPhases.Session
        ): WarmupDonorSessionLifecycle =
            error("Protocol initialization must fail before donor preparation")

        override suspend fun prepareRecipient(
            sessionId: WarmupSessionId,
            start: WarmupDestinationStart,
            completionOutbox: WarmupCompletionOutbox
        ): PreparedWarmupRecipientConsumer = error("Protocol initialization must fail before recipient preparation")

        override fun startMainRuntime() {
            startCalls += 1
            launched.complete(Unit)
        }

        override suspend fun awaitMainReady() {
            allowReadiness.await()
        }

        override suspend fun stopMain() {
            stopCalls += 1
            val action = stopActions.removeFirstOrNull()
                ?: StopAction(error = null, runtimeReleased = runtimeReleased)
            runtimeReleased = action.runtimeReleased
            if (runtimeReleased) stopped.complete(Unit)
            action.error?.let { throw it }
        }

        override suspend fun awaitMainTermination() = Unit
    }
}
