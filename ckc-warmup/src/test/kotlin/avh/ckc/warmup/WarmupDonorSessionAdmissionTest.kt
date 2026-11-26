package avh.ckc.warmup

import avh.ckc.core.awaitFor
import avh.ckc.core.processing.RecordProcessingOutcome
import avh.ckc.warmup.protocol.DonorAccepted
import avh.ckc.warmup.protocol.SessionAccepted
import avh.ckc.warmup.protocol.SessionRejected
import avh.ckc.warmup.protocol.WarmupDonorMapping
import avh.ckc.warmup.protocol.WarmupLivenessProbe
import avh.ckc.warmup.protocol.WarmupSessionId
import avh.ckc.warmup.protocol.WarmupSourcePartition
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

internal class WarmupDonorSessionAdmissionTest : WarmupDonorSessionFixture() {
    @Test
    fun `readiness received during preparation activates exactly once after preparation`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val plane = WarmupDonorDataPlaneFixture<String, String>(2, 20)
        val prepared = CompletableDeferred<WarmupDonorSession<String, String>>()
        val activation = activation(scope, plane) { prepared.await() }
        val acceptance = acceptance()

        assertInstanceOf(DonorAccepted::class.java, acceptAndStart(activation, acceptance))
        assertTrue(activation.onRecipientReady(ready()))
        assertEquals(WarmupDonorSessionManager.State.PREPARING, activation.state(acceptance.sessionId))

        prepared.complete(session(acceptance.sessionId))
        awaitFor(2_000, 10) {
            activation.state(acceptance.sessionId).takeIf { it == WarmupDonorSessionManager.State.ACTIVE }
        }

        assertTrue(activation.onRecipientReady(ready()))
        assertEquals(1, plane.store.admittedSessionCount())
        plane.store.stopDelegating(acceptance.sessionId)
        assertTrue(plane.removeDrainedSession(acceptance.sessionId))
        scope.cancel()
    }

    @Test
    fun `cancellation winning preparation closes late resources and saved readiness cannot revive session`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val plane = WarmupDonorDataPlaneFixture<String, String>(1, 10)
        val preparationStarted = CountDownLatch(1)
        val releasePreparation = CountDownLatch(1)
        val closed = AtomicInteger()
        val lateSession = session(acceptance().sessionId, closed)
        val activation = activation(scope, plane) {
            preparationStarted.countDown()
            releasePreparation.await()
            lateSession
        }
        val acceptance = acceptance()
        acceptAndStart(activation, acceptance)
        assertTrue(activation.onRecipientReady(ready()))
        preparationStarted.await()

        assertTrue(activation.onSessionEnded(fallbackEnded()))
        releasePreparation.countDown()
        awaitFor(2_000, 10) { closed.get().takeIf { it == 1 } }
        awaitFor(2_000, 10) {
            activation.state(acceptance.sessionId).takeIf { it == WarmupDonorSessionManager.State.CLOSED }
        }

        assertEquals(WarmupDonorSessionManager.State.CLOSED, activation.state(acceptance.sessionId))
        assertFalse(activation.onRecipientReady(ready()))
        assertNull(activation.replyToProbe(WarmupLivenessProbe(ready(), 1)))
        assertFalse(activation.onCompletionRequested(completion(acceptance)))
        assertNull(activation.replyToProbe(WarmupLivenessProbe(ready(), 2)))
        assertEquals(0, plane.store.admittedSessionCount())
        scope.cancel()
    }

    @Test
    fun `installed donor session processes locally until matching recipient readiness`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val plane = WarmupDonorDataPlaneFixture<String, String>(1, 10)
        val activation = activation(scope, plane) { session(WarmupSessionId("session")) }
        acceptAndStart(activation, acceptance())
        awaitFor(2_000, 10) {
            activation.state(WarmupSessionId("session"))
                .takeIf { it == WarmupDonorSessionManager.State.WAITING_RECIPIENT_READY }
        }
        val localCalls = AtomicInteger()

        val outcome = plane.records.process(ConsumerRecord("orders", 0, 0, "key", "value")) {
            localCalls.incrementAndGet()
            RecordProcessingOutcome.HandlerSucceeded
        }

        assertEquals(RecordProcessingOutcome.HandlerSucceeded, outcome)
        assertEquals(1, localCalls.get())
        assertTrue(activation.onRecipientReady(ready()))
        assertEquals(WarmupDonorSessionManager.State.ACTIVE, activation.state(WarmupSessionId("session")))
        plane.store.stopDelegating(WarmupSessionId("session"))
        assertTrue(plane.removeDrainedSession(WarmupSessionId("session")))
        scope.cancel()
    }

    @Test
    fun `preparation failure is session local and reports rejection after admission`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val plane = WarmupDonorDataPlaneFixture<String, String>(2, 20)
        val rejection = CompletableDeferred<SessionRejected>()
        val activation = WarmupDonorSessionManager(
            localInstanceId = donor,
            namespace = namespace,
            supportedMappings = listOf(mapping),
            dataPlane = plane.store,
            eligibility = WarmupDonorEligibility { ownership },
            preparation = plane.preparation { throw IllegalStateException("failed") },
            scope = scope,
            onPreparationFailed = { rejection.complete(it) },
            nowEpochMillis = { 1_000 }
        )

        assertInstanceOf(DonorAccepted::class.java, acceptAndStart(activation, acceptance()))
        assertEquals("donor_preparation_failed", rejection.await().reasonCode)
        assertEquals(WarmupDonorSessionManager.State.FAILED, activation.state(WarmupSessionId("session")))
        assertEquals(0, plane.store.admittedSessionCount())
        scope.cancel()
    }

    @Test
    fun `acceptance rechecks offered ownership before accounting capacity`() {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val plane = WarmupDonorDataPlaneFixture<String, String>(1, 10)
        val prepared = AtomicInteger()
        val activation = WarmupDonorSessionManager(
            localInstanceId = donor,
            namespace = namespace,
            supportedMappings = listOf(mapping),
            dataPlane = plane.store,
            eligibility = WarmupDonorEligibility { setOf(WarmupSourcePartition(0, 4)) },
            preparation = plane.preparation {
                prepared.incrementAndGet()
                session(it.sessionId)
            },
            scope = scope,
            onPreparationFailed = {},
            nowEpochMillis = { 1_000 }
        )

        val rejection = assertInstanceOf(SessionRejected::class.java, activation.accept(acceptance()))

        assertEquals("source_ownership_changed", rejection.reasonCode)
        assertEquals(0, plane.store.admittedSessionCount())
        assertEquals(0, prepared.get())
        scope.cancel()
    }

    @Test
    fun `acceptance rechecks complete mapping against local donor configuration`() {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val plane = WarmupDonorDataPlaneFixture<String, String>(1, 10)
        val activation = activation(scope, plane) { session(WarmupSessionId("session")) }
        val changed = mapping.copy(warmupTopic = "other-warmup-topic", mappingFingerprint = "other-fingerprint")
        val acceptance = acceptance().copy(
            mappings = listOf(WarmupDonorMapping(changed, ownership))
        )

        val rejection = assertInstanceOf(SessionRejected::class.java, activation.accept(acceptance))

        assertEquals("mapping_mismatch", rejection.reasonCode)
        assertEquals(0, plane.store.admittedSessionCount())
        scope.cancel()
    }

    @Test
    fun `terminal fence overflow blocks readmission until every omitted fence expires`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val plane = WarmupDonorDataPlaneFixture<String, String>(1, 10)
        val acknowledgements = AtomicInteger()
        val activation = WarmupDonorSessionManager(
            localInstanceId = donor,
            namespace = namespace,
            supportedMappings = listOf(mapping),
            dataPlane = plane.store,
            eligibility = WarmupDonorEligibility { ownership },
            preparation = plane.preparation { session(it.sessionId) },
            scope = scope,
            onPreparationFailed = {},
            onDonorClosed = { acknowledgements.incrementAndGet() },
            maxTerminalSessionFences = 1,
            nowEpochMillis = { 1_000 }
        )
        suspend fun closeSession(accepted: SessionAccepted) {
            assertInstanceOf(DonorAccepted::class.java, acceptAndStart(activation, accepted))
            withTimeout(2_000) {
                while (activation.state(accepted.sessionId) != WarmupDonorSessionManager.State.WAITING_RECIPIENT_READY) {
                    kotlinx.coroutines.yield()
                }
            }
            assertTrue(
                activation.onRecipientReady(
                    ready().copy(
                        sessionId = accepted.sessionId,
                        expiresAtEpochMillis = accepted.expiresAtEpochMillis
                    )
                )
            )
            val expected = acknowledgements.get() + 1
            assertTrue(activation.onCompletionRequested(completion(accepted)))
            awaitFor(2_000, 10) { acknowledgements.get().takeIf { it == expected } }
        }
        val first = acceptance().copy(sessionId = WarmupSessionId("first"))
        val second = acceptance().copy(sessionId = WarmupSessionId("second"))
        closeSession(first)
        closeSession(second)

        val replay = assertInstanceOf(SessionRejected::class.java, activation.accept(first))
        assertEquals("session_conflict", replay.reasonCode)
        assertFalse(activation.startPreparation(first.sessionId))
        assertFalse(
            activation.onRecipientReady(
                ready().copy(sessionId = first.sessionId, expiresAtEpochMillis = first.expiresAtEpochMillis)
            )
        )
        val omittedReplay = assertInstanceOf(SessionRejected::class.java, activation.accept(second))
        assertEquals("terminal_fence_capacity", omittedReplay.reasonCode)
        scope.cancel()
    }
}
