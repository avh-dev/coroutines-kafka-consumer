package avh.ckc.core.warmup

import avh.ckc.core.awaitFor
import avh.ckc.core.processing.RecordProcessingOutcome
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.CountDownLatch
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class WarmupDonorActivationTest {
    private val namespace = WarmupControlNamespace("global5-shared-partition-v6-test")
    private val recipient = WarmupInstanceId("recipient")
    private val donor = WarmupInstanceId("donor")
    private val mapping = WarmupTopicMapping(
        WarmupParticipantId("orders"), "orders", "warmup-orders", "mapping-fingerprint"
    )
    private val ownership = setOf(WarmupSourcePartition(0, 3))

    @Test
    fun `readiness received during preparation activates exactly once after preparation`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val plane = MultiSessionWarmupDataPlane<String, String>(2, 20)
        val prepared = CompletableDeferred<WarmupDonorSession<String, String>>()
        val activation = activation(scope, plane) { prepared.await() }
        val acceptance = acceptance()

        assertInstanceOf(DonorAccepted::class.java, acceptAndStart(activation, acceptance))
        assertTrue(activation.onRecipientReady(ready()))
        assertEquals(WarmupDonorActivation.State.PREPARING, activation.state(acceptance.sessionId))

        prepared.complete(session(acceptance.sessionId))
        awaitFor(2_000, 10) {
            activation.state(acceptance.sessionId).takeIf { it == WarmupDonorActivation.State.ACTIVE }
        }

        assertTrue(activation.onRecipientReady(ready()))
        assertEquals(1, plane.admittedSessionCount())
        plane.stopDelegating(acceptance.sessionId)
        assertTrue(plane.removeDrainedSession(acceptance.sessionId))
        scope.cancel()
    }

    @Test
    fun `cancellation winning preparation closes late resources and saved readiness cannot revive session`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val plane = MultiSessionWarmupDataPlane<String, String>(1, 10)
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
            activation.state(acceptance.sessionId).takeIf { it == WarmupDonorActivation.State.CLOSED }
        }

        assertEquals(WarmupDonorActivation.State.CLOSED, activation.state(acceptance.sessionId))
        assertFalse(activation.onRecipientReady(ready()))
        assertEquals(0, plane.admittedSessionCount())
        scope.cancel()
    }

    @Test
    fun `completion during preparation waits for late resource cleanup before acknowledgement`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val plane = MultiSessionWarmupDataPlane<String, String>(1, 10)
        val preparationStarted = CountDownLatch(1)
        val releasePreparation = CountDownLatch(1)
        val closedResources = AtomicInteger()
        val lateSession = session(acceptance().sessionId, closedResources)
        val acknowledgement = CompletableDeferred<DonorClosed>()
        val activation = WarmupDonorActivation(
            localInstanceId = donor,
            namespace = namespace,
            supportedMappings = listOf(mapping),
            dataPlane = plane,
            eligibility = WarmupDonorEligibility { ownership },
            preparation = WarmupDonorSessionPreparation {
                preparationStarted.countDown()
                releasePreparation.await()
                lateSession
            },
            scope = scope,
            onPreparationFailed = {},
            onDonorClosed = { acknowledgement.complete(it) },
            nowEpochMillis = { 1_000 }
        )
        val accepted = acceptance()
        acceptAndStart(activation, accepted)
        preparationStarted.await()

        assertTrue(
            activation.onCompletionRequested(
                WarmupCompletionRequested(
                    namespace, accepted.sessionId, recipient, donor, "fingerprint", 10_000
                )
            )
        )
        assertEquals(WarmupDonorActivation.State.DRAINING, activation.state(accepted.sessionId))
        assertFalse(acknowledgement.isCompleted)

        releasePreparation.countDown()
        withTimeout(2_000) { acknowledgement.await() }
        assertEquals(1, closedResources.get())
        assertEquals(WarmupDonorActivation.State.CLOSED, activation.state(accepted.sessionId))
        assertEquals(0, plane.admittedSessionCount())
        assertFalse(activation.onRecipientReady(ready()))
        scope.cancel()
    }

    @Test
    fun `completion cancels a running donor preparation without waiting for session expiry`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val plane = MultiSessionWarmupDataPlane<String, String>(1, 10)
        val preparationStarted = CompletableDeferred<Unit>()
        val acknowledgement = CompletableDeferred<DonorClosed>()
        val activation = WarmupDonorActivation(
            localInstanceId = donor,
            namespace = namespace,
            supportedMappings = listOf(mapping),
            dataPlane = plane,
            eligibility = WarmupDonorEligibility { ownership },
            preparation = WarmupDonorSessionPreparation {
                preparationStarted.complete(Unit)
                awaitCancellation()
            },
            scope = scope,
            onPreparationFailed = {},
            onDonorClosed = { acknowledgement.complete(it) },
            nowEpochMillis = { 1_000 }
        )
        val accepted = acceptance()
        acceptAndStart(activation, accepted)
        preparationStarted.await()

        assertTrue(activation.onCompletionRequested(completion(accepted)))
        withTimeout(1_000) { acknowledgement.await() }
        assertEquals(WarmupDonorActivation.State.CLOSED, activation.state(accepted.sessionId))
        assertEquals(0, plane.admittedSessionCount())
        scope.cancel()
    }

    @Test
    fun `failed late resource close is retained and retried without early donor acknowledgement`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val plane = MultiSessionWarmupDataPlane<String, String>(1, 10)
        val releasePreparation = CountDownLatch(1)
        val preparationStarted = CountDownLatch(1)
        val closeAttempts = AtomicInteger()
        val acknowledgements = AtomicInteger()
        val lateSession = session(
            WarmupSessionId("session"),
            onClose = {
                if (closeAttempts.incrementAndGet() == 1) throw IllegalStateException("close failed")
            }
        )
        val activation = WarmupDonorActivation(
            localInstanceId = donor,
            namespace = namespace,
            supportedMappings = listOf(mapping),
            dataPlane = plane,
            eligibility = WarmupDonorEligibility { ownership },
            preparation = WarmupDonorSessionPreparation {
                preparationStarted.countDown()
                releasePreparation.await()
                lateSession
            },
            scope = scope,
            onPreparationFailed = {},
            onDonorClosed = { acknowledgements.incrementAndGet() },
            nowEpochMillis = { 1_000 }
        )
        val accepted = acceptance()
        acceptAndStart(activation, accepted)
        preparationStarted.await()
        assertTrue(activation.onCompletionRequested(completion(accepted)))
        releasePreparation.countDown()

        awaitFor(2_000, 10) { closeAttempts.get().takeIf { it == 1 } }
        assertEquals(0, acknowledgements.get())
        assertEquals(WarmupDonorActivation.State.DRAINING, activation.state(accepted.sessionId))

        assertTrue(activation.onCompletionRequested(completion(accepted)))
        awaitFor(2_000, 10) { acknowledgements.get().takeIf { it == 1 } }
        assertEquals(2, closeAttempts.get())
        assertEquals(WarmupDonorActivation.State.CLOSED, activation.state(accepted.sessionId))
        scope.cancel()
    }

    @Test
    fun `installed donor session processes locally until matching recipient readiness`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val plane = MultiSessionWarmupDataPlane<String, String>(1, 10)
        val activation = activation(scope, plane) { session(WarmupSessionId("session")) }
        acceptAndStart(activation, acceptance())
        awaitFor(2_000, 10) {
            activation.state(WarmupSessionId("session"))
                .takeIf { it == WarmupDonorActivation.State.WAITING_RECIPIENT_READY }
        }
        val localCalls = AtomicInteger()

        val outcome = plane.process(ConsumerRecord("orders", 0, 0, "key", "value")) {
            localCalls.incrementAndGet()
            RecordProcessingOutcome.HandlerSucceeded
        }

        assertEquals(RecordProcessingOutcome.HandlerSucceeded, outcome)
        assertEquals(1, localCalls.get())
        assertTrue(activation.onRecipientReady(ready()))
        assertEquals(WarmupDonorActivation.State.ACTIVE, activation.state(WarmupSessionId("session")))
        plane.stopDelegating(WarmupSessionId("session"))
        assertTrue(plane.removeDrainedSession(WarmupSessionId("session")))
        scope.cancel()
    }

    @Test
    fun `preparation failure is session local and reports rejection after admission`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val plane = MultiSessionWarmupDataPlane<String, String>(2, 20)
        val rejection = CompletableDeferred<SessionRejected>()
        val activation = WarmupDonorActivation(
            localInstanceId = donor,
            namespace = namespace,
            supportedMappings = listOf(mapping),
            dataPlane = plane,
            eligibility = WarmupDonorEligibility { ownership },
            preparation = WarmupDonorSessionPreparation { throw IllegalStateException("failed") },
            scope = scope,
            onPreparationFailed = { rejection.complete(it) },
            nowEpochMillis = { 1_000 }
        )

        assertInstanceOf(DonorAccepted::class.java, acceptAndStart(activation, acceptance()))
        assertEquals("donor_preparation_failed", rejection.await().reasonCode)
        assertEquals(WarmupDonorActivation.State.FAILED, activation.state(WarmupSessionId("session")))
        assertEquals(0, plane.admittedSessionCount())
        scope.cancel()
    }

    @Test
    fun `acceptance rechecks offered ownership before accounting capacity`() {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val plane = MultiSessionWarmupDataPlane<String, String>(1, 10)
        val prepared = AtomicInteger()
        val activation = WarmupDonorActivation(
            localInstanceId = donor,
            namespace = namespace,
            supportedMappings = listOf(mapping),
            dataPlane = plane,
            eligibility = WarmupDonorEligibility { setOf(WarmupSourcePartition(0, 4)) },
            preparation = WarmupDonorSessionPreparation {
                prepared.incrementAndGet()
                session(it.sessionId)
            },
            scope = scope,
            onPreparationFailed = {},
            nowEpochMillis = { 1_000 }
        )

        val rejection = assertInstanceOf(SessionRejected::class.java, activation.accept(acceptance()))

        assertEquals("source_ownership_changed", rejection.reasonCode)
        assertEquals(0, plane.admittedSessionCount())
        assertEquals(0, prepared.get())
        scope.cancel()
    }

    @Test
    fun `acceptance rechecks complete mapping against local donor configuration`() {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val plane = MultiSessionWarmupDataPlane<String, String>(1, 10)
        val activation = activation(scope, plane) { session(WarmupSessionId("session")) }
        val changed = mapping.copy(warmupTopic = "other-warmup-topic", mappingFingerprint = "other-fingerprint")
        val acceptance = acceptance().copy(
            mappings = listOf(WarmupDonorMapping(changed, ownership))
        )

        val rejection = assertInstanceOf(SessionRejected::class.java, activation.accept(acceptance))

        assertEquals("mapping_mismatch", rejection.reasonCode)
        assertEquals(0, plane.admittedSessionCount())
        scope.cancel()
    }

    @Test
    fun `unknown or mismatched terminal messages cannot mutate an active session`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val plane = MultiSessionWarmupDataPlane<String, String>(1, 10)
        val activation = activation(scope, plane) { session(WarmupSessionId("session")) }
        val accepted = acceptance()
        acceptAndStart(activation, accepted)
        withTimeout(2_000) {
            while (activation.state(accepted.sessionId) != WarmupDonorActivation.State.WAITING_RECIPIENT_READY) {
                kotlinx.coroutines.yield()
            }
        }
        assertTrue(activation.onRecipientReady(ready()))

        assertFalse(
            activation.onCompletionRequested(
                WarmupCompletionRequested(
                    namespace, accepted.sessionId, WarmupInstanceId("intruder"), donor,
                    "fingerprint", 10_000
                )
            )
        )
        assertFalse(
            activation.onCompletionRequested(
                WarmupCompletionRequested(
                    namespace, WarmupSessionId("unknown"), recipient, donor,
                    "fingerprint", 10_000
                )
            )
        )
        assertEquals(
            WarmupResultTracker.Completion.IGNORED,
            activation.onRecordCompleted(
                ControlRecordCompleted(
                    namespace, accepted.sessionId, WarmupInstanceId("intruder"), donor,
                    WarmupRecordCompleted(
                        WarmupRecordId(accepted.sessionId, TopicPartition("warmup-orders", 0), 0),
                        WarmupRemoteOutcome.Succeeded
                    ),
                    "fingerprint", 10_000
                )
            )
        )
        assertEquals(WarmupDonorActivation.State.ACTIVE, activation.state(accepted.sessionId))

        assertTrue(activation.onSessionEnded(fallbackEnded()))
        awaitFor(2_000, 10) {
            activation.state(accepted.sessionId).takeIf { it == WarmupDonorActivation.State.CLOSED }
        }
        scope.cancel()
    }

    @Test
    fun `terminal fence overflow blocks readmission until every omitted fence expires`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val plane = MultiSessionWarmupDataPlane<String, String>(1, 10)
        val acknowledgements = AtomicInteger()
        val activation = WarmupDonorActivation(
            localInstanceId = donor,
            namespace = namespace,
            supportedMappings = listOf(mapping),
            dataPlane = plane,
            eligibility = WarmupDonorEligibility { ownership },
            preparation = WarmupDonorSessionPreparation { session(it.sessionId) },
            scope = scope,
            onPreparationFailed = {},
            onDonorClosed = { acknowledgements.incrementAndGet() },
            maxTerminalSessionFences = 1,
            nowEpochMillis = { 1_000 }
        )
        suspend fun closeSession(accepted: SessionAccepted) {
            assertInstanceOf(DonorAccepted::class.java, acceptAndStart(activation, accepted))
            withTimeout(2_000) {
                while (activation.state(accepted.sessionId) != WarmupDonorActivation.State.WAITING_RECIPIENT_READY) {
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

    @Test
    fun `recipient fallback after readiness stops and removes an activated donor session`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val plane = MultiSessionWarmupDataPlane<String, String>(1, 10)
        val activation = activation(scope, plane) { session(WarmupSessionId("session")) }
        assertInstanceOf(DonorAccepted::class.java, acceptAndStart(activation, acceptance()))
        withTimeout(2_000) {
            while (activation.state(WarmupSessionId("session")) != WarmupDonorActivation.State.WAITING_RECIPIENT_READY) {
                kotlinx.coroutines.yield()
            }
        }
        assertTrue(activation.onRecipientReady(ready()))

        assertTrue(
            activation.onSessionEnded(
                SessionEnded(
                    namespace, WarmupSessionId("session"), recipient,
                    WarmupSessionEndState.COLD_FALLBACK, "readiness_failed", "fingerprint", 10_000
                )
            )
        )
        awaitFor(2_000, 10) {
            activation.state(WarmupSessionId("session"))
                .takeIf { it == WarmupDonorActivation.State.CLOSED }
        }
        assertEquals(0, plane.admittedSessionCount())
        scope.cancel()
    }

    @Test
    fun `recipient completion drains inflight fences delegation and releases capacity`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val plane = MultiSessionWarmupDataPlane<String, String>(1, 10)
        val acknowledgements = mutableListOf<DonorClosed>()
        val donorSession = session(WarmupSessionId("session"))
        val activation = WarmupDonorActivation(
            localInstanceId = donor,
            namespace = namespace,
            supportedMappings = listOf(mapping),
            dataPlane = plane,
            eligibility = WarmupDonorEligibility { ownership },
            preparation = WarmupDonorSessionPreparation { donorSession },
            scope = scope,
            onPreparationFailed = {},
            onDonorClosed = { acknowledgements += it },
            nowEpochMillis = { 1_000 }
        )
        val accepted = acceptance()
        acceptAndStart(activation, accepted)
        withTimeout(2_000) {
            while (activation.state(accepted.sessionId) != WarmupDonorActivation.State.WAITING_RECIPIENT_READY) {
                kotlinx.coroutines.yield()
            }
        }
        assertTrue(activation.onRecipientReady(ready()))
        val delegated = async {
            plane.process(ConsumerRecord("orders", 0, 0, "key", "value")) {
                error("Successful remote completion must not fall back")
            }
        }
        withTimeout(2_000) {
            while (donorSession.pendingResultCount() != 1) kotlinx.coroutines.yield()
        }
        val completion = WarmupCompletionRequested(
            namespace, accepted.sessionId, recipient, donor, "fingerprint", 10_000
        )

        assertTrue(activation.onCompletionRequested(completion))
        assertEquals(WarmupDonorActivation.State.DRAINING, activation.state(accepted.sessionId))
        val localCalls = AtomicInteger()
        plane.process(ConsumerRecord("orders", 0, 1, "other", "value")) {
            localCalls.incrementAndGet()
            RecordProcessingOutcome.HandlerSucceeded
        }
        assertEquals(1, localCalls.get())
        assertEquals(
            WarmupResultTracker.Completion.DELIVERED,
            activation.onRecordCompleted(
                ControlRecordCompleted(
                    namespace, accepted.sessionId, recipient, donor,
                    WarmupRecordCompleted(
                        WarmupRecordId(accepted.sessionId, TopicPartition("warmup-orders", 0), 0),
                        WarmupRemoteOutcome.Succeeded
                    ),
                    "fingerprint", 10_000
                )
            )
        )
        assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, delegated.await())
        awaitFor(2_000, 10) { acknowledgements.size.takeIf { it == 1 } }
        assertEquals(WarmupDonorActivation.State.CLOSED, activation.state(accepted.sessionId))
        assertEquals(0, plane.admittedSessionCount())

        assertTrue(activation.onCompletionRequested(completion))
        awaitFor(2_000, 10) { acknowledgements.size.takeIf { it == 2 } }
        assertFalse(activation.onRecipientReady(ready()))
        scope.cancel()
    }

    @Test
    fun `rollback drain timeout falls back locally preserves sibling and eventually releases capacity`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val plane = MultiSessionWarmupDataPlane<String, String>(2, 20)
        val clock = AtomicLong(1_000)
        val firstSession = session(WarmupSessionId("session"))
        val activation = WarmupDonorActivation(
            localInstanceId = donor,
            namespace = namespace,
            supportedMappings = listOf(mapping),
            dataPlane = plane,
            eligibility = WarmupDonorEligibility { ownership },
            preparation = WarmupDonorSessionPreparation { firstSession },
            scope = scope,
            onPreparationFailed = {},
            nowEpochMillis = clock::get
        )
        assertInstanceOf(DonorAccepted::class.java, acceptAndStart(activation, acceptance()))
        withTimeout(2_000) {
            while (activation.state(WarmupSessionId("session")) != WarmupDonorActivation.State.WAITING_RECIPIENT_READY) {
                kotlinx.coroutines.yield()
            }
        }
        assertTrue(activation.onRecipientReady(ready()))
        val siblingId = WarmupSessionId("sibling")
        val sibling = session(siblingId)
        assertEquals(MultiSessionWarmupDataPlane.Admission.ACCEPTED, plane.admit(sibling))
        assertTrue(plane.activate(siblingId))
        val localCalls = AtomicInteger()
        val firstOutcome = async {
            plane.process(ConsumerRecord("orders", 0, 0, "key-0", "value-0")) {
                localCalls.incrementAndGet()
                RecordProcessingOutcome.HandlerSucceeded
            }
        }
        withTimeout(2_000) {
            while (firstSession.pendingResultCount() != 1) kotlinx.coroutines.yield()
        }

        clock.set(9_999)
        assertTrue(activation.onSessionEnded(fallbackEnded()))
        assertEquals(RecordProcessingOutcome.HandlerSucceeded, withTimeout(2_000) { firstOutcome.await() })
        assertEquals(1, localCalls.get())
        withTimeout(2_000) {
            while (plane.admittedSessionCount() != 1) kotlinx.coroutines.yield()
        }

        val siblingOutcome = async {
            plane.process(ConsumerRecord("orders", 0, 1, "key-1", "value-1")) {
                error("Sibling remote success must not use local fallback")
            }
        }
        withTimeout(2_000) { while (sibling.pendingResultCount() != 1) kotlinx.coroutines.yield() }
        plane.acceptResult(
            WarmupRecordCompleted(
                WarmupRecordId(siblingId, TopicPartition("warmup-orders", 0), 0),
                WarmupRemoteOutcome.Succeeded
            )
        )
        assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, siblingOutcome.await())

        val replacement = session(WarmupSessionId("replacement"))
        assertEquals(MultiSessionWarmupDataPlane.Admission.ACCEPTED, plane.admit(replacement))
        plane.stopDelegating(siblingId)
        assertTrue(plane.removeDrainedSession(siblingId))
        plane.stopDelegating(WarmupSessionId("replacement"))
        assertTrue(plane.removeDrainedSession(WarmupSessionId("replacement")))
        scope.cancel()
    }

    @Test
    fun `recipient loss expires donor session falls back inflight and releases capacity`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val plane = MultiSessionWarmupDataPlane<String, String>(1, 10)
        val donorSession = session(WarmupSessionId("expiring"))
        val activation = WarmupDonorActivation(
            localInstanceId = donor,
            namespace = namespace,
            supportedMappings = listOf(mapping),
            dataPlane = plane,
            eligibility = WarmupDonorEligibility { ownership },
            preparation = WarmupDonorSessionPreparation { donorSession },
            scope = scope,
            onPreparationFailed = {},
            drainTimeout = 20.milliseconds
        )
        val expiresAt = System.currentTimeMillis() + 200
        val accepted = acceptance().copy(
            sessionId = WarmupSessionId("expiring"),
            expiresAtEpochMillis = expiresAt
        )
        assertInstanceOf(DonorAccepted::class.java, acceptAndStart(activation, accepted))
        withTimeout(2_000) {
            while (activation.state(accepted.sessionId) != WarmupDonorActivation.State.WAITING_RECIPIENT_READY) {
                kotlinx.coroutines.yield()
            }
        }
        assertTrue(
            activation.onRecipientReady(
                ready().copy(sessionId = accepted.sessionId, expiresAtEpochMillis = expiresAt)
            )
        )
        val localCalls = AtomicInteger()
        val outcome = async {
            plane.process(ConsumerRecord("orders", 0, 0, "key", "value")) {
                localCalls.incrementAndGet()
                RecordProcessingOutcome.HandlerSucceeded
            }
        }
        withTimeout(2_000) {
            while (donorSession.pendingResultCount() != 1) kotlinx.coroutines.yield()
        }

        assertEquals(RecordProcessingOutcome.HandlerSucceeded, withTimeout(2_000) { outcome.await() })
        assertEquals(1, localCalls.get())
        awaitFor(2_000, 10) {
            activation.state(accepted.sessionId).takeIf { it == WarmupDonorActivation.State.CLOSED }
        }
        assertEquals(0, plane.admittedSessionCount())
        scope.cancel()
    }

    private fun activation(
        scope: CoroutineScope,
        plane: MultiSessionWarmupDataPlane<String, String>,
        prepare: suspend () -> WarmupDonorSession<String, String>
    ) = WarmupDonorActivation(
        localInstanceId = donor,
        namespace = namespace,
        supportedMappings = listOf(mapping),
        dataPlane = plane,
        eligibility = WarmupDonorEligibility { ownership },
        preparation = WarmupDonorSessionPreparation { prepare() },
        scope = scope,
        onPreparationFailed = {},
        nowEpochMillis = { 1_000 }
    )

    private fun acceptAndStart(
        activation: WarmupDonorActivation<String, String>,
        acceptance: SessionAccepted
    ): WarmupControlMessage = activation.accept(acceptance).also { response ->
        if (response is DonorAccepted) assertTrue(activation.startPreparation(acceptance.sessionId))
    }

    private fun acceptance() = SessionAccepted(
        namespace, WarmupSessionId("session"), recipient, donor,
        listOf(WarmupDonorMapping(mapping, ownership)), 10, 2, "fingerprint", 10_000
    )

    private fun ready() = RecipientReady(
        namespace, WarmupSessionId("session"), recipient, donor,
        listOf(WarmupPartitionAssignment(mapping)), "fingerprint", 10_000
    )

    private fun completion(acceptance: SessionAccepted) = WarmupCompletionRequested(
        namespace,
        acceptance.sessionId,
        recipient,
        donor,
        acceptance.configurationFingerprint,
        acceptance.expiresAtEpochMillis
    )

    private fun fallbackEnded() = SessionEnded(
        namespace, WarmupSessionId("session"), recipient,
        WarmupSessionEndState.COLD_FALLBACK, "preparation_cancelled", "fingerprint", 10_000
    )

    private fun session(
        sessionId: WarmupSessionId,
        closed: AtomicInteger = AtomicInteger(),
        onClose: () -> Unit = { closed.incrementAndGet() }
    ): WarmupDonorSession<String, String> {
        val sender = object : WarmupRecordSender<String, String>, AutoCloseable {
            override suspend fun send(
                record: ConsumerRecord<String, String>,
                destination: WarmupDestination,
                sessionId: WarmupSessionId
            ) = WarmupSendAcknowledgement(TopicPartition(destination.topic, destination.partition), 0)

            override fun close() {
                onClose()
            }
        }
        return WarmupDonorSession(
            sessionId = sessionId,
            intervalBudget = 10,
            quotaInterval = 1.seconds,
            delegationTimeout = 1.seconds,
            destinationResolver = WarmupDestinationResolver { WarmupDestination("warmup-orders") },
            sender = sender
        )
    }
}
