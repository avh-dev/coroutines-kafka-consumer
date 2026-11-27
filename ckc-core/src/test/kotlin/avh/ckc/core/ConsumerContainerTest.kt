package avh.ckc.core

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

class ConsumerContainerTest {
    @Test
    fun `start requests every participant before collective readiness`() = runBlocking {
        val first = FakeParticipant()
        val second = FakeParticipant()
        val third = FakeParticipant()
        val container = container(first, second, third)

        container.start()

        assertEquals(listOf(1, 1, 1), listOf(first.starts, second.starts, third.starts))
        first.ready.complete(Unit)
        second.ready.complete(Unit)
        assertFalse(async { container.awaitReady() }.isCompleted)
        third.ready.complete(Unit)
        withTimeout(1.seconds) { container.awaitReady() }
    }

    @Test
    fun `start failure does not skip later participants and preserves all cleanup`() = runBlocking {
        val first = FakeParticipant()
        val startFailure = IllegalStateException("start failed")
        val second = FakeParticipant(startFailure = startFailure)
        val third = FakeParticipant()
        val container = container(first, second, third)

        val error = assertFailsWith<ConsumerContainerException> { container.start() }

        assertEquals(listOf(1, 1, 1), listOf(first.starts, second.starts, third.starts))
        assertEquals(ConsumerContainerPhase.START, error.failures.single().phase)
        assertEquals(startFailure, error.failures.single().error)
        container.stop()
        assertEquals(listOf(1, 1, 1), listOf(first.stops, second.stops, third.stops))
    }

    @Test
    fun `fatal start failure is rethrown without starting later participants`() = runBlocking {
        val fatal = TestFatalError("fatal start")
        val first = FakeParticipant(startFailure = fatal)
        val second = FakeParticipant()
        val container = container(first, second)

        val error = assertFailsWith<TestFatalError> { container.start() }

        assertTrue(error === fatal)
        assertEquals(listOf(1, 0), listOf(first.starts, second.starts))
        container.stop()
    }

    @Test
    fun `readiness fails promptly without waiting for another participant`() = runBlocking {
        val first = FakeParticipant()
        val second = FakeParticipant()
        val container = container(first, second)
        container.start()
        val failure = IllegalArgumentException("not ready")

        first.ready.completeExceptionally(failure)

        val error = assertFailsWith<ConsumerContainerException> {
            withTimeout(1.seconds) { container.awaitReady() }
        }
        assertEquals(ConsumerContainerPhase.READINESS, error.failures.single().phase)
        assertIs<IllegalArgumentException>(error.failures.single().error)
        assertEquals(failure.message, error.failures.single().error.message)
        assertFalse(second.ready.isCompleted)
        container.stop()
    }

    @Test
    fun `fatal readiness failure is not wrapped as a participant error`() = runBlocking {
        val first = FakeParticipant()
        val second = FakeParticipant()
        val container = container(first, second)
        container.start()

        first.ready.completeExceptionally(TestFatalError("fatal readiness"))

        assertFailsWith<TestFatalError> { withTimeout(1.seconds) { container.awaitReady() } }
        assertFalse(second.ready.isCompleted)
        container.stop()
    }

    @Test
    fun `one stop failure does not prevent stopping other participants`() = runBlocking {
        val incompleteFailure = IllegalStateException("cleanup failed")
        val terminalFailure = IllegalArgumentException("terminal failure")
        val first = FakeParticipant(
            stopActions = mutableListOf(
                StopAction(incompleteFailure, complete = false),
                StopAction(complete = true)
            )
        )
        val second = FakeParticipant()
        val third = FakeParticipant(
            stopActions = mutableListOf(StopAction(terminalFailure, complete = true))
        )
        val container = container(first, second, third)
        container.start()

        val error = assertFailsWith<ConsumerContainerException> { container.stop() }

        assertEquals(listOf(1, 1, 1), listOf(first.stops, second.stops, third.stops))
        assertEquals(listOf(0, 2), error.failures.map(ConsumerContainerFailure::participantIndex))
        assertEquals(listOf(incompleteFailure, terminalFailure), error.failures.map(ConsumerContainerFailure::error))
        container.stop()
        assertEquals(listOf(2, 1, 1), listOf(first.stops, second.stops, third.stops))
    }

    @Test
    fun `fatal stop failure is rethrown after stopping every participant`() = runBlocking {
        val first = FakeParticipant(
            stopActions = mutableListOf(StopAction(TestFatalError("fatal stop"), complete = true))
        )
        val second = FakeParticipant()
        val container = container(first, second)
        container.start()

        assertFailsWith<TestFatalError> { container.stop() }

        assertEquals(listOf(1, 1), listOf(first.stops, second.stops))
    }

    @Test
    fun `later stop retries only incomplete cleanup`() = runBlocking {
        val first = FakeParticipant(
            stopActions = mutableListOf(
                StopAction(IllegalStateException("incomplete"), complete = false),
                StopAction(complete = true)
            )
        )
        val second = FakeParticipant()
        val container = container(first, second)
        container.start()

        assertFailsWith<ConsumerContainerException> { container.stop() }
        container.stop()

        assertEquals(2, first.stops)
        assertEquals(1, second.stops)
    }

    @Test
    fun `concurrent stop callers share one attempt`() = runBlocking {
        val stopEntered = CompletableDeferred<Unit>()
        val allowStop = CompletableDeferred<Unit>()
        val participant = FakeParticipant(stopGate = allowStop, stopEntered = stopEntered)
        val container = container(participant)
        container.start()

        val first = async { container.stop() }
        stopEntered.await()
        val second = async { container.stop() }
        allowStop.complete(Unit)
        first.await()
        second.await()

        assertEquals(1, participant.stops)
        container.stop()
        assertEquals(1, participant.stops)
    }

    @Test
    fun `individual and collective stop share the participant cleanup attempt`() = runBlocking {
        val firstEntered = CompletableDeferred<Unit>()
        val allowFirst = CompletableDeferred<Unit>()
        val first = FakeParticipant(stopGate = allowFirst, stopEntered = firstEntered)
        val second = FakeParticipant()
        val container = container(first, second)
        container.start()

        val individual = async { container.stopParticipant(0) }
        firstEntered.await()
        val collective = async { container.stop() }
        second.stopFinished.await()
        allowFirst.complete(Unit)
        individual.await()
        collective.await()

        assertEquals(listOf(1, 1), listOf(first.stops, second.stops))
    }

    @Test
    fun `status exposes a completed failure while another participant is still stopping`() = runBlocking {
        val failure = IllegalStateException("failed")
        val failed = FakeParticipant(
            stopActions = mutableListOf(StopAction(failure, complete = true))
        )
        val slowEntered = CompletableDeferred<Unit>()
        val allowSlow = CompletableDeferred<Unit>()
        val slow = FakeParticipant(stopGate = allowSlow, stopEntered = slowEntered)
        val container = container(failed, slow)
        container.start()

        val stop = async { assertFailsWith<ConsumerContainerException> { container.stop() } }
        slowEntered.await()
        failed.stopFinished.await()
        val status = container.status()

        assertTrue(status.participants[0].stopComplete)
        assertEquals(failure, status.participants[0].failure)
        assertFalse(status.participants[1].stopComplete)
        allowSlow.complete(Unit)
        stop.await()
    }

    @Test
    fun `caller cancellation does not abandon an active stop attempt`() = runBlocking {
        val stopEntered = CompletableDeferred<Unit>()
        val allowStop = CompletableDeferred<Unit>()
        val participant = FakeParticipant(stopGate = allowStop, stopEntered = stopEntered)
        val container = container(participant)
        container.start()

        val cancelledCaller = async { container.stop() }
        stopEntered.await()
        cancelledCaller.cancelAndJoin()
        allowStop.complete(Unit)

        withTimeout(1.seconds) { container.stop() }
        assertEquals(1, participant.stops)
    }

    @Test
    fun `caller cancellation cannot retain an incomplete participant attempt`() = runBlocking {
        val stopEntered = CompletableDeferred<Unit>()
        val secondStopEntered = CompletableDeferred<Unit>()
        val allowStop = CompletableDeferred<Unit>()
        val callbackEntered = CompletableDeferred<Unit>()
        val releaseCallback = CountDownLatch(1)
        val callbackInvocations = AtomicInteger()
        val participant = FakeParticipant(
            stopActions = mutableListOf(
                StopAction(IllegalStateException("incomplete"), complete = false),
                StopAction(complete = true)
            ),
            stopGate = allowStop,
            stopEntered = stopEntered,
            onStop = { invocation ->
                if (invocation == 2) secondStopEntered.complete(Unit)
            }
        )
        val container = ConsumerContainer(
            participantLifecycles = listOf(participant),
            beforeIncompleteAttemptCleanup = {
                if (callbackInvocations.incrementAndGet() == 1) {
                    callbackEntered.complete(Unit)
                    check(releaseCallback.await(1, TimeUnit.SECONDS))
                }
            }
        )
        container.start()

        val cancelledCaller = async { container.stopParticipant(0) }
        stopEntered.await()
        cancelledCaller.cancelAndJoin()
        allowStop.complete(Unit)
        callbackEntered.await()

        val retry = async { container.stopParticipant(0) }
        try {
            withTimeout(1.seconds) { secondStopEntered.await() }
        } finally {
            releaseCallback.countDown()
        }
        retry.await()

        assertEquals(2, participant.stops)
        assertTrue(container.status().participants.single().stopComplete)
    }

    @Test
    fun `cancelling readiness wait does not stop consumers or shared observation`() = runBlocking {
        val first = FakeParticipant()
        val second = FakeParticipant()
        val container = container(first, second)
        container.start()

        val waiter = async { container.awaitReady() }
        waiter.cancelAndJoin()

        assertEquals(listOf(0, 0), listOf(first.stops, second.stops))
        first.ready.complete(Unit)
        second.ready.complete(Unit)
        withTimeout(1.seconds) { container.awaitReady() }
        container.stop()
    }

    @Test
    fun `stop during synchronous start waits for startup calls and then cleans everyone`() = runBlocking {
        val startEntered = CompletableDeferred<Unit>()
        val allowStart = CompletableDeferred<Unit>()
        val first = FakeParticipant(startGate = allowStart, startEntered = startEntered)
        val second = FakeParticipant()
        val container = container(first, second)

        val start = async(Dispatchers.Default) { container.start() }
        startEntered.await()
        val stop = async(Dispatchers.Default) { container.stop() }
        assertFalse(stop.isCompleted)
        allowStart.complete(Unit)
        start.await()
        stop.await()

        assertEquals(listOf(1, 1), listOf(first.stops, second.stops))
    }

    @Test
    fun `stop before start is terminal and restart is rejected`() = runBlocking {
        val first = FakeParticipant()
        val second = FakeParticipant()
        val container = container(first, second)

        container.stop()

        assertEquals(listOf(1, 1), listOf(first.stops, second.stops))
        assertFailsWith<IllegalStateException> { container.start() }
        assertFailsWith<IllegalStateException> { container.awaitReady() }
    }

    @Test
    fun `repeated start is rejected without starting participants again`() = runBlocking {
        val participant = FakeParticipant()
        val container = container(participant)
        container.start()

        assertFailsWith<IllegalStateException> { container.start() }

        assertEquals(1, participant.starts)
        container.stop()
    }

    @Test
    fun `participant stopped individually is not cleaned again by container`() = runBlocking {
        val first = FakeParticipant()
        val second = FakeParticipant()
        val container = container(first, second)
        container.start()
        first.stop()

        container.stop()

        assertEquals(2, first.stops)
        assertEquals(1, first.cleanupRuns)
        assertEquals(1, second.stops)
    }

    @Test
    fun `completed stop failure is retained without rerunning cleanup`() = runBlocking {
        val terminalFailure = IllegalStateException("consumer failed before a complete shutdown")
        val participant = FakeParticipant(
            stopActions = mutableListOf(StopAction(terminalFailure, complete = true))
        )
        val container = container(participant)
        container.start()

        val first = assertFailsWith<ConsumerContainerException> { container.stop() }
        val repeated = assertFailsWith<ConsumerContainerException> { container.stop() }

        assertEquals(terminalFailure, first.failures.single().error)
        assertTrue(first === repeated)
        assertEquals(1, participant.stops)
    }

    @Test
    fun `public factory rejects the same consumer instance twice`() {
        val consumer = mock<CoroutinesKafkaConsumer<String, String>>()

        val error = assertFailsWith<IllegalArgumentException> {
            consumerContainer(consumer, consumer)
        }

        assertTrue(error.message.orEmpty().contains("same consumer instance"))
    }

    @Test
    fun `public container stops heterogeneous prepared consumers`() = runBlocking {
        val strings = coroutinesKafkaConsumer<String, String>(stringSerdeProperties()) {
            topics("strings")
            handle { }
        }
        val longs = coroutinesKafkaConsumer<Long, Long>(longSerdeProperties()) {
            topics("longs")
            handle { }
        }
        val container = consumerContainer(strings, longs)

        container.stop()

        assertTrue(strings.stateSnapshot().stopped)
        assertTrue(longs.stateSnapshot().stopped)
    }

    private fun container(vararg participants: FakeParticipant): ConsumerContainer =
        ConsumerContainer(participants.toList())

    private data class StopAction(
        val error: Throwable? = null,
        val complete: Boolean
    )

    private class TestFatalError(message: String) : VirtualMachineError(message)

    private class FakeParticipant(
        private val startFailure: Throwable? = null,
        private val startGate: CompletableDeferred<Unit>? = null,
        private val startEntered: CompletableDeferred<Unit>? = null,
        val ready: CompletableDeferred<Unit> = CompletableDeferred(),
        private val stopActions: MutableList<StopAction> = mutableListOf(StopAction(complete = true)),
        private val stopGate: CompletableDeferred<Unit>? = null,
        private val stopEntered: CompletableDeferred<Unit>? = null,
        private val onStop: (Int) -> Unit = {}
    ) : ConsumerContainerParticipant {
        val stopFinished = CompletableDeferred<Unit>()
        var starts = 0
            private set
        var stops = 0
            private set
        var cleanupRuns = 0
            private set
        private var stopComplete = false

        override fun start() {
            starts += 1
            startEntered?.complete(Unit)
            startGate?.let { runBlocking { it.await() } }
            startFailure?.let { throw it }
        }

        override suspend fun awaitReady() {
            ready.await()
        }

        override suspend fun stop() {
            stops += 1
            onStop(stops)
            if (stopComplete) {
                stopFinished.complete(Unit)
                return
            }
            try {
                cleanupRuns += 1
                stopEntered?.complete(Unit)
                stopGate?.await()
                val action = stopActions.removeFirstOrNull() ?: StopAction(complete = stopComplete)
                stopComplete = action.complete
                action.error?.let { throw it }
            } finally {
                stopFinished.complete(Unit)
            }
        }

        override fun isStopComplete(): Boolean = stopComplete
    }
}
