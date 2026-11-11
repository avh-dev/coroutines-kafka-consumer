package avh.ckc.core.warmup

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.seconds

class WarmupRecipientLifecycleTest {
    private val session = WarmupSessionId("session")

    @Test
    fun `recipient drains final outcome and outbox before starting main`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val warmup = FakeOwnedConsumer()
        val main = FakeOwnedConsumer()
        val tracker = WarmupRecipientDrainTracker(session)
        val sent = mutableListOf<Long>()
        val outbox = WarmupCompletionOutbox(scope, 4) { sent += it.recordId.offset }
        val lifecycle = WarmupRecipientLifecycle({ warmup }, main, tracker, outbox, 1.seconds)
        lifecycle.startWarmup()
        val partition = TopicPartition("warmup", 0)
        val completed = WarmupRecordCompleted(
            WarmupRecordId(session, partition, 7L),
            WarmupRemoteOutcome.Succeeded
        )
        outbox.publish(completed)
        tracker.onCompleted(completed)

        lifecycle.drain(listOf(WarmupFinalOffset(partition, 7L)))
        assertEquals(1, warmup.stopCalls.get())
        assertEquals(listOf(7L), sent)
        assertEquals(0, main.startCalls.get())

        lifecycle.startMain()
        assertEquals(1, main.startCalls.get())
        scope.cancel()
    }

    @Test
    fun `shutdown while recipient readiness is pending never starts main`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val readiness = CompletableDeferred<Unit>()
        val warmup = FakeOwnedConsumer(readiness)
        val main = FakeOwnedConsumer()
        val tracker = WarmupRecipientDrainTracker(session)
        val outbox = WarmupCompletionOutbox(scope, 1) { }
        val lifecycle = WarmupRecipientLifecycle({ warmup }, main, tracker, outbox, 1.seconds)
        val starting = async { runCatching { lifecycle.startWarmup() } }
        while (warmup.startCalls.get() == 0) kotlinx.coroutines.yield()

        val stopping = async { lifecycle.stop() }
        stopping.await()
        starting.await()

        assertEquals(0, main.startCalls.get())
        assertEquals(1, warmup.stopCalls.get())
        scope.cancel()
    }

    @Test
    fun `shutdown while main readiness is pending stops the already started main`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val mainReadiness = CompletableDeferred<Unit>()
        val warmup = FakeOwnedConsumer()
        val main = FakeOwnedConsumer(mainReadiness)
        val tracker = WarmupRecipientDrainTracker(session)
        val outbox = WarmupCompletionOutbox(scope, 1) { }
        val lifecycle = WarmupRecipientLifecycle({ warmup }, main, tracker, outbox, 1.seconds)
        lifecycle.startWarmup()
        lifecycle.drain(listOf(WarmupFinalOffset(TopicPartition("warmup", 0), null)))
        val startingMain = async { runCatching { lifecycle.startMain() } }
        while (main.startCalls.get() == 0) kotlinx.coroutines.yield()

        lifecycle.stop()
        startingMain.await()

        assertEquals(1, main.startCalls.get())
        assertEquals(1, main.stopCalls.get())
        scope.cancel()
    }

    private class FakeOwnedConsumer(
        private val readiness: CompletableDeferred<Unit> = CompletableDeferred(Unit)
    ) : WarmupOwnedConsumer {
        val startCalls = AtomicInteger()
        val stopCalls = AtomicInteger()

        override fun start() { startCalls.incrementAndGet() }
        override suspend fun awaitReady() { readiness.await() }
        override suspend fun stop() { stopCalls.incrementAndGet() }
    }
}
