package avh.ckc.demo.consumer.ckc

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue

class CkcConsumerLifecycleMonitorTest {
    @Test
    fun `automatic readiness completes only after every ordinary consumer is ready`() = runBlocking {
        val firstReady = CompletableDeferred<Unit>()
        val secondReady = CompletableDeferred<Unit>()
        val allReady = CompletableDeferred<Unit>()
        val monitor = CkcConsumerLifecycleMonitor(
            consumers = listOf(
                monitoredConsumer("orders", firstReady),
                monitoredConsumer("batches", secondReady)
            ),
            observeReadiness = true,
            onReady = { allReady.complete(Unit) },
            onTerminalFailure = { allReady.completeExceptionally(it) }
        )

        monitor.start()
        firstReady.complete(Unit)
        assertFalse(allReady.isCompleted)

        secondReady.complete(Unit)
        withTimeout(2_000) { allReady.await() }
        assertTrue(allReady.isCompleted)
        monitor.stop()
    }

    @Test
    fun `terminal failure after successful start makes runtime externally unhealthy`() = runBlocking {
        val termination = CompletableDeferred<Unit>()
        val observedFailure = CompletableDeferred<Throwable>()
        val externallyHealthy = AtomicBoolean(true)
        val expected = IllegalStateException("managed warm-up failed")
        val monitor = CkcConsumerLifecycleMonitor(
            consumers = listOf(
                MonitoredCkcConsumer(
                    name = "orders",
                    awaitReady = { CompletableDeferred<Unit>().await() },
                    awaitTermination = { termination.await() }
                )
            ),
            observeReadiness = true,
            onReady = {},
            onTerminalFailure = { failure ->
                externallyHealthy.set(false)
                observedFailure.complete(failure)
            }
        )

        monitor.start()
        termination.completeExceptionally(expected)

        val actual = withTimeout(2_000) { observedFailure.await() }
        assertIs<IllegalStateException>(actual)
        assertEquals(expected.message, actual.message)
        assertFalse(externallyHealthy.get())
        monitor.stop()
    }

    private fun monitoredConsumer(
        name: String,
        readiness: CompletableDeferred<Unit>
    ): MonitoredCkcConsumer = MonitoredCkcConsumer(
        name = name,
        awaitReady = { readiness.await() },
        awaitTermination = { CompletableDeferred<Unit>().await() }
    )
}
