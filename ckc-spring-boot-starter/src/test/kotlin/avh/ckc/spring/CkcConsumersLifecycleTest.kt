package avh.ckc.spring

import avh.ckc.core.ConsumerStateSnapshot
import avh.ckc.core.ProcessingMode
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.context.support.GenericApplicationContext
import java.time.Duration
import kotlin.system.measureTimeMillis

class CkcConsumersLifecycleTest {
    @Test
    fun `when one consumer fails and another is slow then every stop starts within one timeout`() {
        val failure = IllegalStateException("failed stop")
        val failed = TestManagedConsumer(completeShutdownOnStop = false) { throw failure }
        val slow = TestManagedConsumer(completeShutdownOnStop = false) { awaitCancellation() }
        val successful = TestManagedConsumer { }
        val lifecycle = lifecycle(
            timeout = Duration.ofMillis(100),
            "failed" to failed,
            "slow" to slow,
            "successful" to successful
        )
        lifecycle.start()

        lateinit var thrown: CkcConsumersShutdownException
        val elapsedMillis = measureTimeMillis {
            thrown = assertThrows(CkcConsumersShutdownException::class.java) {
                lifecycle.stop()
            }
        }

        assertTrue(failed.stopStarted.isCompleted)
        assertTrue(slow.stopStarted.isCompleted)
        assertTrue(successful.stopStarted.isCompleted)
        assertTrue(elapsedMillis < 1_000L, "shared timeout took ${elapsedMillis}ms")
        assertEquals(mapOf("failed" to failure), thrown.failures)
        assertEquals(setOf("failed", "slow"), thrown.unfinishedConsumerNames)
        assertTrue(lifecycle.isRunning("failed"))
        assertTrue(lifecycle.isRunning("slow"))
        assertFalse(lifecycle.isRunning("successful"))
        assertTrue(lifecycle.isRunning)
        assertTrue(lifecycle.lifecycleStarted)
    }

    @Test
    fun `when consumer completes after Spring timeout then running state updates automatically`() {
        val consumer = TestManagedConsumer(completeShutdownOnStop = false) { awaitCancellation() }
        val lifecycle = lifecycle(
            timeout = Duration.ofMillis(100),
            "slow" to consumer
        )
        lifecycle.start()

        val thrown = assertThrows(CkcConsumersShutdownException::class.java) {
            lifecycle.stop()
        }

        assertEquals(setOf("slow"), thrown.unfinishedConsumerNames)
        assertTrue(lifecycle.isRunning("slow"))
        assertTrue(lifecycle.isRunning)
        assertTrue(lifecycle.lifecycleStarted)

        consumer.completeShutdown()

        waitUntil { !lifecycle.isRunning("slow") }
        assertFalse(lifecycle.isRunning)
        assertFalse(lifecycle.lifecycleStarted)
    }

    @Test
    fun `when several consumers fail then every error is retained and lifecycle finishes`() {
        val firstFailure = IllegalArgumentException("first")
        val secondFailure = IllegalStateException("second")
        val lifecycle = lifecycle(
            timeout = Duration.ofSeconds(2),
            "first" to TestManagedConsumer { throw firstFailure },
            "second" to TestManagedConsumer { throw secondFailure },
            "successful" to TestManagedConsumer { }
        )
        lifecycle.start()

        val thrown = assertThrows(CkcConsumersShutdownException::class.java) {
            lifecycle.stop()
        }

        assertEquals(
            mapOf("first" to firstFailure, "second" to secondFailure),
            thrown.failures
        )
        assertTrue(thrown.unfinishedConsumerNames.isEmpty())
        assertEquals(2, thrown.suppressed.size)
        assertFalse(lifecycle.isRunning)
        assertFalse(lifecycle.lifecycleStarted)
        assertTrue(lifecycle.consumerNames.none(lifecycle::isRunning))
    }

    private fun lifecycle(
        timeout: Duration,
        vararg consumers: Pair<String, TestManagedConsumer>
    ): CkcConsumersLifecycle {
        val properties = CkcConsumerProperties(
            lifecycle = CkcConsumerProperties.Lifecycle(
                shutdownTimeout = timeout,
                terminalFailureAction = CkcConsumerProperties.TerminalFailureAction.REPORT_ONLY
            )
        )
        val applicationContext = GenericApplicationContext().apply {
            beanFactory.registerSingleton("ckcConsumerProperties", properties)
            refresh()
        }
        return CkcConsumersLifecycle(
            applicationContext = applicationContext,
            providedConsumerRuntimes = consumers.map { (name, consumer) -> runtime(name, consumer) }
        )
    }

    private fun runtime(name: String, consumer: ManagedCkcConsumer) = NamedConsumerRuntime(
        name = name,
        autoStartup = true,
        handler = "test-handler",
        cluster = "test-cluster",
        topics = listOf("test-topic"),
        topicPattern = null,
        groupId = "test-group",
        clientId = null,
        processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
        workerConcurrency = 1,
        consumerPollLoopConcurrency = 1,
        processingDispatcher = "dispatchers-default",
        retrySchema = null,
        metrics = "none",
        consumer = consumer
    )

    private fun waitUntil(condition: () -> Boolean) {
        val deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos()
        while (!condition()) {
            if (System.nanoTime() >= deadline) {
                throw AssertionError("Condition was not met within 2 seconds")
            }
            Thread.sleep(10)
        }
    }

    private class TestManagedConsumer(
        private val completeShutdownOnStop: Boolean = true,
        private val stopBlock: suspend () -> Unit
    ) : ManagedCkcConsumer {
        val stopStarted = CompletableDeferred<Unit>()
        private val termination = CompletableDeferred<Unit>()
        private val shutdownCompletion = CompletableDeferred<Unit>()

        override fun start() = Unit

        override suspend fun stop() {
            stopStarted.complete(Unit)
            try {
                stopBlock()
            } finally {
                if (completeShutdownOnStop) {
                    completeShutdown()
                }
            }
        }

        override suspend fun awaitTermination() = termination.await()

        override suspend fun awaitShutdownCompletion() = shutdownCompletion.await()

        fun completeShutdown() {
            termination.complete(Unit)
            shutdownCompletion.complete(Unit)
        }

        override fun stateSnapshot(): ConsumerStateSnapshot = error("not needed by this test")
    }
}
