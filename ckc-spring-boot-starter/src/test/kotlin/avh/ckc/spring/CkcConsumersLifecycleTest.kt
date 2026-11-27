package avh.ckc.spring

import avh.ckc.core.ConsumerStateSnapshot
import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.ProcessingMode
import avh.ckc.core.coroutinesKafkaConsumer
import avh.ckc.core.extension.ManagedConsumerLifecycle
import avh.ckc.core.metrics.ConsumerMetrics
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.context.support.GenericApplicationContext
import java.time.Duration
import kotlin.system.measureTimeMillis
import kotlin.time.Duration.Companion.seconds

class CkcConsumersLifecycleTest {
    @Test
    fun `automatic and named lifecycle keep manual consumer isolated`() {
        val automatic = TestManagedConsumer { }
        val manual = TestManagedConsumer { }
        val lifecycle = lifecycle(
            timeout = Duration.ofSeconds(2),
            runtimes = listOf(
                runtime("automatic", automatic, autoStartup = true),
                runtime("manual", manual, autoStartup = false)
            )
        )

        lifecycle.start()

        assertEquals(1, automatic.starts)
        assertEquals(0, manual.starts)
        lifecycle.start("manual")
        assertEquals(1, manual.starts)
        lifecycle.stop("manual")
        assertTrue(lifecycle.isRunning("automatic"))
        assertFalse(lifecycle.isRunning("manual"))
        lifecycle.stop()
    }

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
        assertFalse(thrown.timedOut)
        assertTrue(thrown.lifecycleFailures.all { it.phase == avh.ckc.core.ConsumerContainerPhase.STOP })
    }

    @Test
    fun `partial startup failure retains every prepared consumer for application shutdown`() {
        val first = TestManagedConsumer { }
        val failed = StartFailingManagedConsumer(IllegalStateException("start failed"))
        val neverStarted = TestManagedConsumer { }
        val lifecycle = lifecycle(
            timeout = Duration.ofSeconds(2),
            "first" to first,
            "failed" to failed,
            "later" to neverStarted
        )

        assertThrows(IllegalStateException::class.java) { lifecycle.start() }
        lifecycle.stop()

        assertTrue(first.stopStarted.isCompleted)
        assertTrue(failed.stopStarted.isCompleted)
        assertTrue(neverStarted.stopStarted.isCompleted)
        assertFalse(lifecycle.isRunning)
    }

    @Test
    fun `later application stop retries only incomplete cleanup`() {
        val retrying = RetriableManagedConsumer()
        val complete = TestManagedConsumer { }
        val lifecycle = lifecycle(
            timeout = Duration.ofSeconds(2),
            "retrying" to retrying,
            "complete" to complete
        )
        lifecycle.start()

        val first = assertThrows(CkcConsumersShutdownException::class.java) { lifecycle.stop() }
        lifecycle.stop()

        assertEquals(setOf("retrying"), first.unfinishedConsumerNames)
        assertEquals(2, retrying.stopCalls)
        assertEquals(1, complete.stopCalls)
    }

    @Test
    fun `Spring timeout does not cancel container owned cleanup`() = runBlocking {
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val consumer = GatedManagedConsumer(entered, release)
        val lifecycle = lifecycle(Duration.ofMillis(100), "slow" to consumer)
        lifecycle.start()

        val stop = async(Dispatchers.Default) {
            assertThrows(CkcConsumersShutdownException::class.java) { lifecycle.stop() }
        }
        entered.await()
        val timeout = stop.await()

        assertTrue(timeout.timedOut)
        assertFalse(consumer.cancelled)
        release.complete(Unit)
        withTimeout(1.seconds) {
            while (lifecycle.isRunning) kotlinx.coroutines.delay(10)
        }
        assertEquals(1, consumer.stopCalls)
    }

    @Test
    fun `individual and application stop may overlap without losing either cleanup`() = runBlocking {
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val first = GatedManagedConsumer(entered, release)
        val second = TestManagedConsumer { }
        val lifecycle = lifecycle(
            timeout = Duration.ofSeconds(2),
            "first" to first,
            "second" to second
        )
        lifecycle.start()

        val individual = async(Dispatchers.Default) { lifecycle.stop("first") }
        entered.await()
        val application = async(Dispatchers.Default) { lifecycle.stop() }
        second.stopStarted.await()
        release.complete(Unit)
        individual.await()
        application.await()

        assertEquals(1, first.stopCalls)
        assertEquals(1, second.stopCalls)
        assertFalse(lifecycle.isRunning)
    }

    @Test
    fun `stop before lifecycle start releases a real prepared consumer and its parent job`() = runBlocking {
        val parent = Job()
        val consumer = coroutinesKafkaConsumer<String, String>(realConsumerProperties()) {
            topics("prepared")
            parentContext = parent + Dispatchers.Default
            @Suppress("UNCHECKED_CAST")
            metrics = ConsumerMetrics.NOOP as ConsumerMetrics<String, String>
            handle { }
        }
        val lifecycle = lifecycle(
            timeout = Duration.ofSeconds(2),
            "prepared" to ActualManagedConsumer(consumer)
        )

        lifecycle.stop()

        withTimeout(1.seconds) {
            consumer.awaitShutdownCompletion()
            while (parent.children.any()) kotlinx.coroutines.delay(10)
        }
        assertTrue(consumer.isStopComplete())
        assertTrue(parent.children.none())
    }

    @Test
    fun `application stop releases participants with a coordinated consumer lifecycle`() = runBlocking {
        val consumers = List(3) { index ->
            coroutinesKafkaConsumer<String, String>(realConsumerProperties()) {
                topics("source-$index")
                handle { }
            }.also { consumer ->
                val runtime = consumer.runtimeControl
                consumer.manageLifecycle(object : ManagedConsumerLifecycle {
                    override fun start() = runtime.start()
                    override suspend fun awaitReady() = runtime.awaitReady()
                    override suspend fun stop() = runtime.stop()
                    override suspend fun awaitTermination() = runtime.awaitTermination()
                    override fun isStopComplete() = runtime.isShutdownComplete()
                })
            }
        }
        val lifecycle = lifecycle(
            Duration.ofSeconds(2),
            *consumers.mapIndexed { index, consumer ->
                "consumer-$index" to ActualManagedConsumer(consumer)
            }.toTypedArray()
        )

        lifecycle.stop()

        consumers.forEach { consumer ->
            withTimeout(1.seconds) { consumer.awaitShutdownCompletion() }
            assertTrue(consumer.isStopComplete())
        }
    }

    @Test
    fun `fatal JVM stop error is not translated to a Spring aggregate`() {
        val fatal = TestFatalError("fatal")
        val lifecycle = lifecycle(
            timeout = Duration.ofSeconds(2),
            "fatal" to TestManagedConsumer { throw fatal },
            "other" to TestManagedConsumer { }
        )
        lifecycle.start()

        val thrown = assertThrows(TestFatalError::class.java) { lifecycle.stop() }

        assertTrue(thrown === fatal)
    }

    @Test
    fun `prior fatal participant does not contaminate named stop of its neighbor`() {
        val fatal = TestFatalError("fatal")
        val neighbor = TestManagedConsumer { }
        val lifecycle = lifecycle(
            timeout = Duration.ofSeconds(2),
            "fatal" to TestManagedConsumer { throw fatal },
            "neighbor" to neighbor
        )
        lifecycle.start()

        assertThrows(TestFatalError::class.java) { lifecycle.stop("fatal") }
        lifecycle.stop("neighbor")

        assertEquals(1, neighbor.stopCalls)
    }

    private fun lifecycle(
        timeout: Duration,
        vararg consumers: Pair<String, ManagedCkcConsumer>
    ): CkcConsumersLifecycle = lifecycle(
        timeout,
        consumers.map { (name, consumer) -> runtime(name, consumer) }
    )

    private fun lifecycle(
        timeout: Duration,
        runtimes: List<NamedConsumerRuntime>
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
            providedConsumerRuntimes = runtimes
        )
    }

    private fun runtime(
        name: String,
        consumer: ManagedCkcConsumer,
        autoStartup: Boolean = true
    ) = NamedConsumerRuntime(
        name = name,
        autoStartup = autoStartup,
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
        var starts = 0
            private set
        val stopStarted = CompletableDeferred<Unit>()
        private val termination = CompletableDeferred<Unit>()
        private val shutdownCompletion = CompletableDeferred<Unit>()
        var stopCalls = 0
            private set

        override fun start() {
            starts += 1
        }

        override suspend fun stop() {
            stopCalls += 1
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

        override fun isStopComplete(): Boolean = shutdownCompletion.isCompleted

        fun completeShutdown() {
            termination.complete(Unit)
            shutdownCompletion.complete(Unit)
        }

        override fun stateSnapshot(): ConsumerStateSnapshot = error("not needed by this test")
    }

    private class StartFailingManagedConsumer(
        private val failure: Throwable
    ) : ManagedCkcConsumer {
        val stopStarted = CompletableDeferred<Unit>()
        private var stopped = false
        override fun start(): Unit = throw failure
        override suspend fun stop() {
            stopStarted.complete(Unit)
            stopped = true
        }
        override suspend fun awaitTermination() = Unit
        override suspend fun awaitShutdownCompletion() = Unit
        override fun isStopComplete(): Boolean = stopped
        override fun stateSnapshot(): ConsumerStateSnapshot = error("not needed")
    }

    private class RetriableManagedConsumer : ManagedCkcConsumer {
        var stopCalls = 0
            private set
        private var stopped = false
        override fun start() = Unit
        override suspend fun stop() {
            stopCalls += 1
            if (stopCalls == 1) throw IllegalStateException("cleanup incomplete")
            stopped = true
        }
        override suspend fun awaitTermination() = Unit
        override suspend fun awaitShutdownCompletion() = Unit
        override fun isStopComplete(): Boolean = stopped
        override fun stateSnapshot(): ConsumerStateSnapshot = error("not needed")
    }

    private class GatedManagedConsumer(
        private val entered: CompletableDeferred<Unit>,
        private val release: CompletableDeferred<Unit>
    ) : ManagedCkcConsumer {
        var stopCalls = 0
            private set
        var cancelled = false
            private set
        private var stopped = false
        override fun start() = Unit
        override suspend fun stop() {
            stopCalls += 1
            entered.complete(Unit)
            try {
                release.await()
                stopped = true
            } catch (error: kotlinx.coroutines.CancellationException) {
                cancelled = true
                throw error
            }
        }
        override suspend fun awaitTermination() = Unit
        override suspend fun awaitShutdownCompletion() = Unit
        override fun isStopComplete(): Boolean = stopped
        override fun stateSnapshot(): ConsumerStateSnapshot = error("not needed")
    }

    private class ActualManagedConsumer<K, V>(
        private val consumer: CoroutinesKafkaConsumer<K, V>
    ) : ManagedCkcConsumer {
        override fun start() = consumer.start()
        override suspend fun stop() = consumer.stop()
        override suspend fun awaitTermination() = consumer.awaitTermination()
        override suspend fun awaitShutdownCompletion() = consumer.awaitShutdownCompletion()
        override fun isStopComplete(): Boolean = consumer.isStopComplete()
        override fun stateSnapshot(): ConsumerStateSnapshot = consumer.stateSnapshot()
    }

    private class TestFatalError(message: String) : VirtualMachineError(message)

    private fun realConsumerProperties(): Map<String, Any?> = mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "unused:9092",
        ConsumerConfig.GROUP_ID_CONFIG to "spring-container-test",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java
    )
}
