package avh.ckc.core.warmup

import avh.ckc.core.CoroutinesKafkaConsumer
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.withTimeout
import org.apache.kafka.common.TopicPartition
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.mockito.kotlin.doSuspendableAnswer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class WarmupRecipientLifecycleTest {
    private val boundary = WarmupForwardingBoundary(mapOf(TopicPartition("warmup", 0) to null))

    @Test
    fun `main consumer is created only after successful drain`() = runBlocking {
        val warmup = mock<CoroutinesKafkaConsumer<String, String>>()
        val main = mock<CoroutinesKafkaConsumer<String, String>>()
        var created = 0
        val lifecycle = WarmupRecipientLifecycle({ warmup }, { created++; main })
        assertFailsWith<IllegalStateException> { lifecycle.startMain() }
        lifecycle.startWarmup()
        assertFailsWith<IllegalStateException> { lifecycle.startMain() }
        assertEquals(0, created)
        lifecycle.drain(boundary)
        verify(warmup).drainWarmupAndStop(boundary)
        lifecycle.startMain()
        assertEquals(1, created)
        verify(main).start()
        lifecycle.stop()
        verify(main).stop()
    }

    @Test
    fun `failed drain never allows main subscription`() = runBlocking {
        val warmup = mock<CoroutinesKafkaConsumer<String, String>>()
        whenever(warmup.drainWarmupAndStop(boundary)).thenThrow(IllegalStateException("commit failed"))
        val lifecycle = WarmupRecipientLifecycle({ warmup }, { error("Main must not be created") })
        lifecycle.startWarmup()
        assertFailsWith<IllegalStateException> { lifecycle.drain(boundary) }
        assertFailsWith<IllegalStateException> { lifecycle.startMain() }
        lifecycle.stop()
    }

    @Test
    fun `stop interrupts pending readiness without starting main`() = runBlocking {
        val warmup = mock<CoroutinesKafkaConsumer<String, String>>()
        val waiting = CompletableDeferred<Unit>()
        whenever(warmup.awaitReady()).doSuspendableAnswer {
            waiting.complete(Unit)
            awaitCancellation()
        }
        val lifecycle = WarmupRecipientLifecycle({ warmup }, { error("Main must not be created") })
        val start = async(start = CoroutineStart.UNDISPATCHED) {
            runCatching { lifecycle.startWarmup() }
        }
        withTimeout(2_000L) {
            waiting.await()
            lifecycle.stop()
            assertTrue(start.await().isFailure)
        }
        verify(warmup).stop()
        assertFailsWith<IllegalStateException> { lifecycle.startMain() }
        Unit
    }

    @Test
    fun `cleanup ignores the already observed warm-up terminal failure`() = runBlocking {
        val warmup = mock<CoroutinesKafkaConsumer<String, String>>()
        val failure = IllegalStateException("Kafka initialization failed")
        whenever(warmup.awaitReady()).thenThrow(failure)
        whenever(warmup.stop()).thenThrow(failure)
        val lifecycle = WarmupRecipientLifecycle({ warmup }, { error("Main must not be created") })

        val observed = assertFailsWith<IllegalStateException> { lifecycle.startWarmup() }
        lifecycle.stop(IllegalStateException("Coordinator observed warm-up failure", observed))

        verify(warmup).stop()
    }

    @Test
    fun `cleanup still reports an independent stop failure`() = runBlocking {
        val warmup = mock<CoroutinesKafkaConsumer<String, String>>()
        val startupFailure = IllegalStateException("Kafka initialization failed")
        val cleanupFailure = IllegalArgumentException("close failed")
        whenever(warmup.awaitReady()).thenThrow(startupFailure)
        whenever(warmup.stop()).thenThrow(cleanupFailure)
        val lifecycle = WarmupRecipientLifecycle({ warmup }, { error("Main must not be created") })

        val observed = assertFailsWith<IllegalStateException> { lifecycle.startWarmup() }
        val thrown = assertFailsWith<IllegalArgumentException> { lifecycle.stop(observed) }

        assertEquals(cleanupFailure.message, thrown.message)
    }

}
