package avh.ckc.core.warmup

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class WarmupExecutionDispatchersTest {
    @Test
    fun `parallel protocol work stays within the context thread limit`() = runBlocking {
        val execution = WarmupExecutionDispatchers("orders/context", 4, 5.seconds)
        val release = CountDownLatch(1)
        try {
            val workers = List(12) {
                async(execution.blocking) {
                    release.await()
                    Thread.currentThread().name
                }
            }
            withTimeout(5.seconds) {
                while (execution.liveThreadCount() < 3) delay(10)
            }
            val coordinationThread = withContext(execution.coordination) { Thread.currentThread().name }

            assertEquals(4, execution.liveThreadCount())
            assertTrue(coordinationThread.startsWith("ckc-warmup-orders-context-"))

            release.countDown()
            val names = workers.awaitAll()
            assertTrue(names.all { it.startsWith("ckc-warmup-orders-context-") })
        } finally {
            release.countDown()
            assertTrue(execution.close(5.seconds))
        }
    }

    @Test
    fun `idle workers expire and the context can resume service work`() = runBlocking {
        val execution = WarmupExecutionDispatchers("idle", 3, 50.milliseconds)
        try {
            val first = withContext(execution.blocking) { Thread.currentThread().name }
            withTimeout(5.seconds) {
                while (execution.liveThreadCount() != 0) delay(10)
            }

            val resumed = withContext(execution.blocking) { Thread.currentThread().name }

            assertTrue(first.startsWith("ckc-warmup-idle-"))
            assertTrue(resumed.startsWith("ckc-warmup-idle-"))
            assertFalse(first == resumed, "A new worker should be created after idle expiry")
        } finally {
            assertTrue(execution.close(5.seconds))
        }
    }

    @Test
    fun `saturated blocking work cannot starve coordination or retryable shutdown`() = runBlocking {
        val execution = WarmupExecutionDispatchers("saturated", 3, 5.seconds)
        val release = CountDownLatch(1)
        val blockers = List(2) { async(execution.blocking) { release.await() } }
        try {
            withTimeout(5.seconds) {
                while (execution.liveThreadCount() < 2) delay(10)
            }
            val coordinated = withTimeout(1.seconds) {
                withContext(execution.coordination) { true }
            }
            assertTrue(coordinated)

            assertFalse(execution.close(50.milliseconds))
            release.countDown()
            blockers.awaitAll()
            assertTrue(execution.close(5.seconds))
            assertEquals(0, execution.liveThreadCount())
        } finally {
            release.countDown()
        }
    }
}
