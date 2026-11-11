package avh.ckc.core.warmup

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration.Companion.seconds

class IntervalQuotaTest {
    @Test
    fun `concurrent acquisition never exceeds quota`() = runBlocking {
        val quota = IntervalQuota(limit = 10, interval = 1.seconds, nanoTime = { 100L })

        val admitted = List(100) { async(Dispatchers.Default) { quota.tryAcquire() } }.awaitAll().count { it }

        assertEquals(10, admitted)
    }

    @Test
    fun `new interval opens full quota without regard to old work`() {
        val now = AtomicLong(0L)
        val quota = IntervalQuota(limit = 2, interval = 1.seconds, nanoTime = now::get)

        assertEquals(listOf(true, true, false), List(3) { quota.tryAcquire() })
        now.set(1.seconds.inWholeNanoseconds)
        assertEquals(listOf(true, true, false), List(3) { quota.tryAcquire() })
    }
}
