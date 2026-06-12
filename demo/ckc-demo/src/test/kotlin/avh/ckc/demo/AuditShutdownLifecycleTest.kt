package avh.ckc.demo

import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.internal.AuditLogFlusher
import avh.ckc.demo.internal.AuditShutdownLifecycle
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class AuditShutdownLifecycleTest {
    @Test
    fun `audit lifecycle stops in the last shutdown phase and flushes once`() {
        val flusher = CountingAuditLogFlusher()
        val properties = DemoApplicationProperties().apply {
            audit.enabled = true
        }
        val lifecycle = AuditShutdownLifecycle(flusher, properties)

        assertEquals(Int.MIN_VALUE + 2, lifecycle.phase)
        assertTrue(lifecycle.isAutoStartup)
        assertFalse(lifecycle.isRunning)

        lifecycle.start()
        assertTrue(lifecycle.isRunning)

        lifecycle.stop()
        lifecycle.stop()

        assertFalse(lifecycle.isRunning)
        assertEquals(1, flusher.flushCount)
    }

    @Test
    fun `audit lifecycle does not flush when audit is disabled`() {
        val flusher = CountingAuditLogFlusher()
        val properties = DemoApplicationProperties().apply {
            audit.enabled = false
        }
        val lifecycle = AuditShutdownLifecycle(flusher, properties)

        lifecycle.start()
        lifecycle.stop()

        assertFalse(lifecycle.isRunning)
        assertEquals(0, flusher.flushCount)
    }

    private class CountingAuditLogFlusher : AuditLogFlusher {
        var flushCount = 0

        override fun flushAndStop() {
            flushCount++
        }
    }
}
