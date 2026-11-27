package avh.ckc.demo.consumer.ckc

import avh.ckc.core.ConsumerContainerParticipant
import avh.ckc.core.consumerContainer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertFails
import kotlin.test.assertTrue

class CkcConsumersLifecycleTest {
    @Test
    fun `telemetry stop failure does not leave batch and order running`() {
        val telemetry = Participant(stopFailure = IllegalStateException("telemetry stop failed"))
        val batch = Participant()
        val order = Participant()
        val lifecycle = CkcConsumersLifecycle(consumerContainer(listOf(telemetry, batch, order)))
        lifecycle.start()

        assertFails { lifecycle.stop() }

        assertEquals(1, telemetry.stopCalls)
        assertEquals(1, batch.stopCalls)
        assertEquals(1, order.stopCalls)
        assertFalse(lifecycle.isRunning)
    }

    @Test
    fun `incomplete cleanup keeps lifecycle running and is retried`() {
        val telemetry = Participant(completeAfterStop = false)
        val batch = Participant()
        val order = Participant()
        val lifecycle = CkcConsumersLifecycle(consumerContainer(listOf(telemetry, batch, order)))
        lifecycle.start()

        assertFails { lifecycle.stop() }
        assertTrue(lifecycle.isRunning)

        telemetry.completeAfterStop = true
        lifecycle.stop()

        assertEquals(2, telemetry.stopCalls)
        assertEquals(1, batch.stopCalls)
        assertEquals(1, order.stopCalls)
        assertFalse(lifecycle.isRunning)
    }

    private class Participant(
        private val stopFailure: Throwable? = null,
        var completeAfterStop: Boolean = true
    ) : ConsumerContainerParticipant {
        var stopCalls = 0
        private var stopped = false

        override fun start() = Unit

        override suspend fun awaitReady() = Unit

        override suspend fun stop() {
            stopCalls++
            stopped = completeAfterStop
            stopFailure?.let { throw it }
        }

        override fun isStopComplete(): Boolean = stopped
    }
}
