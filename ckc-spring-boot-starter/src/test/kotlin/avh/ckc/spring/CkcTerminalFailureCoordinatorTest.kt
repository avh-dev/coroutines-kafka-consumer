package avh.ckc.spring

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class CkcTerminalFailureCoordinatorTest {
    @Test
    fun `when terminal failure is triggered repeatedly then context closes before one exit`() {
        val order = AtomicInteger(0)
        val exited = CountDownLatch(1)
        val coordinator = CkcTerminalFailureCoordinator(
            closeContext = { assertEquals(0, order.getAndIncrement()) },
            exitHandler = CkcTerminalFailureExitHandler {
                assertEquals(1, order.getAndIncrement())
                exited.countDown()
            },
            haltHandler = CkcTerminalFailureHaltHandler { error("halt must not be called") },
            closeTimeoutMillis = 1_000L
        )

        coordinator.trigger()
        coordinator.trigger()

        assertTrue(exited.await(2, TimeUnit.SECONDS))
        assertEquals(2, order.get())
    }

    @Test
    fun `when context close is stuck then exit is requested after deadline`() {
        val releaseClose = CountDownLatch(1)
        val exited = CountDownLatch(1)
        val coordinator = CkcTerminalFailureCoordinator(
            closeContext = { releaseClose.await() },
            exitHandler = CkcTerminalFailureExitHandler { exited.countDown() },
            haltHandler = CkcTerminalFailureHaltHandler { error("halt must not be called") },
            closeTimeoutMillis = 25L
        )

        coordinator.trigger()

        assertTrue(exited.await(2, TimeUnit.SECONDS))
        releaseClose.countDown()
    }

    @Test
    fun `when normal exit is blocked then forced halt is requested`() {
        val releaseExit = CountDownLatch(1)
        val halted = CountDownLatch(1)
        val coordinator = CkcTerminalFailureCoordinator(
            closeContext = {},
            exitHandler = CkcTerminalFailureExitHandler { releaseExit.await() },
            haltHandler = CkcTerminalFailureHaltHandler {
                halted.countDown()
                releaseExit.countDown()
            },
            closeTimeoutMillis = 25L,
            exitTimeoutMillis = 25L
        )

        coordinator.trigger()

        assertTrue(halted.await(2, TimeUnit.SECONDS))
    }
}
