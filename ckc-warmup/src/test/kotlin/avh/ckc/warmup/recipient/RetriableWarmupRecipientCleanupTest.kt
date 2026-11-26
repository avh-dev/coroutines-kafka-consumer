package avh.ckc.warmup.recipient

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Test

class RetriableWarmupRecipientCleanupTest {
    @Test
    fun `incomplete cleanup remains available for a successful retry`() = runBlocking {
        var attempts = 0
        var completed = 0
        val cleanup = RetriableWarmupRecipientCleanup(
            terminate = {
                attempts += 1
                if (attempts == 1) {
                    WarmupRecipientCleanupResult.Incomplete(emptyList(), listOf("stop timed out"))
                } else {
                    WarmupRecipientCleanupResult.Complete
                }
            },
            afterComplete = { completed += 1 }
        )

        assertInstanceOf(
            WarmupRecipientCleanupIncompleteException::class.java,
            runCatching { cleanup.run() }.exceptionOrNull()
        )
        assertEquals(0, completed)

        cleanup.run()

        assertEquals(2, attempts)
        assertEquals(1, completed)
    }
}
