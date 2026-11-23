package avh.ckc.warmup

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Retains ownership of recipient resources until discovery reports complete cleanup.
 * An incomplete attempt is intentionally repeatable by a later consumer stop call.
 */
internal class RetriableWarmupRecipientCleanup(
    private val terminate: suspend () -> WarmupRecipientCleanupResult,
    private val afterComplete: suspend () -> Unit
) {
    private val lock = Mutex()
    private var complete = false

    suspend fun run() = lock.withLock {
        if (complete) return@withLock
        when (val result = terminate()) {
            WarmupRecipientCleanupResult.Complete,
            is WarmupRecipientCleanupResult.CompleteWithDiagnostics -> {
                afterComplete()
                complete = true
            }
            is WarmupRecipientCleanupResult.Incomplete ->
                throw WarmupRecipientCleanupIncompleteException(result)
        }
    }
}

internal class WarmupRecipientCleanupIncompleteException(
    val result: WarmupRecipientCleanupResult.Incomplete
) : IllegalStateException(
    "Warm-up recipient cleanup is still incomplete for " +
        result.pendingAssignments.joinToString { it.destination.toString() } +
        result.diagnostics.joinToString(prefix = ": ")
)
