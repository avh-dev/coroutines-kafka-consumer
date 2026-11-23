package avh.ckc.warmup

import avh.ckc.warmup.protocol.WarmupRecordCompleted
import avh.ckc.warmup.protocol.WarmupRecordId
import avh.ckc.warmup.protocol.WarmupRemoteOutcome
import avh.ckc.warmup.protocol.WarmupSessionId
import kotlin.time.Duration
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.withTimeoutOrNull

internal class WarmupResultTimeoutException(recordId: WarmupRecordId) :
    IllegalStateException("Timed out waiting for warm-up result $recordId")

internal class WarmupEarlyResultOverflowException(limit: Int) :
    IllegalStateException("Warm-up early-result limit $limit exceeded")

/**
 * Matches remote outcomes to one donor session without performing I/O under its lock.
 *
 * A control result may overtake the producer coroutine between Kafka acknowledgement and waiter
 * registration, so [earlyResults] temporarily retains that result. [recentlyCompletedRecordIds]
 * suppresses late duplicates for already resolved coordinates; it is local duplicate bookkeeping,
 * not protocol session retirement.
 */
internal class WarmupResultTracker(
    private val sessionId: WarmupSessionId,
    private val maxEarlyResults: Int,
    private val earlyResultTtl: Duration,
    private val nanoTime: () -> Long = System::nanoTime,
    private val onPendingChanged: (Int) -> Unit = {}
) {
    internal enum class Completion {
        DELIVERED,
        STORED_EARLY,
        DUPLICATE,
        IGNORED,
        OVERFLOW
    }

    private data class TimedOutcome(val outcome: WarmupRemoteOutcome, val receivedAt: Long)

    private val lock = Any()
    private val waiters = mutableMapOf<WarmupRecordId, CompletableDeferred<WarmupRemoteOutcome>>()
    private val earlyResults = linkedMapOf<WarmupRecordId, TimedOutcome>()
    private val recentlyCompletedRecordIds = linkedSetOf<WarmupRecordId>()
    private val recentlyCompletedLimit =
        (maxEarlyResults.toLong() * 2).coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
    private var failure: Throwable? = null
    private var closed = false

    init {
        require(maxEarlyResults > 0) { "maxEarlyResults must be > 0" }
        require(earlyResultTtl.isPositive()) { "earlyResultTtl must be > 0" }
    }

    suspend fun await(recordId: WarmupRecordId, timeout: Duration): WarmupRemoteOutcome {
        require(recordId.sessionId == sessionId) { "Cannot await a foreign warm-up session" }
        require(timeout.isPositive()) { "Warm-up result timeout must be > 0" }
        val waiter = CompletableDeferred<WarmupRemoteOutcome>()
        synchronized(lock) {
            purgeExpired(nanoTime())
            failure?.let { throw it }
            check(!closed) { "Warm-up result tracker is closed" }
            earlyResults.remove(recordId)?.let {
                rememberCompleted(recordId)
                return it.outcome
            }
            check(recordId !in recentlyCompletedRecordIds) { "Warm-up result $recordId is already completed" }
            check(waiters.putIfAbsent(recordId, waiter) == null) {
                "Warm-up result $recordId is already awaited"
            }
            onPendingChanged(waiters.size)
        }

        try {
            val result = withTimeoutOrNull(timeout) { waiter.await() }
            if (result != null) return result
            val timeoutWon = synchronized(lock) {
                if (waiters[recordId] === waiter) {
                    waiters.remove(recordId)
                    rememberCompleted(recordId)
                    onPendingChanged(waiters.size)
                    true
                } else {
                    false
                }
            }
            if (timeoutWon) throw WarmupResultTimeoutException(recordId)
            // Completion removed this waiter while the timeout path was competing for the same lock.
            // The deferred is therefore already completing; await it instead of reporting a false timeout.
            return waiter.await()
        } catch (error: CancellationException) {
            synchronized(lock) {
                if (waiters[recordId] === waiter) {
                    waiters.remove(recordId)
                    rememberCompleted(recordId)
                    onPendingChanged(waiters.size)
                }
            }
            throw error
        }
    }

    fun complete(completed: WarmupRecordCompleted): Completion = synchronized(lock) {
        val recordId = completed.recordId
        if (recordId.sessionId != sessionId || closed || failure != null) {
            return@synchronized Completion.IGNORED
        }
        val now = nanoTime()
        purgeExpired(now)
        if (recordId in recentlyCompletedRecordIds || recordId in earlyResults) {
            return@synchronized Completion.DUPLICATE
        }
        val waiter = waiters.remove(recordId)
        if (waiter != null) {
            rememberCompleted(recordId)
            waiter.complete(completed.outcome)
            onPendingChanged(waiters.size)
            Completion.DELIVERED
        } else if (earlyResults.size >= maxEarlyResults) {
            Completion.OVERFLOW
        } else {
            earlyResults[recordId] = TimedOutcome(completed.outcome, now)
            Completion.STORED_EARLY
        }
    }

    fun fail(error: Throwable) {
        val pending = synchronized(lock) {
            if (failure != null || closed) return
            failure = error
            clearBufferedStateAndSnapshotWaiters()
        }
        pending.forEach { it.completeExceptionally(error) }
    }

    fun close() {
        val pending = synchronized(lock) {
            if (closed) return
            closed = true
            clearBufferedStateAndSnapshotWaiters()
        }
        val error = CancellationException("Warm-up result tracker closed")
        pending.forEach { it.completeExceptionally(error) }
    }

    fun pendingCount(): Int = synchronized(lock) { waiters.size }

    /** Clears waiters, early results, and duplicate history while returning waiters for out-of-lock completion. */
    private fun clearBufferedStateAndSnapshotWaiters(): List<CompletableDeferred<WarmupRemoteOutcome>> {
        val pending = waiters.values.toList()
        waiters.clear()
        earlyResults.clear()
        recentlyCompletedRecordIds.clear()
        onPendingChanged(0)
        return pending
    }

    private fun purgeExpired(now: Long) {
        val ttlNanos = earlyResultTtl.inWholeNanoseconds
        earlyResults.entries.removeIf { now < it.value.receivedAt || now - it.value.receivedAt >= ttlNanos }
    }

    private fun rememberCompleted(recordId: WarmupRecordId) {
        recentlyCompletedRecordIds += recordId
        while (recentlyCompletedRecordIds.size > recentlyCompletedLimit) {
            recentlyCompletedRecordIds.remove(recentlyCompletedRecordIds.first())
        }
    }
}
