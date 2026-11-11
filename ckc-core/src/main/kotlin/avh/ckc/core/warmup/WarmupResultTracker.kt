package avh.ckc.core.warmup

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.time.Duration

internal class WarmupResultTimeoutException(recordId: WarmupRecordId) :
    IllegalStateException("Timed out waiting for warm-up result $recordId")

internal class WarmupEarlyResultOverflowException(limit: Int) :
    IllegalStateException("Warm-up early-result limit $limit exceeded")

internal class WarmupResultTracker(
    private val sessionId: WarmupSessionId,
    private val maxEarlyResults: Int,
    private val earlyResultTtl: Duration,
    private val nanoTime: () -> Long = System::nanoTime,
    private val onPendingChanged: (Int) -> Unit = {}
) {
    private data class TimedOutcome(val outcome: WarmupRemoteOutcome, val receivedAt: Long)

    enum class Completion {
        DELIVERED,
        STORED_EARLY,
        DUPLICATE,
        IGNORED,
        OVERFLOW
    }

    private val lock = Any()
    private val waiters = mutableMapOf<WarmupRecordId, CompletableDeferred<WarmupRemoteOutcome>>()
    private val early = linkedMapOf<WarmupRecordId, TimedOutcome>()
    private val retired = linkedMapOf<WarmupRecordId, Long>()
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
            early.remove(recordId)?.let {
                retire(recordId, nanoTime())
                return it.outcome
            }
            check(recordId !in retired) { "Warm-up result $recordId is already completed" }
            check(waiters.putIfAbsent(recordId, waiter) == null) { "Warm-up result $recordId is already awaited" }
            onPendingChanged(waiters.size)
        }

        try {
            val result = withTimeoutOrNull(timeout) { waiter.await() }
            if (result != null) return result
            val timeoutWon = synchronized(lock) {
                if (waiters[recordId] === waiter) {
                    waiters.remove(recordId)
                    retire(recordId, nanoTime())
                    onPendingChanged(waiters.size)
                    true
                } else false
            }
            if (timeoutWon) throw WarmupResultTimeoutException(recordId)
            return waiter.await()
        } catch (error: CancellationException) {
            synchronized(lock) {
                if (waiters[recordId] === waiter) {
                    waiters.remove(recordId)
                    retire(recordId, nanoTime())
                    onPendingChanged(waiters.size)
                }
            }
            throw error
        }
    }

    fun complete(completed: WarmupRecordCompleted): Completion = synchronized(lock) {
        val recordId = completed.recordId
        if (recordId.sessionId != sessionId || closed || failure != null) return@synchronized Completion.IGNORED
        val now = nanoTime()
        purgeExpired(now)
        if (recordId in retired || recordId in early) return@synchronized Completion.DUPLICATE
        val waiter = waiters.remove(recordId)
        if (waiter != null) {
            retire(recordId, now)
            waiter.complete(completed.outcome)
            onPendingChanged(waiters.size)
            Completion.DELIVERED
        } else if (early.size >= maxEarlyResults) {
            Completion.OVERFLOW
        } else {
            early[recordId] = TimedOutcome(completed.outcome, now)
            Completion.STORED_EARLY
        }
    }

    fun fail(error: Throwable) {
        val pending = synchronized(lock) {
            if (failure != null || closed) return
            failure = error
            val snapshot = waiters.values.toList()
            waiters.clear()
            early.clear()
            onPendingChanged(0)
            snapshot
        }
        pending.forEach { it.completeExceptionally(error) }
    }

    fun close() {
        val pending = synchronized(lock) {
            if (closed) return
            closed = true
            val snapshot = waiters.values.toList()
            waiters.clear()
            early.clear()
            onPendingChanged(0)
            snapshot
        }
        val error = CancellationException("Warm-up result tracker closed")
        pending.forEach { it.completeExceptionally(error) }
    }

    fun pendingCount(): Int = synchronized(lock) { waiters.size }

    private fun purgeExpired(now: Long) {
        val ttlNanos = earlyResultTtl.inWholeNanoseconds
        early.entries.removeIf { now - it.value.receivedAt >= ttlNanos || now < it.value.receivedAt }
        retired.entries.removeIf { now - it.value >= ttlNanos || now < it.value }
    }

    private fun retire(recordId: WarmupRecordId, now: Long) {
        retired[recordId] = now
        while (retired.size > maxEarlyResults * 2) {
            retired.remove(retired.entries.first().key)
        }
    }
}
