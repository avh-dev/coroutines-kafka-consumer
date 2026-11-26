package avh.ckc.warmup

import avh.ckc.warmup.protocol.WarmupRecordCompleted
import avh.ckc.warmup.protocol.WarmupRecordId
import avh.ckc.warmup.protocol.WarmupRemoteOutcome
import avh.ckc.warmup.protocol.WarmupSessionId
import java.util.UUID
import kotlin.time.Duration
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.kafka.common.TopicPartition

/** Signals that an acknowledged delegation outlived its result wait. */
internal class WarmupResultTimeoutException(recordId: WarmupRecordId) :
    IllegalStateException("Timed out waiting for warm-up result $recordId")

/** Bounds the storage of results waiting for a forwarding acknowledgement. */
internal class WarmupEarlyResultOverflowException(limit: Int) :
    IllegalStateException("Warm-up early-result limit $limit exceeded")

/**
 * Matches remote outcomes to one donor session without performing I/O under its lock.
 *
 * A delegation ID matches a result even before the forwarding ACK identifies the Kafka offset.
 * Results from peers without this header use the offset and may be buffered in [earlyResults].
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

    class Delegation(val destination: TopicPartition) {
        val id: UUID = UUID.randomUUID()
        val result = CompletableDeferred<WarmupRecordCompleted>()
    }

    private val lock = Any()
    private val delegations = mutableMapOf<UUID, Delegation>()
    private val waiters = mutableMapOf<WarmupRecordId, CompletableDeferred<WarmupRecordCompleted>>()
    private val earlyResults = linkedMapOf<WarmupRecordId, TimedOutcome>()
    private val recentlyCompletedRecordIds = linkedSetOf<WarmupRecordId>()
    private val recentlyCompletedLimit =
        (maxEarlyResults.toLong() * 2).coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
    private var failure: Throwable? = null
    private var closed = false
    private val forwardingAckWaitEnded = CompletableDeferred<Unit>()

    /** A forwarding ACK can still identify a legacy result buffered before the session fence. */
    val forwardingAckCutoff: Deferred<Unit> get() = forwardingAckWaitEnded

    fun stopWaitingForForwardingAcks() {
        forwardingAckWaitEnded.complete(Unit)
    }

    init {
        require(maxEarlyResults > 0) { "maxEarlyResults must be > 0" }
        require(earlyResultTtl.isPositive()) { "earlyResultTtl must be > 0" }
    }

    fun register(destination: TopicPartition): Delegation {
        val delegation = Delegation(destination)
        synchronized(lock) { delegations[delegation.id] = delegation }
        return delegation
    }

    fun unregister(delegation: Delegation) {
        synchronized(lock) { delegations.remove(delegation.id) }
    }

    suspend fun await(
        recordId: WarmupRecordId,
        timeout: Duration,
        delegation: Delegation? = null
    ): WarmupRemoteOutcome {
        require(recordId.sessionId == sessionId) { "Cannot await a foreign warm-up session" }
        require(timeout.isPositive()) { "Warm-up result timeout must be > 0" }
        val waiter = delegation?.result ?: CompletableDeferred()
        val registered = synchronized(lock) {
            check(!closed) { "Warm-up result tracker is closed" }
            if (waiter.isCompleted) return@synchronized false
            if (failure == null) purgeExpired(nanoTime())
            earlyResults.remove(recordId)?.let {
                rememberCompleted(recordId)
                if (failure != null && earlyResults.isEmpty()) stopWaitingForForwardingAcks()
                return it.outcome
            }
            failure?.let { throw it }
            check(recordId !in recentlyCompletedRecordIds) { "Warm-up result $recordId is already completed" }
            check(waiters.putIfAbsent(recordId, waiter) == null) {
                "Warm-up result $recordId is already awaited"
            }
            onPendingChanged(waiters.size)
            true
        }
        if (!registered) return waiter.await().outcome

        try {
            val result = withTimeoutOrNull(timeout) { waiter.await() }
            if (result != null) return result.outcome
            val timeoutWon = synchronized(lock) {
                !waiter.isCompleted && removeWaiter(recordId, waiter)
            }
            if (timeoutWon) throw WarmupResultTimeoutException(recordId)
            // Completion removed this waiter while the timeout path was competing for the same lock.
            // The deferred is therefore already completing; await it instead of reporting a false timeout.
            return waiter.await().outcome
        } finally {
            synchronized(lock) { removeWaiter(recordId, waiter) }
        }
    }

    private fun removeWaiter(
        recordId: WarmupRecordId,
        waiter: CompletableDeferred<WarmupRecordCompleted>
    ): Boolean {
        if (!waiters.remove(recordId, waiter)) return false
        rememberCompleted(recordId)
        onPendingChanged(waiters.size)
        return true
    }

    fun complete(completed: WarmupRecordCompleted): Completion = synchronized(lock) {
        val recordId = completed.recordId
        if (recordId.sessionId != sessionId || closed || failure != null) {
            return@synchronized Completion.IGNORED
        }
        completed.delegationId?.let { id ->
            val delegation = delegations[id] ?: return@synchronized Completion.IGNORED
            if (recordId.topicPartition != delegation.destination) return@synchronized Completion.IGNORED
            return@synchronized if (delegation.result.complete(completed)) {
                Completion.DELIVERED
            } else {
                Completion.DUPLICATE
            }
        }
        val now = nanoTime()
        purgeExpired(now)
        if (recordId in recentlyCompletedRecordIds || recordId in earlyResults) {
            return@synchronized Completion.DUPLICATE
        }
        val waiter = waiters.remove(recordId)
        if (waiter != null) {
            rememberCompleted(recordId)
            waiter.complete(completed)
            onPendingChanged(waiters.size)
            Completion.DELIVERED
        } else if (earlyResults.size >= maxEarlyResults) {
            Completion.OVERFLOW
        } else {
            earlyResults[recordId] = TimedOutcome(completed.outcome, now)
            Completion.STORED_EARLY
        }
    }

    fun fail(error: Throwable, preserveEarlyResults: Boolean = false) {
        val pending = synchronized(lock) {
            if (closed) return
            val existingFailure = failure
            if (existingFailure == null) failure = error
            if (preserveEarlyResults && earlyResults.isEmpty()) stopWaitingForForwardingAcks()
            if (existingFailure != null) return
            clearBufferedStateAndSnapshotWaiters(preserveEarlyResults)
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
    private fun clearBufferedStateAndSnapshotWaiters(
        preserveEarlyResults: Boolean = false
    ): List<CompletableDeferred<WarmupRecordCompleted>> {
        val pending = waiters.values.toList()
        waiters.clear()
        if (!preserveEarlyResults) earlyResults.clear()
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
