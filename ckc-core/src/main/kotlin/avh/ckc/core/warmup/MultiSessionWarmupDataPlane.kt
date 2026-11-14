package avh.ckc.core.warmup

import avh.ckc.core.processing.AdmittedRecordInterceptor
import avh.ckc.core.processing.RecordProcessingOutcome
import org.apache.kafka.clients.consumer.ConsumerRecord
import kotlin.time.Duration

/**
 * Atomically admits bounded donor sessions and selects at most one eligible session per record.
 * Session failures and drains remain isolated from the main consumer and from sibling sessions.
 */
internal class MultiSessionWarmupDataPlane<K, V>(
    private val maxSessions: Int,
    private val maxAggregateIntervalBudget: Int
) : AdmittedRecordInterceptor<K, V>, AutoCloseable {
    internal enum class Admission { ACCEPTED, DUPLICATE, SESSION_LIMIT, BUDGET_LIMIT }

    private val lock = Any()
    private val sessions = linkedMapOf<WarmupSessionId, WarmupDonorSession<K, V>>()
    private var aggregateIntervalBudget = 0
    private var nextIndex = 0

    init {
        require(maxSessions > 0) { "Maximum donor sessions must be > 0" }
        require(maxAggregateIntervalBudget > 0) { "Maximum aggregate warm-up budget must be > 0" }
    }

    fun admit(session: WarmupDonorSession<K, V>): Admission = synchronized(lock) {
        if (session.sessionId in sessions) return@synchronized Admission.DUPLICATE
        if (sessions.size >= maxSessions) return@synchronized Admission.SESSION_LIMIT
        if (session.intervalBudget > maxAggregateIntervalBudget - aggregateIntervalBudget) {
            return@synchronized Admission.BUDGET_LIMIT
        }
        sessions[session.sessionId] = session
        aggregateIntervalBudget += session.intervalBudget
        Admission.ACCEPTED
    }

    fun activate(sessionId: WarmupSessionId): Boolean =
        synchronized(lock) { sessions[sessionId] }?.activate() ?: false

    override suspend fun process(
        record: ConsumerRecord<K, V>,
        processLocally: suspend () -> RecordProcessingOutcome
    ): RecordProcessingOutcome {
        val selected = synchronized(lock) {
            val snapshot = sessions.values.filter(WarmupDonorSession<K, V>::isActive)
            if (snapshot.isEmpty()) {
                null
            } else {
                var reservation: Pair<WarmupDonorSession<K, V>, WarmupDestination>? = null
                repeat(snapshot.size) { offset ->
                    if (reservation == null) {
                        val index = (nextIndex + offset).mod(snapshot.size)
                        val session = snapshot[index]
                        session.tryReserve(record)?.let { destination ->
                            reservation = session to destination
                            nextIndex = (index + 1).mod(snapshot.size)
                        }
                    }
                }
                reservation
            }
        }
        return selected?.first?.processReserved(record, selected.second, processLocally) ?: processLocally()
    }

    fun accept(completed: WarmupRecordCompleted): WarmupResultTracker.Completion =
        synchronized(lock) { sessions[completed.recordId.sessionId] }
            ?.accept(completed)
            ?: WarmupResultTracker.Completion.IGNORED

    fun stopDelegating(sessionId: WarmupSessionId) {
        synchronized(lock) { sessions[sessionId] }?.stopDelegating()
    }

    suspend fun stopDelegatingAndAwaitInflight(sessionId: WarmupSessionId, timeout: Duration) {
        val session = synchronized(lock) { sessions[sessionId] }
        session?.stopDelegatingAndAwaitInflight(timeout)
    }

    fun fail(sessionId: WarmupSessionId, error: Throwable) {
        synchronized(lock) { sessions[sessionId] }?.fail(error)
    }

    fun finish(sessionId: WarmupSessionId): Boolean {
        val removed = synchronized(lock) {
            val session = sessions.remove(sessionId) ?: return@synchronized null
            aggregateIntervalBudget -= session.intervalBudget
            if (sessions.isEmpty()) nextIndex = 0 else nextIndex %= sessions.size
            session
        } ?: return false
        removed.close()
        return true
    }

    internal fun admittedSessionCount(): Int = synchronized(lock) { sessions.size }

    internal fun admittedIntervalBudget(): Int = synchronized(lock) { aggregateIntervalBudget }

    override fun close() {
        val removed = synchronized(lock) {
            sessions.values.toList().also {
                sessions.clear()
                aggregateIntervalBudget = 0
                nextIndex = 0
            }
        }
        removed.forEach(WarmupDonorSession<K, V>::close)
    }
}
