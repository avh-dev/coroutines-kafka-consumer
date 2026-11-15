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

    private data class SelectedDelegation<K, V>(
        val session: WarmupDonorSession<K, V>,
        val destination: WarmupDestination
    )

    private data class AdmittedSlot<K, V>(
        val intervalBudget: Int,
        var session: WarmupDonorSession<K, V>? = null
    )

    /** Protects membership, aggregate capacity, the round-robin cursor, and activation versus removal. */
    private val lock = Any()
    private val sessions = linkedMapOf<WarmupSessionId, AdmittedSlot<K, V>>()
    private var aggregateIntervalBudget = 0
    private var nextSessionIndex = 0

    init {
        require(maxSessions > 0) { "Maximum donor sessions must be > 0" }
        require(maxAggregateIntervalBudget > 0) { "Maximum aggregate warm-up budget must be > 0" }
    }

    fun admit(session: WarmupDonorSession<K, V>): Admission {
        val admission = tryAdmitPreparation(session.sessionId, session.intervalBudget)
        if (admission == Admission.ACCEPTED) check(installAdmittedSession(session))
        return admission
    }

    /**
     * Atomically accounts local capacity before asynchronous donor preparation starts. This is a
     * local admission, not a distributed partition reservation: no transport coordinate is owned.
     */
    fun tryAdmitPreparation(sessionId: WarmupSessionId, intervalBudget: Int): Admission = synchronized(lock) {
        require(intervalBudget > 0) { "Warm-up interval budget must be > 0" }
        if (sessionId in sessions) return@synchronized Admission.DUPLICATE
        if (sessions.size >= maxSessions) return@synchronized Admission.SESSION_LIMIT
        if (intervalBudget > maxAggregateIntervalBudget - aggregateIntervalBudget) {
            return@synchronized Admission.BUDGET_LIMIT
        }
        sessions[sessionId] = AdmittedSlot(intervalBudget)
        aggregateIntervalBudget += intervalBudget
        Admission.ACCEPTED
    }

    /** Installs exactly one prepared session into its already accounted admission slot. */
    fun installAdmittedSession(session: WarmupDonorSession<K, V>): Boolean = synchronized(lock) {
        val slot = sessions[session.sessionId] ?: return@synchronized false
        if (slot.session != null || slot.intervalBudget != session.intervalBudget) return@synchronized false
        slot.session = session
        true
    }

    /** Releases capacity only while preparation has not installed a data-plane session. */
    fun cancelPreparingAdmission(sessionId: WarmupSessionId): Boolean = synchronized(lock) {
        val slot = sessions[sessionId] ?: return@synchronized false
        if (slot.session != null) return@synchronized false
        sessions.remove(sessionId)
        aggregateIntervalBudget -= slot.intervalBudget
        if (sessions.isEmpty()) nextSessionIndex = 0
        true
    }

    fun activate(sessionId: WarmupSessionId): Boolean = synchronized(lock) {
        sessions[sessionId]?.session?.activate() ?: false
    }

    override suspend fun process(
        record: ConsumerRecord<K, V>,
        processLocally: suspend () -> RecordProcessingOutcome
    ): RecordProcessingOutcome {
        val selected = selectDelegation(record) ?: return processLocally()
        return selected.session.processDelegatedRecord(record, selected.destination, processLocally)
    }

    private fun selectDelegation(record: ConsumerRecord<K, V>): SelectedDelegation<K, V>? =
        synchronized(lock) {
            val activeSessions = sessions.values.mapNotNull { it.session }.filter(WarmupDonorSession<K, V>::isActive)
            for (offset in activeSessions.indices) {
                val index = (nextSessionIndex + offset).mod(activeSessions.size)
                val session = activeSessions[index]
                val destination = session.tryBeginDelegation(record) ?: continue
                nextSessionIndex = (index + 1).mod(activeSessions.size)
                return@synchronized SelectedDelegation(session, destination)
            }
            null
        }

    fun acceptResult(completed: WarmupRecordCompleted): WarmupResultTracker.Completion =
        synchronized(lock) { sessions[completed.recordId.sessionId] }
            ?.session
            ?.acceptResult(completed)
            ?: WarmupResultTracker.Completion.IGNORED

    fun stopDelegating(sessionId: WarmupSessionId) {
        synchronized(lock) { sessions[sessionId]?.session }?.stopDelegating()
    }

    suspend fun stopDelegatingAndAwaitInflight(sessionId: WarmupSessionId, timeout: Duration) {
        val session = synchronized(lock) { sessions[sessionId]?.session }
        session?.stopDelegatingAndAwaitInflight(timeout)
    }

    suspend fun awaitInflightCompletion(sessionId: WarmupSessionId) {
        val session = synchronized(lock) { sessions[sessionId]?.session }
        session?.awaitInflightCompletion()
    }

    fun fail(sessionId: WarmupSessionId, error: Throwable) {
        synchronized(lock) { sessions[sessionId]?.session }?.fail(error)
    }

    /**
     * Removes only an unactivated, drained, or failed session and transfers cleanup ownership to
     * the caller. An early removal attempt leaves the session and its accounted capacity intact.
     */
    fun detachDrainedSession(sessionId: WarmupSessionId): WarmupDonorSession<K, V>? =
        synchronized(lock) {
            val slot = sessions[sessionId] ?: return@synchronized null
            val session = slot.session ?: return@synchronized null
            if (!session.trySealForRemoval()) return@synchronized null
            sessions.remove(sessionId)
            aggregateIntervalBudget -= slot.intervalBudget
            if (sessions.isEmpty()) {
                nextSessionIndex = 0
            } else {
                nextSessionIndex %= sessions.size
            }
            session
        }

    /** Removes and closes a session when the caller does not need retryable cleanup ownership. */
    fun removeDrainedSession(sessionId: WarmupSessionId): Boolean {
        val detached = detachDrainedSession(sessionId) ?: return false
        detached.close()
        return true
    }

    internal fun admittedSessionCount(): Int = synchronized(lock) { sessions.size }

    internal fun admittedIntervalBudget(): Int = synchronized(lock) { aggregateIntervalBudget }

    /** Stops admission and closes the plane only when no session still has inflight work. */
    override fun close() {
        val removed = synchronized(lock) {
            val installed = sessions.values.mapNotNull { it.session }
            installed.forEach(WarmupDonorSession<K, V>::stopDelegating)
            check(installed.all(WarmupDonorSession<K, V>::trySealForRemoval)) {
                "Warm-up data plane must drain every donor session before close"
            }
            installed.also {
                sessions.clear()
                aggregateIntervalBudget = 0
                nextSessionIndex = 0
            }
        }
        removed.forEach(WarmupDonorSession<K, V>::close)
    }
}
