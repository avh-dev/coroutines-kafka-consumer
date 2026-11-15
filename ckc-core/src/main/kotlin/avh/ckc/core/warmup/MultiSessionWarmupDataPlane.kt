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
    private val maxAggregateBudgetPerSecond: Int
) : AdmittedRecordInterceptor<K, V>, WarmupDonorSessionStore<WarmupDonorSession<K, V>>, AutoCloseable {

    private data class SelectedDelegation<K, V>(
        val session: WarmupDonorSession<K, V>,
        val destination: WarmupDestination
    )

    private data class AdmittedSlot<K, V>(
        val budgetPerSecond: Int,
        var session: WarmupDonorSession<K, V>? = null
    )

    /** Protects membership, aggregate capacity, the round-robin cursor, and activation versus removal. */
    private val lock = Any()
    private val sessions = linkedMapOf<WarmupSessionId, AdmittedSlot<K, V>>()
    private var aggregateBudgetPerSecond = 0
    private var nextSessionIndex = 0

    init {
        require(maxSessions > 0) { "Maximum donor sessions must be > 0" }
        require(maxAggregateBudgetPerSecond > 0) { "Maximum aggregate warm-up budget must be > 0" }
    }

    fun admit(session: WarmupDonorSession<K, V>): DonorSessionAdmission {
        val admission = tryAdmitPreparation(session.sessionId, session.admissionBudgetPerSecond)
        if (admission == DonorSessionAdmission.ACCEPTED) check(installAdmittedSession(session))
        return admission
    }

    /**
     * Atomically accounts local capacity before asynchronous donor preparation starts. This is a
     * local admission, not a distributed partition reservation: no transport coordinate is owned.
     */
    override fun tryAdmitPreparation(
        sessionId: WarmupSessionId,
        budgetPerSecond: Int
    ): DonorSessionAdmission = synchronized(lock) {
        require(budgetPerSecond > 0) { "Warm-up budget per second must be > 0" }
        if (sessionId in sessions) return@synchronized DonorSessionAdmission.DUPLICATE
        if (sessions.size >= maxSessions) return@synchronized DonorSessionAdmission.SESSION_LIMIT
        if (budgetPerSecond > maxAggregateBudgetPerSecond - aggregateBudgetPerSecond) {
            return@synchronized DonorSessionAdmission.BUDGET_LIMIT
        }
        sessions[sessionId] = AdmittedSlot(budgetPerSecond)
        aggregateBudgetPerSecond += budgetPerSecond
        DonorSessionAdmission.ACCEPTED
    }

    /** Installs exactly one prepared session into its already accounted admission slot. */
    override fun installAdmittedSession(session: WarmupDonorSession<K, V>): Boolean = synchronized(lock) {
        val slot = sessions[session.sessionId] ?: return@synchronized false
        if (slot.session != null || slot.budgetPerSecond != session.admissionBudgetPerSecond) return@synchronized false
        slot.session = session
        true
    }

    /** Releases capacity only while preparation has not installed a data-plane session. */
    override fun cancelPreparingAdmission(sessionId: WarmupSessionId): Boolean = synchronized(lock) {
        val slot = sessions[sessionId] ?: return@synchronized false
        if (slot.session != null) return@synchronized false
        sessions.remove(sessionId)
        aggregateBudgetPerSecond -= slot.budgetPerSecond
        if (sessions.isEmpty()) nextSessionIndex = 0
        true
    }

    override fun activate(sessionId: WarmupSessionId): Boolean = synchronized(lock) {
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

    override fun acceptResult(completed: WarmupRecordCompleted): WarmupResultTracker.Completion =
        synchronized(lock) { sessions[completed.recordId.sessionId] }
            ?.session
            ?.acceptResult(completed)
            ?: WarmupResultTracker.Completion.IGNORED

    override fun stopDelegating(sessionId: WarmupSessionId) {
        synchronized(lock) { sessions[sessionId]?.session }?.stopDelegating()
    }

    override suspend fun stopDelegatingAndAwaitInflight(sessionId: WarmupSessionId, timeout: Duration) {
        val session = synchronized(lock) { sessions[sessionId]?.session }
        session?.stopDelegatingAndAwaitInflight(timeout)
    }

    override suspend fun awaitInflightCompletion(sessionId: WarmupSessionId) {
        val session = synchronized(lock) { sessions[sessionId]?.session }
        session?.awaitInflightCompletion()
    }

    override fun fail(sessionId: WarmupSessionId, error: Throwable) {
        synchronized(lock) { sessions[sessionId]?.session }?.fail(error)
    }

    /**
     * Removes only an unactivated, drained, or failed session and transfers cleanup ownership to
     * the caller. An early removal attempt leaves the session and its accounted capacity intact.
     */
    override fun detachDrainedSession(sessionId: WarmupSessionId): WarmupDonorSession<K, V>? =
        synchronized(lock) {
            val slot = sessions[sessionId] ?: return@synchronized null
            val session = slot.session ?: return@synchronized null
            if (!session.trySealForRemoval()) return@synchronized null
            sessions.remove(sessionId)
            aggregateBudgetPerSecond -= slot.budgetPerSecond
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

    internal fun admittedBudgetPerSecond(): Int = synchronized(lock) { aggregateBudgetPerSecond }

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
                aggregateBudgetPerSecond = 0
                nextSessionIndex = 0
            }
        }
        removed.forEach(WarmupDonorSession<K, V>::close)
    }
}
