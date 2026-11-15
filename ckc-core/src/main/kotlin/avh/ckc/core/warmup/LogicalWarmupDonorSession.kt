package avh.ckc.core.warmup

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlin.time.Duration

/** Common admission store for logical sessions spanning heterogeneous participant lanes. */
internal class LogicalWarmupDonorSessionStore(
    private val maxSessions: Int,
    private val maxAggregateBudgetPerSecond: Int
) : WarmupDonorSessionStore<LogicalWarmupDonorSession> {
    private data class Slot(
        val budgetPerSecond: Int,
        var session: LogicalWarmupDonorSession? = null
    )

    private val lock = Any()
    private val sessions = linkedMapOf<WarmupSessionId, Slot>()
    private var aggregateBudgetPerSecond = 0

    init {
        require(maxSessions > 0) { "Maximum donor sessions must be positive" }
        require(maxAggregateBudgetPerSecond > 0) { "Maximum aggregate donor budget must be positive" }
    }

    override fun tryAdmitPreparation(
        sessionId: WarmupSessionId,
        budgetPerSecond: Int
    ): DonorSessionAdmission = synchronized(lock) {
        require(budgetPerSecond > 0) { "Donor session budget must be positive" }
        when {
            sessionId in sessions -> DonorSessionAdmission.DUPLICATE
            sessions.size >= maxSessions -> DonorSessionAdmission.SESSION_LIMIT
            budgetPerSecond > maxAggregateBudgetPerSecond - aggregateBudgetPerSecond ->
                DonorSessionAdmission.BUDGET_LIMIT
            else -> {
                sessions[sessionId] = Slot(budgetPerSecond)
                aggregateBudgetPerSecond += budgetPerSecond
                DonorSessionAdmission.ACCEPTED
            }
        }
    }

    override fun installAdmittedSession(session: LogicalWarmupDonorSession): Boolean = synchronized(lock) {
        val slot = sessions[session.sessionId] ?: return@synchronized false
        if (slot.session != null || slot.budgetPerSecond != session.admissionBudgetPerSecond) {
            return@synchronized false
        }
        slot.session = session
        true
    }

    override fun cancelPreparingAdmission(sessionId: WarmupSessionId): Boolean = synchronized(lock) {
        val slot = sessions[sessionId] ?: return@synchronized false
        if (slot.session != null) return@synchronized false
        sessions.remove(sessionId)
        aggregateBudgetPerSecond -= slot.budgetPerSecond
        true
    }

    override fun activate(sessionId: WarmupSessionId): Boolean =
        synchronized(lock) { sessions[sessionId]?.session }?.activate() ?: false

    override fun acceptResult(completed: WarmupRecordCompleted): WarmupResultTracker.Completion =
        synchronized(lock) { sessions[completed.recordId.sessionId]?.session }
            ?.acceptResult(completed)
            ?: WarmupResultTracker.Completion.IGNORED

    override fun stopDelegating(sessionId: WarmupSessionId) {
        synchronized(lock) { sessions[sessionId]?.session }?.stopDelegating()
    }

    override suspend fun stopDelegatingAndAwaitInflight(sessionId: WarmupSessionId, timeout: Duration) {
        synchronized(lock) { sessions[sessionId]?.session }?.stopDelegatingAndAwaitInflight(timeout)
    }

    override suspend fun awaitInflightCompletion(sessionId: WarmupSessionId) {
        synchronized(lock) { sessions[sessionId]?.session }?.awaitInflightCompletion()
    }

    override fun fail(sessionId: WarmupSessionId, error: Throwable) {
        synchronized(lock) { sessions[sessionId]?.session }?.fail(error)
    }

    override fun detachDrainedSession(sessionId: WarmupSessionId): LogicalWarmupDonorSession? = synchronized(lock) {
        val slot = sessions[sessionId] ?: return@synchronized null
        val session = slot.session ?: return@synchronized null
        if (!session.trySealForRemoval()) return@synchronized null
        sessions.remove(sessionId)
        aggregateBudgetPerSecond -= slot.budgetPerSecond
        session
    }

    internal fun admittedSessionCount(): Int = synchronized(lock) { sessions.size }
    internal fun admittedBudgetPerSecond(): Int = synchronized(lock) { aggregateBudgetPerSecond }
}

/** One protocol session whose lanes retain their concrete record types. */
internal class LogicalWarmupDonorSession(
    override val sessionId: WarmupSessionId,
    override val admissionBudgetPerSecond: Int,
    lanes: List<WarmupDonorSessionLifecycle>
) : WarmupDonorSessionLifecycle {
    private val lanes = lanes.toList()

    init {
        require(lanes.isNotEmpty()) { "Logical donor session must contain at least one participant lane" }
        require(lanes.all { it.sessionId == sessionId }) { "Every donor lane must belong to the logical session" }
        require(lanes.sumOf { it.admissionBudgetPerSecond.toLong() } == admissionBudgetPerSecond.toLong()) {
            "Logical donor session budget must equal its participant lane budgets"
        }
    }

    override fun activate(): Boolean {
        val activated = mutableListOf<WarmupDonorSessionLifecycle>()
        for (lane in lanes) {
            if (!lane.activate()) {
                activated.forEach { it.fail(IllegalStateException("Logical donor activation was incomplete")) }
                return false
            }
            activated += lane
        }
        return true
    }

    override fun acceptResult(completed: WarmupRecordCompleted): WarmupResultTracker.Completion {
        lanes.forEach { lane ->
            val result = lane.acceptResult(completed)
            if (result != WarmupResultTracker.Completion.IGNORED) return result
        }
        return WarmupResultTracker.Completion.IGNORED
    }

    override fun stopDelegating() = lanes.forEach(WarmupDonorSessionLifecycle::stopDelegating)

    override suspend fun stopDelegatingAndAwaitInflight(timeout: Duration) = coroutineScope {
        stopDelegating()
        lanes.map { lane -> async { lane.stopDelegatingAndAwaitInflight(timeout) } }.awaitAll()
        Unit
    }

    override suspend fun awaitInflightCompletion() = coroutineScope {
        lanes.map { lane -> async { lane.awaitInflightCompletion() } }.awaitAll()
        Unit
    }

    override fun fail(error: Throwable) = lanes.forEach { it.fail(error) }

    override fun trySealForRemoval(): Boolean {
        // The protocol calls this only after every lane has drained. A partial seal therefore
        // indicates an invariant violation and must not be hidden as a retryable state.
        val results = lanes.map(WarmupDonorSessionLifecycle::trySealForRemoval)
        check(results.all { it } || results.none { it }) { "Logical donor lanes sealed inconsistently" }
        return results.all { it }
    }

    override fun close() = lanes.asReversed().forEach(WarmupDonorSessionLifecycle::close)
}
