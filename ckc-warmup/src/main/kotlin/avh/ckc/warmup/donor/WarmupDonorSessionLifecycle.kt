package avh.ckc.warmup.donor

import avh.ckc.warmup.dataplane.WarmupResultTracker
import avh.ckc.warmup.protocol.WarmupRecordCompleted
import avh.ckc.warmup.protocol.WarmupSessionId
import kotlin.time.Duration

/**
 * Payload-free lifecycle owned by one logical donor session.
 *
 * A session may contain several typed participant lanes. The protocol layer controls them through
 * this interface and never observes or converts their record payloads.
 */
internal interface WarmupDonorSessionLifecycle : AutoCloseable {
    val sessionId: WarmupSessionId
    val admissionBudgetPerSecond: Int

    fun activate(): Boolean
    fun acceptResult(completed: WarmupRecordCompleted): WarmupResultTracker.Completion
    fun stopDelegating()
    suspend fun stopDelegatingAndAwaitInflight(timeout: Duration)
    suspend fun awaitInflightCompletion()
    fun fail(error: Throwable)
    fun finishResults()
    fun stopWaitingForForwardingAcks()
    fun trySealForRemoval(): Boolean
}

/** Atomically accounts and owns locally accepted logical donor sessions. */
internal interface WarmupDonorSessionStore<S : WarmupDonorSessionLifecycle> {
    fun tryAdmitPreparation(sessionId: WarmupSessionId, budgetPerSecond: Int): DonorSessionAdmission
    fun installAdmittedSession(session: S): Boolean
    fun cancelPreparingAdmission(sessionId: WarmupSessionId): Boolean
    fun activate(sessionId: WarmupSessionId): Boolean
    fun acceptResult(completed: WarmupRecordCompleted): WarmupResultTracker.Completion
    fun stopDelegating(sessionId: WarmupSessionId)
    suspend fun stopDelegatingAndAwaitInflight(sessionId: WarmupSessionId, timeout: Duration)
    suspend fun awaitInflightCompletion(sessionId: WarmupSessionId)
    fun fail(sessionId: WarmupSessionId, error: Throwable)
    fun detachDrainedSession(sessionId: WarmupSessionId): S?
}

/** Outcome of reserving capacity in the logical donor session store. */
internal enum class DonorSessionAdmission { ACCEPTED, DUPLICATE, SESSION_LIMIT, BUDGET_LIMIT }
