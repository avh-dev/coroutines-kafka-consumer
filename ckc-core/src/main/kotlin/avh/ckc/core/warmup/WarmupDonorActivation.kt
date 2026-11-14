package avh.ckc.core.warmup

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlin.time.Duration.Companion.milliseconds

internal fun interface WarmupDonorSessionPreparation<K, V> {
    suspend fun prepare(acceptance: SessionAccepted): WarmupDonorSession<K, V>
}

/**
 * Owns only locally accepted donor sessions. [RecipientReady] may race preparation: readiness is
 * remembered under [lock], but the data plane is activated only after the matching prepared
 * session is installed. Cancellation wins the same lock and makes every late resource close.
 */
internal class WarmupDonorActivation<K, V>(
    private val localInstanceId: WarmupInstanceId,
    private val namespace: WarmupControlNamespace,
    supportedMappings: Collection<WarmupTopicMapping>,
    private val dataPlane: MultiSessionWarmupDataPlane<K, V>,
    private val eligibility: WarmupDonorEligibility,
    private val preparation: WarmupDonorSessionPreparation<K, V>,
    private val scope: CoroutineScope,
    private val onPreparationFailed: suspend (SessionRejected) -> Unit,
    private val nowEpochMillis: () -> Long = System::currentTimeMillis
) {
    internal enum class State { PREPARING, WAITING_RECIPIENT_READY, ACTIVE, FAILED, CANCELLED }

    private class LocalSession<K, V>(val acceptance: SessionAccepted) {
        var state = State.PREPARING
        var readySeen = false
        var preparationStarted = false
        var prepared: WarmupDonorSession<K, V>? = null
    }

    private val lock = Any()
    private val supportedMappings = supportedMappings.associateBy(::mappingIdentity)
    private val sessions = linkedMapOf<WarmupSessionId, LocalSession<K, V>>()

    /** Atomically accounts capacity and ownership before confirming donor admission. */
    fun accept(acceptance: SessionAccepted): WarmupControlMessage {
        rejectionReason(acceptance)?.let { return rejection(acceptance, it) }
        synchronized(lock) {
            sessions[acceptance.sessionId]?.let { existing ->
                return if (existing.acceptance == acceptance && existing.state !in terminalStates) {
                    accepted(acceptance)
                } else {
                    rejection(acceptance, "session_conflict")
                }
            }
            if (!supportsAcceptedMappings(acceptance)) return rejection(acceptance, "mapping_mismatch")
            if (!ownsAcceptedMappings(acceptance)) return rejection(acceptance, "source_ownership_changed")
            when (dataPlane.tryAdmitPreparation(acceptance.sessionId, acceptance.intervalBudget)) {
                MultiSessionWarmupDataPlane.Admission.ACCEPTED -> LocalSession<K, V>(acceptance).also {
                    sessions[acceptance.sessionId] = it
                }
                MultiSessionWarmupDataPlane.Admission.DUPLICATE -> return rejection(acceptance, "session_conflict")
                MultiSessionWarmupDataPlane.Admission.SESSION_LIMIT -> return rejection(acceptance, "session_limit")
                MultiSessionWarmupDataPlane.Admission.BUDGET_LIMIT -> return rejection(acceptance, "budget_limit")
            }
        }
        return accepted(acceptance)
    }

    /** Starts preparation only after the caller has durably published [DonorAccepted]. */
    fun startPreparation(sessionId: WarmupSessionId): Boolean {
        val local = synchronized(lock) {
            val existing = sessions[sessionId] ?: return@synchronized null
            if (existing.state != State.PREPARING || existing.preparationStarted) return@synchronized null
            existing.preparationStarted = true
            existing
        } ?: return false
        scope.launch { prepare(local) }
        return true
    }

    /** Unknown, foreign, expired, or mismatched readiness cannot create or mutate local state. */
    fun onRecipientReady(ready: RecipientReady): Boolean = synchronized(lock) {
        val local = sessions[ready.sessionId] ?: return@synchronized false
        if (!matches(local.acceptance, ready) || nowEpochMillis() > ready.expiresAtEpochMillis) {
            return@synchronized false
        }
        when (local.state) {
            State.PREPARING -> {
                local.readySeen = true
                true
            }
            State.WAITING_RECIPIENT_READY -> {
                if (!dataPlane.activate(ready.sessionId)) {
                    failLocked(local)
                    false
                } else {
                    local.state = State.ACTIVE
                    true
                }
            }
            State.ACTIVE -> true
            State.FAILED,
            State.CANCELLED -> false
        }
    }

    /** Used by recipient fallback/shutdown; late preparation is fenced and cleans its own result. */
    fun cancelPreparation(sessionId: WarmupSessionId): Boolean = synchronized(lock) {
        val local = sessions[sessionId] ?: return@synchronized false
        if (local.state != State.PREPARING) return@synchronized false
        local.state = State.CANCELLED
        local.readySeen = false
        dataPlane.cancelPreparingAdmission(sessionId)
        true
    }

    /** Broadcast fallback fences new delegation in every pre-completion state. */
    fun onSessionEnded(ended: SessionEnded): Boolean {
        var drainActive = false
        val handled = synchronized(lock) {
            val local = sessions[ended.sessionId] ?: return@synchronized false
            val acceptance = local.acceptance
            if (ended.namespace != acceptance.namespace || ended.senderInstanceId != acceptance.senderInstanceId ||
                ended.configurationFingerprint != acceptance.configurationFingerprint ||
                ended.expiresAtEpochMillis != acceptance.expiresAtEpochMillis ||
                ended.state == WarmupSessionEndState.COMPLETED
            ) {
                return@synchronized false
            }
            when (local.state) {
                State.PREPARING -> dataPlane.cancelPreparingAdmission(ended.sessionId)
                State.WAITING_RECIPIENT_READY -> {
                    dataPlane.stopDelegating(ended.sessionId)
                    check(dataPlane.removeDrainedSession(ended.sessionId))
                }
                State.ACTIVE -> {
                    dataPlane.stopDelegating(ended.sessionId)
                    drainActive = !dataPlane.removeDrainedSession(ended.sessionId)
                }
                State.FAILED,
                State.CANCELLED -> return@synchronized false
            }
            local.readySeen = false
            local.state = State.CANCELLED
            true
        }
        if (handled && drainActive) scope.launch { drainCancelledSession(ended.sessionId, ended.expiresAtEpochMillis) }
        return handled
    }

    private suspend fun drainCancelledSession(sessionId: WarmupSessionId, expiresAtEpochMillis: Long) {
        val timeout = (expiresAtEpochMillis - nowEpochMillis()).coerceAtLeast(1).milliseconds
        try {
            dataPlane.stopDelegatingAndAwaitInflight(sessionId, timeout)
        } catch (error: CancellationException) {
            throw error
        } catch (error: WarmupInflightDrainTimeoutException) {
            // A session failure is deliberately not a CancellationException: pending source records
            // must take their ownership-safe local fallback before the session can be removed.
            dataPlane.fail(sessionId, error)
            dataPlane.awaitInflightCompletion(sessionId)
        }
        check(dataPlane.removeDrainedSession(sessionId)) {
            "Cancelled warm-up donor session ${sessionId.value} did not drain"
        }
    }

    internal fun state(sessionId: WarmupSessionId): State? = synchronized(lock) { sessions[sessionId]?.state }

    private suspend fun prepare(local: LocalSession<K, V>) {
        val prepared = try {
            val remainingMillis = local.acceptance.expiresAtEpochMillis - nowEpochMillis()
            require(remainingMillis > 0) { "Warm-up donor preparation deadline expired" }
            withTimeout(remainingMillis) { preparation.prepare(local.acceptance) }
        } catch (_: TimeoutCancellationException) {
            failPreparation(local, "donor_preparation_timeout")
            return
        } catch (_: CancellationException) {
            failPreparation(local, "donor_preparation_cancelled")
            return
        } catch (_: Throwable) {
            failPreparation(local, "donor_preparation_failed")
            return
        }
        val keep = synchronized(lock) {
            if (local.state != State.PREPARING) return@synchronized false
            if (!dataPlane.installAdmittedSession(prepared)) {
                failLocked(local)
                return@synchronized false
            }
            local.prepared = prepared
            if (local.readySeen) {
                if (!dataPlane.activate(local.acceptance.sessionId)) {
                    failLocked(local)
                    return@synchronized false
                }
                local.state = State.ACTIVE
            } else {
                local.state = State.WAITING_RECIPIENT_READY
            }
            true
        }
        if (!keep) prepared.close()
    }

    private suspend fun failPreparation(local: LocalSession<K, V>, reasonCode: String) {
        val notify = synchronized(lock) {
            if (local.state != State.PREPARING) return@synchronized false
            failLocked(local)
            true
        }
        if (notify) onPreparationFailed(rejection(local.acceptance, reasonCode))
    }

    private fun failLocked(local: LocalSession<K, V>) {
        local.state = State.FAILED
        local.readySeen = false
        if (local.prepared == null) dataPlane.cancelPreparingAdmission(local.acceptance.sessionId)
    }

    private fun rejectionReason(acceptance: SessionAccepted): String? = when {
        acceptance.protocolVersion != WARMUP_CONTROL_PROTOCOL_VERSION -> "protocol_mismatch"
        acceptance.namespace != namespace -> "namespace_mismatch"
        acceptance.targetInstanceId != localInstanceId -> "wrong_target"
        acceptance.senderInstanceId == localInstanceId -> "invalid_sender"
        nowEpochMillis() > acceptance.expiresAtEpochMillis -> "session_expired"
        else -> null
    }

    private fun supportsAcceptedMappings(acceptance: SessionAccepted): Boolean =
        acceptance.mappings.all { selected ->
            supportedMappings[mappingIdentity(selected.mapping)] == selected.mapping
        }

    private fun ownsAcceptedMappings(acceptance: SessionAccepted): Boolean =
        acceptance.mappings.all { selected ->
            eligibility.eligibleOwnership(selected.mapping) == selected.sourcePartitions
        }

    private fun matches(acceptance: SessionAccepted, ready: RecipientReady): Boolean =
        ready.namespace == acceptance.namespace &&
            ready.protocolVersion == WARMUP_CONTROL_PROTOCOL_VERSION &&
            ready.senderInstanceId == acceptance.senderInstanceId &&
            ready.targetInstanceId == localInstanceId &&
            ready.configurationFingerprint == acceptance.configurationFingerprint &&
            ready.expiresAtEpochMillis == acceptance.expiresAtEpochMillis &&
            ready.assignments.toSet() == acceptance.mappings.map { WarmupPartitionAssignment(it.mapping) }.toSet()

    private fun accepted(acceptance: SessionAccepted) = DonorAccepted(
        namespace = acceptance.namespace,
        sessionId = acceptance.sessionId,
        senderInstanceId = localInstanceId,
        targetInstanceId = acceptance.senderInstanceId,
        mappings = acceptance.mappings,
        intervalBudget = acceptance.intervalBudget,
        initialQuota = acceptance.initialQuota,
        configurationFingerprint = acceptance.configurationFingerprint,
        expiresAtEpochMillis = acceptance.expiresAtEpochMillis
    )

    private fun rejection(acceptance: SessionAccepted, reasonCode: String) = SessionRejected(
        namespace = acceptance.namespace,
        sessionId = acceptance.sessionId,
        senderInstanceId = localInstanceId,
        targetInstanceId = acceptance.senderInstanceId,
        reasonCode = reasonCode,
        configurationFingerprint = acceptance.configurationFingerprint,
        expiresAtEpochMillis = acceptance.expiresAtEpochMillis
    )

    private companion object {
        val terminalStates = setOf(State.FAILED, State.CANCELLED)
    }
}
