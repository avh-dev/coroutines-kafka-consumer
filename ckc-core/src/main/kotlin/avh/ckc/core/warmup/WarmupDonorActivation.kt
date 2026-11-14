package avh.ckc.core.warmup

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

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
    private val onDonorClosed: suspend (DonorClosed) -> Unit = {},
    private val drainTimeout: Duration = 5.seconds,
    private val maxTerminalSessionFences: Int = 1_024,
    private val nowEpochMillis: () -> Long = System::currentTimeMillis
) {
    internal enum class State {
        PREPARING, WAITING_RECIPIENT_READY, ACTIVE, DRAINING, CLOSED, FAILED, CANCELLED
    }

    private class LocalSession<K, V>(val acceptance: SessionAccepted) {
        var state = State.PREPARING
        var readySeen = false
        var preparationStarted = false
        var prepared: WarmupDonorSession<K, V>? = null
        var closeStarted = false
        var acknowledgeClose = false
        val preparationFinished = CompletableDeferred<Unit>()
    }

    private val lock = Any()
    private val supportedMappings = supportedMappings.associateBy(::mappingIdentity)
    private val sessions = linkedMapOf<WarmupSessionId, LocalSession<K, V>>()
    private val terminalSessionOrder = linkedSetOf<WarmupSessionId>()

    init {
        require(drainTimeout.isPositive()) { "Donor drain timeout must be > 0" }
        require(maxTerminalSessionFences > 0) { "Maximum terminal session fences must be > 0" }
    }

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
        scheduleExpiry(acceptance.sessionId, acceptance.expiresAtEpochMillis)
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
            State.DRAINING,
            State.CLOSED,
            State.FAILED,
            State.CANCELLED -> false
        }
    }

    /** Routes a remote terminal result only to its exact admitted recipient and destination. */
    fun onRecordCompleted(message: ControlRecordCompleted): WarmupResultTracker.Completion {
        val accepted = synchronized(lock) {
            val local = sessions[message.sessionId] ?: return@synchronized false
            matchesSession(local.acceptance, message) &&
                local.state in setOf(State.ACTIVE, State.DRAINING) &&
                message.completed.recordId.topicPartition in local.acceptance.mappings
                    .map { WarmupPartitionAssignment(it.mapping).destination }
        }
        return if (accepted) {
            dataPlane.acceptResult(message.completed)
        } else {
            WarmupResultTracker.Completion.IGNORED
        }
    }

    /** Completion is idempotent and closes delegation before any in-flight wait begins. */
    fun onCompletionRequested(request: WarmupCompletionRequested): Boolean {
        val local = synchronized(lock) {
            val existing = sessions[request.sessionId] ?: return@synchronized null
            if (!matchesSession(existing.acceptance, request)) return@synchronized null
            when (existing.state) {
                State.PREPARING -> {
                    dataPlane.cancelPreparingAdmission(request.sessionId)
                    if (!existing.preparationStarted) existing.preparationFinished.complete(Unit)
                }
                State.WAITING_RECIPIENT_READY,
                State.ACTIVE -> dataPlane.stopDelegating(request.sessionId)
                State.DRAINING -> {
                    existing.acknowledgeClose = true
                    return@synchronized existing
                }
                State.CLOSED -> {
                    scope.launch { onDonorClosed(closedMessage(existing.acceptance)) }
                    return@synchronized existing
                }
                State.FAILED,
                State.CANCELLED -> return@synchronized null
            }
            existing.state = State.DRAINING
            existing.readySeen = false
            existing.acknowledgeClose = true
            if (!existing.closeStarted) {
                existing.closeStarted = true
                scope.launch { finishClose(existing) }
            }
            existing
        }
        return local != null
    }

    /** Used by recipient fallback/shutdown; late preparation is fenced and cleans its own result. */
    fun cancelPreparation(sessionId: WarmupSessionId): Boolean = synchronized(lock) {
        val local = sessions[sessionId] ?: return@synchronized false
        if (local.state != State.PREPARING) return@synchronized false
        local.state = State.CANCELLED
        local.readySeen = false
        dataPlane.cancelPreparingAdmission(sessionId)
        recordTerminalLocked(local)
        true
    }

    /** Broadcast fallback fences new delegation in every pre-completion state. */
    fun onSessionEnded(ended: SessionEnded): Boolean {
        return synchronized(lock) {
            val local = sessions[ended.sessionId] ?: return@synchronized false
            val acceptance = local.acceptance
            if (ended.protocolVersion != WARMUP_CONTROL_PROTOCOL_VERSION ||
                ended.namespace != acceptance.namespace || ended.senderInstanceId != acceptance.senderInstanceId ||
                ended.configurationFingerprint != acceptance.configurationFingerprint ||
                ended.expiresAtEpochMillis != acceptance.expiresAtEpochMillis ||
                ended.state == WarmupSessionEndState.COMPLETED
            ) {
                return@synchronized false
            }
            when (local.state) {
                State.PREPARING -> {
                    dataPlane.cancelPreparingAdmission(ended.sessionId)
                    if (!local.preparationStarted) local.preparationFinished.complete(Unit)
                }
                State.WAITING_RECIPIENT_READY,
                State.ACTIVE -> dataPlane.stopDelegating(ended.sessionId)
                State.DRAINING -> Unit
                State.CLOSED,
                State.FAILED,
                State.CANCELLED -> return@synchronized false
            }
            local.readySeen = false
            local.acknowledgeClose = false
            local.state = State.DRAINING
            if (!local.closeStarted) {
                local.closeStarted = true
                scope.launch { finishClose(local) }
            }
            true
        }
    }

    private suspend fun finishClose(local: LocalSession<K, V>) {
        val acceptance = local.acceptance
        local.preparationFinished.await()
        if (local.prepared != null) {
            val remaining = (acceptance.expiresAtEpochMillis - nowEpochMillis()).coerceAtLeast(1).milliseconds
            val timeout = minOf(drainTimeout, remaining)
            try {
                dataPlane.stopDelegatingAndAwaitInflight(acceptance.sessionId, timeout)
            } catch (error: CancellationException) {
                throw error
            } catch (error: WarmupInflightDrainTimeoutException) {
                // This deadline is a session failure, not worker cancellation: still-owned source
                // records must take their ownership-safe fallback before capacity can be released.
                dataPlane.fail(acceptance.sessionId, error)
                dataPlane.awaitInflightCompletion(acceptance.sessionId)
            }
            check(dataPlane.removeDrainedSession(acceptance.sessionId)) {
                "Warm-up donor session ${acceptance.sessionId.value} did not drain"
            }
        }
        val acknowledge = synchronized(lock) {
            if (local.state != State.DRAINING) return@synchronized false
            local.state = State.CLOSED
            recordTerminalLocked(local)
            local.acknowledgeClose
        }
        if (acknowledge) onDonorClosed(closedMessage(acceptance))
    }

    /** Recipient loss has no coordinator observer, so the immutable session deadline owns cleanup. */
    private fun scheduleExpiry(sessionId: WarmupSessionId, expiresAtEpochMillis: Long) {
        scope.launch {
            val remainingMillis = expiresAtEpochMillis - nowEpochMillis()
            if (remainingMillis > 0) delay(remainingMillis)
            expire(sessionId)
        }
    }

    private fun expire(sessionId: WarmupSessionId) = synchronized(lock) {
        val local = sessions[sessionId] ?: return@synchronized
        when (local.state) {
            State.PREPARING -> {
                dataPlane.cancelPreparingAdmission(sessionId)
                if (!local.preparationStarted) local.preparationFinished.complete(Unit)
            }
            State.WAITING_RECIPIENT_READY,
            State.ACTIVE -> dataPlane.stopDelegating(sessionId)
            State.DRAINING -> Unit
            State.CLOSED,
            State.FAILED,
            State.CANCELLED -> return@synchronized
        }
        local.readySeen = false
        local.acknowledgeClose = false
        local.state = State.DRAINING
        if (!local.closeStarted) {
            local.closeStarted = true
            scope.launch { finishClose(local) }
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
            local.preparationFinished.complete(Unit)
            return
        } catch (_: CancellationException) {
            failPreparation(local, "donor_preparation_cancelled")
            local.preparationFinished.complete(Unit)
            return
        } catch (_: Throwable) {
            failPreparation(local, "donor_preparation_failed")
            local.preparationFinished.complete(Unit)
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
        local.preparationFinished.complete(Unit)
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
        if (local.prepared == null) {
            dataPlane.cancelPreparingAdmission(local.acceptance.sessionId)
        } else {
            dataPlane.stopDelegating(local.acceptance.sessionId)
            check(dataPlane.removeDrainedSession(local.acceptance.sessionId)) {
                "Failed unactivated donor session ${local.acceptance.sessionId.value} did not close"
            }
        }
        recordTerminalLocked(local)
    }

    /** Keeps duplicate fencing bounded; eviction never owns a live resource or data-plane slot. */
    private fun recordTerminalLocked(local: LocalSession<K, V>) {
        val sessionId = local.acceptance.sessionId
        terminalSessionOrder.remove(sessionId)
        terminalSessionOrder.add(sessionId)
        while (terminalSessionOrder.size > maxTerminalSessionFences) {
            val oldest = terminalSessionOrder.first()
            terminalSessionOrder.remove(oldest)
            sessions.remove(oldest)
        }
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

    private fun matchesSession(acceptance: SessionAccepted, message: WarmupControlMessage): Boolean =
        message.protocolVersion == WARMUP_CONTROL_PROTOCOL_VERSION &&
            message.namespace == acceptance.namespace &&
            message.sessionId == acceptance.sessionId &&
            message.senderInstanceId == acceptance.senderInstanceId &&
            message.targetInstanceId == localInstanceId &&
            message.configurationFingerprint == acceptance.configurationFingerprint &&
            message.expiresAtEpochMillis == acceptance.expiresAtEpochMillis

    private fun closedMessage(acceptance: SessionAccepted) = DonorClosed(
        namespace = acceptance.namespace,
        sessionId = acceptance.sessionId,
        senderInstanceId = localInstanceId,
        targetInstanceId = acceptance.senderInstanceId,
        configurationFingerprint = acceptance.configurationFingerprint,
        expiresAtEpochMillis = acceptance.expiresAtEpochMillis
    )

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
        val terminalStates = setOf(State.CLOSED, State.FAILED, State.CANCELLED)
    }
}
