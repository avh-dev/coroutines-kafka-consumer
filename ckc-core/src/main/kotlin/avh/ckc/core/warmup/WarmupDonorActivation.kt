package avh.ckc.core.warmup

import avh.ckc.core.isFatalJvmError
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

internal fun interface WarmupDonorSessionPreparation<S : WarmupDonorSessionLifecycle> {
    suspend fun prepare(acceptance: SessionAccepted): S
}

/**
 * Owns only locally accepted donor sessions. [RecipientReady] may race preparation: readiness is
 * remembered under [lock], but the data plane is activated only after the matching prepared
 * session is installed. Cancellation wins the same lock and makes every late resource close.
 */
internal class WarmupDonorActivation<S : WarmupDonorSessionLifecycle>(
    private val localInstanceId: WarmupInstanceId,
    private val namespace: WarmupControlNamespace,
    supportedMappings: Collection<WarmupTopicMapping>,
    private val dataPlane: WarmupDonorSessionStore<S>,
    private val eligibility: WarmupDonorEligibility,
    private val preparation: WarmupDonorSessionPreparation<S>,
    private val scope: CoroutineScope,
    private val cleanupDispatcher: CoroutineDispatcher = Dispatchers.IO,
    private val onPreparationFailed: suspend (SessionRejected) -> Unit,
    private val onDonorClosed: suspend (DonorClosed) -> Unit = {},
    private val drainTimeout: Duration = 5.seconds,
    private val maxTerminalSessionFences: Int = 1_024,
    private val nowEpochMillis: () -> Long = System::currentTimeMillis
) {
    internal enum class State {
        PREPARING, WAITING_RECIPIENT_READY, ACTIVE, DRAINING, CLOSED, FAILED, CANCELLED
    }

    private sealed interface PreparationCompletion {
        data object Installed : PreparationCompletion
        data object Cancelled : PreparationCompletion
        data class ResourceCleanupFailed(val error: Throwable) : PreparationCompletion
    }

    private class LocalSession<S : WarmupDonorSessionLifecycle>(val acceptance: SessionAccepted) {
        var state = State.PREPARING
        var readySeen = false
        var preparationStarted = false
        var preparationBodyEntered = false
        var prepared: S? = null
        var closeStarted = false
        var closeRetryRequested = false
        var acknowledgeClose = false
        var preparationJob: Job? = null
        var retainedCleanupResource: S? = null
        var preparationCleanupFailureObserved = false
        val preparationFinished = CompletableDeferred<PreparationCompletion>()
    }

    private val lock = Any()
    private val supportedMappings = supportedMappings.associateBy(::mappingIdentity)
    private val sessions = linkedMapOf<WarmupSessionId, LocalSession<S>>()
    // Terminal fences may expire or overflow, but cleanup ownership must remain reachable until
    // close succeeds. This registry is bounded by actual failed resources rather than protocol IDs.
    private val retainedCleanupSessions = linkedMapOf<WarmupSessionId, LocalSession<S>>()
    private val terminalSessionOrder = linkedSetOf<WarmupSessionId>()
    private var admissionBlockedUntilEpochMillis = 0L

    init {
        require(drainTimeout.isPositive()) { "Donor drain timeout must be > 0" }
        require(maxTerminalSessionFences > 0) { "Maximum terminal session fences must be > 0" }
    }

    /** Atomically accounts capacity and ownership before confirming donor admission. */
    fun accept(acceptance: SessionAccepted): WarmupControlMessage {
        rejectionReason(acceptance)?.let { return rejection(acceptance, it) }
        synchronized(lock) {
            pruneExpiredTerminalFencesLocked()
            (sessions[acceptance.sessionId] ?: retainedCleanupSessions[acceptance.sessionId])?.let { existing ->
                return if (existing.acceptance == acceptance && existing.state !in terminalStates) {
                    accepted(acceptance)
                } else {
                    rejection(acceptance, "session_conflict")
                }
            }
            if (nowEpochMillis() <= admissionBlockedUntilEpochMillis) {
                return rejection(acceptance, "terminal_fence_capacity")
            }
            if (!supportsAcceptedMappings(acceptance)) return rejection(acceptance, "mapping_mismatch")
            if (!ownsAcceptedMappings(acceptance)) return rejection(acceptance, "source_ownership_changed")
            when (dataPlane.tryAdmitPreparation(acceptance.sessionId, acceptance.admissionBudgetPerSecond)) {
                DonorSessionAdmission.ACCEPTED -> LocalSession<S>(acceptance).also {
                    sessions[acceptance.sessionId] = it
                }
                DonorSessionAdmission.DUPLICATE -> return rejection(acceptance, "session_conflict")
                DonorSessionAdmission.SESSION_LIMIT -> return rejection(acceptance, "session_limit")
                DonorSessionAdmission.BUDGET_LIMIT -> return rejection(acceptance, "budget_limit")
            }
        }
        scheduleExpiry(acceptance.sessionId, acceptance.expiresAtEpochMillis)
        return accepted(acceptance)
    }

    /** Starts preparation only after the caller has durably published [DonorAccepted]. */
    fun startPreparation(sessionId: WarmupSessionId): Boolean {
        val pending = synchronized(lock) {
            val existing = sessions[sessionId] ?: return@synchronized null
            if (existing.state != State.PREPARING || existing.preparationStarted) return@synchronized null
            val job = scope.launch(start = CoroutineStart.LAZY) {
                synchronized(lock) { existing.preparationBodyEntered = true }
                prepare(existing)
            }
            existing.preparationStarted = true
            existing.preparationJob = job
            existing to job
        } ?: return false
        val (local, job) = pending
        job.invokeOnCompletion {
            synchronized(lock) {
                // Cancellation before dispatch never enters prepare(), so no body can publish the
                // terminal signal. Once entered, the body owns both resource cleanup and signaling.
                if (!local.preparationBodyEntered) {
                    local.preparationFinished.complete(PreparationCompletion.Cancelled)
                }
            }
        }
        job.start()
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
        return synchronized(lock) {
            val existing = sessions[request.sessionId]
                ?: retainedCleanupSessions[request.sessionId]
                ?: return@synchronized false
            if (!matchesSession(existing.acceptance, request)) return@synchronized false
            if (existing.state == State.CLOSED) {
                scope.launch { onDonorClosed(closedMessage(existing.acceptance)) }
                return@synchronized true
            }
            beginCloseLocked(existing, acknowledge = true)
        }
    }

    /** Used by recipient fallback/shutdown; late preparation is fenced and cleans its own result. */
    fun cancelPreparation(sessionId: WarmupSessionId): Boolean = synchronized(lock) {
        val local = sessions[sessionId] ?: return@synchronized false
        if (local.state != State.PREPARING) return@synchronized false
        beginCloseLocked(local, acknowledge = false)
    }

    /** Broadcast fallback fences new delegation in every pre-completion state. */
    fun onSessionEnded(ended: SessionEnded): Boolean {
        return synchronized(lock) {
            val local = sessions[ended.sessionId]
                ?: retainedCleanupSessions[ended.sessionId]
                ?: return@synchronized false
            val acceptance = local.acceptance
            if (ended.protocolVersion != WARMUP_CONTROL_PROTOCOL_VERSION ||
                ended.namespace != acceptance.namespace || ended.senderInstanceId != acceptance.senderInstanceId ||
                ended.configurationFingerprint != acceptance.configurationFingerprint ||
                ended.expiresAtEpochMillis != acceptance.expiresAtEpochMillis ||
                ended.state == WarmupSessionEndState.COMPLETED
            ) {
                return@synchronized false
            }
            beginCloseLocked(local, acknowledge = false)
        }
    }

    /** Ends only logical sessions that depend on a participant whose main runtime is stopping. */
    fun onParticipantUnavailable(participantId: WarmupParticipantId): Int {
        val affected = synchronized(lock) {
            val closing = sessions.values.filter { local ->
                local.state !in terminalStates &&
                    local.acceptance.mappings.any { it.mapping.participantId == participantId }
            }.filter { beginCloseLocked(it, acknowledge = false) }
            val retrying = retainedCleanupSessions.values.filter { local ->
                local.state == State.FAILED &&
                    local.acceptance.mappings.any { it.mapping.participantId == participantId }
            }.filter(::requestFailedCleanupLocked)
            (closing + retrying).distinct().map(LocalSession<S>::acceptance)
        }
        affected.forEach { acceptance ->
            scope.launch { onPreparationFailed(rejection(acceptance, "donor_participant_unavailable")) }
        }
        return affected.size
    }

    /** All completion, fallback, and expiry paths share the same close transition. */
    private fun beginCloseLocked(local: LocalSession<S>, acknowledge: Boolean): Boolean {
        when (local.state) {
            State.PREPARING -> {
                dataPlane.cancelPreparingAdmission(local.acceptance.sessionId)
                if (!local.preparationStarted) {
                    local.preparationFinished.complete(PreparationCompletion.Cancelled)
                }
                local.preparationJob?.cancel()
            }
            State.WAITING_RECIPIENT_READY,
            State.ACTIVE -> dataPlane.stopDelegating(local.acceptance.sessionId)
            State.DRAINING -> Unit
            State.CLOSED,
            State.CANCELLED -> return false
            State.FAILED -> return requestFailedCleanupLocked(local)
        }
        local.readySeen = false
        local.acknowledgeClose = local.acknowledgeClose || acknowledge
        local.state = State.DRAINING
        if (!local.closeStarted) {
            local.closeStarted = true
            scope.launch { finishClose(local) }
        } else {
            // A close trigger racing a failed cleanup must not be lost before closeStarted resets.
            local.closeRetryRequested = true
        }
        return true
    }

    private suspend fun finishClose(local: LocalSession<S>) {
        val acceptance = local.acceptance
        val preparationCompletion = local.preparationFinished.await()
        if (preparationCompletion is PreparationCompletion.ResourceCleanupFailed) {
            val retry = synchronized(lock) {
                if (local.preparationCleanupFailureObserved) true else {
                    local.preparationCleanupFailureObserved = true
                    false
                }
            }
            if (!retry) {
                awaitAnotherCloseTrigger(local)
                return
            }
        }
        if (synchronized(lock) { local.prepared != null }) {
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
            val detached = dataPlane.detachDrainedSession(acceptance.sessionId)
            if (detached == null) {
                awaitAnotherCloseTrigger(local)
                return
            }
            synchronized(lock) {
                local.prepared = null
                retainCleanupResourceLocked(local, detached)
            }
        }
        if (!closeRetainedResource(local)) {
            awaitAnotherCloseTrigger(local)
            return
        }
        val acknowledge = synchronized(lock) {
            if (local.state != State.DRAINING) return@synchronized false
            local.state = State.CLOSED
            recordTerminalLocked(local)
            local.acknowledgeClose
        }
        if (acknowledge) onDonorClosed(closedMessage(acceptance))
    }

    /** Leaves cleanup dormant, or consumes one close trigger that raced the failed attempt. */
    private fun awaitAnotherCloseTrigger(local: LocalSession<S>) {
        val retryNow = synchronized(lock) {
            if (local.state != State.DRAINING) return@synchronized false
            if (local.closeRetryRequested) {
                local.closeRetryRequested = false
                true
            } else {
                local.closeStarted = false
                false
            }
        }
        if (retryNow) scope.launch { finishClose(local) }
    }

    /** Both late and installed resources remain reachable until a close attempt succeeds. */
    private suspend fun closeRetainedResource(local: LocalSession<S>): Boolean {
        val resource = synchronized(lock) { local.retainedCleanupResource } ?: return true
        return try {
            withContext(NonCancellable + cleanupDispatcher) { resource.close() }
            synchronized(lock) {
                if (local.retainedCleanupResource === resource) {
                    local.retainedCleanupResource = null
                    retainedCleanupSessions.remove(local.acceptance.sessionId, local)
                }
            }
            true
        } catch (error: Throwable) {
            if (error.isFatalJvmError()) throw error
            false
        }
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
        beginCloseLocked(local, acknowledge = false)
    }

    internal fun state(sessionId: WarmupSessionId): State? = synchronized(lock) {
        (sessions[sessionId] ?: retainedCleanupSessions[sessionId])?.state
    }

    internal fun pendingCleanupResourceCount(): Int = synchronized(lock) { retainedCleanupSessions.size }

    /** A context stop owns another bounded attempt without depending on terminal fence retention. */
    suspend fun retryRetainedCleanupAndAwait(timeout: Duration): Boolean {
        require(timeout.isPositive()) { "Donor cleanup timeout must be > 0" }
        synchronized(lock) {
            retainedCleanupSessions.values.toSet().forEach { local ->
                if (local.closeStarted) local.closeRetryRequested = true else scheduleFailedCleanupLocked(local)
            }
        }
        return withTimeoutOrNull(timeout) {
            while (pendingCleanupResourceCount() != 0) delay(10)
            true
        } == true
    }

    private suspend fun prepare(local: LocalSession<S>) {
        val prepared = try {
            val remainingMillis = local.acceptance.expiresAtEpochMillis - nowEpochMillis()
            require(remainingMillis > 0) { "Warm-up donor preparation deadline expired" }
            // Session expiry cancels this stored job. Calling preparation directly lets a
            // non-cooperative implementation that returns after cancellation transfer its late
            // resource to the cleanup path instead of losing it at a withTimeout boundary.
            preparation.prepare(local.acceptance)
        } catch (_: CancellationException) {
            failPreparation(local, "donor_preparation_cancelled")
            local.preparationFinished.complete(PreparationCompletion.Cancelled)
            return
        } catch (_: Throwable) {
            failPreparation(local, "donor_preparation_failed")
            local.preparationFinished.complete(PreparationCompletion.Cancelled)
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
        if (keep) {
            local.preparationFinished.complete(PreparationCompletion.Installed)
            return
        }
        // Cancellation may have won before a non-cooperative preparation returned. Publish the
        // cleanup result inside NonCancellable as well: withContext's return to a cancelled caller
        // otherwise reports cancellation after close succeeded and strands finishClose().
        withContext(NonCancellable) {
            val completion = try {
                withContext(cleanupDispatcher) { prepared.close() }
                PreparationCompletion.Cancelled
            } catch (error: Throwable) {
                synchronized(lock) { retainCleanupResourceLocked(local, prepared) }
                PreparationCompletion.ResourceCleanupFailed(error)
            }
            local.preparationFinished.complete(completion)
            if (completion is PreparationCompletion.ResourceCleanupFailed && completion.error.isFatalJvmError()) {
                throw completion.error
            }
        }
    }

    private suspend fun failPreparation(local: LocalSession<S>, reasonCode: String) {
        val notify = synchronized(lock) {
            if (local.state != State.PREPARING) return@synchronized false
            failLocked(local)
            true
        }
        if (notify) onPreparationFailed(rejection(local.acceptance, reasonCode))
    }

    private fun failLocked(local: LocalSession<S>) {
        local.state = State.FAILED
        local.readySeen = false
        if (local.prepared == null) {
            dataPlane.cancelPreparingAdmission(local.acceptance.sessionId)
        } else {
            dataPlane.stopDelegating(local.acceptance.sessionId)
            val detached = dataPlane.detachDrainedSession(local.acceptance.sessionId)
            check(detached != null) {
                "Failed unactivated donor session ${local.acceptance.sessionId.value} did not close"
            }
            retainCleanupResourceLocked(local, detached)
            scheduleFailedCleanupLocked(local)
        }
        recordTerminalLocked(local)
    }

    /** Failed activation has no close acknowledgement, but its resource remains retryable. */
    private fun scheduleFailedCleanupLocked(local: LocalSession<S>) {
        if (local.closeStarted || local.retainedCleanupResource == null) return
        local.closeStarted = true
        scope.launch {
            val closed = closeRetainedResource(local)
            synchronized(lock) {
                local.closeStarted = false
                if (!closed && local.closeRetryRequested) {
                    local.closeRetryRequested = false
                    scheduleFailedCleanupLocked(local)
                }
            }
        }
    }

    /** A later terminal message or expiry retries a failed resource without reviving its session. */
    private fun requestFailedCleanupLocked(local: LocalSession<S>): Boolean {
        if (local.retainedCleanupResource == null) return false
        if (local.closeStarted) local.closeRetryRequested = true else scheduleFailedCleanupLocked(local)
        return true
    }

    private fun retainCleanupResourceLocked(local: LocalSession<S>, resource: S) {
        check(local.retainedCleanupResource == null || local.retainedCleanupResource === resource) {
            "Warm-up session ${local.acceptance.sessionId.value} already owns another cleanup resource"
        }
        local.retainedCleanupResource = resource
        retainedCleanupSessions[local.acceptance.sessionId] = local
    }

    /**
     * Keeps exact fences bounded without forgetting a still-valid session. When exact storage is
     * full, the omitted fence blocks every new admission until its immutable deadline expires.
     */
    private fun recordTerminalLocked(local: LocalSession<S>) {
        pruneExpiredTerminalFencesLocked()
        val sessionId = local.acceptance.sessionId
        if (sessionId in terminalSessionOrder) return
        if (terminalSessionOrder.size < maxTerminalSessionFences) {
            terminalSessionOrder.add(sessionId)
        } else {
            sessions.remove(sessionId)
            admissionBlockedUntilEpochMillis = maxOf(
                admissionBlockedUntilEpochMillis,
                local.acceptance.expiresAtEpochMillis
            )
        }
    }

    private fun pruneExpiredTerminalFencesLocked() {
        val now = nowEpochMillis()
        val expired = terminalSessionOrder.filter { sessionId ->
            sessions[sessionId]?.acceptance?.expiresAtEpochMillis?.let { it < now } != false
        }
        expired.forEach { sessionId ->
            terminalSessionOrder.remove(sessionId)
            sessions.remove(sessionId)
        }
        if (admissionBlockedUntilEpochMillis < now) admissionBlockedUntilEpochMillis = 0L
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
        admissionBudgetPerSecond = acceptance.admissionBudgetPerSecond,
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
