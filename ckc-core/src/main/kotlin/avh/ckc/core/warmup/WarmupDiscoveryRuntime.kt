package avh.ckc.core.warmup

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.time.Duration

internal sealed interface WarmupDiscoveryResult {
    data class Ready(val messages: List<RecipientReady>) : WarmupDiscoveryResult
    data class ColdFallback(val reasonCode: String) : WarmupDiscoveryResult
    data class ColdFallbackWithDiagnostics(
        val reasonCode: String,
        val cleanup: WarmupRecipientCleanupResult.CompleteWithDiagnostics
    ) : WarmupDiscoveryResult
    data class CleanupIncomplete(
        val reasonCode: String,
        val cleanup: WarmupRecipientCleanupResult.Incomplete
    ) : WarmupDiscoveryResult
}

internal sealed interface WarmupCompletionResult {
    data object Completed : WarmupCompletionResult
    data class CompletedWithDiagnostics(val diagnostics: List<String>) : WarmupCompletionResult
    data class ColdFallback(val reasonCode: String) : WarmupCompletionResult
    data class ColdFallbackWithDiagnostics(
        val reasonCode: String,
        val cleanup: WarmupRecipientCleanupResult.CompleteWithDiagnostics
    ) : WarmupCompletionResult
    data class CleanupIncomplete(
        val reasonCode: String,
        val cleanup: WarmupRecipientCleanupResult.Incomplete
    ) : WarmupCompletionResult
}

/**
 * Connects the tail-started control transport to stateless donor offers and one local recipient.
 * It is not a coordinator in the protocol sense: it owns no namespace table and dispatches only
 * the local recipient session plus donor sessions admitted by [WarmupDonorActivation].
 */
internal class WarmupDiscoveryRuntime<S : WarmupDonorSessionLifecycle>(
    private val localInstanceId: WarmupInstanceId,
    private val namespace: WarmupControlNamespace,
    router: WarmupControlRouter,
    private val channel: WarmupControlChannel,
    private val donorDiscovery: StatelessWarmupDonorDiscovery,
    private val donorActivation: WarmupDonorActivation<S>,
    private val offerCollectionWindow: Duration,
    private val donorAcceptanceTimeout: Duration,
    private val donorCompletionTimeout: Duration = donorAcceptanceTimeout
) : WarmupControlMessageHandler, AutoCloseable {
    private val lock = Any()
    private val recipientSignal = Channel<Unit>(Channel.CONFLATED)
    private val registration = router.register(this)
    private var activeRecipient: WarmupRecipientDiscovery? = null
    private var completedRecipient: Pair<WarmupRecipientDiscovery, WarmupCompletionResult>? = null
    @Volatile private var controlFailure: Throwable? = null

    init {
        require(offerCollectionWindow.isPositive()) { "Offer collection window must be > 0" }
        require(donorAcceptanceTimeout.isPositive()) { "Donor acceptance timeout must be > 0" }
        require(donorCompletionTimeout.isPositive()) { "Donor completion timeout must be > 0" }
    }

    suspend fun startControl(): Boolean = try {
        channel.start()
        channel.awaitReady()
        true
    } catch (error: CancellationException) {
        throw error
    } catch (error: Exception) {
        controlFailure = error
        false
    }

    suspend fun discoverAndPrepare(recipient: WarmupRecipientDiscovery): WarmupDiscoveryResult {
        val request = recipient.request
        require(request.namespace == namespace && request.senderInstanceId == localInstanceId) {
            "Recipient request must belong to the local namespace and instance"
        }
        synchronized(lock) {
            check(activeRecipient == null) { "A recipient discovery is already active" }
            activeRecipient = recipient
        }
        try {
            if (controlFailure != null) return fallback(recipient, "control_unavailable")
            channel.send(request)
            val offerWaitMillis = minOf(
                offerCollectionWindow.inWholeMilliseconds,
                request.offerDeadlineEpochMillis - System.currentTimeMillis()
            )
            if (offerWaitMillis > 0) delay(offerWaitMillis)
            val selection = recipient.selectDonors()
            if (selection is WarmupRecipientSelection.ColdFallback) {
                return fallback(recipient, selection.reasonCode)
            }
            selection as WarmupRecipientSelection.Accepted
            selection.messages.forEach { channel.send(it) }

            val acceptanceMillis = minOf(
                donorAcceptanceTimeout.inWholeMilliseconds,
                request.expiresAtEpochMillis - System.currentTimeMillis()
            )
            val admitted = if (acceptanceMillis > 0) {
                withTimeoutOrNull(acceptanceMillis) {
                    while (!recipient.allDonorsAccepted() &&
                        recipient.state() == WarmupRecipientDiscovery.State.ADMITTING_DONORS
                    ) {
                        recipientSignal.receive()
                    }
                    recipient.allDonorsAccepted()
                } == true
            } else {
                false
            }
            if (!admitted) return fallback(recipient, "donor_acceptance_failed")

            return when (val preparation = recipient.prepareConsumers()) {
                is WarmupRecipientPreparationResult.Ready -> {
                    preparation.messages.forEach { channel.send(it) }
                    if (recipient.confirmReadyPublished()) {
                        WarmupDiscoveryResult.Ready(preparation.messages)
                    } else {
                        fallback(recipient, "readiness_cancelled")
                    }
                }
                is WarmupRecipientPreparationResult.ColdFallback ->
                    fallback(recipient, preparation.reasonCode, preparation.cleanup)
            }
        } catch (error: CancellationException) {
            recipient.terminate()
            throw error
        } catch (_: Exception) {
            return fallback(recipient, "discovery_failed")
        } finally {
            synchronized(lock) {
                if (activeRecipient === recipient && recipient.state() !in
                    setOf(WarmupRecipientDiscovery.State.READY, WarmupRecipientDiscovery.State.DRAINING)
                ) {
                    activeRecipient = null
                }
            }
        }
    }

    /**
     * Closes every selected donor before stopping recipient consumers. A repeated call after an
     * incomplete local cleanup re-sends the same requests and retries the retained resources.
     */
    suspend fun completeWarmup(recipient: WarmupRecipientDiscovery): WarmupCompletionResult {
        synchronized(lock) {
            completedRecipient?.takeIf { it.first === recipient }?.let { return it.second }
        }
        require(synchronized(lock) { activeRecipient === recipient }) {
            "Recipient is not the active local warm-up session"
        }
        return try {
            recipient.completionRequests().forEach { channel.send(it) }
            val remainingMillis = recipient.request.expiresAtEpochMillis - System.currentTimeMillis()
            val completionMillis = minOf(donorCompletionTimeout.inWholeMilliseconds, remainingMillis)
            val closed = completionMillis > 0 && withTimeoutOrNull(completionMillis) {
                while (!recipient.allDonorsClosed() &&
                    recipient.state() == WarmupRecipientDiscovery.State.DRAINING
                ) {
                    recipientSignal.receive()
                }
                recipient.allDonorsClosed()
            } == true
            if (!closed) return completionFallback(recipient, "donor_completion_failed")

            when (val cleanup = recipient.completeAfterDonorsClosed()) {
                WarmupRecipientCleanupResult.Complete -> {
                    publishCompletedFence(recipient)
                    WarmupCompletionResult.Completed.also { rememberCompletion(recipient, it) }
                }
                is WarmupRecipientCleanupResult.CompleteWithDiagnostics -> {
                    publishCompletedFence(recipient)
                    WarmupCompletionResult.CompletedWithDiagnostics(cleanup.diagnostics).also {
                        rememberCompletion(recipient, it)
                    }
                }
                is WarmupRecipientCleanupResult.Incomplete ->
                    WarmupCompletionResult.CleanupIncomplete("recipient_cleanup_incomplete", cleanup)
            }
        } catch (error: CancellationException) {
            recipient.terminate()
            clearRecipient(recipient)
            throw error
        } catch (_: Exception) {
            completionFallback(recipient, "completion_failed")
        }
    }

    /** Rolls back an already prepared local recipient and fences its donors before cold start. */
    suspend fun abortWarmup(recipient: WarmupRecipientDiscovery, reasonCode: String): WarmupDiscoveryResult =
        fallback(recipient, reasonCode)

    /** Returns early when an accepted donor terminates the active recipient session. */
    suspend fun awaitWarmupInterruption(
        recipient: WarmupRecipientDiscovery,
        duration: Duration
    ): Boolean = withTimeoutOrNull(duration) {
        while (recipient.state() == WarmupRecipientDiscovery.State.READY) {
            recipientSignal.receive()
        }
        true
    } == true

    override suspend fun onMessage(record: WarmupControlRecord) {
        val message = record.message
        if (message.namespace != namespace) return
        when (message) {
            is WarmupRequested -> donorDiscovery.offer(message, System.currentTimeMillis())?.let { channel.send(it) }
            is SessionAccepted -> if (message.targetInstanceId == localInstanceId) {
                val response = donorActivation.accept(message)
                channel.send(response)
                if (response is DonorAccepted) donorActivation.startPreparation(message.sessionId)
            }
            is RecipientReady -> donorActivation.onRecipientReady(message)
            is ControlRecordCompleted -> donorActivation.onRecordCompleted(message)
            is WarmupCompletionRequested -> donorActivation.onCompletionRequested(message)
            is SessionEnded -> donorActivation.onSessionEnded(message)
            is DonorOffered -> withRecipient(message.sessionId) { it.onOffer(message) }
            is DonorAccepted -> withRecipient(message.sessionId) { it.onDonorAccepted(message) }
            is SessionRejected -> withRecipient(message.sessionId) { it.onDonorRejected(message) }
            is DonorClosed -> withRecipient(message.sessionId) { it.onDonorClosed(message) }
        }
    }

    suspend fun stop() {
        channel.stop()
    }

    override fun close() {
        registration.close()
        recipientSignal.close()
    }

    private fun withRecipient(sessionId: WarmupSessionId, action: (WarmupRecipientDiscovery) -> Boolean) {
        val recipient = synchronized(lock) { activeRecipient }
        if (recipient?.request?.sessionId == sessionId && action(recipient)) recipientSignal.trySend(Unit)
    }

    private suspend fun fallback(
        recipient: WarmupRecipientDiscovery,
        reasonCode: String,
        knownCleanup: WarmupRecipientCleanupResult? = null
    ): WarmupDiscoveryResult {
        val cleanup = knownCleanup ?: recipient.terminate()
        val request = recipient.request
        try {
            channel.send(
                SessionEnded(
                    namespace = namespace,
                    sessionId = request.sessionId,
                    senderInstanceId = localInstanceId,
                    state = WarmupSessionEndState.COLD_FALLBACK,
                    reasonCode = reasonCode,
                    configurationFingerprint = request.configurationFingerprint,
                    expiresAtEpochMillis = request.expiresAtEpochMillis
                )
            )
        } catch (error: CancellationException) {
            throw error
        } catch (_: Exception) {
            // Cold fallback is already local; failure to publish its best-effort fence is diagnostic only.
        }
        clearRecipient(recipient)
        return when (cleanup) {
            WarmupRecipientCleanupResult.Complete -> WarmupDiscoveryResult.ColdFallback(reasonCode)
            is WarmupRecipientCleanupResult.CompleteWithDiagnostics ->
                WarmupDiscoveryResult.ColdFallbackWithDiagnostics(reasonCode, cleanup)
            is WarmupRecipientCleanupResult.Incomplete ->
                WarmupDiscoveryResult.CleanupIncomplete(reasonCode, cleanup)
        }
    }

    private suspend fun completionFallback(
        recipient: WarmupRecipientDiscovery,
        reasonCode: String
    ): WarmupCompletionResult {
        val cleanup = recipient.terminate()
        publishEndedFence(recipient, WarmupSessionEndState.COLD_FALLBACK, reasonCode)
        // The terminated recipient retains any unresolved resources for direct cleanup retry, but
        // no late control message may keep it registered as the current protocol session.
        clearRecipient(recipient)
        return when (cleanup) {
            WarmupRecipientCleanupResult.Complete -> WarmupCompletionResult.ColdFallback(reasonCode)
            is WarmupRecipientCleanupResult.CompleteWithDiagnostics ->
                WarmupCompletionResult.ColdFallbackWithDiagnostics(reasonCode, cleanup)
            is WarmupRecipientCleanupResult.Incomplete ->
                WarmupCompletionResult.CleanupIncomplete(reasonCode, cleanup)
        }
    }

    private suspend fun publishCompletedFence(recipient: WarmupRecipientDiscovery) {
        publishEndedFence(recipient, WarmupSessionEndState.COMPLETED, null)
    }

    private suspend fun publishEndedFence(
        recipient: WarmupRecipientDiscovery,
        state: WarmupSessionEndState,
        reasonCode: String?
    ) {
        val request = recipient.request
        try {
            channel.send(
                SessionEnded(
                    namespace = namespace,
                    sessionId = request.sessionId,
                    senderInstanceId = localInstanceId,
                    state = state,
                    reasonCode = reasonCode,
                    configurationFingerprint = request.configurationFingerprint,
                    expiresAtEpochMillis = request.expiresAtEpochMillis
                )
            )
        } catch (error: CancellationException) {
            throw error
        } catch (_: Exception) {
            // The local terminal state is authoritative; this broadcast is a best-effort diagnostic fence.
        }
    }

    private fun clearRecipient(recipient: WarmupRecipientDiscovery) {
        synchronized(lock) {
            if (activeRecipient === recipient) activeRecipient = null
        }
    }

    private fun rememberCompletion(
        recipient: WarmupRecipientDiscovery,
        result: WarmupCompletionResult
    ) {
        synchronized(lock) {
            completedRecipient = recipient to result
            if (activeRecipient === recipient) activeRecipient = null
        }
    }
}
