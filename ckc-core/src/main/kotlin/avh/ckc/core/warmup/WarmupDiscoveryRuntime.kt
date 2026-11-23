package avh.ckc.core.warmup

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.selects.select
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

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

internal enum class WarmupInterruption(val reasonCode: String) {
    DONOR_SESSION_ENDED("donor_session_ended"),
    RECIPIENT_CONSUMER_TERMINATED("recipient_consumer_terminated"),
    DONOR_LIVENESS_TIMEOUT("donor_liveness_timeout")
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
    private val donorCompletionTimeout: Duration = donorAcceptanceTimeout,
    private val donorLivenessTimeout: Duration = 10.seconds
) : WarmupControlMessageHandler, AutoCloseable {
    private val lock = Any()
    private val recipientSignal = Channel<Unit>(Channel.CONFLATED)
    private val registration = router.register(this)
    private var activeRecipient: WarmupRecipientDiscovery? = null
    private var completedRecipient: Pair<WarmupRecipientDiscovery, WarmupCompletionResult>? = null
    private val probeInterval = donorLivenessTimeout / 3
    @Volatile private var donorLiveness: WarmupDonorLiveness? = null
    @Volatile private var controlFailure: Throwable? = null

    init {
        require(offerCollectionWindow.isPositive()) { "Offer collection window must be > 0" }
        require(donorAcceptanceTimeout.isPositive()) { "Donor acceptance timeout must be > 0" }
        require(donorCompletionTimeout.isPositive()) { "Donor completion timeout must be > 0" }
        require(donorLivenessTimeout.isFinite() && probeInterval.inWholeMilliseconds >= 1) {
            "Donor liveness timeout must be finite and at least 3 milliseconds"
        }
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
                }
            } else {
                null
            }
            if (admitted != true) {
                val reason = if (admitted == null) "donor_acceptance_timeout" else "donor_acceptance_failed"
                return fallback(recipient, reason)
            }

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
            val closed = if (completionMillis > 0) {
                withTimeoutOrNull(completionMillis) {
                    while (!recipient.allDonorsClosed() &&
                        recipient.state() == WarmupRecipientDiscovery.State.DRAINING
                    ) {
                        recipientSignal.receive()
                    }
                    recipient.allDonorsClosed()
                }
            } else {
                null
            }
            if (closed != true) {
                val reason = if (closed == null) "donor_completion_timeout" else "donor_completion_failed"
                return completionFallback(recipient, reason)
            }

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

    /** Rolls back a prepared recipient and gives its best-effort fence a bounded send window. */
    suspend fun abortWarmup(recipient: WarmupRecipientDiscovery, reasonCode: String): WarmupDiscoveryResult =
        fallback(recipient, reasonCode, fenceTimeout = probeInterval)

    /** An absent round trip may indicate either donor loss or a control-path outage. */
    suspend fun awaitWarmupInterruption(
        recipient: WarmupRecipientDiscovery,
        duration: Duration
    ): WarmupInterruption? = withTimeoutOrNull(duration) {
        val liveness = WarmupDonorLiveness(
            recipient.request, recipient.selectedDonorIds(), channel, donorLivenessTimeout, probeInterval
        )
        donorLiveness = liveness
        try {
            supervisorScope {
                val controlInterruption = async {
                    while (recipient.state() == WarmupRecipientDiscovery.State.READY) {
                        recipientSignal.receive()
                    }
                    WarmupInterruption.DONOR_SESSION_ENDED
                }
                val consumerTermination = async {
                    recipient.awaitPreparedConsumerTermination()
                    WarmupInterruption.RECIPIENT_CONSUMER_TERMINATED
                }
                val livenessTimeout = async {
                    liveness.awaitTimeout()
                    WarmupInterruption.DONOR_LIVENESS_TIMEOUT
                }
                try {
                    select {
                        controlInterruption.onAwait { it }
                        consumerTermination.onAwait { it }
                        livenessTimeout.onAwait { it }
                    }
                } finally {
                    controlInterruption.cancel()
                    consumerTermination.cancel()
                    livenessTimeout.cancel()
                }
            }
        } finally {
            donorLiveness = null
        }
    }

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
            is WarmupLivenessProbe -> if (message.reply) {
                donorLiveness?.onReply(message)
            } else {
                replyToProbe(message)
            }
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

    private suspend fun replyToProbe(probe: WarmupLivenessProbe) {
        val reply = donorActivation.replyToProbe(probe) ?: return
        try {
            withTimeoutOrNull(probeInterval) { channel.send(reply) }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            // The recipient retries until its liveness deadline.
        }
    }

    private suspend fun fallback(
        recipient: WarmupRecipientDiscovery,
        reasonCode: String,
        knownCleanup: WarmupRecipientCleanupResult? = null,
        fenceTimeout: Duration? = null
    ): WarmupDiscoveryResult {
        val cleanup = knownCleanup ?: recipient.terminate()
        if (fenceTimeout == null) {
            publishEndedFence(recipient, WarmupSessionEndState.COLD_FALLBACK, reasonCode)
        } else {
            withTimeoutOrNull(fenceTimeout) {
                publishEndedFence(recipient, WarmupSessionEndState.COLD_FALLBACK, reasonCode)
            }
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
