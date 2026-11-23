package avh.ckc.warmup

import avh.ckc.warmup.protocol.ControlRecordCompleted
import avh.ckc.warmup.protocol.DonorAccepted
import avh.ckc.warmup.protocol.DonorClosed
import avh.ckc.warmup.protocol.DonorOffered
import avh.ckc.warmup.protocol.RecipientReady
import avh.ckc.warmup.protocol.SessionAccepted
import avh.ckc.warmup.protocol.SessionEnded
import avh.ckc.warmup.protocol.SessionRejected
import avh.ckc.warmup.protocol.WarmupCompletionRequested
import avh.ckc.warmup.protocol.WarmupControlMessageHandler
import avh.ckc.warmup.protocol.WarmupControlNamespace
import avh.ckc.warmup.protocol.WarmupControlRecord
import avh.ckc.warmup.protocol.WarmupControlRouter
import avh.ckc.warmup.protocol.WarmupInstanceId
import avh.ckc.warmup.protocol.WarmupLivenessProbe
import avh.ckc.warmup.protocol.WarmupRequested
import avh.ckc.warmup.protocol.WarmupSessionEndState
import avh.ckc.warmup.protocol.WarmupSessionId
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.withTimeoutOrNull

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
    private val donorLivenessTimeout: Duration = 10.seconds,
    private val scope: CoroutineScope
) : WarmupControlMessageHandler, AutoCloseable {
    private val lock = Any()
    private val recipientSignal = Channel<Unit>(Channel.CONFLATED)
    private val recipientInterruptionSignal = Channel<Unit>(Channel.CONFLATED)
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

    private val probesToReply = Channel<WarmupLivenessProbe>(32)
    private val probeReplyWorkers = List(2) {
        scope.launch(start = CoroutineStart.LAZY) {
            for (probe in probesToReply) {
                val reply = donorActivation.replyToProbe(probe) ?: continue
                try {
                    withTimeoutOrNull(probeInterval) { channel.send(reply) }
                } catch (cancelled: CancellationException) {
                    throw cancelled
                } catch (_: Exception) {
                    // A missed probe reply is retried by the recipient within its liveness window.
                }
            }
        }
    }

    suspend fun startControl(): Boolean = try {
        channel.start()
        channel.awaitReady()
        probeReplyWorkers.forEach { it.start() }
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
     * Closes donors and recipient consumers before finishing local sends and publishing the outcome.
     * Repeated calls reuse the recorded result. Incomplete cleanup retains resources for shutdown.
     */
    suspend fun completeWarmup(
        recipient: WarmupRecipientDiscovery,
        interruption: Deferred<String>? = null,
        onFailureDetected: (String) -> Unit = {},
        finishSends: suspend (WarmupCompletionResult) -> WarmupCompletionResult = { it }
    ): WarmupCompletionResult {
        completedResult(recipient)?.let { return it }
        require(synchronized(lock) { activeRecipient === recipient }) {
            "Recipient is not the active local warm-up session"
        }
        val consumerResult = try {
            val reason = try {
                if (interruption == null) awaitDonorCompletion(recipient)?.also(onFailureDetected)
                else coroutineScope {
                    val completion = async { awaitDonorCompletion(recipient) }
                    try {
                        select {
                            interruption.onAwait { it.also(onFailureDetected) }
                            completion.onAwait { it?.also(onFailureDetected) }
                        }
                    } finally {
                        completion.cancel()
                    }
                }
            } finally {
                // Intentional recipient shutdown after all donors close must not trigger interruption.
                interruption?.cancelAndJoin()
            }
            if (reason == null) {
                when (val cleanup = recipient.completeAfterDonorsClosed()) {
                    WarmupRecipientCleanupResult.Complete -> WarmupCompletionResult.Completed
                    is WarmupRecipientCleanupResult.CompleteWithDiagnostics ->
                        WarmupCompletionResult.CompletedWithDiagnostics(cleanup.diagnostics)
                    is WarmupRecipientCleanupResult.Incomplete ->
                        WarmupCompletionResult.CleanupIncomplete("recipient_cleanup_incomplete", cleanup)
                }
            } else {
                completionFallback(recipient, reason)
            }
        } catch (error: CancellationException) {
            recipient.terminate()
            clearRecipient(recipient)
            throw error
        } catch (_: Exception) {
            onFailureDetected("completion_failed")
            completionFallback(recipient, "completion_failed")
        }

        val result = finishSends(consumerResult)
        val failureReason = when (result) {
            WarmupCompletionResult.Completed,
            is WarmupCompletionResult.CompletedWithDiagnostics -> null
            is WarmupCompletionResult.ColdFallback -> result.reasonCode
            is WarmupCompletionResult.ColdFallbackWithDiagnostics -> result.reasonCode
            is WarmupCompletionResult.CleanupIncomplete -> result.reasonCode
        }
        val terminalState = if (failureReason == null) WarmupSessionEndState.COMPLETED
            else WarmupSessionEndState.COLD_FALLBACK
        // Commit the cleanup result before Kafka can accept the fence. A cancelled ACK wait must
        // report the same local outcome even if delivery of the broadcast is uncertain.
        rememberCompletion(recipient, result)
        withTimeoutOrNull(probeInterval) {
            publishEndedFence(recipient, terminalState, failureReason)
        }
        return result
    }

    fun completedResult(recipient: WarmupRecipientDiscovery): WarmupCompletionResult? = synchronized(lock) {
        completedRecipient?.takeIf { it.first === recipient }?.second
    }

    private suspend fun awaitDonorCompletion(recipient: WarmupRecipientDiscovery): String? {
        val remainingMillis = recipient.request.expiresAtEpochMillis - System.currentTimeMillis()
        val completionMillis = minOf(donorCompletionTimeout.inWholeMilliseconds, remainingMillis)
        val closed = withTimeoutOrNull(completionMillis) {
            recipient.completionRequests().forEach { channel.send(it) }
            while (!recipient.allDonorsClosed() && recipient.state() == WarmupRecipientDiscovery.State.DRAINING) {
                recipientSignal.receive()
            }
            recipient.allDonorsClosed()
        }
        return when (closed) {
            true -> null
            false -> WarmupInterruption.DONOR_SESSION_ENDED.reasonCode
            null -> "donor_completion_timeout"
        }
    }

    /** Rolls back a prepared recipient and gives its best-effort fence a bounded send window. */
    suspend fun abortWarmup(recipient: WarmupRecipientDiscovery, reasonCode: String): WarmupDiscoveryResult =
        fallback(recipient, reasonCode, fenceTimeout = probeInterval)

    /** An absent round trip may indicate either donor loss or a control-path outage. */
    suspend fun awaitWarmupInterruption(
        recipient: WarmupRecipientDiscovery,
        readiness: List<RecipientReady>
    ): WarmupInterruption {
        val liveness = WarmupDonorLiveness(
            recipient.request, readiness.filter { recipient.supportsDonorLiveness(it.targetInstanceId) },
            channel, donorLivenessTimeout, probeInterval, scope
        )
        donorLiveness = liveness
        return try {
            supervisorScope {
                val controlInterruption = async {
                    while (recipient.state() != WarmupRecipientDiscovery.State.COLD_FALLBACK) {
                        recipientInterruptionSignal.receive()
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
            liveness.cancelSenders()
        }
    }

    suspend fun stopRecipientProbes() {
        val liveness = donorLiveness ?: return
        liveness.cancelSenders()
        liveness.awaitSendersStopped()
        if (donorLiveness === liveness) donorLiveness = null
    }

    override suspend fun onMessage(record: WarmupControlRecord) {
        val message = record.message
        if (message.namespace != namespace) return
        when (message) {
            is WarmupRequested -> if (donorActivation.isAdmissionOpen()) {
                donorDiscovery.offer(message, System.currentTimeMillis())?.let { channel.send(it) }
            }
            is SessionAccepted -> if (message.targetInstanceId == localInstanceId && donorActivation.isAdmissionOpen()) {
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
            is SessionRejected -> withRecipient(message.sessionId) {
                it.onDonorRejected(message).also { rejected ->
                    if (rejected) recipientInterruptionSignal.trySend(Unit)
                }
            }
            is DonorClosed -> withRecipient(message.sessionId) {
                if (!it.onDonorClosed(message)) false
                else {
                    donorLiveness?.onDonorClosed(message.senderInstanceId)
                    true
                }
            }
            is WarmupLivenessProbe -> if (message.reply) {
                donorLiveness?.onReply(message)
            } else {
                // The recipient retries if the bounded reply queue is full.
                probesToReply.trySend(message)
            }
        }
    }

    suspend fun stop() {
        stopProbeReplies()
        donorLiveness?.cancelSenders()
        channel.stop()
        stopRecipientProbes()
        probeReplyWorkers.joinAll()
    }

    override fun close() {
        stopProbeReplies()
        donorLiveness?.cancelSenders()
        registration.close()
        recipientSignal.close()
        recipientInterruptionSignal.close()
    }

    private fun stopProbeReplies() {
        probesToReply.cancel()
        probeReplyWorkers.forEach { it.cancel() }
    }

    private fun withRecipient(sessionId: WarmupSessionId, action: (WarmupRecipientDiscovery) -> Boolean) {
        val recipient = synchronized(lock) { activeRecipient }
        if (recipient?.request?.sessionId == sessionId && action(recipient)) recipientSignal.trySend(Unit)
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
        return when (cleanup) {
            WarmupRecipientCleanupResult.Complete -> WarmupCompletionResult.ColdFallback(reasonCode)
            is WarmupRecipientCleanupResult.CompleteWithDiagnostics ->
                WarmupCompletionResult.ColdFallbackWithDiagnostics(reasonCode, cleanup)
            is WarmupRecipientCleanupResult.Incomplete ->
                WarmupCompletionResult.CleanupIncomplete(reasonCode, cleanup)
        }
    }

    private suspend fun publishEndedFence(
        recipient: WarmupRecipientDiscovery,
        state: WarmupSessionEndState,
        reasonCode: String?
    ) {
        val request = recipient.request
        val ended = SessionEnded(
            namespace = namespace,
            sessionId = request.sessionId,
            senderInstanceId = localInstanceId,
            state = state,
            reasonCode = reasonCode,
            configurationFingerprint = request.configurationFingerprint,
            expiresAtEpochMillis = request.expiresAtEpochMillis
        )
        if (!recipient.claimTerminalFence(ended)) return
        try {
            channel.send(ended)
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
