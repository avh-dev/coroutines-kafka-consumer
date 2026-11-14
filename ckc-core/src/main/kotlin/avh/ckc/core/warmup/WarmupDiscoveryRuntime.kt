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

/**
 * Connects the tail-started control transport to stateless donor offers and one local recipient.
 * It is not a coordinator in the protocol sense: it owns no namespace table and dispatches only
 * the local recipient session plus donor sessions admitted by [WarmupDonorActivation].
 */
internal class WarmupDiscoveryRuntime<K, V>(
    private val localInstanceId: WarmupInstanceId,
    private val namespace: WarmupControlNamespace,
    router: WarmupControlRouter,
    private val channel: WarmupControlChannel,
    private val donorDiscovery: StatelessWarmupDonorDiscovery,
    private val donorActivation: WarmupDonorActivation<K, V>,
    private val offerCollectionWindow: Duration,
    private val donorAcceptanceTimeout: Duration
) : WarmupControlMessageHandler, AutoCloseable {
    private val lock = Any()
    private val recipientSignal = Channel<Unit>(Channel.CONFLATED)
    private val registration = router.register(this)
    private var activeRecipient: WarmupRecipientDiscovery? = null
    @Volatile private var controlFailure: Throwable? = null

    init {
        require(offerCollectionWindow.isPositive()) { "Offer collection window must be > 0" }
        require(donorAcceptanceTimeout.isPositive()) { "Donor acceptance timeout must be > 0" }
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
                if (activeRecipient === recipient) activeRecipient = null
            }
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
            is SessionEnded -> donorActivation.onSessionEnded(message)
            is DonorOffered -> withRecipient(message.sessionId) { it.onOffer(message) }
            is DonorAccepted -> withRecipient(message.sessionId) { it.onDonorAccepted(message) }
            is SessionRejected -> withRecipient(message.sessionId) { it.onDonorRejected(message) }
            is ControlRecordCompleted,
            is WarmupCompletionRequested,
            is DonorClosed -> Unit
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
        return when (cleanup) {
            WarmupRecipientCleanupResult.Complete -> WarmupDiscoveryResult.ColdFallback(reasonCode)
            is WarmupRecipientCleanupResult.CompleteWithDiagnostics ->
                WarmupDiscoveryResult.ColdFallbackWithDiagnostics(reasonCode, cleanup)
            is WarmupRecipientCleanupResult.Incomplete ->
                WarmupDiscoveryResult.CleanupIncomplete(reasonCode, cleanup)
        }
    }
}
