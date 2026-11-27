package avh.ckc.warmup.recipient

import avh.ckc.warmup.TrafficWarmupContextConfiguration
import avh.ckc.warmup.TrafficWarmupLifecycleEvent
import avh.ckc.warmup.TrafficWarmupLifecyclePhase
import avh.ckc.warmup.broker.BROKER_DRAINING
import avh.ckc.warmup.broker.WarmupBrokerDrain
import avh.ckc.warmup.broker.WarmupBrokerLifecycle
import avh.ckc.warmup.broker.remaining
import avh.ckc.warmup.dataplane.WarmupCompletionOutbox
import avh.ckc.warmup.dataplane.WarmupResultDeliveryException
import avh.ckc.warmup.dataplane.isWarmupResultDeliveryTimeout
import avh.ckc.warmup.donor.LogicalWarmupDonorSession
import avh.ckc.warmup.kafka.KafkaWarmupDestinationMetadata
import avh.ckc.warmup.kafka.WarmupControlChannel
import avh.ckc.warmup.protocol.ControlRecordCompleted
import avh.ckc.warmup.protocol.RecipientReady
import avh.ckc.warmup.protocol.WarmupControlNamespace
import avh.ckc.warmup.protocol.WarmupInstanceId
import avh.ckc.warmup.protocol.WarmupParticipantId
import avh.ckc.warmup.protocol.WarmupRequested
import avh.ckc.warmup.protocol.WarmupSessionId
import avh.ckc.warmup.protocol.WarmupTopicMapping
import avh.ckc.warmup.protocol.normalizedAdmissionBudget
import avh.ckc.warmup.runtime.RegisteredTrafficWarmupParticipant
import avh.ckc.warmup.runtime.WarmupCompletionResult
import avh.ckc.warmup.runtime.WarmupDiscoveryResult
import avh.ckc.warmup.runtime.WarmupDiscoveryRuntime
import avh.ckc.warmup.runtime.mappingFingerprint
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.TimeSource
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory

/**
 * Owns the context's one recipient attempt, including its terminal outcome and any resources
 * retained for a later cleanup retry. The context schedules the main consumers after completion.
 * A shared monitor orders attempt transitions with participant start, stop, and broker fencing.
 */
internal class WarmupRecipientAttempt(
    private val configuration: TrafficWarmupContextConfiguration,
    private val instanceId: WarmupInstanceId,
    private val namespace: WarmupControlNamespace,
    private val participants: List<RegisteredTrafficWarmupParticipant>,
    private val scope: CoroutineScope,
    private val blockingDispatcher: CoroutineDispatcher,
    private val lock: Any,
    private val runtime: WarmupDiscoveryRuntime<LogicalWarmupDonorSession>?,
    private val transport: WarmupControlChannel?,
    private val brokerLifecycle: WarmupBrokerLifecycle?,
    private val protocolStartup: Deferred<String?>?,
    private val awaitParticipants: suspend () -> Boolean,
    private val observe: (TrafficWarmupLifecycleEvent) -> Unit,
    private val onCompleted: () -> Unit,
    private val onCleanupIncomplete: (Throwable) -> Unit
) {
    /** Terminal result, including whether unfinished cleanup prevented main startup. */
    data class Outcome(val reasonCode: String?, val cleanupFailure: Throwable? = null)

    /** Snapshot taken when the broker closes admission, before waiting for cleanup. */
    data class BrokerFence(val sessionId: String?, val reason: String, val workflow: Job?)

    private data class Interruption(
        val sessionId: String,
        val reasonCode: String,
        val detectedAt: TimeSource.Monotonic.ValueTimeMark = TimeSource.Monotonic.markNow()
    )

    private val log = LoggerFactory.getLogger(javaClass)
    private val protocolRuntime get() = checkNotNull(runtime)
    private var terminalOutcome: Outcome? = null
    private var workflow: Job? = null
    private var activeRecipient: WarmupRecipientSession? = null
    private var activeOutbox: WarmupCompletionOutbox? = null
    private var warmingStarted: TimeSource.Monotonic.ValueTimeMark? = null
    private var interruption: Interruption? = null
    private var activeCleanup: RetriableWarmupRecipientCleanup? = null
    private var readinessStarted = false
    private val brokerDrain = CompletableDeferred<WarmupBrokerDrain>()
    private var brokerInterruptionReason: String? = null

    fun prepareWorkflow(): Job = synchronized(lock) {
        check(workflow == null) { "Warm-up recipient workflow already prepared" }
        scope.launch(start = CoroutineStart.LAZY) {
            if (protocolStartup != null) runInitialRecipientAttempt(protocolStartup)
            else runProtocolInitializationFallback()
        }.also { workflow = it }
    }

    fun outcome(): Outcome? = synchronized(lock) { terminalOutcome }

    fun pendingWorkflow(): Job? = synchronized(lock) {
        workflow?.takeUnless { it.isCompleted || terminalOutcome != null }
    }

    suspend fun stopForParticipant() {
        val pending = pendingWorkflow()
        pending?.cancel(CancellationException("Warm-up participant stopped"))
        awaitInterruptedWorkflow(pending, "participant_stopped")
    }

    fun fenceBrokerAdmission(reason: String, drain: WarmupBrokerDrain?): BrokerFence = synchronized(lock) {
        val recipient = activeRecipient
        val pending = pendingWorkflow()
        if (drain != null && readinessStarted) {
            brokerDrain.complete(drain)
        } else {
            if (recipient != null && protocolRuntime.completedResult(recipient) == null &&
                recipient.terminalOutcome() == null && terminalOutcome == null
            ) {
                observeInterruption(recipient.request.sessionId, reason)
            }
            pending?.cancel(CancellationException("Warm-up broker admission closed"))
        }
        BrokerFence(recipient?.request?.sessionId?.value, reason, pending)
    }

    suspend fun awaitBrokerCleanup(fence: BrokerFence) {
        awaitInterruptedWorkflow(fence.workflow, fence.reason)
        check(outcome()?.cleanupFailure == null) { "Broker recipient cleanup incomplete" }
    }

    private suspend fun awaitInterruptedWorkflow(pending: Job?, reason: String) {
        pending?.join()
        // A coroutine cancelled before entering its body cannot publish an outcome itself.
        if (outcome() == null) finishInterruptedAttempt(reason)
    }

    fun recordBrokerInterruption(reason: String?) = synchronized(lock) {
        brokerInterruptionReason = reason
    }

    fun mainReadyEvent(participantId: WarmupParticipantId): TrafficWarmupLifecycleEvent {
        val recovery = synchronized(lock) { interruption }
        return TrafficWarmupLifecycleEvent(
            TrafficWarmupLifecyclePhase.MAIN_READY,
            participantId = participantId.value,
            sessionId = recovery?.sessionId,
            reasonCode = recovery?.reasonCode,
            recoveryDurationMillis = recovery?.detectedAt?.elapsedNow()?.inWholeMilliseconds
        )
    }

    private suspend fun runProtocolInitializationFallback(reasonCode: String = "protocol_initialization_failed") {
        try {
            completeRecipientPhase(
                if (awaitParticipants()) reasonCode else "participant_start_timeout"
            )
        } catch (_: CancellationException) {
            withContext(NonCancellable) { completeRecipientPhase("participant_stopped") }
        }
    }

    private suspend fun runInitialRecipientAttempt(startup: Deferred<String?>) {
        try {
            when (val startupFailure = startup.await()) {
                "control_unavailable" -> {
                    observe(TrafficWarmupLifecyclePhase.CONTROL_UNAVAILABLE, reasonCode = startupFailure)
                    completeRecipientPhase(startupFailure)
                    return
                }
                null -> Unit
                else -> {
                    runProtocolInitializationFallback(startupFailure)
                    return
                }
            }
            val lifecycle = brokerLifecycle
            if (lifecycle != null && !lifecycle.awaitInitialAdmission(configuration.preparationTimeout)) {
                val reason = synchronized(lock) { brokerInterruptionReason } ?: "control_unavailable"
                completeRecipientPhase(reason)
                return
            }
            if (!awaitParticipants()) {
                completeRecipientPhase("participant_start_timeout")
                return
            }
            runDiscoveryAndWarmup()
        } catch (_: TimeoutCancellationException) {
            withContext(NonCancellable) { finishInterruptedAttempt("recipient_cleanup_timeout") }
        } catch (cancelled: CancellationException) {
            withContext(NonCancellable) {
                finishInterruptedAttempt(synchronized(lock) { brokerInterruptionReason } ?: "participant_stopped")
            }
        } catch (error: Throwable) {
            log.warn("Warm-up recipient attempt failed; using cold start", error)
            withContext(NonCancellable) { finishInterruptedAttempt("recipient_failure") }
        }
    }

    private suspend fun runDiscoveryAndWarmup() {
        val mappings = participants.flatMap(RegisteredTrafficWarmupParticipant::mappings)
        val now = System.currentTimeMillis()
        val expiresAt = now + sessionLifetime().inWholeMilliseconds
        val sessionId = WarmupSessionId.random()
        val request = WarmupRequested(
            namespace = namespace,
            sessionId = sessionId,
            senderInstanceId = instanceId,
            mappings = mappings,
            offerDeadlineEpochMillis = now + configuration.offerWindow.inWholeMilliseconds,
            admissionBudgetPerSecond = normalizedAdmissionBudget(mappings),
            configurationFingerprint = configurationFingerprint(mappings),
            expiresAtEpochMillis = expiresAt
        )
        lateinit var discovery: WarmupRecipientSession
        val outbox = WarmupCompletionOutbox(scope, 1_024) { completed ->
            val donor = requireNotNull(discovery.donorFor(completed.recordId.topicPartition)) {
                "No selected donor owns ${completed.recordId.topicPartition}"
            }
            checkNotNull(transport).send(
                ControlRecordCompleted(
                    namespace,
                    sessionId,
                    instanceId,
                    donor,
                    completed,
                    request.configurationFingerprint,
                    expiresAt
                )
            )
        }
        val participantById = participants.associateBy(RegisteredTrafficWarmupParticipant::participantId)
        discovery = WarmupRecipientSession(
            request,
            KafkaWarmupDestinationMetadata(
                participantById.mapValues { (_, participant) -> participant.warmupConsumerProperties },
                configuration.preparationTimeout,
                blockingDispatcher
            ),
            WarmupRecipientConsumerPreparation { recipientSessionId, start ->
                requireNotNull(participantById[start.assignment.mapping.participantId])
                    .prepareRecipient(recipientSessionId, start, outbox)
            },
            preparationTimeout = configuration.preparationTimeout,
            cleanupTimeout = configuration.completionTimeout
        )
        synchronized(lock) {
            activeRecipient = discovery
            activeOutbox = outbox
            activeCleanup = RetriableWarmupRecipientCleanup(
                terminate = { discovery.terminate() },
                afterComplete = { abortRecipientSends(outbox) }
            )
        }
        observe(TrafficWarmupLifecyclePhase.DISCOVERING, sessionId = sessionId.value)
        when (val result = protocolRuntime.discoverAndPrepare(discovery, brokerDrain) {
            synchronized(lock) { readinessStarted = true }
        }) {
            is WarmupDiscoveryResult.Ready -> {
                observe(TrafficWarmupLifecyclePhase.READY, sessionId = sessionId.value)
                synchronized(lock) { warmingStarted = TimeSource.Monotonic.markNow() }
                observe(TrafficWarmupLifecyclePhase.WARMING, sessionId = sessionId.value)
                runPreparedWarmup(discovery, result.messages, outbox)
            }
            is WarmupDiscoveryResult.ColdFallback -> {
                outbox.stop(configuration.completionTimeout)
                completeRecipientPhase(result.reasonCode)
            }
            is WarmupDiscoveryResult.ColdFallbackWithDiagnostics -> {
                outbox.stop(configuration.completionTimeout)
                completeRecipientPhase(result.reasonCode)
            }
            is WarmupDiscoveryResult.CleanupIncomplete -> failRecipientCleanup(result.reasonCode, result.cleanup)
        }
    }

    private suspend fun runPreparedWarmup(
        discovery: WarmupRecipientSession,
        readiness: List<RecipientReady>,
        outbox: WarmupCompletionOutbox
    ) = coroutineScope {
        val sessionId = discovery.request.sessionId
        val failure = async { awaitWarmupFailure(discovery, readiness, outbox) }
        val earlyFailure = withTimeoutOrNull(configuration.warmupDuration) {
            select {
                failure.onAwait { it }
                brokerDrain.onAwait { null }
            }
        }
        if (earlyFailure != null) {
            observeInterruption(sessionId, earlyFailure)
            rollbackRecipient(earlyFailure)
            return@coroutineScope
        }

        observe(TrafficWarmupLifecyclePhase.COMPLETING, sessionId = sessionId.value)
        val completion = protocolRuntime.completeWarmup(
            discovery,
            failure,
            onFailureDetected = {
                observeInterruption(sessionId, it)
                if (it == BROKER_DRAINING) outbox.closeAdmission() else outbox.close()
            },
            brokerDrain = brokerDrain,
            canPublishFence = outbox::sendsStopped,
            finishSends = { finishRecipientSends(sessionId, outbox, it) }
        )
        applyCompletion(completion)
    }

    private suspend fun finishBrokerDrainSends(
        sessionId: WarmupSessionId,
        outbox: WarmupCompletionOutbox,
        result: WarmupCompletionResult,
        drain: WarmupBrokerDrain
    ): WarmupCompletionResult {
        outbox.closeAdmission()
        val failure = try {
            outbox.flush(drain.resultFlushDeadline.remaining().coerceAtLeast(Duration.ZERO))
            null
        } catch (error: TimeoutCancellationException) {
            currentCoroutineContext().ensureActive()
            error
        } catch (error: WarmupResultDeliveryException) {
            error
        }
        val detectedReason = result.reasonCode ?: failure?.let(::resultDeliveryFailureReason)
        detectedReason?.let { observeInterruption(sessionId, it) }

        val cleaned = withTimeoutOrNull(drain.cleanupDeadline.remaining()) {
            if (failure == null) {
                try {
                    outbox.stop(Duration.INFINITE)
                } catch (error: WarmupResultDeliveryException) {
                    observeInterruption(sessionId, detectedReason ?: resultDeliveryFailureReason(error))
                }
            } else if (!outbox.sendsStopped()) {
                outbox.abort(Duration.INFINITE, failure)
            }
            protocolRuntime.stopRecipientProbes()
            true
        }
        val observedReason = synchronized(lock) {
            interruption?.takeIf { it.sessionId == sessionId.value }?.reasonCode
        }
        val reason = result.reasonCode ?: observedReason
        if (cleaned == true) {
            return if (reason != null && result.reasonCode == null) WarmupCompletionResult.ColdFallback(reason)
            else result
        }
        outbox.close()
        val incomplete = (result as? WarmupCompletionResult.CleanupIncomplete)?.cleanup
        return WarmupCompletionResult.CleanupIncomplete(
            reason ?: "recipient_cleanup_timeout",
            WarmupRecipientCleanupResult.Incomplete(
                pendingAssignments = incomplete?.pendingAssignments.orEmpty(),
                diagnostics = incomplete?.diagnostics.orEmpty() + "warm-up sender cleanup timed out"
            )
        )
    }

    private fun applyCompletion(completion: WarmupCompletionResult) {
        when (completion) {
            WarmupCompletionResult.Completed,
            is WarmupCompletionResult.CompletedWithDiagnostics -> completeRecipientPhase(null)
            is WarmupCompletionResult.ColdFallback -> completeRecipientPhase(completion.reasonCode)
            is WarmupCompletionResult.ColdFallbackWithDiagnostics ->
                completeRecipientPhase(completion.reasonCode)
            is WarmupCompletionResult.CleanupIncomplete ->
                failRecipientCleanup(completion.reasonCode, completion.cleanup)
        }
    }

    private suspend fun finishRecipientSends(
        sessionId: WarmupSessionId,
        outbox: WarmupCompletionOutbox,
        result: WarmupCompletionResult
    ): WarmupCompletionResult {
        if (configuration.brokerLifecycleConfiguration == null) {
            return finishRegularRecipientSends(sessionId, outbox, result)
        }
        if (brokerDrain.isCompleted) {
            return finishBrokerDrainSends(sessionId, outbox, result, brokerDrain.await())
        }
        return coroutineScope {
            val regular = async { finishRegularRecipientSends(sessionId, outbox, result) }
            val drain = select {
                regular.onAwait { null }
                brokerDrain.onAwait { it }
            }
            if (drain == null) regular.await() else {
                regular.cancelAndJoin()
                finishBrokerDrainSends(sessionId, outbox, result, drain)
            }
        }
    }

    private suspend fun finishRegularRecipientSends(
        sessionId: WarmupSessionId,
        outbox: WarmupCompletionOutbox,
        consumerResult: WarmupCompletionResult
    ): WarmupCompletionResult {
        when (consumerResult) {
            WarmupCompletionResult.Completed,
            is WarmupCompletionResult.CompletedWithDiagnostics -> Unit
            else -> return cleanupRecipientSends(consumerResult as? WarmupCompletionResult.CleanupIncomplete)
                ?: consumerResult
        }
        val resultFailure = try {
            outbox.stop(configuration.completionTimeout)
            null
        } catch (error: TimeoutCancellationException) {
            currentCoroutineContext().ensureActive()
            error
        } catch (error: WarmupResultDeliveryException) {
            error
        }
        val reason = if (resultFailure != null) {
            resultDeliveryFailureReason(resultFailure)
        } else {
            val stopped = withTimeoutOrNull(configuration.completionTimeout) {
                protocolRuntime.stopRecipientProbes()
                true
            }
            if (stopped == true) return consumerResult
            "recipient_cleanup_timeout"
        }

        observeInterruption(sessionId, reason)
        // A sender can finish unwinding during one bounded retry. Commit the outcome afterward.
        val cleanup = cleanupRecipientSends(null, resultFailure)
        if (cleanup != null) cleanupRecipientSends(null, resultFailure)?.let { return it }
        return WarmupCompletionResult.ColdFallback(reason)
    }

    private suspend fun awaitWarmupFailure(
        discovery: WarmupRecipientSession,
        readiness: List<RecipientReady>,
        outbox: WarmupCompletionOutbox
    ): String = coroutineScope {
        val protocol = async { protocolRuntime.awaitWarmupInterruption(discovery, readiness).reasonCode }
        val delivery = async { resultDeliveryFailureReason(outbox.awaitFailure()) }
        try {
            select {
                protocol.onAwait { it }
                delivery.onAwait { it }
            }
        } finally {
            protocol.cancel()
            delivery.cancel()
        }
    }

    private fun resultDeliveryFailureReason(error: Throwable): String {
        log.warn("Warm-up result delivery failed; ending recipient session", error)
        return if (isWarmupResultDeliveryTimeout(error)) "result_delivery_timeout" else "result_delivery_failed"
    }

    private fun observeInterruption(sessionId: WarmupSessionId, reasonCode: String) {
        synchronized(lock) {
            if (interruption != null) return
            interruption = Interruption(sessionId.value, reasonCode)
        }
        observe(TrafficWarmupLifecyclePhase.WARMUP_INTERRUPTED, sessionId = sessionId.value, reasonCode = reasonCode)
    }

    private suspend fun finishInterruptedAttempt(reasonCode: String) {
        val (recipient, observedInterruption) = synchronized(lock) { activeRecipient to interruption }
        val outcome = recipient?.let { protocolRuntime.completedResult(it) }
        if (outcome != null) {
            applyCompletion(outcome)
            return
        }
        val observedReason = observedInterruption
            ?.takeIf { it.sessionId == recipient?.request?.sessionId?.value }
            ?.reasonCode
        rollbackRecipient(recipient?.terminalOutcome()?.reasonCode ?: observedReason ?: reasonCode)
    }

    private suspend fun rollbackRecipient(reasonCode: String) {
        activeOutbox?.close()
        val rollback = activeRecipient?.let { protocolRuntime.abortWarmup(it, reasonCode) }
        val incomplete = (rollback as? WarmupDiscoveryResult.CleanupIncomplete)?.cleanup
        finishFailedRecipient(reasonCode, incomplete)
    }

    private suspend fun finishFailedRecipient(
        reasonCode: String,
        incomplete: WarmupRecipientCleanupResult.Incomplete?
    ) {
        val unresolved = cleanupRecipientSends(
            incomplete?.let { WarmupCompletionResult.CleanupIncomplete(reasonCode, it) }
        )
        if (unresolved == null) completeRecipientPhase(reasonCode)
        else failRecipientCleanup(unresolved.reasonCode, unresolved.cleanup)
    }

    private suspend fun cleanupRecipientSends(
        incomplete: WarmupCompletionResult.CleanupIncomplete?,
        cause: Throwable? = null
    ): WarmupCompletionResult.CleanupIncomplete? = try {
        abortRecipientSends(activeOutbox, cause)
        incomplete
    } catch (_: TimeoutCancellationException) {
        currentCoroutineContext().ensureActive()
        WarmupCompletionResult.CleanupIncomplete(
            "recipient_cleanup_timeout",
            WarmupRecipientCleanupResult.Incomplete(
                pendingAssignments = incomplete?.cleanup?.pendingAssignments.orEmpty(),
                diagnostics = incomplete?.cleanup?.diagnostics.orEmpty() + "warm-up sender cleanup timed out"
            )
        )
    }

    private suspend fun abortRecipientSends(outbox: WarmupCompletionOutbox?, cause: Throwable? = null) {
        withTimeout(configuration.completionTimeout) {
            outbox?.abort(configuration.completionTimeout, cause)
            runtime?.stopRecipientProbes()
        }
    }

    private fun completeRecipientPhase(reasonCode: String?) {
        val summary = synchronized(lock) {
            if (terminalOutcome != null) return
            val summary = recipientSummary(reasonCode)
            terminalOutcome = Outcome(reasonCode)
            clearResources()
            summary
        }
        observe(summary)
        if (reasonCode != null) {
            observe(TrafficWarmupLifecyclePhase.COLD_FALLBACK, reasonCode = reasonCode)
        }
        onCompleted()
    }

    private fun failRecipientCleanup(
        reasonCode: String,
        cleanup: WarmupRecipientCleanupResult.Incomplete
    ) {
        val failure = IllegalStateException(
            "Warm-up cleanup incomplete: $reasonCode; " + cleanup.diagnostics.joinToString()
        )
        val summary = synchronized(lock) {
            if (terminalOutcome != null) return
            terminalOutcome = Outcome(reasonCode, failure)
            // Publish participant readiness failure in the same transition seen by start and stop.
            onCleanupIncomplete(failure)
            recipientSummary(reasonCode)
        }
        observe(summary)
    }

    private fun recipientSummary(reasonCode: String?) = TrafficWarmupLifecycleEvent(
        TrafficWarmupLifecyclePhase.RECIPIENT_FINISHED,
        sessionId = activeRecipient?.request?.sessionId?.value,
        reasonCode = reasonCode,
        durationMillis = warmingStarted?.elapsedNow()?.inWholeMilliseconds,
        processedRecords = activeOutbox?.processedRecords() ?: 0L
    )

    suspend fun retryIncompleteCleanup() {
        val cleanup = synchronized(lock) { activeCleanup } ?: return
        cleanup.run()
        synchronized(lock) {
            if (activeCleanup === cleanup) clearResources()
        }
    }

    private fun clearResources() {
        activeRecipient = null
        activeOutbox = null
        activeCleanup = null
    }

    private fun sessionLifetime(): Duration =
        configuration.offerWindow + configuration.preparationTimeout + configuration.preparationTimeout +
            configuration.warmupDuration +
            configuration.completionTimeout + configuration.delegationTimeout

    private fun configurationFingerprint(mappings: List<WarmupTopicMapping>): String =
        UUID.nameUUIDFromBytes(
            mappings.sortedWith(compareBy({ it.participantId.value }, { it.sourceTopic }))
                .joinToString("\u0000") { it.mappingFingerprint }
                .toByteArray(Charsets.UTF_8)
        ).toString()

    private fun observe(
        phase: TrafficWarmupLifecyclePhase,
        sessionId: String? = null,
        reasonCode: String? = null
    ) = observe(TrafficWarmupLifecycleEvent(phase, sessionId = sessionId, reasonCode = reasonCode))
}
