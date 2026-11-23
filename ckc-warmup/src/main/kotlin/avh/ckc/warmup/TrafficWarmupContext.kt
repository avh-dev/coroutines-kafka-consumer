package avh.ckc.warmup

import avh.ckc.core.extension.ManagedConsumerLifecycle
import avh.ckc.core.isFatalJvmError
import avh.ckc.warmup.protocol.ControlRecordCompleted
import avh.ckc.warmup.protocol.RecipientReady
import avh.ckc.warmup.protocol.SessionAccepted
import avh.ckc.warmup.protocol.WarmupControlMessageHandler
import avh.ckc.warmup.protocol.WarmupControlNamespace
import avh.ckc.warmup.protocol.WarmupControlRouter
import avh.ckc.warmup.protocol.WarmupInstanceId
import avh.ckc.warmup.protocol.WarmupParticipantId
import avh.ckc.warmup.protocol.WarmupRequested
import avh.ckc.warmup.protocol.WarmupSessionId
import avh.ckc.warmup.protocol.WarmupTopicMapping
import avh.ckc.warmup.protocol.normalizedAdmissionBudget
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.TimeSource
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory

/**
 * Registers typed participants and owns only the optional protocol lifecycle.
 *
 * Each consumer retains its own main-runtime lifecycle. The context temporarily coordinates a
 * single all-or-cold recipient attempt and remains available afterward only to serve donor sessions.
 */
class TrafficWarmupContext internal constructor(
    private val configuration: TrafficWarmupContextConfiguration,
    private val controlTransportFactory: (
        WarmupControlTransportConfiguration, WarmupControlMessageHandler, CoroutineScope,
        CoroutineDispatcher
    ) -> WarmupControlChannel = { config, handler, scope, dispatcher ->
        WarmupControlTransport(config, handler, parentContext = scope.coroutineContext, dispatcher = dispatcher)
    }
) {
    /**
     * All mutable fields are protected by the context lock. Runtime ownership begins when
     * startMainRuntime returns, before readiness, so stop can never lose an in-progress startup.
     */
    private class ParticipantState(val participant: RegisteredTrafficWarmupParticipant) {
        val ready = CompletableDeferred<Unit>()
        var startRequested = false
        var stopRequested = false
        var mainLaunchClaimed = false
        var mainRuntimeOwned = false
        var mainReady = false
        var mainLaunchFinished: CompletableDeferred<Unit>? = null
        var terminationMonitorStarted = false
        var stopAttempt: CompletableDeferred<Unit>? = null
        // A terminal consumer error may be reported after both protocol and runtime resources are released.
        var stopCompleted = false
    }

    private data class RecipientInterruption(
        val sessionId: String,
        val reasonCode: String,
        val detectedAt: TimeSource.Monotonic.ValueTimeMark = TimeSource.Monotonic.markNow()
    )

    private val log = LoggerFactory.getLogger(javaClass)
    private val lock = Any()
    private val instanceId = WarmupInstanceId(configuration.instanceId)
    private val namespace = WarmupControlNamespace(configuration.namespace)
    internal val inflightDelegations = Semaphore(configuration.maxInflightDelegations.also {
        require(it > 0) { "maxInflightDelegations must be positive" }
    })
    private val execution = WarmupExecutionDispatchers(
        configuration.instanceId,
        configuration.serviceThreadLimit,
        configuration.serviceThreadIdleTimeout
    )
    private val lifecycleJob = SupervisorJob(configuration.parentContext[Job])
    private val scope = CoroutineScope(
        configuration.parentContext.minusKey(Job) + execution.coordination + lifecycleJob +
            CoroutineName("TrafficWarmupContext")
    )
    private val startSignal = Channel<Unit>(Channel.CONFLATED)
    private val participants = linkedMapOf<WarmupParticipantId, ParticipantState>()
    private var registrationFrozen = false
    private var recipientFinished = false
    private var recipientCleanupFailure: Throwable? = null
    private var sharedClosed = false
    private var startupJob: Job? = null
    private var activeRecipient: WarmupRecipientDiscovery? = null
    private var activeOutbox: WarmupCompletionOutbox? = null
    private var warmingStarted: TimeSource.Monotonic.ValueTimeMark? = null
    private var interruption: RecipientInterruption? = null
    private var activeRecipientCleanup: RetriableWarmupRecipientCleanup? = null

    private lateinit var transport: WarmupControlChannel
    private lateinit var router: WarmupControlRouter
    private lateinit var donorStore: LogicalWarmupDonorSessionStore
    private lateinit var donorActivation: WarmupDonorActivation<LogicalWarmupDonorSession>
    private lateinit var runtime: WarmupDiscoveryRuntime<LogicalWarmupDonorSession>

    init {
        require(configuration.controlTopic.isNotBlank()) { "Warm-up control topic must not be blank" }
        listOf(
            "warmupDuration" to configuration.warmupDuration,
            "offerWindow" to configuration.offerWindow,
            "participantStartTimeout" to configuration.participantStartTimeout,
            "preparationTimeout" to configuration.preparationTimeout,
            "completionTimeout" to configuration.completionTimeout,
            "delegationTimeout" to configuration.delegationTimeout
        ).forEach { (name, value) -> require(value.isPositive()) { "$name must be positive" } }
        require(configuration.donorLivenessTimeout.isFinite() &&
            configuration.donorLivenessTimeout.inWholeMilliseconds >= 3
        ) { "donorLivenessTimeout must be finite and at least 3 milliseconds" }
        require(configuration.maxDonorSessions > 0) { "Maximum donor sessions must be positive" }
        require(configuration.maxAggregateQuotaPerSecond > 0) {
            "Maximum aggregate quota per second must be positive"
        }
    }

    internal fun delegationTimeoutMillis(): Long = configuration.delegationTimeout.inWholeMilliseconds
    internal fun recipientShutdownTimeoutMillis(): Long =
        (configuration.completionTimeout.inWholeMilliseconds / 2).coerceAtLeast(1)
    internal fun serviceDispatcher() = execution.blocking

    internal fun register(participant: RegisteredTrafficWarmupParticipant): ManagedConsumerLifecycle {
        val state = synchronized(lock) {
            check(!registrationFrozen) { "Warm-up participants must be registered before the first start" }
            check(participants.putIfAbsent(participant.participantId, ParticipantState(participant)) == null) {
                "Warm-up participant ${participant.participantId.value} is already registered"
            }
            participants.getValue(participant.participantId)
        }
        return object : ManagedConsumerLifecycle {
            override fun start() = requestStart(state)
            override suspend fun awaitReady() = state.ready.await()
            override suspend fun stop() = requestStop(state)
            override suspend fun awaitTermination() {
                state.ready.await()
                if (synchronized(lock) { state.mainRuntimeOwned }) participant.awaitMainTermination()
            }
            override fun isStopComplete(): Boolean = synchronized(lock) { state.stopCompleted }
        }
    }

    private fun requestStart(state: ParticipantState) {
        var workflowToStart: Job? = null
        var launchCold = false
        var terminalFailure: Throwable? = null
        var initializationFailure: Throwable? = null
        synchronized(lock) {
            check(!state.startRequested) { "Consumer already started" }
            check(!state.stopRequested) { "Stopped consumer cannot be started" }
            state.startRequested = true
            if (!registrationFrozen) {
                registrationFrozen = true
                initializationFailure = try {
                    initializeProtocolLocked()
                    null
                } catch (error: Throwable) {
                    if (error.isFatalJvmError()) throw error
                    log.warn("Traffic warm-up protocol initialization failed; using cold start", error)
                    error
                }
                workflowToStart = scope.launch(start = CoroutineStart.LAZY) {
                    val failure = initializationFailure
                    if (failure == null) runInitialRecipientAttempt()
                    else runProtocolInitializationFallback()
                }
                startupJob = workflowToStart
            } else if (recipientFinished) {
                terminalFailure = recipientCleanupFailure
                launchCold = terminalFailure == null
            }
        }
        observe(TrafficWarmupLifecyclePhase.WAITING_FOR_PARTICIPANTS, state.participant.participantId)
        startSignal.trySend(Unit)
        workflowToStart?.start()
        terminalFailure?.let { state.ready.completeExceptionally(it) }
        if (launchCold) scope.launch { startMain(state) }
    }

    private suspend fun requestStop(state: ParticipantState) {
        val attempt: CompletableDeferred<Unit>
        val ownsAttempt: Boolean
        val workflow: Job?
        val launchFinished: CompletableDeferred<Unit>?
        synchronized(lock) {
            state.stopAttempt?.let {
                attempt = it
                ownsAttempt = false
                workflow = null
                launchFinished = null
                return@synchronized
            }
            attempt = CompletableDeferred()
            state.stopAttempt = attempt
            ownsAttempt = true
            state.stopRequested = true
            workflow = startupJob?.takeIf { !recipientFinished && !it.isCompleted }
            launchFinished = state.mainLaunchFinished
            if (!state.ready.isCompleted) {
                state.ready.completeExceptionally(CancellationException("Consumer stopped before main readiness"))
            }
        }
        if (!ownsAttempt) {
            attempt.await()
            return
        }
        var failure: Throwable? = null
        var cleanupIncomplete = false
        fun recordFailure(error: Throwable, incomplete: Boolean = true) {
            cleanupIncomplete = cleanupIncomplete || incomplete
            val current = failure
            if (current == null) failure = error else current.addSuppressed(error)
        }
        try {
            workflow?.cancel(CancellationException("Warm-up participant stopped"))
            workflow?.join()
            launchFinished?.await()
        } catch (error: Throwable) {
            recordFailure(error)
        }
        try {
            if (::donorActivation.isInitialized) {
                donorActivation.onParticipantUnavailable(state.participant.participantId)
            }
        } catch (error: Throwable) {
            recordFailure(error)
        }
        try {
            retryIncompleteRecipientCleanup()
        } catch (error: Throwable) {
            recordFailure(error)
        }
        if (!state.participant.mainConsumer.runtimeControl.isShutdownComplete()) {
            try {
                state.participant.stopMain()
                synchronized(lock) {
                    state.mainRuntimeOwned = false
                    state.mainReady = false
                }
            } catch (error: Throwable) {
                val runtimeReleased = state.participant.mainConsumer.runtimeControl.isShutdownComplete()
                if (runtimeReleased) {
                    synchronized(lock) {
                        state.mainRuntimeOwned = false
                        state.mainReady = false
                    }
                }
                recordFailure(error, incomplete = !runtimeReleased)
            }
        } else {
            synchronized(lock) {
                state.mainRuntimeOwned = false
                state.mainReady = false
            }
        }
        try {
            closeSharedResourcesIfAllStopped()
        } catch (error: Throwable) {
            recordFailure(error)
        }

        val complete = !cleanupIncomplete && state.participant.mainConsumer.runtimeControl.isShutdownComplete()
        synchronized(lock) {
            state.stopCompleted = complete
            if (!complete && state.stopAttempt === attempt) state.stopAttempt = null
        }
        if (complete) {
            observe(TrafficWarmupLifecyclePhase.PARTICIPANT_STOPPED, state.participant.participantId)
        }
        failure?.let { error ->
            attempt.completeExceptionally(error)
            throw error
        }
        if (complete) {
            attempt.complete(Unit)
        } else {
            val error = IllegalStateException(
                "Warm-up participant ${state.participant.participantId.value} cleanup is incomplete"
            )
            attempt.completeExceptionally(error)
            throw error
        }
    }

    private fun initializeProtocolLocked() {
        check(participants.isNotEmpty()) { "Warm-up context has no registered participants" }
        val registered = participants.values.map(ParticipantState::participant)
        val mappings = registered.flatMap(RegisteredTrafficWarmupParticipant::mappings)
        require(mappings.map { it.warmupTopic }.distinct().size == mappings.size) {
            "Warm-up destination topics must be unique within the context namespace"
        }
        val newRouter = WarmupControlRouter(instanceId, namespace)
        val newTransport = controlTransportFactory(
            WarmupControlTransportConfiguration(
                instanceId = instanceId,
                namespace = namespace,
                topic = configuration.controlTopic,
                producerProperties = configuration.controlProducerProperties,
                consumerProperties = configuration.controlConsumerProperties,
                sendTimeout = configuration.preparationTimeout,
                metadataTimeout = configuration.preparationTimeout
            ),
            newRouter,
            scope,
            execution.blocking
        )
        val newDonorStore = LogicalWarmupDonorSessionStore(
            configuration.maxDonorSessions,
            configuration.maxAggregateQuotaPerSecond
        )
        val eligibility = MainConsumerWarmupDonorEligibility(
            registered.associate { it.participantId to it.mainConsumer }
        )
        val newDonorActivation = WarmupDonorActivation(
            localInstanceId = instanceId,
            namespace = namespace,
            supportedMappings = mappings,
            dataPlane = newDonorStore,
            eligibility = eligibility,
            preparation = WarmupDonorSessionPreparation(::prepareLogicalDonorSession),
            scope = scope,
            cleanupDispatcher = execution.blocking,
            onPreparationFailed = { newTransport.send(it) },
            onDonorClosed = { newTransport.send(it) },
            drainTimeout = configuration.completionTimeout
        )
        val newRuntime = WarmupDiscoveryRuntime(
            instanceId,
            namespace,
            newRouter,
            newTransport,
            StatelessWarmupDonorDiscovery(instanceId, namespace, mappings, eligibility),
            newDonorActivation,
            configuration.offerWindow,
            configuration.preparationTimeout,
            configuration.completionTimeout,
            configuration.donorLivenessTimeout,
            scope
        )
        router = newRouter
        transport = newTransport
        donorStore = newDonorStore
        donorActivation = newDonorActivation
        runtime = newRuntime
    }

    private suspend fun runProtocolInitializationFallback() {
        try {
            val allStarted = withTimeoutOrNull(configuration.participantStartTimeout) {
                while (!allParticipantsStarted()) startSignal.receive()
                true
            } == true
            completeRecipientPhaseAndLaunchMains(
                if (allStarted) "protocol_initialization_failed" else "participant_start_timeout"
            )
        } catch (cancelled: CancellationException) {
            withContext(NonCancellable) {
                completeRecipientPhaseAndLaunchMains("participant_stopped")
            }
        }
    }

    private suspend fun prepareLogicalDonorSession(acceptance: SessionAccepted): LogicalWarmupDonorSession {
        val prepared = mutableListOf<WarmupDonorSessionLifecycle>()
        try {
            acceptance.mappings.groupBy { it.mapping.participantId }.forEach { (participantId, _) ->
                val participant = synchronized(lock) { participants[participantId]?.participant }
                    ?: error("Unknown warm-up participant ${participantId.value}")
                prepared += participant.prepareDonorLane(acceptance)
            }
            return LogicalWarmupDonorSession(
                acceptance.sessionId,
                acceptance.admissionBudgetPerSecond,
                prepared
            )
        } catch (error: Throwable) {
            withContext(execution.blocking) {
                prepared.asReversed().forEach { lane ->
                    lane.stopDelegating()
                    if (lane.trySealForRemoval()) runCatching(lane::close)
                }
            }
            throw error
        }
    }

    private suspend fun runInitialRecipientAttempt() {
        var controlReady = false
        try {
            controlReady = runtime.startControl()
            if (!controlReady) {
                observe(TrafficWarmupLifecyclePhase.CONTROL_UNAVAILABLE, reasonCode = "control_unavailable")
                completeRecipientPhaseAndLaunchMains("control_unavailable")
                return
            }
            val allStarted = withTimeoutOrNull(configuration.participantStartTimeout) {
                while (!allParticipantsStarted()) startSignal.receive()
                true
            } == true
            if (!allStarted) {
                completeRecipientPhaseAndLaunchMains("participant_start_timeout")
                return
            }
            runDiscoveryAndWarmup()
        } catch (_: TimeoutCancellationException) {
            withContext(NonCancellable) { finishInterruptedAttempt("recipient_cleanup_timeout") }
        } catch (cancelled: CancellationException) {
            withContext(NonCancellable) {
                finishInterruptedAttempt("participant_stopped")
            }
        } catch (error: Throwable) {
            log.warn("Warm-up recipient attempt failed; using cold start", error)
            withContext(NonCancellable) { finishInterruptedAttempt("recipient_failure") }
        }
    }

    private suspend fun runDiscoveryAndWarmup() {
        val registered = synchronized(lock) { participants.values.map(ParticipantState::participant) }
        val mappings = registered.flatMap(RegisteredTrafficWarmupParticipant::mappings)
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
        lateinit var discovery: WarmupRecipientDiscovery
        val outbox = WarmupCompletionOutbox(scope, 1_024) { completed ->
            val donor = requireNotNull(discovery.donorFor(completed.recordId.topicPartition)) {
                "No selected donor owns ${completed.recordId.topicPartition}"
            }
            transport.send(
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
        val participantById = registered.associateBy(RegisteredTrafficWarmupParticipant::participantId)
        discovery = WarmupRecipientDiscovery(
            request,
            KafkaWarmupDestinationMetadata(
                participantById.mapValues { (_, participant) -> participant.warmupConsumerProperties },
                configuration.preparationTimeout,
                execution.blocking
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
            activeRecipientCleanup = RetriableWarmupRecipientCleanup(
                terminate = discovery::terminate,
                afterComplete = { abortRecipientSends(outbox) }
            )
        }
        observe(TrafficWarmupLifecyclePhase.DISCOVERING, sessionId = sessionId.value)
        when (val result = runtime.discoverAndPrepare(discovery)) {
            is WarmupDiscoveryResult.Ready -> {
                observe(TrafficWarmupLifecyclePhase.READY, sessionId = sessionId.value)
                synchronized(lock) { warmingStarted = TimeSource.Monotonic.markNow() }
                observe(TrafficWarmupLifecyclePhase.WARMING, sessionId = sessionId.value)
                runPreparedWarmup(discovery, result.messages, outbox)
            }
            is WarmupDiscoveryResult.ColdFallback -> {
                outbox.stop(configuration.completionTimeout)
                completeRecipientPhaseAndLaunchMains(result.reasonCode)
            }
            is WarmupDiscoveryResult.ColdFallbackWithDiagnostics -> {
                outbox.stop(configuration.completionTimeout)
                completeRecipientPhaseAndLaunchMains(result.reasonCode)
            }
            is WarmupDiscoveryResult.CleanupIncomplete -> failRecipientCleanup(result.reasonCode, result.cleanup)
        }
    }

    private suspend fun runPreparedWarmup(
        discovery: WarmupRecipientDiscovery,
        readiness: List<RecipientReady>,
        outbox: WarmupCompletionOutbox
    ) = coroutineScope {
        val sessionId = discovery.request.sessionId
        val failure = async { awaitWarmupFailure(discovery, readiness, outbox) }
        val earlyFailure = withTimeoutOrNull(configuration.warmupDuration) { failure.await() }
        if (earlyFailure != null) {
            observeInterruption(sessionId, earlyFailure)
            rollbackRecipientAndStartCold(earlyFailure)
            return@coroutineScope
        }

        observe(TrafficWarmupLifecyclePhase.COMPLETING, sessionId = sessionId.value)
        val completion = runtime.completeWarmup(
            discovery,
            failure,
            onFailureDetected = {
                observeInterruption(sessionId, it)
                outbox.close()
            },
            finishSends = { finishRecipientSends(sessionId, outbox, it) }
        )
        applyCompletion(completion)
    }

    private fun applyCompletion(completion: WarmupCompletionResult) {
        when (completion) {
            WarmupCompletionResult.Completed,
            is WarmupCompletionResult.CompletedWithDiagnostics -> completeRecipientPhaseAndLaunchMains(null)
            is WarmupCompletionResult.ColdFallback -> completeRecipientPhaseAndLaunchMains(completion.reasonCode)
            is WarmupCompletionResult.ColdFallbackWithDiagnostics ->
                completeRecipientPhaseAndLaunchMains(completion.reasonCode)
            is WarmupCompletionResult.CleanupIncomplete ->
                failRecipientCleanup(completion.reasonCode, completion.cleanup)
        }
    }

    private suspend fun finishRecipientSends(
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
                runtime.stopRecipientProbes()
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
        discovery: WarmupRecipientDiscovery,
        readiness: List<RecipientReady>,
        outbox: WarmupCompletionOutbox
    ): String = coroutineScope {
        val protocol = async { runtime.awaitWarmupInterruption(discovery, readiness).reasonCode }
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
            interruption = RecipientInterruption(sessionId.value, reasonCode)
        }
        observe(TrafficWarmupLifecyclePhase.WARMUP_INTERRUPTED, sessionId = sessionId.value, reasonCode = reasonCode)
    }

    private suspend fun finishInterruptedAttempt(reasonCode: String) {
        val (recipient, observedInterruption) = synchronized(lock) { activeRecipient to interruption }
        val outcome = recipient?.let { runtime.completedResult(it) }
        if (outcome != null) {
            applyCompletion(outcome)
            return
        }
        val observedReason = observedInterruption
            ?.takeIf { it.sessionId == recipient?.request?.sessionId?.value }
            ?.reasonCode
        rollbackRecipientAndStartCold(recipient?.terminalOutcome()?.reasonCode ?: observedReason ?: reasonCode)
    }

    private suspend fun rollbackRecipientAndStartCold(reasonCode: String) {
        activeOutbox?.close()
        val rollback = activeRecipient?.let { runtime.abortWarmup(it, reasonCode) }
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
        if (unresolved == null) completeRecipientPhaseAndLaunchMains(reasonCode)
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
            if (::runtime.isInitialized) runtime.stopRecipientProbes()
        }
    }

    /**
     * Publishes the recipient terminal state after cleanup and only schedules main runtimes.
     * Their Kafka readiness is participant-local and must never extend another participant's stop.
     */
    private fun completeRecipientPhaseAndLaunchMains(reasonCode: String?) {
        val summary = synchronized(lock) {
            if (recipientFinished) return
            val summary = recipientSummary(reasonCode)
            recipientFinished = true
            recipientCleanupFailure = null
            activeRecipient = null
            activeOutbox = null
            activeRecipientCleanup = null
            summary
        }
        observe(summary)
        if (reasonCode != null) {
            observe(TrafficWarmupLifecyclePhase.COLD_FALLBACK, reasonCode = reasonCode)
        }
        val requested = synchronized(lock) {
            participants.values.filter { it.startRequested && !it.stopRequested }
        }
        requested.forEach { state -> scope.launch { startMain(state) } }
    }

    private fun failRecipientCleanup(
        reasonCode: String,
        cleanup: WarmupRecipientCleanupResult.Incomplete
    ) {
        val failure = IllegalStateException(
            "Warm-up cleanup incomplete: $reasonCode; " + cleanup.diagnostics.joinToString()
        )
        val summary = synchronized(lock) {
            if (recipientFinished) return
            recipientFinished = true
            recipientCleanupFailure = failure
            participants.values.filter { it.startRequested && !it.ready.isCompleted }
                .forEach { it.ready.completeExceptionally(failure) }
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

    private suspend fun retryIncompleteRecipientCleanup() {
        val cleanup = synchronized(lock) { activeRecipientCleanup } ?: return
        cleanup.run()
        synchronized(lock) {
            if (activeRecipientCleanup === cleanup) {
                activeRecipient = null
                activeOutbox = null
                activeRecipientCleanup = null
            }
        }
    }

    private suspend fun startMain(state: ParticipantState) {
        val launchFinished = synchronized(lock) {
            if (state.stopRequested || state.mainLaunchClaimed) null
            else {
                state.mainLaunchClaimed = true
                CompletableDeferred<Unit>().also { state.mainLaunchFinished = it }
            }
        } ?: return
        try {
            state.participant.startMainRuntime()
            synchronized(lock) { state.mainRuntimeOwned = true }
            launchFinished.complete(Unit)
            state.participant.awaitMainReady()
            val publishReady = synchronized(lock) {
                if (state.stopRequested) false
                else {
                    state.mainReady = true
                    state.ready.complete(Unit)
                }
            }
            if (publishReady) {
                val recovery = synchronized(lock) { interruption }
                observe(
                    TrafficWarmupLifecycleEvent(
                        TrafficWarmupLifecyclePhase.MAIN_READY,
                        participantId = state.participant.participantId.value,
                        sessionId = recovery?.sessionId,
                        reasonCode = recovery?.reasonCode,
                        recoveryDurationMillis = recovery?.detectedAt?.elapsedNow()?.inWholeMilliseconds
                    )
                )
                monitorMainTermination(state)
            }
        } catch (error: Throwable) {
            launchFinished.complete(Unit)
            val stopped = synchronized(lock) { state.stopRequested }
            if (!stopped && synchronized(lock) { state.mainRuntimeOwned }) {
                try {
                    state.participant.stopMain()
                    synchronized(lock) { state.mainRuntimeOwned = false }
                } catch (stopError: Throwable) {
                    error.addSuppressed(stopError)
                }
            }
            state.ready.completeExceptionally(error)
        }
    }

    private fun monitorMainTermination(state: ParticipantState) {
        val launch = synchronized(lock) {
            if (state.terminationMonitorStarted) false
            else {
                state.terminationMonitorStarted = true
                true
            }
        }
        if (!launch) return
        scope.launch {
            try {
                state.participant.awaitMainTermination()
            } catch (error: CancellationException) {
                throw error
            } catch (error: Exception) {
                log.warn(
                    "Warm-up participant {} main runtime terminated with failure",
                    state.participant.participantId.value,
                    error
                )
            } finally {
                val unexpectedlyUnavailable = synchronized(lock) { !state.stopRequested }
                if (unexpectedlyUnavailable && ::donorActivation.isInitialized) {
                    donorActivation.onParticipantUnavailable(state.participant.participantId)
                }
            }
        }
    }

    private fun allParticipantsStarted(): Boolean = synchronized(lock) {
        participants.values.all { it.startRequested && !it.stopRequested }
    }

    private suspend fun closeSharedResourcesIfAllStopped() {
        val close = synchronized(lock) {
            if (sharedClosed || participants.values.any { !it.stopRequested }) false
            else {
                sharedClosed = true
                true
            }
        }
        if (!close) return
        val donorsDrained = !::donorStore.isInitialized || withTimeoutOrNull(configuration.completionTimeout) {
            while (donorStore.admittedSessionCount() != 0) delay(10)
            true
        } == true
        if (!donorsDrained) {
            synchronized(lock) { sharedClosed = false }
            error("Warm-up donor sessions did not drain before context shutdown")
        }
        val donorCleanupComplete = !::donorActivation.isInitialized ||
            donorActivation.awaitCleanupBarrier(configuration.completionTimeout)
        if (!donorCleanupComplete) {
            synchronized(lock) { sharedClosed = false }
            error("Warm-up donor resource cleanup did not complete before context shutdown")
        }
        try {
            if (::runtime.isInitialized) {
                runtime.stop()
                runtime.close()
            }
            lifecycleJob.cancelAndJoin()
            check(execution.close(configuration.completionTimeout)) {
                "Warm-up service executors did not terminate before the context shutdown deadline"
            }
        } catch (error: Throwable) {
            synchronized(lock) { sharedClosed = false }
            throw error
        }
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
        participantId: WarmupParticipantId? = null,
        sessionId: String? = null,
        reasonCode: String? = null
    ) = observe(TrafficWarmupLifecycleEvent(phase, participantId?.value, sessionId, reasonCode))

    private fun observe(event: TrafficWarmupLifecycleEvent) {
        try {
            configuration.lifecycleObserver(event)
        } catch (error: Exception) {
            log.warn("Traffic warm-up lifecycle observer failed", error)
        }
    }
}
