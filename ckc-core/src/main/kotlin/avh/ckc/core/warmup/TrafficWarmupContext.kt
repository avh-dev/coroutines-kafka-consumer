package avh.ckc.core.warmup

import avh.ckc.core.ManagedConsumerLifecycle
import avh.ckc.core.isFatalJvmError
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import java.util.UUID
import kotlin.time.Duration

/**
 * Registers typed participants and owns only the optional protocol lifecycle.
 *
 * Each consumer retains its own main-runtime lifecycle. The context temporarily coordinates a
 * single all-or-cold recipient attempt and remains available afterward only to serve donor sessions.
 */
class TrafficWarmupContext internal constructor(
    private val configuration: TrafficWarmupContextConfiguration
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

    private val log = LoggerFactory.getLogger(javaClass)
    private val lock = Any()
    private val instanceId = WarmupInstanceId(configuration.instanceId)
    private val namespace = WarmupControlNamespace(configuration.namespace)
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
    private var activeRecipientCleanup: RetriableWarmupRecipientCleanup? = null

    private lateinit var transport: WarmupControlTransport
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
        if (!state.participant.mainConsumer.isShutdownComplete()) {
            try {
                state.participant.stopMain()
                synchronized(lock) {
                    state.mainRuntimeOwned = false
                    state.mainReady = false
                }
            } catch (error: Throwable) {
                val runtimeReleased = state.participant.mainConsumer.isShutdownComplete()
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

        val complete = !cleanupIncomplete && state.participant.mainConsumer.isShutdownComplete()
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
        val newTransport = WarmupControlTransport(
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
            parentContext = scope.coroutineContext,
            dispatcher = execution.blocking
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
            configuration.completionTimeout
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
        } catch (cancelled: CancellationException) {
            withContext(NonCancellable) {
                rollbackRecipientAndStartCold("participant_stopped")
            }
        } catch (error: Throwable) {
            log.warn("Warm-up recipient attempt failed; using cold start", error)
            withContext(NonCancellable) { rollbackRecipientAndStartCold("recipient_failure") }
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
                afterComplete = { outbox.stop(configuration.completionTimeout) }
            )
        }
        observe(TrafficWarmupLifecyclePhase.DISCOVERING, sessionId = sessionId.value)
        when (val result = runtime.discoverAndPrepare(discovery)) {
            is WarmupDiscoveryResult.Ready -> {
                observe(TrafficWarmupLifecyclePhase.READY, sessionId = sessionId.value)
                observe(TrafficWarmupLifecyclePhase.WARMING, sessionId = sessionId.value)
                when (val outcome = awaitWarmupOutcome(discovery, outbox)) {
                    WarmupRunOutcome.DurationElapsed -> Unit
                    WarmupRunOutcome.DonorSessionEnded -> {
                        rollbackRecipientAndStartCold("donor_session_ended")
                        return
                    }
                    is WarmupRunOutcome.ResultDeliveryFailed -> {
                        log.warn("Warm-up result delivery failed; ending recipient session", outcome.error)
                        rollbackRecipientAndStartCold("result_delivery_failed")
                        return
                    }
                }
                observe(TrafficWarmupLifecyclePhase.COMPLETING, sessionId = sessionId.value)
                when (val completion = runtime.completeWarmup(discovery)) {
                    WarmupCompletionResult.Completed,
                    is WarmupCompletionResult.CompletedWithDiagnostics,
                    is WarmupCompletionResult.ColdFallback,
                    is WarmupCompletionResult.ColdFallbackWithDiagnostics -> {
                        outbox.stop(configuration.completionTimeout)
                        val reasonCode = when (completion) {
                            is WarmupCompletionResult.ColdFallback -> completion.reasonCode
                            is WarmupCompletionResult.ColdFallbackWithDiagnostics -> completion.reasonCode
                            else -> null
                        }
                        completeRecipientPhaseAndLaunchMains(reasonCode)
                    }
                    is WarmupCompletionResult.CleanupIncomplete ->
                        failRecipientCleanup(completion.reasonCode, completion.cleanup)
                }
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

    private suspend fun awaitWarmupOutcome(
        discovery: WarmupRecipientDiscovery,
        outbox: WarmupCompletionOutbox
    ): WarmupRunOutcome = coroutineScope {
        val protocol = async {
            if (runtime.awaitWarmupInterruption(discovery, configuration.warmupDuration)) {
                WarmupRunOutcome.DonorSessionEnded
            } else {
                WarmupRunOutcome.DurationElapsed
            }
        }
        val delivery = async {
            WarmupRunOutcome.ResultDeliveryFailed(outbox.awaitFailure())
        }
        try {
            select {
                protocol.onAwait { it }
                delivery.onAwait { it }
            }
        } finally {
            protocol.cancelAndJoin()
            delivery.cancelAndJoin()
        }
    }

    private sealed interface WarmupRunOutcome {
        data object DurationElapsed : WarmupRunOutcome
        data object DonorSessionEnded : WarmupRunOutcome
        data class ResultDeliveryFailed(val error: Throwable) : WarmupRunOutcome
    }

    private suspend fun rollbackRecipientAndStartCold(reasonCode: String) {
        val rollback = activeRecipient?.let { runtime.abortWarmup(it, reasonCode) }
        activeOutbox?.let { outbox ->
            try {
                outbox.stop(configuration.completionTimeout)
            } catch (error: TimeoutCancellationException) {
                failRecipientCleanup(
                    reasonCode,
                    WarmupRecipientCleanupResult.Incomplete(
                        pendingAssignments = emptyList(),
                        diagnostics = listOf("result delivery cleanup timed out")
                    )
                )
                return
            } catch (error: Exception) {
                log.warn("Warm-up completion outbox failed during recipient rollback", error)
            }
        }
        if (rollback is WarmupDiscoveryResult.CleanupIncomplete) {
            failRecipientCleanup(rollback.reasonCode, rollback.cleanup)
        } else {
            completeRecipientPhaseAndLaunchMains(reasonCode)
        }
    }

    /**
     * Publishes the recipient terminal state after cleanup and only schedules main runtimes.
     * Their Kafka readiness is participant-local and must never extend another participant's stop.
     */
    private fun completeRecipientPhaseAndLaunchMains(reasonCode: String?) {
        synchronized(lock) {
            recipientFinished = true
            recipientCleanupFailure = null
            activeRecipient = null
            activeOutbox = null
            activeRecipientCleanup = null
        }
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
        synchronized(lock) {
            recipientFinished = true
            recipientCleanupFailure = failure
            participants.values.filter { it.startRequested && !it.ready.isCompleted }
                .forEach { it.ready.completeExceptionally(failure) }
        }
    }

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
                observe(TrafficWarmupLifecyclePhase.MAIN_READY, state.participant.participantId)
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
    ) {
        try {
            configuration.lifecycleObserver(
                TrafficWarmupLifecycleEvent(phase, participantId?.value, sessionId, reasonCode)
            )
        } catch (error: Exception) {
            log.warn("Traffic warm-up lifecycle observer failed", error)
        }
    }
}
