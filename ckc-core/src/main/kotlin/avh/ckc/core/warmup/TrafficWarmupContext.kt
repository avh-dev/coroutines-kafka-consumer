package avh.ckc.core.warmup

import avh.ckc.core.ManagedConsumerLifecycle
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
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
    private class ParticipantState(val participant: RegisteredTrafficWarmupParticipant) {
        val ready = CompletableDeferred<Unit>()
        var startRequested = false
        var stopRequested = false
        var mainStarting = false
        var mainStarted = false
        var terminationMonitorStarted = false
        var stopAttempt: CompletableDeferred<Unit>? = null
    }

    private val log = LoggerFactory.getLogger(javaClass)
    private val lock = Any()
    private val instanceId = WarmupInstanceId(configuration.instanceId)
    private val namespace = WarmupControlNamespace(configuration.namespace)
    private val lifecycleJob = SupervisorJob(configuration.parentContext[Job])
    private val scope = CoroutineScope(
        configuration.parentContext.minusKey(Job) + lifecycleJob + CoroutineName("TrafficWarmupContext")
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
                if (synchronized(lock) { state.mainStarted }) participant.awaitMainTermination()
            }
        }
    }

    private fun requestStart(state: ParticipantState) {
        var workflowToStart: Job? = null
        var launchCold = false
        var terminalFailure: Throwable? = null
        synchronized(lock) {
            check(!state.startRequested) { "Consumer already started" }
            check(!state.stopRequested) { "Stopped consumer cannot be started" }
            state.startRequested = true
            if (!registrationFrozen) {
                registrationFrozen = true
                initializeProtocolLocked()
                workflowToStart = scope.launch(start = CoroutineStart.LAZY) { runInitialRecipientAttempt() }
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
        val stopMain: Boolean
        synchronized(lock) {
            state.stopAttempt?.let {
                attempt = it
                ownsAttempt = false
                workflow = null
                stopMain = false
                return@synchronized
            }
            attempt = CompletableDeferred()
            state.stopAttempt = attempt
            ownsAttempt = true
            state.stopRequested = true
            workflow = startupJob?.takeIf { !recipientFinished && !it.isCompleted }
            stopMain = state.mainStarted
            if (!state.ready.isCompleted && !stopMain) {
                state.ready.completeExceptionally(CancellationException("Consumer stopped before main readiness"))
            }
        }
        if (!ownsAttempt) {
            attempt.await()
            return
        }
        try {
            workflow?.cancel(CancellationException("Warm-up participant stopped"))
            workflow?.join()
            if (::donorActivation.isInitialized) {
                donorActivation.onParticipantUnavailable(state.participant.participantId)
            }
            retryIncompleteRecipientCleanup()
            if (stopMain || synchronized(lock) { state.mainStarted }) {
                state.participant.stopMain()
                synchronized(lock) { state.mainStarted = false }
            }
            observe(TrafficWarmupLifecyclePhase.PARTICIPANT_STOPPED, state.participant.participantId)
            closeSharedResourcesIfAllStopped()
            attempt.complete(Unit)
        } catch (error: Throwable) {
            synchronized(lock) {
                if (state.stopAttempt === attempt) state.stopAttempt = null
            }
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
        router = WarmupControlRouter(instanceId, namespace)
        transport = WarmupControlTransport(
            WarmupControlTransportConfiguration(
                instanceId = instanceId,
                namespace = namespace,
                topic = configuration.controlTopic,
                producerProperties = configuration.controlProducerProperties,
                consumerProperties = configuration.controlConsumerProperties,
                sendTimeout = configuration.preparationTimeout,
                metadataTimeout = configuration.preparationTimeout
            ),
            router,
            parentContext = scope.coroutineContext
        )
        donorStore = LogicalWarmupDonorSessionStore(
            configuration.maxDonorSessions,
            configuration.maxAggregateQuotaPerSecond
        )
        val eligibility = MainConsumerWarmupDonorEligibility(
            registered.associate { it.participantId to it.mainConsumer }
        )
        donorActivation = WarmupDonorActivation(
            localInstanceId = instanceId,
            namespace = namespace,
            supportedMappings = mappings,
            dataPlane = donorStore,
            eligibility = eligibility,
            preparation = WarmupDonorSessionPreparation(::prepareLogicalDonorSession),
            scope = scope,
            onPreparationFailed = { transport.send(it) },
            onDonorClosed = { transport.send(it) },
            drainTimeout = configuration.completionTimeout
        )
        runtime = WarmupDiscoveryRuntime(
            instanceId,
            namespace,
            router,
            transport,
            StatelessWarmupDonorDiscovery(instanceId, namespace, mappings, eligibility),
            donorActivation,
            configuration.offerWindow,
            configuration.preparationTimeout,
            configuration.completionTimeout
        )
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
            prepared.asReversed().forEach { lane ->
                lane.stopDelegating()
                if (lane.trySealForRemoval()) runCatching(lane::close)
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
                finishRecipientAndStartCold("control_unavailable")
                return
            }
            val allStarted = withTimeoutOrNull(configuration.participantStartTimeout) {
                while (!allParticipantsStarted()) startSignal.receive()
                true
            } == true
            if (!allStarted) {
                finishRecipientAndStartCold("participant_start_timeout")
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
                configuration.preparationTimeout
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
        }
        observe(TrafficWarmupLifecyclePhase.DISCOVERING, sessionId = sessionId.value)
        when (val result = runtime.discoverAndPrepare(discovery)) {
            is WarmupDiscoveryResult.Ready -> {
                observe(TrafficWarmupLifecyclePhase.READY, sessionId = sessionId.value)
                observe(TrafficWarmupLifecyclePhase.WARMING, sessionId = sessionId.value)
                if (runtime.awaitWarmupInterruption(discovery, configuration.warmupDuration)) {
                    rollbackRecipientAndStartCold("donor_session_ended")
                    return
                }
                observe(TrafficWarmupLifecyclePhase.COMPLETING, sessionId = sessionId.value)
                when (val completion = runtime.completeWarmup(discovery)) {
                    WarmupCompletionResult.Completed,
                    is WarmupCompletionResult.CompletedWithDiagnostics,
                    is WarmupCompletionResult.ColdFallback,
                    is WarmupCompletionResult.ColdFallbackWithDiagnostics -> {
                        outbox.stop()
                        val reasonCode = when (completion) {
                            is WarmupCompletionResult.ColdFallback -> completion.reasonCode
                            is WarmupCompletionResult.ColdFallbackWithDiagnostics -> completion.reasonCode
                            else -> null
                        }
                        finishRecipientAndStartCold(reasonCode)
                    }
                    is WarmupCompletionResult.CleanupIncomplete -> failRecipientCleanup(completion.reasonCode)
                }
            }
            is WarmupDiscoveryResult.ColdFallback -> {
                outbox.stop()
                finishRecipientAndStartCold(result.reasonCode)
            }
            is WarmupDiscoveryResult.ColdFallbackWithDiagnostics -> {
                outbox.stop()
                finishRecipientAndStartCold(result.reasonCode)
            }
            is WarmupDiscoveryResult.CleanupIncomplete -> failRecipientCleanup(result.reasonCode)
        }
    }

    private suspend fun rollbackRecipientAndStartCold(reasonCode: String) {
        val rollback = activeRecipient?.let { runtime.abortWarmup(it, reasonCode) }
        activeOutbox?.let { outbox ->
            try {
                outbox.stop()
            } catch (error: Exception) {
                log.warn("Warm-up completion outbox failed during recipient rollback", error)
            }
        }
        if (rollback is WarmupDiscoveryResult.CleanupIncomplete) {
            failRecipientCleanup(rollback.reasonCode)
        } else {
            finishRecipientAndStartCold(reasonCode)
        }
    }

    private suspend fun finishRecipientAndStartCold(reasonCode: String?) {
        synchronized(lock) {
            recipientFinished = true
            recipientCleanupFailure = null
            activeRecipient = null
            activeOutbox = null
        }
        if (reasonCode != null) {
            observe(TrafficWarmupLifecyclePhase.COLD_FALLBACK, reasonCode = reasonCode)
        }
        val requested = synchronized(lock) {
            participants.values.filter { it.startRequested && !it.stopRequested }
        }
        supervisorScope { requested.map { async { startMain(it) } }.awaitAll() }
    }

    private fun failRecipientCleanup(reasonCode: String) {
        val failure = IllegalStateException("Warm-up cleanup incomplete: $reasonCode")
        synchronized(lock) {
            recipientFinished = true
            recipientCleanupFailure = failure
            participants.values.filter { it.startRequested && !it.ready.isCompleted }
                .forEach { it.ready.completeExceptionally(failure) }
        }
    }

    private suspend fun retryIncompleteRecipientCleanup() {
        val recipient = synchronized(lock) { activeRecipient } ?: return
        when (recipient.terminate()) {
            WarmupRecipientCleanupResult.Complete,
            is WarmupRecipientCleanupResult.CompleteWithDiagnostics -> {
                activeOutbox?.stop()
                synchronized(lock) {
                    if (activeRecipient === recipient) activeRecipient = null
                    activeOutbox = null
                }
            }
            is WarmupRecipientCleanupResult.Incomplete -> Unit
        }
    }

    private suspend fun startMain(state: ParticipantState) {
        val shouldStart = synchronized(lock) {
            if (state.stopRequested || state.mainStarted || state.mainStarting) false
            else {
                state.mainStarting = true
                true
            }
        }
        if (!shouldStart) return
        try {
            state.participant.startMain()
            val stopped = synchronized(lock) {
                state.mainStarting = false
                state.mainStarted = true
                state.stopRequested
            }
            if (stopped) {
                state.participant.stopMain()
                synchronized(lock) { state.mainStarted = false }
            } else {
                state.ready.complete(Unit)
                observe(TrafficWarmupLifecyclePhase.MAIN_READY, state.participant.participantId)
                monitorMainTermination(state)
            }
        } catch (error: Throwable) {
            synchronized(lock) { state.mainStarting = false }
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
            if (!registrationFrozen || sharedClosed || participants.values.any { !it.stopRequested }) false
            else {
                sharedClosed = true
                true
            }
        }
        if (!close) return
        val donorsDrained = withTimeoutOrNull(configuration.completionTimeout) {
            while (donorStore.admittedSessionCount() != 0) delay(10)
            true
        } == true
        if (!donorsDrained) {
            synchronized(lock) { sharedClosed = false }
            error("Warm-up donor sessions did not drain before context shutdown")
        }
        try {
            runtime.stop()
            runtime.close()
            scope.cancel()
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
