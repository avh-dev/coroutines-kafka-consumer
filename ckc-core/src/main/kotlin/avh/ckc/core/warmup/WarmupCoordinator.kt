package avh.ckc.core.warmup

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.withTimeout
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * MVP scope configuration. If every replica restarts and no active replica can answer a state
 * snapshot, a non-empty journal is not replayed: the pod cold-starts and stays election-ineligible.
 */
class WarmupCoordinatorConfiguration(
    val instanceId: WarmupInstanceId,
    warmupPartitions: Collection<Int> = listOf(0),
    val donorSearchTimeout: Duration = 5.seconds,
    val stageTimeout: Duration = 30.seconds,
    val heartbeatInterval: Duration = 1.seconds,
    val electionTimeout: Duration = 5.seconds,
    val donorLoadScore: () -> Double? = { null },
    internal val controlPlaneLimits: WarmupControlPlaneLimits = WarmupControlPlaneLimits()
) {
    val warmupPartitions = immutableSetSnapshot(warmupPartitions)
    init {
        require(this.warmupPartitions.isNotEmpty() && this.warmupPartitions.all { it >= 0 })
        require(donorSearchTimeout.isPositive())
        require(stageTimeout.isPositive())
        require(heartbeatInterval.isPositive())
        require(electionTimeout > heartbeatInterval)
    }
}

enum class WarmupStartupResult { WARMED, NO_DONOR_FALLBACK }

enum class WarmupCoordinatorState {
    NEW, CONTROL_STARTING, SYNCHRONIZING, SEEKING_DONOR, RECIPIENT_STARTING,
    RECIPIENT_WARMING, RECIPIENT_DRAINING, FALLBACK_STARTING, MAIN,
    DONOR_RESERVED, DONOR_FORWARDING, DONOR_STOPPING, FAILED, STOPPED
}

/** Public facade: replicated scheduling and local data-plane execution remain separate internally. */
class WarmupCoordinator internal constructor(
    private val configuration: WarmupCoordinatorConfiguration,
    parentContext: CoroutineContext,
    channelFactory: (WarmupControlMessageHandler) -> WarmupControlChannel
) {
    constructor(
        configuration: WarmupCoordinatorConfiguration,
        controlTransportConfiguration: WarmupControlTransportConfiguration,
        parentContext: CoroutineContext = Dispatchers.Default
    ) : this(configuration, parentContext, { handler ->
        WarmupControlTransport(controlTransportConfiguration, handler, parentContext = parentContext)
    }) {
        require(configuration.instanceId == controlTransportConfiguration.instanceId)
    }

    private val scope = CoroutineScope(parentContext + SupervisorJob(parentContext[Job]))
    private val records = Channel<WarmupControlRecord>(configuration.controlPlaneLimits.maximumBufferedRecords)
    private val channel = channelFactory(WarmupControlMessageHandler { records.send(it) })
    private val participants = LinkedHashMap<WarmupConsumerId, CoordinatorParticipant>()
    private val started = AtomicBoolean(false)
    private val fallbackStarted = AtomicBoolean(false)
    private val failureStarted = AtomicBoolean(false)
    private val stateLock = Any()
    private val startup = CompletableDeferred<WarmupStartupResult>()
    private val termination = CompletableDeferred<Throwable?>()
    private val synchronized = CompletableDeferred<Unit>()
    private val localReady = AtomicBoolean(false)
    private val executed = ConcurrentHashMap.newKeySet<String>()
    private val activeSessions = ConcurrentHashMap<WarmupSessionId, ActiveSession>()
    private val localAssignments = ConcurrentHashMap<WarmupSessionId, WarmupSessionAssignment>()
    private val dataPlaneStarted = ConcurrentHashMap.newKeySet<WarmupSessionId>()
    private val sessionEffects = ConcurrentHashMap<WarmupSessionId, SessionEffectExecutor>()
    private val terminalSessions = ConcurrentHashMap.newKeySet<WarmupSessionId>()
    private val donorEffects = Channel<DonorEffect>(configuration.controlPlaneLimits.maximumPendingSessionCommands)
    private val currentDonorOperation = AtomicReference<Pair<WarmupSessionId, Job>?>(null)
    private val donorEffectJob = scope.launch {
        try {
            for (effect in donorEffects) {
                supervisorScope {
                    val operation = async(start = CoroutineStart.LAZY) { handleDonorEffect(effect) }
                    val current = effect.sessionId to operation
                    currentDonorOperation.set(current)
                    operation.start()
                    try {
                        operation.await()
                    } catch (error: CancellationException) {
                        if (effect.sessionId !in terminalSessions) throw error
                    } finally {
                        currentDonorOperation.compareAndSet(current, null)
                    }
                }
            }
        } catch (error: Throwable) {
            if (error !is CancellationException || state != WarmupCoordinatorState.STOPPED) {
                fail(error, notify = true)
            }
        }
    }
    private val offers = ConcurrentHashMap<WarmupSessionId, MutableMap<WarmupInstanceId, WarmupDonorCapability>>()
    private val timers = ConcurrentHashMap<WarmupSessionId, Job>()
    private lateinit var replica: WarmupControlPlaneReplica
    private val syncRequestId = WarmupSessionId.random()
    @Volatile private var state = WarmupCoordinatorState.NEW
    @Volatile private var ownSessionId: WarmupSessionId? = null
    @Volatile private var ownAssignment: WarmupSessionAssignment? = null
    @Volatile private var lastHeartbeatNanos = System.nanoTime()

    fun <K, V> register(definition: WarmupConsumerDefinition<K, V>) {
        register(KafkaCoordinatorParticipant(definition, scope, onConsumerFailure = { error ->
            onParticipantFailure(definition.configuration.consumerId, error)
        }))
    }

    internal fun register(participant: CoordinatorParticipant) {
        check(state == WarmupCoordinatorState.NEW && !started.get())
        check(participants.putIfAbsent(participant.configuration.consumerId, participant) == null)
    }

    suspend fun start(): WarmupStartupResult {
        check(participants.isNotEmpty())
        check(started.compareAndSet(false, true))
        state = WarmupCoordinatorState.CONTROL_STARTING
        try {
            channel.start()
            withTimeout(configuration.stageTimeout) { channel.awaitReady() }
            replica = WarmupControlPlaneReplica(
                configuration.instanceId, channel.logPosition(), syncRequestId, configuration.controlPlaneLimits
            )
            scope.launch { recordLoop() }
            scope.launch { observeTransport() }
            if (replica.bootstrapEmptyJournal()) synchronized.complete(Unit)
            else {
                state = WarmupCoordinatorState.SYNCHRONIZING
                val syncRequests = scope.launch {
                    while (!synchronized.isCompleted) {
                        runCatching { channel.send(StateSyncRequest(syncRequestId, configuration.instanceId)) }
                        delay(configuration.heartbeatInterval)
                    }
                }
                try {
                    withTimeout(configuration.stageTimeout) { synchronized.await() }
                } catch (error: Throwable) {
                    syncRequests.cancel()
                    return coldStart(error, electionEligible = false)
                }
                syncRequests.cancel()
            }

            scope.launch { electionLoop() }
            val session = WarmupSessionId.random()
            val hadActiveCoordinator = replica.view().activeCoordinator != null
            ownSessionId = session
            state = WarmupCoordinatorState.SEEKING_DONOR
            channel.send(WarmupRequest(session, configuration.instanceId, participantConfigurations()))
            timers[session] = scope.launch {
                delay(configuration.donorSearchTimeout +
                    if (hadActiveCoordinator) configuration.stageTimeout else Duration.ZERO)
                if (!startup.isCompleted && ownSessionId == session && session !in dataPlaneStarted) {
                    scope.launch {
                        coldStart(IllegalStateException("No warm-up assignment was applied"), electionEligible = true)
                    }
                }
            }
            return startup.await()
        } catch (error: Throwable) {
            if (!ownDataPlaneStarted()) return coldStart(error, electionEligible = false)
            fail(error, notify = true)
            return startup.await()
        }
    }

    fun state(): WarmupCoordinatorState = state
    internal fun controlPlaneView(): WarmupControlPlaneView = replica.view()
    internal fun localSessionExecutorCount(): Int = sessionEffects.size
    internal fun pendingRequestCount(): Int = activeSessions.size

    suspend fun awaitTermination() { termination.await()?.let { throw it } }

    suspend fun stop() {
        if (state == WarmupCoordinatorState.STOPPED) return
        state = WarmupCoordinatorState.STOPPED
        timers.values.forEach(Job::cancel)
        sessionEffects.values.forEach { it.cancel() }
        donorEffects.close()
        donorEffectJob.cancelAndJoin()
        coroutineScope { participants.values.map { async { runCatching { it.stop() } } }.awaitAll() }
        channel.stop()
        records.close()
        scope.cancel()
        termination.complete(null)
    }

    private suspend fun recordLoop() {
        try {
            for (record in records) {
                val application = replica.apply(record)
                if (application.synchronizedNow) {
                    lastHeartbeatNanos = System.nanoTime()
                    synchronized.complete(Unit)
                }
                if (!application.accepted) continue
                dispatchApplied(record.message)
            }
        } catch (error: Throwable) {
            if (!ownDataPlaneStarted() && state == WarmupCoordinatorState.SYNCHRONIZING) {
                synchronized.completeExceptionally(error)
                coldStart(error, electionEligible = false)
            } else {
                fail(error, notify = false)
            }
        }
    }

    private fun dispatchApplied(message: WarmupControlMessage) {
        when (message) {
            is StateSyncRequest -> scope.launch { replica.snapshot(message)?.let { runCatching { channel.send(it) } } }
            is StateSnapshot -> Unit
            is InstanceReady -> Unit
            is CoordinatorClaim -> if (replica.view().activeCoordinator == configuration.instanceId) {
                startHeartbeat()
                reconcileActiveSessions()
            }
            is CoordinatorHeartbeat -> lastHeartbeatNanos = System.nanoTime()
            is WarmupRequest -> if (isActive()) scheduleRequest(message)
            is DonorProbe -> if (targeted(message)) scope.launch { offer(message) }
            is DonorOffer -> if (targeted(message) && isActive() &&
                message.generation == replica.view().generation && activeSessions.containsKey(message.sessionId)) {
                offers.computeIfAbsent(message.sessionId) { ConcurrentHashMap() }[message.senderInstanceId] =
                    message.capability
            }
            is AssignmentProposed -> {
                val assignment = message.assignment
                if (assignment.donorInstanceId == configuration.instanceId ||
                    assignment.recipientInstanceId == configuration.instanceId) {
                    localAssignments.putIfAbsent(message.sessionId, assignment)
                }
                if (assignment.donorInstanceId == configuration.instanceId) {
                    enqueueDonorEffect(DonorEffect.Reserve(assignment))
                }
                if (assignment.recipientInstanceId == configuration.instanceId) {
                    ownAssignment = message.assignment
                    timers.remove(message.sessionId)?.cancel()
                    enqueueSessionEffect(message)
                }
            }
            is RecipientReady -> if (isActive()) enqueueSessionEffect(message)
            is StartForwarding -> {
                if (isActive()) scheduleReplicatedStop(message.sessionId)
                if (ownSessionId == message.sessionId) {
                    dataPlaneStarted += message.sessionId
                    timers.remove(message.sessionId)?.cancel()
                }
                if (targeted(message)) enqueueDonorEffect(DonorEffect.Start(message))
            }
            is StopForwarding -> if (targeted(message)) enqueueDonorEffect(DonorEffect.Stop(message))
            is ForwardingStopped -> if (isActive()) enqueueSessionEffect(message)
            is DrainRecipient -> if (targeted(message)) enqueueSessionEffect(message)
            is SessionCompleted -> dispatchSessionCompleted(message)
            is SessionFailed -> dispatchSessionFailed(message)
        }
    }

    private fun dispatchSessionCompleted(message: SessionCompleted) {
        val assignment = localAssignments[message.sessionId]
            ?: ownAssignment.takeIf { it?.sessionId == message.sessionId }
            ?: activeSessions[message.sessionId]?.assignment
        val existingExecutor = sessionEffects[message.sessionId]
        if (assignment?.donorInstanceId == configuration.instanceId) {
            enqueueDonorEffect(DonorEffect.Complete(message.sessionId))
        }
        timers.remove(message.sessionId)?.cancel()
        activeSessions.remove(message.sessionId)
        offers.remove(message.sessionId)
        if (assignment?.recipientInstanceId == configuration.instanceId && ownSessionId == message.sessionId) {
            enqueueSessionEffect(message)
        } else {
            existingExecutor?.let { enqueueExistingSessionEffect(message, it) }
            localAssignments.remove(message.sessionId)
            dataPlaneStarted.remove(message.sessionId)
        }
    }

    private fun dispatchSessionFailed(message: SessionFailed) {
        terminalSessions += message.sessionId
        sessionEffects.remove(message.sessionId)?.cancel()
        currentDonorOperation.get()?.takeIf { it.first == message.sessionId }?.second?.cancel(
            CancellationException("Warm-up session terminated")
        )
        timers.remove(message.sessionId)?.cancel()
        activeSessions.remove(message.sessionId)
        offers.remove(message.sessionId)
        val assignment = matchingAssignment(message.sessionId) ?: return
        val started = message.sessionId in dataPlaneStarted
        when {
            assignment.recipientInstanceId == configuration.instanceId && !started -> scope.launch {
                localAssignments.remove(message.sessionId)
                coldStart(IllegalStateException(message.reason), electionEligible = true)
            }
            assignment.donorInstanceId == configuration.instanceId && !started ->
                enqueueDonorEffect(DonorEffect.ReleaseFailed(message.sessionId))
            assignment.donorInstanceId == configuration.instanceId ||
                assignment.recipientInstanceId == configuration.instanceId -> scope.launch {
                    fail(IllegalStateException(message.reason), notify = false)
                }
        }
    }

    private fun enqueueDonorEffect(effect: DonorEffect) {
        if (donorEffects.trySend(effect).isFailure) {
            scope.launch { fail(IllegalStateException("Warm-up donor lifecycle queue is full"), true) }
        }
    }

    private suspend fun handleDonorEffect(effect: DonorEffect) {
        when (effect) {
            is DonorEffect.Reserve -> if (effect.assignment.sessionId !in terminalSessions &&
                once("reserve:${effect.assignment.sessionId}")) {
                participants.values.forEach {
                    it.reserveDonor(effect.assignment.assignmentsFor(it.configuration.consumerId))
                }
                state = WarmupCoordinatorState.DONOR_RESERVED
            }
            is DonorEffect.Complete -> {
                if (once("finish:${effect.sessionId}")) {
                    participants.values.forEach { it.finishDonor(effect.sessionId) }
                }
                localAssignments.remove(effect.sessionId)
                dataPlaneStarted.remove(effect.sessionId)
                if (state != WarmupCoordinatorState.STOPPED && state != WarmupCoordinatorState.FAILED) {
                    state = WarmupCoordinatorState.MAIN
                }
            }
            is DonorEffect.Start -> if (effect.message.sessionId !in terminalSessions) startForwarding(effect.message)
            is DonorEffect.Stop -> if (effect.message.sessionId !in terminalSessions) stopForwarding(effect.message)
            is DonorEffect.ReleaseFailed -> {
                if ("reserve:${effect.sessionId}" in executed && once("release-failed:${effect.sessionId}")) {
                    participants.values.forEach { it.releaseDonorReservation() }
                }
                localAssignments.remove(effect.sessionId)
                dataPlaneStarted.remove(effect.sessionId)
                if (state != WarmupCoordinatorState.STOPPED && state != WarmupCoordinatorState.FAILED) {
                    state = WarmupCoordinatorState.MAIN
                }
            }
        }
    }

    private fun enqueueSessionEffect(message: WarmupControlMessage) {
        if (message.sessionId in terminalSessions) return
        val effects = synchronized(sessionEffects) {
            sessionEffects[message.sessionId] ?: run {
                if (sessionEffects.size >= configuration.controlPlaneLimits.maximumSessionExecutors) return@synchronized null
                val executor = SessionEffectExecutor(
                    Channel(configuration.controlPlaneLimits.maximumPendingSessionCommands)
                )
                sessionEffects[message.sessionId] = executor
                executor.job = scope.launch {
                    try {
                        for (queued in executor.queue) {
                            handleSessionApplied(queued)
                            if (queued is SessionCompleted) break
                        }
                    } catch (error: Throwable) {
                        if (error !is CancellationException || !executor.terminal.get()) {
                            fail(error, notify = true)
                        }
                    } finally {
                        sessionEffects.remove(message.sessionId, executor)
                        executor.queue.close()
                    }
                }
                executor
            }
        }
        if (effects == null) {
            scope.launch { fail(IllegalStateException("Warm-up session executor limit exceeded"), true) }
        } else if (effects.queue.trySend(message).isFailure) {
            scope.launch {
                fail(IllegalStateException("Warm-up session command queue is full for ${message.sessionId}"), true)
            }
        }
    }

    private fun enqueueExistingSessionEffect(message: WarmupControlMessage, executor: SessionEffectExecutor) {
        if (executor.queue.trySend(message).isFailure) {
            scope.launch {
                fail(IllegalStateException("Warm-up session command queue is full for ${message.sessionId}"), true)
            }
        }
    }

    private suspend fun handleSessionApplied(message: WarmupControlMessage) {
        when (message) {
            is AssignmentProposed -> startRecipient(message.assignment)
            is RecipientReady -> if (isActive()) onRecipientReady(message)
            is ForwardingStopped -> if (isActive()) onForwardingStopped(message)
            is DrainRecipient -> if (targeted(message)) drainRecipient(message)
            is SessionCompleted -> onSessionCompleted(message)
            is StateSyncRequest, is StateSnapshot, is InstanceReady, is CoordinatorClaim,
            is CoordinatorHeartbeat, is WarmupRequest, is DonorProbe, is DonorOffer,
            is StartForwarding, is StopForwarding, is SessionFailed -> Unit
        }
    }

    private fun scheduleRequest(request: WarmupRequest) {
        if (request.senderInstanceId == configuration.instanceId ||
            replica.view().sessions.containsKey(request.sessionId) || activeSessions.containsKey(request.sessionId)) return
        activeSessions[request.sessionId] = ActiveSession(request)
        val view = replica.view()
        view.readyInstances.filter { it != request.senderInstanceId }.forEach { donor ->
            scope.launch { runCatching { channel.send(DonorProbe(
                request.sessionId, configuration.instanceId, donor, view.generation,
                request.senderInstanceId, request.participants
            )) } }
        }
        timers[request.sessionId] = scope.launch {
            delay(configuration.donorSearchTimeout)
            chooseDonor(request.sessionId)
        }
    }

    private suspend fun chooseDonor(sessionId: WarmupSessionId) {
        if (!isActive()) {
            finishUnassignedRequest(sessionId)
            return
        }
        val session = activeSessions[sessionId] ?: return
        val busyDonors = replica.view().sessions.values.map { it.assignment.donorInstanceId }.toSet()
        val selected = offers.remove(sessionId)?.values
            ?.filter { it.instanceId !in busyDonors && fullyCompatible(session.request, it) }
            ?.maxWithOrNull(compareBy<WarmupDonorCapability> { it.loadScore ?: 0.0 }
                .thenBy { it.participants.sumOf { p -> p.topics.count(WarmupTopicCapability::hasObservedTraffic) } })
        if (selected == null) {
            finishUnassignedRequest(sessionId)
            return
        }
        val assignment = allocate(session.request, selected.instanceId)
        if (assignment == null) {
            finishUnassignedRequest(sessionId)
            return
        }
        session.assignment = assignment
        val decision = AssignmentProposed(
            sessionId, configuration.instanceId, replica.view().generation, assignment,
            warmupDuration(session.request).inWholeMilliseconds
        )
        // An error is deliberately not treated as rollback. Re-publishing the same decision is idempotent;
        // only observing it in the journal changes replicated state.
        if (runCatching { channel.send(decision) }.isFailure) runCatching { channel.send(decision) }
        scope.launch {
            delay(configuration.stageTimeout)
            if (sessionId !in replica.view().sessions) finishUnassignedRequest(sessionId)
        }
    }

    private fun finishUnassignedRequest(sessionId: WarmupSessionId) {
        activeSessions.remove(sessionId)
        offers.remove(sessionId)
        timers.remove(sessionId)?.cancel()
    }

    private suspend fun startRecipient(assignment: WarmupSessionAssignment) {
        if (configuration.instanceId == assignment.recipientInstanceId && once("recipient:${assignment.sessionId}")) {
            state = WarmupCoordinatorState.RECIPIENT_STARTING
            try {
                withTimeout(configuration.stageTimeout) {
                    coroutineScope { participants.values.map { participant -> async {
                        participant.startRecipient(assignment.assignmentsFor(participant.configuration.consumerId))
                    } }.awaitAll() }
                }
                state = WarmupCoordinatorState.RECIPIENT_WARMING
                channel.send(RecipientReady(
                    assignment.sessionId, configuration.instanceId,
                    requireNotNull(replica.view().activeCoordinator), participants.keys
                ))
                armRecipientStartTimeout(assignment.sessionId)
            } catch (error: Throwable) {
                if (assignment.sessionId !in dataPlaneStarted) {
                    val reported = runCatching { channel.send(SessionFailed(
                        assignment.sessionId, configuration.instanceId, replica.view().activeCoordinator,
                        error.message?.take(4096) ?: error.javaClass.simpleName
                    )) }
                    if (reported.isFailure) coldStart(error, electionEligible = true)
                } else fail(error, notify = true)
            }
        }
    }

    private suspend fun onRecipientReady(message: RecipientReady) {
        val session = replica.view().sessions[message.sessionId] ?: return
        if (session.assignment.recipientInstanceId != message.senderInstanceId ||
            message.consumerIds != session.assignment.partitions.map { it.consumerId }.toSet()) return
        val stopAtEpochMillis = System.currentTimeMillis() + session.warmupDurationMillis
        channel.send(StartForwarding(
            message.sessionId, configuration.instanceId, session.assignment.donorInstanceId,
            replica.view().generation, stopAtEpochMillis
        ))
    }

    private fun scheduleReplicatedStop(sessionId: WarmupSessionId) {
        val replicated = replica.view().sessions[sessionId] ?: return
        if (replicated.stage != WarmupReplicatedStage.FORWARDING) return
        val deadline = replicated.stopAtEpochMillis ?: return
        timers.remove(sessionId)?.cancel()
        timers[sessionId] = scope.launch {
            delay((deadline - System.currentTimeMillis()).coerceAtLeast(0).milliseconds)
            val current = replica.view().sessions[sessionId]
            if (isActive() && current?.stage == WarmupReplicatedStage.FORWARDING) {
                val command = StopForwarding(
                    sessionId, configuration.instanceId, current.assignment.donorInstanceId,
                    replica.view().generation
                )
                if (runCatching { channel.send(command) }.isFailure) runCatching { channel.send(command) }
            }
        }
    }

    private fun reconcileActiveSessions() {
        replica.view().sessions.forEach { (sessionId, session) ->
            if (session.stage == WarmupReplicatedStage.FORWARDING) scheduleReplicatedStop(sessionId)
        }
    }

    private fun armRecipientStartTimeout(sessionId: WarmupSessionId) {
        timers.remove(sessionId)?.cancel()
        timers[sessionId] = scope.launch {
            delay(configuration.stageTimeout)
            if (ownSessionId == sessionId && sessionId !in dataPlaneStarted && !startup.isCompleted) {
                val error = IllegalStateException("Timed out waiting for forwarding start")
                runCatching { channel.send(SessionFailed(
                    sessionId, configuration.instanceId, replica.view().activeCoordinator,
                    error.message ?: error.javaClass.simpleName
                )) }
                coldStart(error, electionEligible = true)
            }
        }
    }

    private suspend fun startForwarding(message: StartForwarding) {
        val assignment = matchingAssignment(message.sessionId) ?: return
        if (assignment.donorInstanceId != configuration.instanceId || !once("forward:${message.sessionId}")) return
        // From this point activation may be partial, so any failure requires terminal participant cleanup.
        dataPlaneStarted += message.sessionId
        state = WarmupCoordinatorState.DONOR_FORWARDING
        participants.values.forEach { it.activateDonor(message.sessionId) }
    }

    private suspend fun stopForwarding(message: StopForwarding) {
        val assignment = matchingAssignment(message.sessionId) ?: return
        if (assignment.donorInstanceId != configuration.instanceId || !once("stop:${message.sessionId}")) return
        state = WarmupCoordinatorState.DONOR_STOPPING
        val offsets = coroutineScope {
            participants.map { (id, participant) -> id to async { participant.completeForwarding() } }
                .flatMap { (id, result) -> result.await().offsets.map { WarmupFinalOffset(id, it.key, it.value) } }
        }
        channel.send(ForwardingStopped(
            message.sessionId, configuration.instanceId,
            requireNotNull(replica.view().activeCoordinator), offsets
        ))
    }

    private suspend fun onForwardingStopped(message: ForwardingStopped) {
        val assignment = matchingAssignment(message.sessionId) ?: return
        if (assignment.donorInstanceId != message.senderInstanceId) return
        channel.send(DrainRecipient(
            message.sessionId, configuration.instanceId, assignment.recipientInstanceId,
            replica.view().generation, message.finalOffsets
        ))
    }

    private suspend fun drainRecipient(message: DrainRecipient) {
        val assignment = matchingAssignment(message.sessionId) ?: return
        if (assignment.recipientInstanceId != configuration.instanceId || !once("drain:${message.sessionId}")) return
        state = WarmupCoordinatorState.RECIPIENT_DRAINING
        val byConsumer = message.finalOffsets.groupBy(WarmupFinalOffset::consumerId)
        coroutineScope { participants.map { (id, participant) -> async {
            participant.drainRecipient(WarmupForwardingBoundary(
                byConsumer[id].orEmpty().associate { it.topicPartition to it.offset }
            ))
        } }.awaitAll() }
        coroutineScope { participants.values.map { async { it.startMainAfterDrain() } }.awaitAll() }
        localReady.set(true)
        channel.send(SessionCompleted(message.sessionId, configuration.instanceId, participants.keys))
    }

    private suspend fun onSessionCompleted(message: SessionCompleted) {
        val assignment = localAssignments[message.sessionId]
            ?: ownAssignment.takeIf { it?.sessionId == message.sessionId }
            ?: activeSessions[message.sessionId]?.assignment
        dataPlaneStarted.remove(message.sessionId)
        localAssignments.remove(message.sessionId)
        if (assignment?.recipientInstanceId == configuration.instanceId && ownSessionId == message.sessionId) {
            ownAssignment = null
            ownSessionId = null
            dataPlaneStarted.remove(message.sessionId)
            state = WarmupCoordinatorState.MAIN
            startup.complete(WarmupStartupResult.WARMED)
            publishReadyAndMaybeClaim()
        }
    }

    private suspend fun offer(probe: DonorProbe) {
        if (state != WarmupCoordinatorState.MAIN || !localReady.get()) return
        val requested = probe.participants.associateBy { it.consumerId }
        if (requested.keys != participants.keys) return
        val capabilities = participants.map { (id, participant) -> participant.capability(requested.getValue(id)) }
        if (capabilities.any { p -> p.topics.any { !it.compatible } } ||
            capabilities.none { p -> p.topics.any { it.ownsCandidatePartitions } }) return
        channel.send(DonorOffer(
            probe.sessionId, configuration.instanceId, probe.senderInstanceId, probe.generation,
            WarmupDonorCapability(configuration.instanceId, true, capabilities, configuration.donorLoadScore())
        ))
    }

    private suspend fun coldStart(cause: Throwable, electionEligible: Boolean): WarmupStartupResult {
        if (startup.isCompleted) return startup.await()
        if (!fallbackStarted.compareAndSet(false, true)) return startup.await()
        val failedBeforeFallback = synchronized(stateLock) {
            if (failureStarted.get()) true
            else { state = WarmupCoordinatorState.FALLBACK_STARTING; false }
        }
        if (failedBeforeFallback) return startup.await()
        ownSessionId?.let { timers.remove(it)?.cancel() }
        try {
            coroutineScope { participants.values.map { async { it.prepareFallback(cause) } }.awaitAll() }
            coroutineScope { participants.values.map { async { it.startMain() } }.awaitAll() }
            localReady.set(true)
            ownSessionId = null
            ownAssignment = null
            val failedDuringFallback = synchronized(stateLock) {
                if (failureStarted.get()) true
                else {
                    state = WarmupCoordinatorState.MAIN
                    startup.complete(WarmupStartupResult.NO_DONOR_FALLBACK)
                    false
                }
            }
            if (failedDuringFallback) return startup.await()
            if (electionEligible || replica.view().synchronized) publishReadyAndMaybeClaim()
            return startup.await()
        } catch (error: Throwable) {
            error.addSuppressed(cause)
            fail(error, notify = false)
            return startup.await()
        }
    }

    private suspend fun publishReadyAndMaybeClaim() {
        if (!replica.view().synchronized || !localReady.get()) return
        channel.send(InstanceReady(WarmupSessionId.random(), configuration.instanceId))
        delay(10.milliseconds)
        val view = replica.view()
        if (view.activeCoordinator == null) channel.send(CoordinatorClaim(
            WarmupSessionId.random(), configuration.instanceId, view.generation, null, view.lastHeartbeatOffset
        ))
    }

    private fun startHeartbeat() {
        if (!once("heartbeat:${replica.view().generation}")) return
        scope.launch {
            while (state != WarmupCoordinatorState.STOPPED && state != WarmupCoordinatorState.FAILED && isActive()) {
                runCatching { channel.send(CoordinatorHeartbeat(
                    WarmupSessionId.random(), configuration.instanceId, replica.view().generation
                )) }
                delay(configuration.heartbeatInterval)
            }
        }
    }

    private suspend fun electionLoop() {
        while (state != WarmupCoordinatorState.STOPPED && state != WarmupCoordinatorState.FAILED) {
            delay(configuration.heartbeatInterval)
            val view = replica.view()
            if (localReady.get() && view.activeCoordinator != configuration.instanceId &&
                System.nanoTime() - lastHeartbeatNanos > configuration.electionTimeout.inWholeNanoseconds) {
                runCatching { channel.send(CoordinatorClaim(
                    WarmupSessionId.random(), configuration.instanceId, view.generation,
                    view.activeCoordinator, view.lastHeartbeatOffset
                )) }
                lastHeartbeatNanos = System.nanoTime()
            }
        }
    }

    private suspend fun observeTransport() {
        try {
            channel.awaitTermination()
            if (state != WarmupCoordinatorState.STOPPED) fail(IllegalStateException("Control transport terminated"), false)
        } catch (error: Throwable) {
            if (state != WarmupCoordinatorState.STOPPED) fail(error, false)
        }
    }

    internal suspend fun onParticipantFailure(consumerId: WarmupConsumerId, error: Throwable) {
        val wrapped = IllegalStateException("Warm-up consumer $consumerId failed", error)
        if (!ownDataPlaneStarted() && state == WarmupCoordinatorState.RECIPIENT_STARTING) coldStart(wrapped, true)
        else fail(wrapped, true)
    }

    private suspend fun fail(error: Throwable, notify: Boolean) {
        val accepted = synchronized(stateLock) {
            if (state == WarmupCoordinatorState.STOPPED || !failureStarted.compareAndSet(false, true)) false
            else { state = WarmupCoordinatorState.FAILED; true }
        }
        if (!accepted) return
        if (notify) {
            val affectedSessions = buildSet {
                ownSessionId?.let(::add)
                localAssignments.keys.forEach(::add)
            }
            affectedSessions.forEach { session ->
                runCatching { channel.send(SessionFailed(
                    session, configuration.instanceId, replica.view().activeCoordinator,
                    error.message?.take(4096) ?: error.javaClass.simpleName
                )) }
            }
        }
        coroutineScope { participants.values.map { async { runCatching { it.stop() } } }.awaitAll() }
        startup.completeExceptionally(error)
        termination.complete(error)
    }

    private fun allocate(request: WarmupRequest, donor: WarmupInstanceId): WarmupSessionAssignment? {
        val used = replica.view().sessions.values.flatMap { it.assignment.partitions }
            .map { it.topicPartition }.toSet()
        val partition = configuration.warmupPartitions.sorted().firstOrNull { candidate ->
            request.participants.all { participant -> participant.topicMappings.all { mapping ->
                org.apache.kafka.common.TopicPartition(mapping.warmupTopic, candidate) !in used
            } }
        } ?: return null
        return WarmupSessionAssignment(request.sessionId, donor, request.senderInstanceId,
            request.participants.flatMap { participant -> participant.topicMappings.map { mapping ->
                WarmupPartitionAssignment(participant.consumerId, mapping.sourceTopic, mapping.warmupTopic, partition)
            } })
    }

    private fun fullyCompatible(request: WarmupRequest, capability: WarmupDonorCapability): Boolean {
        val expected = request.participants.associateBy { it.consumerId }
        return capability.available && capability.participants.map { it.consumerId }.toSet() == expected.keys &&
            capability.participants.all { offered ->
                offered.topics.all { it.compatible } && offered.topics.map { it.sourceTopic }.toSet() ==
                    expected.getValue(offered.consumerId).topicMappings.map { it.sourceTopic }.toSet()
            }
    }

    private fun warmupDuration(request: WarmupRequest): Duration = request.participants.maxOf {
        it.forwardingProfile.rampUpDuration + (it.forwardingProfile.holdDuration ?: Duration.ZERO)
    }

    private fun matchingAssignment(sessionId: WarmupSessionId): WarmupSessionAssignment? =
        replica.view().sessions[sessionId]?.assignment ?: localAssignments[sessionId]
        ?: ownAssignment?.takeIf { it.sessionId == sessionId }

    private fun participantConfigurations() = participants.values.map(CoordinatorParticipant::configuration)
    private fun targeted(message: WarmupControlMessage) = message.targetInstanceId == configuration.instanceId
    private fun isActive() = replica.view().activeCoordinator == configuration.instanceId
    private fun ownDataPlaneStarted() = ownSessionId?.let(dataPlaneStarted::contains) == true
    private fun once(key: String) = executed.add(key)

    private class SessionEffectExecutor(val queue: Channel<WarmupControlMessage>) {
        val terminal = AtomicBoolean(false)
        lateinit var job: Job
        fun cancel() {
            terminal.set(true)
            queue.close()
            if (::job.isInitialized) job.cancel(CancellationException("Warm-up session terminated"))
        }
    }

    private sealed interface DonorEffect {
        val sessionId: WarmupSessionId
        data class Reserve(val assignment: WarmupSessionAssignment) : DonorEffect {
            override val sessionId = assignment.sessionId
        }
        data class Start(val message: StartForwarding) : DonorEffect {
            override val sessionId = message.sessionId
        }
        data class Stop(val message: StopForwarding) : DonorEffect {
            override val sessionId = message.sessionId
        }
        data class Complete(override val sessionId: WarmupSessionId) : DonorEffect
        data class ReleaseFailed(override val sessionId: WarmupSessionId) : DonorEffect
    }

    private data class ActiveSession(
        val request: WarmupRequest,
        @Volatile var assignment: WarmupSessionAssignment? = null
    )
}
