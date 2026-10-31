package avh.ckc.core.warmup

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class WarmupCoordinatorConfiguration(
    val instanceId: WarmupInstanceId,
    val assignmentLeaseProvider: WarmupAssignmentLeaseProvider,
    val donorSearchTimeout: Duration = 5.seconds,
    val stageTimeout: Duration = 30.seconds,
    val donorLoadScore: () -> Double? = { null }
) {
    init {
        require(donorSearchTimeout.isPositive()) { "donorSearchTimeout must be positive" }
        require(stageTimeout.isPositive()) { "stageTimeout must be positive" }
    }
}

enum class WarmupStartupResult { WARMED, NO_DONOR_FALLBACK }

enum class WarmupCoordinatorState {
    NEW,
    CONTROL_STARTING,
    SEEKING_DONOR,
    WAITING_FOR_ASSIGNMENT,
    RECIPIENT_STARTING,
    RECIPIENT_WARMING,
    RECIPIENT_STOPPING,
    RECIPIENT_DRAINING,
    FALLBACK_STARTING,
    MAIN,
    DONOR_RESERVING,
    DONOR_RESERVED,
    DONOR_ABORTING,
    DONOR_STARTING,
    DONOR_FORWARDING,
    DONOR_STOPPING,
    DONOR_WAITING_COMPLETION,
    FAILED,
    STOPPED
}

/** One explicit coordinator owns all locally registered CKC warm-up participants. */
class WarmupCoordinator internal constructor(
    private val configuration: WarmupCoordinatorConfiguration,
    parentContext: CoroutineContext,
    channelFactory: (WarmupControlMessageHandler) -> WarmupControlChannel
) {
    constructor(
        configuration: WarmupCoordinatorConfiguration,
        controlTransportConfiguration: WarmupControlTransportConfiguration,
        parentContext: CoroutineContext = Dispatchers.Default
    ) : this(
        configuration,
        parentContext,
        { handler -> WarmupControlTransport(controlTransportConfiguration, handler, parentContext = parentContext) }
    ) {
        require(configuration.instanceId == controlTransportConfiguration.instanceId) {
            "Coordinator and control transport instance IDs must match"
        }
    }

    private val scope = CoroutineScope(parentContext + SupervisorJob(parentContext[Job]))
    private val channel = channelFactory(WarmupControlMessageHandler(::onMessage))
    private val participants = LinkedHashMap<WarmupConsumerId, CoordinatorParticipant>()
    private val stateLock = Any()
    private val started = AtomicBoolean(false)
    private val startup = CompletableDeferred<WarmupStartupResult>()
    private val termination = CompletableDeferred<Throwable?>()
    private val offers = ConcurrentHashMap<WarmupInstanceId, WarmupDonorCapability>()
    private val offeredRequests = ConcurrentHashMap<WarmupSessionId, WarmupRequest>()
    private val failureStarted = AtomicBoolean(false)
    @Volatile private var state = WarmupCoordinatorState.NEW
    @Volatile private var sessionId: WarmupSessionId? = null
    @Volatile private var peerId: WarmupInstanceId? = null
    @Volatile private var assignment: WarmupSessionAssignment? = null
    @Volatile private var stageTimer: Job? = null
    @Volatile private var forwardingStarted = false
    @Volatile private var assignmentLease: WarmupAssignmentLease? = null

    fun <K, V> register(definition: WarmupConsumerDefinition<K, V>) {
        check(state == WarmupCoordinatorState.NEW && !started.get()) {
            "Warm-up participants must be registered before coordinator start"
        }
        val consumerId = definition.configuration.consumerId
        val participant = KafkaCoordinatorParticipant(definition, scope) { error ->
            onParticipantFailure(consumerId, error)
        }
        check(participants.putIfAbsent(definition.configuration.consumerId, participant) == null) {
            "Warm-up consumer ${definition.configuration.consumerId} is already registered"
        }
    }

    internal fun register(participant: CoordinatorParticipant) {
        check(state == WarmupCoordinatorState.NEW && !started.get()) {
            "Warm-up participants must be registered before coordinator start"
        }
        check(participants.putIfAbsent(participant.configuration.consumerId, participant) == null) {
            "Warm-up consumer ${participant.configuration.consumerId} is already registered"
        }
    }

    suspend fun start(): WarmupStartupResult {
        check(participants.isNotEmpty()) { "At least one warm-up participant must be registered" }
        check(transition(setOf(WarmupCoordinatorState.NEW), WarmupCoordinatorState.CONTROL_STARTING)) {
            "Warm-up coordinator cannot start from state $state"
        }
        check(started.compareAndSet(false, true)) { "Warm-up coordinator already started" }
        try {
            channel.start()
            withTimeout(configuration.stageTimeout) { channel.awaitReady() }
            scope.launch {
                try {
                    channel.awaitTermination()
                    if (state != WarmupCoordinatorState.STOPPED) {
                        onControlFailure(IllegalStateException("Warm-up control transport terminated"))
                    }
                } catch (error: Throwable) {
                    onControlFailure(error)
                }
            }
            val newSession = WarmupSessionId.random()
            sessionId = newSession
            check(transition(
                setOf(WarmupCoordinatorState.CONTROL_STARTING),
                WarmupCoordinatorState.SEEKING_DONOR
            )) { "Coordinator state changed during control startup" }
            channel.send(WarmupRequest(
                sessionId = newSession,
                senderInstanceId = configuration.instanceId,
                participants = participants.values.map(CoordinatorParticipant::configuration)
            ))
            stageTimer = scope.launch {
                delay(configuration.donorSearchTimeout)
                stageTimer = null
                runGuarded { selectDonorOrFallback(newSession) }
            }
            return startup.await()
        } catch (error: Throwable) {
            if (currentState() == WarmupCoordinatorState.CONTROL_STARTING ||
                currentState() == WarmupCoordinatorState.SEEKING_DONOR) {
                fallbackBeforeForwarding(error, notifyPeer = false)
                return startup.await()
            }
            fail(error, notifyPeer = false)
            return startup.await()
        }
    }

    fun state(): WarmupCoordinatorState = state

    suspend fun awaitTermination() {
        termination.await()?.let { throw it }
    }

    suspend fun stop() {
        val shouldStop = synchronized(stateLock) {
            if (state == WarmupCoordinatorState.STOPPED) false
            else {
                state = WarmupCoordinatorState.STOPPED
                true
            }
        }
        if (!shouldStop) return
        stageTimer?.cancel()
        takeAssignmentLease()?.runCatching { release() }
        participants.values.map { participant -> scope.async { runCatching { participant.stop() } } }.awaitAll()
        channel.stop()
        scope.cancel()
        termination.complete(null)
        if (!startup.isCompleted) startup.completeExceptionally(
            IllegalStateException("Warm-up coordinator stopped before startup completed")
        )
    }

    private suspend fun onMessage(message: WarmupControlMessage) {
        if (message.senderInstanceId == configuration.instanceId) return
        if (message.targetInstanceId != null && message.targetInstanceId != configuration.instanceId) return
        try {
            when (message) {
                is WarmupRequest -> onRequest(message)
                is DonorOffer -> onOffer(message)
                is DonorSelected -> onSelected(message)
                is WarmupAssigned -> onAssigned(message)
                is RecipientReady -> onReady(message)
                is WarmupFinished -> onFinished(message)
                is ForwardingStopped -> onForwardingStopped(message)
                is SessionCompleted -> onCompleted(message)
                is SessionFailed -> onSessionFailed(message)
            }
        } catch (error: Throwable) {
            handleOperationFailure(error, notifyPeer = message !is SessionFailed)
        }
    }

    private suspend fun onRequest(request: WarmupRequest) {
        if (state != WarmupCoordinatorState.MAIN || offeredRequests.containsKey(request.sessionId)) return
        val requestedById = request.participants.associateBy(WarmupParticipantConfiguration::consumerId)
        if (requestedById.keys != participants.keys) return
        val capabilities = participants.map { (id, participant) ->
            participant.capability(requireNotNull(requestedById[id]))
        }
        val compatible = capabilities.all { participant -> participant.topics.all { it.compatible } }
        val ownsTrafficSource = capabilities.any { participant ->
            participant.topics.any(WarmupTopicCapability::ownsCandidatePartitions)
        }
        if (!compatible || !ownsTrafficSource) return
        val capability = WarmupDonorCapability(
            instanceId = configuration.instanceId,
            available = true,
            participants = capabilities,
            loadScore = configuration.donorLoadScore()
        )
        offeredRequests[request.sessionId] = request
        scope.launch {
            delay(configuration.stageTimeout)
            if (state == WarmupCoordinatorState.MAIN) offeredRequests.remove(request.sessionId, request)
        }
        channel.send(DonorOffer(
            sessionId = request.sessionId,
            senderInstanceId = configuration.instanceId,
            targetInstanceId = request.senderInstanceId,
            capability = capability
        ))
    }

    private fun onOffer(offer: DonorOffer) {
        if (state != WarmupCoordinatorState.SEEKING_DONOR || offer.sessionId != sessionId) return
        if (offer.capability.available && fullyCompatible(offer.capability)) {
            offers.putIfAbsent(offer.senderInstanceId, offer.capability)
        }
    }

    private suspend fun selectDonorOrFallback(expectedSession: WarmupSessionId) {
        if (currentState() != WarmupCoordinatorState.SEEKING_DONOR || sessionId != expectedSession) return
        val selected = offers.values.maxWithOrNull(
            compareBy<WarmupDonorCapability> { it.loadScore ?: 0.0 }
                .thenBy { capability -> capability.participants.sumOf { p -> p.topics.count { it.hasObservedTraffic } } }
                .thenBy { capability -> capability.participants.sumOf { p -> p.topics.sumOf { it.ownedPartitions.size } } }
        )
        if (selected == null) {
            if (!transition(
                    setOf(WarmupCoordinatorState.SEEKING_DONOR),
                    WarmupCoordinatorState.FALLBACK_STARTING
                )) return
            participants.values.forEach { it.startMain() }
            sessionId = null
            offers.clear()
            if (transition(setOf(WarmupCoordinatorState.FALLBACK_STARTING), WarmupCoordinatorState.MAIN)) {
                startup.complete(WarmupStartupResult.NO_DONOR_FALLBACK)
            }
            return
        }
        peerId = selected.instanceId
        offers.clear()
        if (!transition(
                setOf(WarmupCoordinatorState.SEEKING_DONOR),
                WarmupCoordinatorState.WAITING_FOR_ASSIGNMENT
            )) return
        armStageTimeout(expectedSession, "donor assignment")
        channel.send(DonorSelected(expectedSession, configuration.instanceId, selected.instanceId))
    }

    private suspend fun onSelected(selected: DonorSelected) {
        val request = offeredRequests[selected.sessionId] ?: return
        if (!beginDonorReservation(selected)) return
        val lease = configuration.assignmentLeaseProvider.tryAcquire(
            selected.sessionId,
            configuration.instanceId,
            selected.senderInstanceId,
            request.participants
        )
        if (lease == null) {
            offeredRequests.remove(selected.sessionId)
            if (!completeRejectedReservation(selected.sessionId)) return
            channel.send(SessionFailed(
                selected.sessionId,
                configuration.instanceId,
                selected.senderInstanceId,
                "Global warm-up assignment is already reserved"
            ))
            return
        }
        if (!installAssignmentLease(selected.sessionId, lease)) {
            lease.release()
            return
        }
        val assigned = WarmupSessionAssignment(
            selected.sessionId,
            configuration.instanceId,
            selected.senderInstanceId,
            lease.assignments
        )
        validateLeaseAssignment(request, assigned)
        participants.values.forEach { participant ->
            participant.reserveDonor(assigned.assignmentsFor(participant.configuration.consumerId))
        }
        assignment = assigned
        check(transition(
            setOf(WarmupCoordinatorState.DONOR_RESERVING),
            WarmupCoordinatorState.DONOR_RESERVED
        )) { "Coordinator state changed during donor reservation" }
        armStageTimeout(selected.sessionId, "recipient readiness")
        channel.send(WarmupAssigned(
            selected.sessionId,
            configuration.instanceId,
            selected.senderInstanceId,
            assigned
        ))
    }

    private suspend fun onAssigned(message: WarmupAssigned) {
        if (message.sessionId != sessionId || message.senderInstanceId != peerId) return
        if (state != WarmupCoordinatorState.WAITING_FOR_ASSIGNMENT) return
        validateAssignment(message.assignment)
        stageTimer?.cancel()
        assignment = message.assignment
        check(transition(
            setOf(WarmupCoordinatorState.WAITING_FOR_ASSIGNMENT),
            WarmupCoordinatorState.RECIPIENT_STARTING
        )) { "Coordinator state changed before recipient startup" }
        withTimeout(configuration.stageTimeout) {
            participants.values.map { participant -> scope.async {
                participant.startRecipient(message.assignment.assignmentsFor(participant.configuration.consumerId))
            } }.awaitAll()
        }
        check(transition(
            setOf(WarmupCoordinatorState.RECIPIENT_STARTING),
            WarmupCoordinatorState.RECIPIENT_WARMING
        )) { "Coordinator state changed during recipient startup" }
        forwardingStarted = true
        channel.send(RecipientReady(
            message.sessionId,
            configuration.instanceId,
            message.senderInstanceId,
            participants.keys
        ))
        val duration = participants.values.maxOf { participant ->
            val profile = participant.configuration.forwardingProfile
            profile.rampUpDuration + (profile.holdDuration ?: Duration.ZERO)
        }
        stageTimer = scope.launch {
            delay(duration)
            if (transition(
                    setOf(WarmupCoordinatorState.RECIPIENT_WARMING),
                    WarmupCoordinatorState.RECIPIENT_STOPPING,
                    expectedSession = message.sessionId
                )) {
                stageTimer = null
                runGuarded {
                    armStageTimeout(message.sessionId, "forwarding completion")
                    channel.send(WarmupFinished(
                        message.sessionId,
                        configuration.instanceId,
                        message.senderInstanceId
                    ))
                }
            }
        }
    }

    private suspend fun onReady(message: RecipientReady) {
        if (currentState() != WarmupCoordinatorState.DONOR_RESERVED || message.sessionId != sessionId ||
            message.senderInstanceId != peerId) return
        check(message.consumerIds == participants.keys) { "Recipient readiness does not cover all participants" }
        stageTimer?.cancel()
        check(transition(
            setOf(WarmupCoordinatorState.DONOR_RESERVED),
            WarmupCoordinatorState.DONOR_STARTING
        )) { "Coordinator state changed before donor activation" }
        forwardingStarted = true
        participants.values.forEach { it.activateDonor(message.sessionId) }
        check(transition(
            setOf(WarmupCoordinatorState.DONOR_STARTING),
            WarmupCoordinatorState.DONOR_FORWARDING
        )) { "Coordinator state changed during donor activation" }
    }

    private suspend fun onFinished(message: WarmupFinished) {
        if (message.sessionId != sessionId || message.senderInstanceId != peerId) return
        if (!transition(
                setOf(WarmupCoordinatorState.DONOR_FORWARDING),
                WarmupCoordinatorState.DONOR_STOPPING,
                expectedSession = message.sessionId
            )) return
        val boundaries = participants.map { (consumerId, participant) ->
            consumerId to scope.async { participant.completeForwarding() }
        }.associate { (id, deferred) -> id to deferred.await() }
        val offsets = boundaries.flatMap { (consumerId, boundary) ->
            boundary.offsets.map { (partition, offset) -> WarmupFinalOffset(consumerId, partition, offset) }
        }
        check(transition(
            setOf(WarmupCoordinatorState.DONOR_STOPPING),
            WarmupCoordinatorState.DONOR_WAITING_COMPLETION
        )) { "Coordinator state changed during forwarding completion" }
        armStageTimeout(message.sessionId, "recipient drain")
        channel.send(ForwardingStopped(
            message.sessionId,
            configuration.instanceId,
            message.senderInstanceId,
            offsets
        ))
    }

    private suspend fun onForwardingStopped(message: ForwardingStopped) {
        if (message.sessionId != sessionId || message.senderInstanceId != peerId) return
        if (!transition(
                setOf(WarmupCoordinatorState.RECIPIENT_STOPPING),
                WarmupCoordinatorState.RECIPIENT_DRAINING,
                expectedSession = message.sessionId
            )) return
        stageTimer?.cancel()
        val assigned = requireNotNull(assignment)
        val reported = message.finalOffsets.associateBy { it.topicPartition }
        check(reported.keys == assigned.partitions.map { it.topicPartition }.toSet()) {
            "Forwarding boundary does not match the session assignment"
        }
        participants.map { (consumerId, participant) -> scope.async {
            val offsets = message.finalOffsets.filter { it.consumerId == consumerId }
                .associate { it.topicPartition to it.offset }
            participant.drainRecipient(WarmupForwardingBoundary(offsets))
        } }.awaitAll()
        participants.values.map { participant -> scope.async { participant.startMainAfterDrain() } }.awaitAll()
        channel.send(SessionCompleted(
            message.sessionId,
            configuration.instanceId,
            message.senderInstanceId,
            participants.keys
        ))
        check(completeRecipientSession(message.sessionId)) {
            "Coordinator state changed during recipient drain"
        }
        startup.complete(WarmupStartupResult.WARMED)
    }

    private suspend fun onCompleted(message: SessionCompleted) {
        if (message.sessionId != sessionId || message.senderInstanceId != peerId) return
        if (state != WarmupCoordinatorState.DONOR_WAITING_COMPLETION) return
        check(message.consumerIds == participants.keys) { "Session completion does not cover all participants" }
        stageTimer?.cancel()
        participants.values.map { participant -> scope.async {
            participant.finishDonor(message.sessionId)
        } }.awaitAll()
        takeAssignmentLease()?.release()
        offeredRequests.remove(message.sessionId)
        assignment = null
        sessionId = null
        peerId = null
        forwardingStarted = false
        check(transition(
            setOf(WarmupCoordinatorState.DONOR_WAITING_COMPLETION),
            WarmupCoordinatorState.MAIN
        )) { "Coordinator state changed during donor release" }
    }

    private suspend fun onSessionFailed(message: SessionFailed) {
        if (message.sessionId != sessionId) {
            offeredRequests.remove(message.sessionId)
            return
        }
        handleOperationFailure(
            IllegalStateException("Warm-up peer failed: ${message.reason}"),
            notifyPeer = false
        )
    }

    private fun fullyCompatible(capability: WarmupDonorCapability): Boolean {
        val requested = participants.values.associateBy { it.configuration.consumerId }
        if (capability.participants.map { it.consumerId }.toSet() != requested.keys) return false
        return capability.participants.all { offered ->
            val expectedTopics = requested.getValue(offered.consumerId).configuration.topicMappings
                .map(WarmupTopicMapping::sourceTopic).toSet()
            offered.topics.map(WarmupTopicCapability::sourceTopic).toSet() == expectedTopics &&
                offered.topics.all(WarmupTopicCapability::compatible)
        }
    }

    private fun validateAssignment(value: WarmupSessionAssignment) {
        check(value.sessionId == sessionId && value.recipientInstanceId == configuration.instanceId &&
            value.donorInstanceId == peerId) { "Assignment identity does not match the selected session" }
        val expected = participants.values.flatMap { participant ->
            participant.configuration.topicMappings.map { participant.configuration.consumerId to it }
        }.associate { (consumerId, mapping) -> (consumerId to mapping.sourceTopic) to mapping.warmupTopic }
        val actual = value.partitions.associate { (it.consumerId to it.sourceTopic) to it.warmupTopic }
        check(actual == expected) { "Assignment does not match registered participant mappings" }
    }

    private fun validateLeaseAssignment(request: WarmupRequest, value: WarmupSessionAssignment) {
        val expected = request.participants.flatMap { participant ->
            participant.topicMappings.map { (participant.consumerId to it.sourceTopic) to it.warmupTopic }
        }.toMap()
        val actual = value.partitions.associate { (it.consumerId to it.sourceTopic) to it.warmupTopic }
        check(actual == expected) { "Global assignment lease does not cover the requested mappings" }
    }

    private fun armStageTimeout(expectedSession: WarmupSessionId, stage: String) {
        stageTimer?.cancel()
        stageTimer = scope.launch {
            delay(configuration.stageTimeout)
            if (sessionId == expectedSession && state !in setOf(
                    WarmupCoordinatorState.MAIN,
                    WarmupCoordinatorState.FAILED,
                    WarmupCoordinatorState.STOPPED
                )) {
                stageTimer = null
                handleOperationFailure(
                    IllegalStateException(
                        "Warm-up coordinator ${configuration.instanceId.value} timed out waiting for $stage in ${currentState()}"
                    ),
                    notifyPeer = true
                )
            }
        }
    }

    private suspend fun runGuarded(operation: suspend () -> Unit) {
        try {
            operation()
        } catch (error: Throwable) {
            handleOperationFailure(error, notifyPeer = true)
        }
    }

    private suspend fun handleOperationFailure(error: Throwable, notifyPeer: Boolean) {
        when {
            !forwardingStarted && currentState() in RECIPIENT_PRE_FORWARDING_STATES ->
                fallbackBeforeForwarding(error, notifyPeer)
            !forwardingStarted && currentState() in DONOR_PRE_FORWARDING_STATES ->
                abortDonorBeforeForwarding(error, notifyPeer)
            else -> fail(error, notifyPeer)
        }
    }

    private suspend fun onControlFailure(error: Throwable) {
        when {
            currentState() in setOf(WarmupCoordinatorState.CONTROL_STARTING) ->
                fallbackBeforeForwarding(error, notifyPeer = false)
            !forwardingStarted && currentState() in RECIPIENT_PRE_FORWARDING_STATES ->
                fallbackBeforeForwarding(error, notifyPeer = false)
            !forwardingStarted && currentState() in DONOR_PRE_FORWARDING_STATES ->
                abortDonorBeforeForwarding(error, notifyPeer = false)
            currentState() == WarmupCoordinatorState.DONOR_ABORTING -> Unit
            currentState() == WarmupCoordinatorState.MAIN && sessionId == null -> Unit
            else -> fail(error, notifyPeer = false)
        }
    }

    internal suspend fun onParticipantFailure(consumerId: WarmupConsumerId, error: Throwable) {
        val failure = IllegalStateException("Warm-up consumer $consumerId failed", error)
        if (!forwardingStarted && currentState() == WarmupCoordinatorState.RECIPIENT_STARTING) {
            fallbackBeforeForwarding(failure, notifyPeer = true)
        } else {
            fail(failure, notifyPeer = true)
        }
    }

    private suspend fun fallbackBeforeForwarding(error: Throwable, notifyPeer: Boolean) {
        if (forwardingStarted || !transition(
                RECIPIENT_PRE_FORWARDING_STATES + WarmupCoordinatorState.CONTROL_STARTING,
                WarmupCoordinatorState.FALLBACK_STARTING
            )) return
        stageTimer?.cancel()
        val failedSession = sessionId
        val target = peerId
        if (notifyPeer && failedSession != null) sendFailure(failedSession, target, error)
        try {
            participants.values.map { participant -> scope.async { participant.prepareFallback() } }.awaitAll()
            if (currentState() != WarmupCoordinatorState.FALLBACK_STARTING) return
            participants.values.map { participant -> scope.async { participant.startMain() } }.awaitAll()
            sessionId = null
            peerId = null
            assignment = null
            offers.clear()
            if (transition(setOf(WarmupCoordinatorState.FALLBACK_STARTING), WarmupCoordinatorState.MAIN)) {
                startup.complete(WarmupStartupResult.NO_DONOR_FALLBACK)
            }
        } catch (fallbackError: Throwable) {
            fallbackError.addSuppressed(error)
            fail(fallbackError, notifyPeer = false)
        }
    }

    private suspend fun abortDonorBeforeForwarding(error: Throwable, notifyPeer: Boolean) {
        if (!transition(DONOR_PRE_FORWARDING_STATES, WarmupCoordinatorState.DONOR_ABORTING)) return
        stageTimer?.cancel()
        val failedSession = sessionId
        val target = peerId
        participants.values.map { participant -> scope.async {
            runCatching { participant.releaseDonorReservation() }
        } }.awaitAll()
        takeAssignmentLease()?.release()
        assignment = null
        sessionId = null
        peerId = null
        if (notifyPeer && failedSession != null) sendFailure(failedSession, target, error)
        transition(setOf(WarmupCoordinatorState.DONOR_ABORTING), WarmupCoordinatorState.MAIN)
    }

    private suspend fun sendFailure(
        failedSession: WarmupSessionId,
        target: WarmupInstanceId?,
        error: Throwable
    ) {
        runCatching {
            channel.send(SessionFailed(
                failedSession,
                configuration.instanceId,
                target,
                error.message?.take(4096)?.ifBlank { error.javaClass.simpleName }
                    ?: error.javaClass.simpleName
            ))
        }
    }

    private fun currentState(): WarmupCoordinatorState = synchronized(stateLock) { state }

    private fun beginDonorReservation(selected: DonorSelected): Boolean = synchronized(stateLock) {
        if (state != WarmupCoordinatorState.MAIN) return false
        state = WarmupCoordinatorState.DONOR_RESERVING
        sessionId = selected.sessionId
        peerId = selected.senderInstanceId
        true
    }

    private fun completeRejectedReservation(expectedSession: WarmupSessionId): Boolean = synchronized(stateLock) {
        if (state != WarmupCoordinatorState.DONOR_RESERVING || sessionId != expectedSession) return false
        sessionId = null
        peerId = null
        state = WarmupCoordinatorState.MAIN
        true
    }

    private fun installAssignmentLease(
        expectedSession: WarmupSessionId,
        lease: WarmupAssignmentLease
    ): Boolean = synchronized(stateLock) {
        if (state != WarmupCoordinatorState.DONOR_RESERVING || sessionId != expectedSession ||
            assignmentLease != null) return false
        assignmentLease = lease
        true
    }

    private fun completeRecipientSession(expectedSession: WarmupSessionId): Boolean = synchronized(stateLock) {
        if (state != WarmupCoordinatorState.RECIPIENT_DRAINING || sessionId != expectedSession) return false
        assignment = null
        sessionId = null
        peerId = null
        forwardingStarted = false
        state = WarmupCoordinatorState.MAIN
        true
    }

    private fun takeAssignmentLease(): WarmupAssignmentLease? = synchronized(stateLock) {
        assignmentLease.also { assignmentLease = null }
    }

    private fun transition(
        expected: Set<WarmupCoordinatorState>,
        next: WarmupCoordinatorState,
        expectedSession: WarmupSessionId? = null
    ): Boolean = synchronized(stateLock) {
        if (state !in expected || (expectedSession != null && sessionId != expectedSession)) return false
        state = next
        true
    }

    private suspend fun fail(error: Throwable, notifyPeer: Boolean) {
        val shouldFail = synchronized(stateLock) {
            if (state == WarmupCoordinatorState.STOPPED || !failureStarted.compareAndSet(false, true)) {
                false
            } else {
                state = WarmupCoordinatorState.FAILED
                true
            }
        }
        if (!shouldFail) return
        stageTimer?.cancel()
        val failedSession = sessionId
        val target = peerId
        if (notifyPeer && failedSession != null) sendFailure(failedSession, target, error)
        takeAssignmentLease()?.runCatching { release() }
        participants.values.map { participant -> scope.async { runCatching { participant.stop() } } }.awaitAll()
        startup.completeExceptionally(error)
        termination.complete(error)
    }

    private companion object {
        val RECIPIENT_PRE_FORWARDING_STATES = setOf(
            WarmupCoordinatorState.SEEKING_DONOR,
            WarmupCoordinatorState.WAITING_FOR_ASSIGNMENT,
            WarmupCoordinatorState.RECIPIENT_STARTING,
            WarmupCoordinatorState.RECIPIENT_WARMING
        )
        val DONOR_PRE_FORWARDING_STATES = setOf(
            WarmupCoordinatorState.DONOR_RESERVING,
            WarmupCoordinatorState.DONOR_RESERVED
        )
    }
}
