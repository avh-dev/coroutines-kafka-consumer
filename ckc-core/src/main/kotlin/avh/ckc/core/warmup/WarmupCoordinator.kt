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
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

fun interface WarmupPartitionAllocator {
    fun partition(consumerId: WarmupConsumerId, mapping: WarmupTopicMapping): Int
}

class WarmupCoordinatorConfiguration(
    val instanceId: WarmupInstanceId,
    val donorSearchTimeout: Duration = 5.seconds,
    val stageTimeout: Duration = 30.seconds,
    val donorLoadScore: () -> Double? = { null },
    val partitionAllocator: WarmupPartitionAllocator = WarmupPartitionAllocator { _, _ -> 0 }
) {
    init {
        require(donorSearchTimeout.isPositive()) { "donorSearchTimeout must be positive" }
        require(stageTimeout.isPositive()) { "stageTimeout must be positive" }
    }
}

enum class WarmupStartupResult { WARMED, NO_DONOR_FALLBACK }

enum class WarmupCoordinatorState {
    NEW,
    SEEKING_DONOR,
    WAITING_FOR_ASSIGNMENT,
    RECIPIENT_STARTING,
    RECIPIENT_WARMING,
    RECIPIENT_STOPPING,
    RECIPIENT_DRAINING,
    MAIN,
    DONOR_RESERVED,
    DONOR_FORWARDING,
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
    private val started = AtomicBoolean(false)
    private val startup = CompletableDeferred<WarmupStartupResult>()
    private val offers = ConcurrentHashMap<WarmupInstanceId, WarmupDonorCapability>()
    private val offeredRequests = ConcurrentHashMap<WarmupSessionId, WarmupRequest>()
    private val failureStarted = AtomicBoolean(false)
    @Volatile private var state = WarmupCoordinatorState.NEW
    @Volatile private var sessionId: WarmupSessionId? = null
    @Volatile private var peerId: WarmupInstanceId? = null
    @Volatile private var assignment: WarmupSessionAssignment? = null
    @Volatile private var stageTimer: Job? = null

    fun <K, V> register(definition: WarmupConsumerDefinition<K, V>) {
        check(state == WarmupCoordinatorState.NEW && !started.get()) {
            "Warm-up participants must be registered before coordinator start"
        }
        val participant = KafkaCoordinatorParticipant(definition)
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
        check(state == WarmupCoordinatorState.NEW) { "Warm-up coordinator cannot start from state $state" }
        check(participants.isNotEmpty()) { "At least one warm-up participant must be registered" }
        check(started.compareAndSet(false, true)) { "Warm-up coordinator already started" }
        channel.start()
        try {
            channel.awaitReady()
            scope.launch {
                try {
                    channel.awaitTermination()
                    if (state != WarmupCoordinatorState.STOPPED) {
                        fail(IllegalStateException("Warm-up control transport terminated"), notifyPeer = false)
                    }
                } catch (error: Throwable) {
                    fail(error, notifyPeer = false)
                }
            }
            val newSession = WarmupSessionId.random()
            sessionId = newSession
            state = WarmupCoordinatorState.SEEKING_DONOR
            channel.send(WarmupRequest(
                sessionId = newSession,
                senderInstanceId = configuration.instanceId,
                participants = participants.values.map(CoordinatorParticipant::configuration)
            ))
            stageTimer = scope.launch {
                delay(configuration.donorSearchTimeout)
                stageTimer = null
                selectDonorOrFallback(newSession)
            }
            return startup.await()
        } catch (error: Throwable) {
            fail(error, notifyPeer = false)
            throw error
        }
    }

    fun state(): WarmupCoordinatorState = state

    suspend fun stop() {
        if (state == WarmupCoordinatorState.STOPPED) return
        state = WarmupCoordinatorState.STOPPED
        stageTimer?.cancel()
        participants.values.map { participant -> scope.async { runCatching { participant.stop() } } }.awaitAll()
        channel.stop()
        scope.cancel()
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
            fail(error, notifyPeer = message !is SessionFailed)
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
        if (state != WarmupCoordinatorState.SEEKING_DONOR || sessionId != expectedSession) return
        val selected = offers.values.maxWithOrNull(
            compareBy<WarmupDonorCapability> { it.loadScore ?: 0.0 }
                .thenBy { capability -> capability.participants.sumOf { p -> p.topics.count { it.hasObservedTraffic } } }
                .thenBy { capability -> capability.participants.sumOf { p -> p.topics.sumOf { it.ownedPartitions.size } } }
        )
        if (selected == null) {
            participants.values.forEach { it.startMain() }
            sessionId = null
            offers.clear()
            state = WarmupCoordinatorState.MAIN
            startup.complete(WarmupStartupResult.NO_DONOR_FALLBACK)
            return
        }
        peerId = selected.instanceId
        offers.clear()
        state = WarmupCoordinatorState.WAITING_FOR_ASSIGNMENT
        channel.send(DonorSelected(expectedSession, configuration.instanceId, selected.instanceId))
        armStageTimeout(expectedSession, "donor assignment")
    }

    private suspend fun onSelected(selected: DonorSelected) {
        val request = offeredRequests[selected.sessionId] ?: return
        if (state != WarmupCoordinatorState.MAIN) return
        val partitions = request.participants.flatMap { participant ->
            participant.topicMappings.map { mapping ->
                WarmupPartitionAssignment(
                    consumerId = participant.consumerId,
                    sourceTopic = mapping.sourceTopic,
                    warmupTopic = mapping.warmupTopic,
                    warmupPartition = configuration.partitionAllocator.partition(participant.consumerId, mapping)
                )
            }
        }
        val assigned = WarmupSessionAssignment(
            selected.sessionId,
            configuration.instanceId,
            selected.senderInstanceId,
            partitions
        )
        participants.values.forEach { participant ->
            participant.reserveDonor(assigned.assignmentsFor(participant.configuration.consumerId))
        }
        sessionId = selected.sessionId
        peerId = selected.senderInstanceId
        assignment = assigned
        state = WarmupCoordinatorState.DONOR_RESERVED
        channel.send(WarmupAssigned(
            selected.sessionId,
            configuration.instanceId,
            selected.senderInstanceId,
            assigned
        ))
        armStageTimeout(selected.sessionId, "recipient readiness")
    }

    private suspend fun onAssigned(message: WarmupAssigned) {
        if (message.sessionId != sessionId || message.senderInstanceId != peerId) return
        if (state != WarmupCoordinatorState.WAITING_FOR_ASSIGNMENT) return
        validateAssignment(message.assignment)
        stageTimer?.cancel()
        assignment = message.assignment
        state = WarmupCoordinatorState.RECIPIENT_STARTING
        participants.values.map { participant -> scope.async {
            participant.startRecipient(message.assignment.assignmentsFor(participant.configuration.consumerId))
        } }.awaitAll()
        state = WarmupCoordinatorState.RECIPIENT_WARMING
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
            if (state == WarmupCoordinatorState.RECIPIENT_WARMING && sessionId == message.sessionId) {
                stageTimer = null
                state = WarmupCoordinatorState.RECIPIENT_STOPPING
                channel.send(WarmupFinished(
                    message.sessionId,
                    configuration.instanceId,
                    message.senderInstanceId
                ))
                armStageTimeout(message.sessionId, "forwarding completion")
            }
        }
    }

    private suspend fun onReady(message: RecipientReady) {
        if (state != WarmupCoordinatorState.DONOR_RESERVED || message.sessionId != sessionId ||
            message.senderInstanceId != peerId) return
        check(message.consumerIds == participants.keys) { "Recipient readiness does not cover all participants" }
        stageTimer?.cancel()
        participants.values.forEach { it.activateDonor(message.sessionId) }
        state = WarmupCoordinatorState.DONOR_FORWARDING
    }

    private suspend fun onFinished(message: WarmupFinished) {
        if (message.sessionId != sessionId || message.senderInstanceId != peerId) return
        if (state != WarmupCoordinatorState.DONOR_FORWARDING) return
        val boundaries = participants.map { (consumerId, participant) ->
            consumerId to scope.async { participant.completeForwarding() }
        }.associate { (id, deferred) -> id to deferred.await() }
        val offsets = boundaries.flatMap { (consumerId, boundary) ->
            boundary.offsets.map { (partition, offset) -> WarmupFinalOffset(consumerId, partition, offset) }
        }
        state = WarmupCoordinatorState.DONOR_WAITING_COMPLETION
        channel.send(ForwardingStopped(
            message.sessionId,
            configuration.instanceId,
            message.senderInstanceId,
            offsets
        ))
        armStageTimeout(message.sessionId, "recipient drain")
    }

    private suspend fun onForwardingStopped(message: ForwardingStopped) {
        if (message.sessionId != sessionId || message.senderInstanceId != peerId) return
        if (state != WarmupCoordinatorState.RECIPIENT_STOPPING) return
        stageTimer?.cancel()
        val assigned = requireNotNull(assignment)
        val reported = message.finalOffsets.associateBy { it.topicPartition }
        check(reported.keys == assigned.partitions.map { it.topicPartition }.toSet()) {
            "Forwarding boundary does not match the session assignment"
        }
        state = WarmupCoordinatorState.RECIPIENT_DRAINING
        participants.map { (consumerId, participant) -> scope.async {
            val offsets = message.finalOffsets.filter { it.consumerId == consumerId }
                .associate { it.topicPartition to it.offset }
            participant.drainRecipient(WarmupForwardingBoundary(offsets))
        } }.awaitAll()
        participants.values.map { participant -> scope.async { participant.startMainAfterDrain() } }.awaitAll()
        state = WarmupCoordinatorState.MAIN
        channel.send(SessionCompleted(
            message.sessionId,
            configuration.instanceId,
            message.senderInstanceId,
            participants.keys
        ))
        assignment = null
        sessionId = null
        peerId = null
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
        offeredRequests.remove(message.sessionId)
        assignment = null
        sessionId = null
        peerId = null
        state = WarmupCoordinatorState.MAIN
    }

    private suspend fun onSessionFailed(message: SessionFailed) {
        if (message.sessionId != sessionId && !offeredRequests.containsKey(message.sessionId)) return
        fail(IllegalStateException("Warm-up peer failed: ${message.reason}"), notifyPeer = false)
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
                fail(IllegalStateException("Timed out waiting for $stage"), notifyPeer = true)
            }
        }
    }

    private suspend fun fail(error: Throwable, notifyPeer: Boolean) {
        if (state == WarmupCoordinatorState.STOPPED || !failureStarted.compareAndSet(false, true)) return
        state = WarmupCoordinatorState.FAILED
        stageTimer?.cancel()
        val failedSession = sessionId
        val target = peerId
        if (notifyPeer && failedSession != null) {
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
        participants.values.map { participant -> scope.async { runCatching { participant.stop() } } }.awaitAll()
        startup.completeExceptionally(error)
    }
}
