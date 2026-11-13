package avh.ckc.core.warmup

import avh.ckc.core.ManagedConsumerLifecycle
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import java.security.MessageDigest
import java.util.ArrayDeque
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.CoroutineContext
import kotlin.math.max
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/** Configuration shared by every consumer registered in one application warm-up context. */
class TrafficWarmupContextConfiguration {
    var instanceId: WarmupInstanceId = WarmupInstanceId.random()
    var controlTopic: String = ""
    var controlKafkaProperties: Map<String, Any?> = emptyMap()
    var quotaPerInterval: Int = 10
    /** Null keeps the quota fixed at [quotaPerInterval]. */
    var initialQuotaPerInterval: Int? = null
    var quotaInterval: Duration = 1.seconds
    var quotaRampDuration: Duration = Duration.ZERO
    var warmupDuration: Duration = 120.seconds
    var delegationTimeout: Duration = 30.seconds
    var offerWindow: Duration = 2.seconds
    var negotiationTimeout: Duration = 10.seconds
    var cleanupTimeout: Duration = 30.seconds
    var controlReadinessTimeout: Duration = 30.seconds
    var completionBufferCapacity: Int = 1_024
    var controlObservationLimit: Int = 10_000
    /** Maximum warm-up recipients admitted concurrently by this context. */
    var maxDonorSessions: Int = 4
    /** Aggregate interval quota cap across all concurrently active donor sessions. */
    var maxDonorDelegationsPerInterval: Int = 40
    var parentContext: CoroutineContext = Dispatchers.Default
}

/** Per-consumer mapping and Kafka settings; the application never creates the recipient consumer. */
class TrafficWarmupConsumerConfiguration internal constructor() {
    internal val mappings = linkedMapOf<String, String>()

    /** Kafka producer properties, including serializers, for copies sent to warm-up topics. */
    var forwardingProducerProperties: Map<String, Any?> = emptyMap()

    /** Optional overrides for the library-owned warm-up consumer. Main deserializers are reused by default. */
    var warmupConsumerProperties: Map<String, Any?> = emptyMap()

    var forwardingObserver: WarmupForwardingObserver = WarmupForwardingObserver { }

    /** Maps one main topic to a warm-up topic created by infrastructure. */
    fun mapTopic(sourceTopic: String, warmupTopic: String) {
        require(mappings.putIfAbsent(sourceTopic, warmupTopic) == null) {
            "Source topic $sourceTopic already has a warm-up mapping"
        }
    }
}

fun trafficWarmupContext(
    block: TrafficWarmupContextConfiguration.() -> Unit
): TrafficWarmupContext = TrafficWarmupContext(TrafficWarmupContextConfiguration().apply(block))

/**
 * Owns one control consumer and the warm-up/main transition for all locally registered consumers.
 * Registration is frozen by the first consumer start, so applications should construct the context's consumers first.
 */
class TrafficWarmupContext internal constructor(
    private val configuration: TrafficWarmupContextConfiguration,
    transportFactory: ((WarmupControlMessageHandler) -> WarmupControlChannel)? = null,
    private val enforceDistributedProtocol: Boolean = transportFactory == null
) : ManagedConsumerLifecycle {
    private class MainConsumerStartException(cause: Throwable) :
        IllegalStateException("Failed to start main consumers after warm-up", cause)

    private data class Observed(
        val record: WarmupControlRecord,
        val decision: WarmupTableDecision
    )

    private data class DonorSession(
        val recipient: WarmupInstanceId,
        val prepared: List<PreparedWarmupDonor>,
        val activationStarted: AtomicBoolean = AtomicBoolean(false)
    ) {
        private val stateLock = Any()
        private var activated = false
        private var closing = false
        private var pendingCompletion: WarmupCompletionRequested? = null

        val preparedByDestination: Map<TopicPartition, PreparedWarmupDonor> = buildMap {
            prepared.forEach { donor ->
                donor.assignments.forEach { assignment ->
                    check(put(assignment.destination, donor) == null) {
                        "Multiple warm-up donors own destination ${assignment.destination}"
                    }
                }
            }
        }

        fun completeActivation(): WarmupCompletionRequested? = synchronized(stateLock) {
            activated = true
            pendingCompletion?.takeUnless { closing }?.also {
                pendingCompletion = null
                closing = true
            }
        }

        fun requestCompletion(request: WarmupCompletionRequested): Boolean = synchronized(stateLock) {
            if (closing) return@synchronized false
            if (activated) {
                closing = true
                true
            } else {
                if (pendingCompletion == null) pendingCompletion = request
                false
            }
        }
    }

    private val lifecycleJob = SupervisorJob(configuration.parentContext[Job])
    private val scope = CoroutineScope(
        configuration.parentContext.minusKey(Job) + lifecycleJob + CoroutineName("TrafficWarmupContext")
    )
    private val shutdownScope = CoroutineScope(
        configuration.parentContext.minusKey(Job) + SupervisorJob() + CoroutineName("TrafficWarmupContextShutdown")
    )
    private val router = WarmupControlRouter(configuration.instanceId)
    private val sessionTable = WarmupSessionTable()
    private val participants = linkedMapOf<WarmupConsumerId, RegisteredWarmupParticipant>()
    private val observations = ArrayDeque<Observed>()
    private val observationSignal = Channel<Unit>(Channel.CONFLATED)
    private val donorSessions = linkedMapOf<WarmupSessionId, DonorSession>()
    private val donorAdmissions = linkedMapOf<WarmupSessionId, Int>()
    private val recipientPrepared = mutableListOf<PreparedWarmupRecipient>()
    private val started = AtomicBoolean(false)
    private val stopping = AtomicBoolean(false)
    private val ready = CompletableDeferred<Unit>()
    private val termination = CompletableDeferred<Throwable?>()
    private val lifecycleFailure = AtomicReference<Throwable?>()
    private val shutdownJob = AtomicReference<Job?>()
    private val activeRecipientRequest = AtomicReference<WarmupRequested?>()
    private val activeReservationId = AtomicReference<WarmupReservationId?>()
    private lateinit var fingerprint: String
    private var recipientJob: Job? = null
    private val transport: WarmupControlChannel

    init {
        validateConfiguration()
        router.registerGlobal(::onControlRecord)
        transport = transportFactory?.invoke(router) ?: WarmupControlTransport(
            configuration = WarmupControlTransportConfiguration(
                instanceId = configuration.instanceId,
                topic = configuration.controlTopic,
                producerProperties = configuration.controlKafkaProperties,
                consumerProperties = configuration.controlKafkaProperties
            ),
            handler = router,
            parentContext = scope.coroutineContext
        )
    }

    internal fun register(participant: RegisteredWarmupParticipant) {
        check(!started.get()) { "Warm-up consumers must be registered before the context starts" }
        check(participants.putIfAbsent(participant.consumerId, participant) == null) {
            "Warm-up consumer id ${participant.consumerId.value} is already registered"
        }
    }

    internal fun participantSettings(): ParticipantSettings = ParticipantSettings(
        scope = scope,
        quota = configuration.quotaPerInterval,
        initialQuota = configuration.initialQuotaPerInterval ?: configuration.quotaPerInterval,
        quotaInterval = configuration.quotaInterval,
        quotaRampDuration = configuration.quotaRampDuration,
        delegationTimeout = configuration.delegationTimeout,
        cleanupTimeout = configuration.cleanupTimeout,
        completionBufferCapacity = configuration.completionBufferCapacity
    )

    internal fun recipientConsumerProperties(
        mainProperties: Map<String, Any?>,
        overrides: Map<String, Any?>,
        consumerId: WarmupConsumerId
    ): Map<String, Any?> {
        val uniqueId = "ckc.traffic-warmup.v1.${configuration.instanceId.value}.${consumerId.value}.${UUID.randomUUID()}"
        return (mainProperties + overrides - ConsumerConfig.GROUP_INSTANCE_ID_CONFIG) + mapOf(
            ConsumerConfig.GROUP_ID_CONFIG to uniqueId,
            ConsumerConfig.CLIENT_ID_CONFIG to uniqueId
        )
    }

    internal data class ParticipantSettings(
        val scope: CoroutineScope,
        val quota: Int,
        val initialQuota: Int,
        val quotaInterval: Duration,
        val quotaRampDuration: Duration,
        val delegationTimeout: Duration,
        val cleanupTimeout: Duration,
        val completionBufferCapacity: Int
    )

    override fun start() {
        if (!started.compareAndSet(false, true)) return
        check(participants.isNotEmpty()) { "Warm-up context has no registered consumers" }
        validateMappings()
        fingerprint = configurationFingerprint()
        transport.start()
        recipientJob = scope.launch {
            try {
                withTimeout(configuration.controlReadinessTimeout) { transport.awaitReady() }
                if (!enforceDistributedProtocol) {
                    sessionTable.markBootstrapAuthoritative(transport.initialOffsetBoundary() ?: -1L)
                } else {
                    scheduleBootstrapAuthority()
                }
                runRecipientSession()
            } catch (timeout: TimeoutCancellationException) {
                fallbackToCold(timeout)
            } catch (cancelled: CancellationException) {
                if (!stopping.get()) fail(cancelled)
                throw cancelled
            } catch (error: MainConsumerStartException) {
                fail(error)
            } catch (error: Throwable) {
                fallbackToCold(error)
            }
        }
    }

    override suspend fun awaitReady() {
        check(started.get()) { "Warm-up context has not started" }
        ready.await()
    }

    override suspend fun stop() {
        shutdownOperation().join()
        termination.await()?.let { throw it }
    }

    private fun shutdownOperation(): Job {
        shutdownJob.get()?.let { return it }
        val created = shutdownScope.launch(start = CoroutineStart.LAZY) { shutdown() }
        if (shutdownJob.compareAndSet(null, created)) {
            created.start()
            return created
        }
        created.cancel()
        return requireNotNull(shutdownJob.get())
    }

    private suspend fun shutdown() {
        stopping.set(true)
        recipientJob?.cancelAndJoin()
        lifecycleJob.cancel()
        val failures = mutableListOf<Throwable>()
        lifecycleFailure.get()?.let(failures::add)
        recipientPrepared.toList().forEach { prepared ->
            runCatching { prepared.stop() }.exceptionOrNull()?.let(failures::add)
        }
        synchronized(donorSessions) {
            donorSessions.values.flatMap(DonorSession::prepared).toList().also { donorSessions.clear() }
        }.forEach { prepared -> runCatching { prepared.finish() }.exceptionOrNull()?.let(failures::add) }
        participants.values.forEach { participant ->
            runCatching { participant.stop() }.exceptionOrNull()?.let(failures::add)
        }
        runCatching { transport.stop() }.exceptionOrNull()?.let(failures::add)
        if (!ready.isCompleted) ready.completeExceptionally(CancellationException("Warm-up context stopped"))
        val failure = failures.firstOrNull()?.also { first ->
            failures.drop(1).filter { it !== first }.forEach(first::addSuppressed)
        }
        termination.complete(failure)
    }

    override suspend fun awaitTermination() {
        termination.await()?.let { throw it }
    }

    private suspend fun onControlRecord(record: WarmupControlRecord) {
        val decision = sessionTable.apply(record)
        synchronized(observations) {
            observations.addLast(Observed(record, decision))
            while (observations.size > configuration.controlObservationLimit) observations.removeFirst()
        }
        observationSignal.trySend(Unit)
        val message = record.message
        if (message is WarmupRequested && message.senderInstanceId != configuration.instanceId) {
            scope.launch { offer(message) }
        }
        if (message.targetInstanceId == null || message.targetInstanceId == configuration.instanceId) {
            when (message) {
                is SessionAccepted, is RecipientReady, is WarmupCompletionRequested, is ControlRecordCompleted ->
                    handleAccepted(message)
                is SessionEnded -> endDonor(message)
                else -> Unit
            }
        }
    }

    private fun handleAccepted(message: WarmupControlMessage) {
        when (message) {
            is SessionAccepted -> scope.launch { acceptAsDonor(message) }
            is RecipientReady -> scope.launch { activateDonor(message) }
            is WarmupCompletionRequested -> requestDonorClose(message)
            is ControlRecordCompleted -> synchronized(donorSessions) {
                donorSessions[message.sessionId]
                    ?.preparedByDestination
                    ?.get(message.completed.recordId.topicPartition)
                    ?.accept(message.completed)
            }
            else -> Unit
        }
    }

    private suspend fun offer(request: WarmupRequested) {
        if (request.configurationFingerprint != fingerprint) return
        val snapshot = sessionTable.snapshot() ?: return
        val candidate = participants.values.flatMap { it.donorMappings(request.mappings) }
        val requestedBudget = candidate.map { it.mapping.consumerId }.distinct().size * configuration.quotaPerInterval
        val hasCapacity = synchronized(donorSessions) {
            val activeBudget = donorSessions.values.sumOf { it.prepared.size * configuration.quotaPerInterval } +
                donorAdmissions.values.sum()
            donorSessions.size + donorAdmissions.size < configuration.maxDonorSessions &&
                activeBudget + requestedBudget <= configuration.maxDonorDelegationsPerInterval
        }
        val offered = if (hasCapacity) candidate else emptyList()
        send(
            DonorOffered(
                request.sessionId,
                configuration.instanceId,
                request.senderInstanceId,
                offered,
                fingerprint,
                request.expiresAtEpochMillis,
                stateSnapshot = snapshot
            )
        )
    }

    private suspend fun acceptAsDonor(accepted: SessionAccepted) {
        if (accepted.configurationFingerprint != fingerprint) return
        var alreadyKnown = false
        val admitted = synchronized(donorSessions) {
            if (donorSessions.containsKey(accepted.sessionId) || donorAdmissions.containsKey(accepted.sessionId)) {
                alreadyKnown = true
                return@synchronized false
            }
            val nextCount = donorSessions.size + donorAdmissions.size + 1
            val requestedBudget = accepted.assignments.map { it.mapping.consumerId }.distinct().size *
                configuration.quotaPerInterval
            val activeBudget = donorSessions.values.sumOf { it.prepared.size * configuration.quotaPerInterval } +
                donorAdmissions.values.sum()
            if (nextCount > configuration.maxDonorSessions || activeBudget + requestedBudget >
                configuration.maxDonorDelegationsPerInterval) return@synchronized false
            donorAdmissions[accepted.sessionId] = requestedBudget
            true
        }
        if (alreadyKnown) return
        if (!admitted) {
            send(
                SessionRejected(
                    accepted.sessionId, configuration.instanceId, accepted.senderInstanceId,
                    "donor concurrent-session or aggregate-quota limit reached",
                    fingerprint, accepted.expiresAtEpochMillis
                )
            )
            return
        }
        val prepared = mutableListOf<PreparedWarmupDonor>()
        try {
            accepted.assignments.groupBy { it.mapping.consumerId }.forEach { (id, assignments) ->
                val participant = requireNotNull(participants[id]) { "Unknown local consumer ${id.value}" }
                val currentlyOwned = participant.donorMappings(assignments.map { it.mapping })
                    .mapTo(mutableSetOf()) { it.mapping }
                require(assignments.all { it.mapping in currentlyOwned }) {
                    "Donor no longer owns the accepted source mapping for ${id.value}"
                }
                prepared += participant.prepareDonor(accepted.sessionId, assignments)
            }
            synchronized(donorSessions) {
                donorAdmissions.remove(accepted.sessionId)
                donorSessions[accepted.sessionId] = DonorSession(
                    accepted.senderInstanceId,
                    prepared
                )
            }
            scheduleDonorExpiry(accepted.sessionId, accepted.expiresAtEpochMillis)
            send(
                DonorAccepted(
                    accepted.sessionId,
                    configuration.instanceId,
                    accepted.senderInstanceId,
                    fingerprint,
                    accepted.expiresAtEpochMillis
                )
            )
        } catch (cancelled: CancellationException) {
            synchronized(donorSessions) {
                donorAdmissions.remove(accepted.sessionId)
                donorSessions.remove(accepted.sessionId)
            }
            prepared.forEach(PreparedWarmupDonor::finish)
            if (stopping.get()) throw cancelled
            send(
                SessionRejected(
                    accepted.sessionId,
                    configuration.instanceId,
                    accepted.senderInstanceId,
                    "donor preparation was cancelled",
                    fingerprint,
                    accepted.expiresAtEpochMillis
                )
            )
        } catch (error: Throwable) {
            synchronized(donorSessions) {
                donorAdmissions.remove(accepted.sessionId)
                donorSessions.remove(accepted.sessionId)
            }
            prepared.forEach(PreparedWarmupDonor::finish)
            send(
                SessionRejected(
                    accepted.sessionId,
                    configuration.instanceId,
                    accepted.senderInstanceId,
                    error.message ?: "donor preparation failed",
                    fingerprint,
                    accepted.expiresAtEpochMillis
                )
            )
        }
    }

    private suspend fun activateDonor(readyMessage: RecipientReady) {
        val donor = synchronized(donorSessions) { donorSessions[readyMessage.sessionId] } ?: return
        if (donor.recipient != readyMessage.senderInstanceId) return
        if (!donor.activationStarted.compareAndSet(false, true)) return
        try {
            require(
                readyMessage.assignments.toSet() == donor.prepared.flatMap(PreparedWarmupDonor::assignments).toSet()
            ) { "Recipient readiness assignments differ from the accepted donor assignments" }
            donor.prepared.forEach(PreparedWarmupDonor::activate)
            donor.completeActivation()?.let { performDonorClose(donor, it) }
        } catch (cancelled: CancellationException) {
            if (stopping.get()) {
                finishDonor(readyMessage.sessionId)
                throw cancelled
            }
            runCatching { abortDonor(readyMessage.sessionId, "donor forwarding was cancelled") }
            send(
                SessionRejected(
                    readyMessage.sessionId,
                    configuration.instanceId,
                    readyMessage.senderInstanceId,
                    "donor forwarding was cancelled",
                    fingerprint,
                    readyMessage.expiresAtEpochMillis
                )
            )
        } catch (error: Throwable) {
            runCatching { abortDonor(readyMessage.sessionId, error.message ?: "donor forwarding failed") }
            send(
                SessionRejected(
                    readyMessage.sessionId,
                    configuration.instanceId,
                    readyMessage.senderInstanceId,
                    error.message ?: "donor forwarding failed",
                    fingerprint,
                    readyMessage.expiresAtEpochMillis
                )
            )
        }
    }

    private fun requestDonorClose(request: WarmupCompletionRequested) {
        val donor = synchronized(donorSessions) { donorSessions[request.sessionId] } ?: return
        if (donor.recipient != request.senderInstanceId) return
        if (!donor.requestCompletion(request)) return
        scope.launch { performDonorClose(donor, request) }
    }

    private suspend fun performDonorClose(donor: DonorSession, request: WarmupCompletionRequested) {
        try {
            donor.prepared.forEach(PreparedWarmupDonor::stopDelegating)
            withTimeout(configuration.cleanupTimeout) {
                donor.prepared.forEach { it.awaitInflight(configuration.cleanupTimeout) }
            }
            send(
                DonorClosed(
                    request.sessionId,
                    configuration.instanceId,
                    request.senderInstanceId,
                    fingerprint,
                    request.expiresAtEpochMillis
                )
            )
        } catch (timeout: TimeoutCancellationException) {
            runCatching { abortDonor(request.sessionId, "donor completion timed out") }
            send(
                SessionRejected(
                    request.sessionId,
                    configuration.instanceId,
                    request.senderInstanceId,
                    "donor completion timed out",
                    fingerprint,
                    request.expiresAtEpochMillis
                )
            )
        } catch (cancelled: CancellationException) {
            if (stopping.get()) {
                finishDonor(request.sessionId)
                throw cancelled
            }
            runCatching { abortDonor(request.sessionId, "donor completion was cancelled") }
            send(
                SessionRejected(
                    request.sessionId,
                    configuration.instanceId,
                    request.senderInstanceId,
                    "donor completion was cancelled",
                    fingerprint,
                    request.expiresAtEpochMillis
                )
            )
        } catch (error: Throwable) {
            runCatching { abortDonor(request.sessionId, error.message ?: "donor completion failed") }
            send(
                SessionRejected(
                    request.sessionId,
                    configuration.instanceId,
                    request.senderInstanceId,
                    error.message ?: "donor completion failed",
                    fingerprint,
                    request.expiresAtEpochMillis
                )
            )
        }
    }

    private fun finishDonor(sessionId: WarmupSessionId) {
        val donor = synchronized(donorSessions) { donorSessions.remove(sessionId) }
        donor?.prepared?.forEach(PreparedWarmupDonor::finish)
    }

    private fun endDonor(ended: SessionEnded) {
        if (ended.state == WarmupSessionEndState.COMPLETED) {
            finishDonor(ended.sessionId)
        } else {
            scope.launch { abortDonor(ended.sessionId, ended.reason ?: "warm-up session failed") }
        }
    }

    private suspend fun abortDonor(sessionId: WarmupSessionId, reason: String) {
        val donor = synchronized(donorSessions) { donorSessions[sessionId] } ?: return
        try {
            val failure = WarmupRecipientUnavailableException(reason)
            donor.prepared.forEach { it.failDelegation(failure) }
            withTimeout(configuration.cleanupTimeout) {
                donor.prepared.forEach { it.awaitInflight(configuration.cleanupTimeout) }
            }
        } finally {
            finishDonor(sessionId)
        }
    }

    private fun scheduleDonorExpiry(sessionId: WarmupSessionId, expiresAtEpochMillis: Long) {
        scope.launch {
            delay(max(0, expiresAtEpochMillis - System.currentTimeMillis()))
            abortDonor(sessionId, "warm-up recipient deadline expired")
        }
    }

    private suspend fun runRecipientSession() {
        val now = System.currentTimeMillis()
        val session = WarmupSessionId.random()
        val offerDeadline = now + configuration.offerWindow.inWholeMilliseconds
        val expiresAt = offerDeadline + configuration.negotiationTimeout.inWholeMilliseconds +
            configuration.warmupDuration.inWholeMilliseconds + configuration.cleanupTimeout.inWholeMilliseconds * 2 +
            configuration.delegationTimeout.inWholeMilliseconds
        val mappings = participants.values.flatMap(RegisteredWarmupParticipant::mappings)
        val request = WarmupRequested(
            session,
            configuration.instanceId,
            mappings,
            offerDeadline,
            fingerprint,
            expiresAt
        )
        activeRecipientRequest.set(request)
        send(request)
        awaitObservation(offerDeadline) {
            it.record.message is WarmupRequested &&
                it.record.message.sessionId == session
        }
        delay(max(0, offerDeadline - System.currentTimeMillis()))
        val offers = observationSnapshot().mapNotNull { observed ->
            (observed.record.message as? DonorOffered)?.takeIf {
                it.sessionId == session && it.configurationFingerprint == fingerprint &&
                    it.expiresAtEpochMillis == expiresAt
            }
        }
        if (enforceDistributedProtocol) {
            val installed = offers.asSequence().mapNotNull(DonorOffered::stateSnapshot).any { snapshot ->
                sessionTable.install(snapshot, observationSnapshot().map(Observed::record))
            }
            check(installed) { "no complete authoritative warm-up state snapshot was received" }
        }
        val selected = selectDonors(mappings, offers)
        val metadata = withTimeout(configuration.negotiationTimeout) {
            transport.topicPartitions(mappings.mapTo(linkedSetOf(), WarmupTopicMapping::warmupTopic) + configuration.controlTopic)
        }
        metadata?.get(configuration.controlTopic)?.let {
            check(it.size == 1) { "Warm-up control topic must have exactly one partition" }
        }
        val negotiationDeadline = System.currentTimeMillis() + configuration.negotiationTimeout.inWholeMilliseconds
        val reservationId = reserveDestinations(session, mappings, metadata, expiresAt, negotiationDeadline)
        activeReservationId.set(reservationId.first)
        val partitionByMapping = reservationId.second.associate { it.mapping to it.warmupPartition }
        val assignmentsByDonor = selected.entries.groupBy({ it.value }, { (mapping, _) ->
            WarmupPartitionAssignment(mapping, partitionByMapping.getValue(mapping))
        })
        assignmentsByDonor.forEach { (donor, assignments) ->
            send(
                SessionAccepted(
                    session,
                    configuration.instanceId,
                    donor,
                    assignments,
                    fingerprint,
                    expiresAt
                )
            )
        }
        assignmentsByDonor.keys.forEach { donor ->
            val response = awaitObservation(negotiationDeadline) {
                it.record.message.sessionId == session &&
                    it.record.message.senderInstanceId == donor &&
                    (it.record.message is DonorAccepted || it.record.message is SessionRejected)
            }.record.message
            check(response is DonorAccepted) {
                (response as SessionRejected).reason
            }
        }
        val donorByDestination = assignmentsByDonor.flatMap { (donor, assignments) ->
            assignments.map { it.destination to donor }
        }.toMap()
        val allAssignments = assignmentsByDonor.values.flatten()
        val prepared = mutableListOf<PreparedWarmupRecipient>()
        allAssignments.groupBy { it.mapping.consumerId }.forEach { (consumerId, assignments) ->
            val recipient = requireNotNull(participants[consumerId]).prepareRecipient(
                session,
                assignments,
                donorByDestination
            ) { donor, completed ->
                send(
                    ControlRecordCompleted(
                        session,
                        configuration.instanceId,
                        donor,
                        completed,
                        fingerprint,
                        expiresAt
                    )
                )
            }
            prepared += recipient
            // Register immediately so a later participant preparation failure is cleaned up by cold fallback.
            recipientPrepared += recipient
        }
        val warmupStartedAt = System.currentTimeMillis()
        withTimeout(configuration.negotiationTimeout) { prepared.forEach { it.start() } }
        assignmentsByDonor.forEach { (donor, assignments) ->
            send(
                RecipientReady(
                    session,
                    configuration.instanceId,
                    donor,
                    assignments,
                    fingerprint,
                    expiresAt
                )
            )
        }
        awaitRecipientReadiness(warmupStartedAt, session, assignmentsByDonor.keys)
        assignmentsByDonor.keys.forEach { donor ->
            send(
                WarmupCompletionRequested(
                    session,
                    configuration.instanceId,
                    donor,
                    fingerprint,
                    expiresAt
                )
            )
        }
        assignmentsByDonor.keys.forEach { donor ->
            val response = awaitObservation(expiresAt) {
                val message = it.record.message
                message.sessionId == session && message.senderInstanceId == donor &&
                    (message is DonorClosed || message is SessionRejected)
            }.record.message
            check(response is DonorClosed) { (response as SessionRejected).reason }
        }
        withTimeout(configuration.cleanupTimeout) {
            prepared.forEach { it.completeAfterDonorsClosed() }
        }
        send(
            SessionEnded(
                session,
                configuration.instanceId,
                WarmupSessionEndState.COMPLETED,
                null,
                fingerprint,
                expiresAt
            )
        )
        send(
            ReservationReleased(
                session,
                configuration.instanceId,
                reservationId.first,
                fingerprint,
                expiresAt
            )
        )
        activeReservationId.compareAndSet(reservationId.first, null)
        activeRecipientRequest.compareAndSet(request, null)
        try {
            prepared.forEach { it.startMain() }
        } catch (error: Throwable) {
            prepared.forEach { runCatching { it.stop() } }
            throw MainConsumerStartException(error)
        }
        ready.complete(Unit)
        monitorMainConsumers()
    }

    private suspend fun awaitRecipientReadiness(
        warmupStartedAtEpochMillis: Long,
        sessionId: WarmupSessionId,
        donors: Set<WarmupInstanceId>
    ) {
        val readyAt = warmupStartedAtEpochMillis + configuration.warmupDuration.inWholeMilliseconds
        while (true) {
            val rejection = observationSnapshot().asSequence()
                .map { it.record.message }
                .filterIsInstance<SessionRejected>()
                .firstOrNull { it.sessionId == sessionId && it.senderInstanceId in donors }
            check(rejection == null) { rejection?.reason ?: "warm-up donor rejected the session" }
            val remaining = readyAt - System.currentTimeMillis()
            if (remaining <= 0) return
            if (withTimeoutOrNull(remaining.milliseconds) { observationSignal.receive() } == null) return
        }
    }

    private fun scheduleBootstrapAuthority() {
        val boundary = transport.initialOffsetBoundary() ?: -1L
        val recoveryWindow = configuration.offerWindow + configuration.negotiationTimeout +
            configuration.warmupDuration + configuration.cleanupTimeout * 2 + configuration.delegationTimeout
        scope.launch {
            delay(recoveryWindow)
            sessionTable.markBootstrapAuthoritative(maxOf(boundary, sessionTable.lastOffset()))
        }
    }

    private suspend fun reserveDestinations(
        sessionId: WarmupSessionId,
        mappings: List<WarmupTopicMapping>,
        metadata: Map<String, Set<Int>>?,
        expiresAtEpochMillis: Long,
        negotiationDeadlineEpochMillis: Long
    ): Pair<WarmupReservationId, List<WarmupPartitionAssignment>> {
        while (System.currentTimeMillis() < negotiationDeadlineEpochMillis) {
            val unavailable = sessionTable.reservedPartitions().toMutableSet()
            val assignments = mappings
                .groupBy(WarmupTopicMapping::warmupTopic)
                .values
                .flatMap { sameTopic ->
                    val topic = sameTopic.first().warmupTopic
                    val existing = metadata?.get(topic)?.sorted()
                        ?: sameTopic.indices.toList() // in-memory compatibility; production always uses metadata
                    val free = existing.filter { TopicPartition(topic, it) !in unavailable }.iterator()
                    sameTopic.sortedWith(compareBy({ it.consumerId.value }, WarmupTopicMapping::sourceTopic)).map { mapping ->
                        check(free.hasNext()) { "no free warm-up partition for $topic" }
                        WarmupPartitionAssignment(mapping, free.next()).also { unavailable += it.destination }
                    }
                }
            val reservationId = WarmupReservationId.random()
            send(
                ReservationRequested(
                    sessionId,
                    configuration.instanceId,
                    reservationId,
                    assignments,
                    fingerprint,
                    expiresAtEpochMillis
                )
            )
            val observed = awaitObservation(negotiationDeadlineEpochMillis) {
                val message = it.record.message
                message is ReservationRequested && message.sessionId == sessionId &&
                    message.reservationId == reservationId
            }
            if (observed.decision.kind == WarmupTableDecisionKind.RESERVATION_WON ||
                observed.decision.kind == WarmupTableDecisionKind.DUPLICATE
            ) return reservationId to assignments
        }
        error("warm-up partition reservation deadline expired")
    }

    private fun selectDonors(
        mappings: List<WarmupTopicMapping>,
        offers: List<DonorOffered>
    ): Map<WarmupTopicMapping, WarmupInstanceId> = mappings.associateWith { mapping ->
        offers.asSequence()
            .mapNotNull { offer ->
                offer.mappings.firstOrNull { it.mapping == mapping }
                    ?.let { offered -> offer.senderInstanceId to offered.sourcePartitions.size }
            }
            .distinctBy { it.first }
            .sortedWith(compareByDescending<Pair<WarmupInstanceId, Int>> { it.second }.thenBy { it.first.value })
            .firstOrNull()
            ?.first
            ?: error("no donor offered ${mapping.consumerId.value}:${mapping.sourceTopic}")
    }

    private suspend fun coldStart(reason: String) {
        if (stopping.get()) return
        activeRecipientRequest.getAndSet(null)?.let { request ->
            runCatching {
                send(
                    SessionEnded(
                        request.sessionId,
                        configuration.instanceId,
                        WarmupSessionEndState.COLD_FALLBACK,
                        reason.take(4_096).ifBlank { "warm-up unavailable" },
                        request.configurationFingerprint,
                        request.expiresAtEpochMillis
                    )
                )
                activeReservationId.getAndSet(null)?.let { reservationId ->
                    send(
                        ReservationReleased(
                            request.sessionId,
                            configuration.instanceId,
                            reservationId,
                            request.configurationFingerprint,
                            request.expiresAtEpochMillis
                        )
                    )
                }
            }
        }
        runCatching {
            withTimeout(configuration.cleanupTimeout) {
                recipientPrepared.toList().forEach { prepared -> runCatching { prepared.stop() } }
            }
        }
        try {
            participants.values.forEach { it.startMain() }
        } catch (error: Throwable) {
            participants.values.forEach { runCatching { it.stop() } }
            throw MainConsumerStartException(error)
        }
        ready.complete(Unit)
        monitorMainConsumers()
    }

    private suspend fun fallbackToCold(error: Throwable) {
        if (stopping.get()) return
        try {
            coldStart(error.message ?: error::class.java.simpleName)
        } catch (fallbackError: Throwable) {
            fallbackError.addSuppressed(error)
            fail(fallbackError)
        }
    }

    private fun monitorMainConsumers() {
        participants.values.forEach { participant ->
            scope.launch {
                try {
                    participant.awaitTermination()
                } catch (error: Throwable) {
                    if (!stopping.get()) fail(error)
                }
            }
        }
    }

    private fun fail(error: Throwable) {
        if (lifecycleFailure.compareAndSet(null, error)) {
            if (!ready.isCompleted) ready.completeExceptionally(error)
            termination.complete(error)
        }
    }

    private suspend fun send(message: WarmupControlMessage) {
        transport.send(message)
    }

    private suspend fun awaitObservation(
        deadlineEpochMillis: Long,
        predicate: (Observed) -> Boolean
    ): Observed {
        while (true) {
            synchronized(observations) { observations.firstOrNull(predicate) }?.let { return it }
            val remaining = deadlineEpochMillis - System.currentTimeMillis()
            check(remaining > 0) { "warm-up control deadline expired" }
            withTimeout(remaining.milliseconds) { observationSignal.receive() }
        }
    }

    private fun observationSnapshot(): List<Observed> = synchronized(observations) { observations.toList() }

    private fun validateMappings() {
        val mappings = participants.values.flatMap(RegisteredWarmupParticipant::mappings)
        require(mappings.isNotEmpty()) { "At least one warm-up topic mapping is required" }
        require(mappings.distinct().size == mappings.size) { "Warm-up topic mappings must be unique" }
    }

    private fun configurationFingerprint(): String {
        val mappings = participants.values
            .flatMap(RegisteredWarmupParticipant::mappings)
            .sortedWith(compareBy({ it.consumerId.value }, { it.sourceTopic }, { it.warmupTopic }))
            .joinToString("|") { "${it.consumerId.value}:${it.sourceTopic}:${it.warmupTopic}" }
        val canonical = listOf(
            WARMUP_CONTROL_PROTOCOL_VERSION,
            configuration.quotaPerInterval,
            configuration.initialQuotaPerInterval ?: configuration.quotaPerInterval,
            configuration.quotaInterval.inWholeMilliseconds,
            configuration.quotaRampDuration.inWholeMilliseconds,
            configuration.delegationTimeout.inWholeMilliseconds,
            mappings
        ).joinToString("|")
        return MessageDigest.getInstance("SHA-256")
            .digest(canonical.toByteArray())
            .joinToString("") { "%02x".format(it) }
    }

    private fun validateConfiguration() {
        require(configuration.controlTopic.isNotBlank()) { "controlTopic must not be blank" }
        require(configuration.quotaPerInterval > 0) { "quotaPerInterval must be positive" }
        require((configuration.initialQuotaPerInterval ?: configuration.quotaPerInterval) in 0..configuration.quotaPerInterval) {
            "initialQuotaPerInterval must be between zero and quotaPerInterval"
        }
        require(!configuration.quotaRampDuration.isNegative()) { "quotaRampDuration must not be negative" }
        listOf(
            "quotaInterval" to configuration.quotaInterval,
            "warmupDuration" to configuration.warmupDuration,
            "delegationTimeout" to configuration.delegationTimeout,
            "offerWindow" to configuration.offerWindow,
            "negotiationTimeout" to configuration.negotiationTimeout,
            "cleanupTimeout" to configuration.cleanupTimeout,
            "controlReadinessTimeout" to configuration.controlReadinessTimeout
        ).forEach { (name, duration) -> require(duration.isPositive()) { "$name must be positive" } }
        require(configuration.completionBufferCapacity > 0) { "completionBufferCapacity must be positive" }
        require(configuration.controlObservationLimit > 0) { "controlObservationLimit must be positive" }
        require(configuration.maxDonorSessions > 0) { "maxDonorSessions must be positive" }
        require(configuration.maxDonorDelegationsPerInterval >= configuration.quotaPerInterval) {
            "maxDonorDelegationsPerInterval must be at least quotaPerInterval"
        }
    }
}
