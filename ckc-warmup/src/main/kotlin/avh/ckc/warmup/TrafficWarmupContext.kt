package avh.ckc.warmup

import avh.ckc.core.extension.ManagedConsumerLifecycle
import avh.ckc.core.isFatalJvmError
import avh.ckc.warmup.broker.WarmupBrokerDrain
import avh.ckc.warmup.broker.WarmupBrokerLifecycle
import avh.ckc.warmup.donor.LogicalWarmupDonorSession
import avh.ckc.warmup.donor.LogicalWarmupDonorSessionStore
import avh.ckc.warmup.donor.MainConsumerWarmupDonorEligibility
import avh.ckc.warmup.donor.StatelessWarmupDonorDiscovery
import avh.ckc.warmup.donor.WarmupDonorQuotaPhases
import avh.ckc.warmup.donor.WarmupDonorSessionLifecycle
import avh.ckc.warmup.donor.WarmupDonorSessionManager
import avh.ckc.warmup.donor.WarmupDonorSessionPreparation
import avh.ckc.warmup.kafka.WarmupControlChannel
import avh.ckc.warmup.kafka.WarmupControlTransport
import avh.ckc.warmup.kafka.WarmupControlTransportConfiguration
import avh.ckc.warmup.kafka.WarmupSubscriptionValidation
import avh.ckc.warmup.protocol.SessionAccepted
import avh.ckc.warmup.protocol.WarmupControlMessageHandler
import avh.ckc.warmup.protocol.WarmupControlNamespace
import avh.ckc.warmup.protocol.WarmupControlRouter
import avh.ckc.warmup.protocol.WarmupInstanceId
import avh.ckc.warmup.protocol.WarmupParticipantId
import avh.ckc.warmup.recipient.WarmupRecipientAttempt
import avh.ckc.warmup.runtime.RegisteredTrafficWarmupParticipant
import avh.ckc.warmup.runtime.WarmupDiscoveryRuntime
import avh.ckc.warmup.runtime.WarmupExecutionDispatchers
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.kafka.clients.admin.Admin
import org.slf4j.LoggerFactory

/**
 * Coordinates one recipient attempt for the consumers registered through [trafficWarmup] and
 * keeps the shared transport available to serve donor sessions afterward.
 *
 * Build all participants before the first start. Use individual consumer operations or
 * [avh.ckc.core.ConsumerContainer] for heterogeneous participants. Startup waits for every
 * registered participant, and readiness means that the participant's main runtime has its
 * Kafka assignment and positions. A cold fallback starts the main runtime after recipient cleanup.
 *
 * Stopping one participant during the attempt interrupts warm-up and allows other requested
 * participants to start cold after cleanup, or preserves an outcome already selected. Individual
 * stop waits for its own runtime startup handoff and cleanup. Shared resources close after all
 * participants stop. Stop before start permanently stops that participant. If a stop is cancelled
 * or reports incomplete cleanup, call stop again. Container stop keeps cleanup running if the
 * waiting caller is cancelled.
 */
class TrafficWarmupContext internal constructor(
    private val configuration: TrafficWarmupContextConfiguration,
    private val subscriptionAdminFactory: (Map<String, Any?>) -> Admin = Admin::create,
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

    private class ParticipantStoppedBeforeMainReady :
        CancellationException("Consumer stopped before main readiness")

    private val log = LoggerFactory.getLogger(javaClass)
    private val lock = Any()
    private val instanceId = WarmupInstanceId(configuration.instanceId)
    private val namespace = WarmupControlNamespace(configuration.namespace)
    private val donorQuotaPhases = WarmupDonorQuotaPhases()
    private val delegationAdmissionOpen = AtomicBoolean(configuration.brokerLifecycleConfiguration == null)
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
    private var sharedClosed = false
    private var recipientAttempt: WarmupRecipientAttempt? = null
    private var brokerCleanup: Deferred<Set<String>>? = null
    private var brokerLifecycle: WarmupBrokerLifecycle? = null
    private var protocolStartup: Deferred<String?>? = null

    private lateinit var transport: WarmupControlChannel
    private lateinit var donorStore: LogicalWarmupDonorSessionStore
    private lateinit var donorSessionManager: WarmupDonorSessionManager<LogicalWarmupDonorSession>
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
        configuration.brokerLifecycleConfiguration?.let { broker ->
            require(broker.topic.isNotBlank() && broker.topic != configuration.controlTopic) {
                "Broker lifecycle requires a dedicated topic"
            }
            require(broker.operationTimeout.isFinite() && broker.operationTimeout.inWholeMilliseconds in 1..Int.MAX_VALUE) {
                "Broker lifecycle operation timeout must be finite and positive"
            }
        }
    }

    internal fun delegationTimeoutMillis(): Long = configuration.delegationTimeout.inWholeMilliseconds
    internal fun canDelegate(): Boolean = delegationAdmissionOpen.get()
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
                try {
                    state.ready.await()
                } catch (_: ParticipantStoppedBeforeMainReady) {
                    // Stop cancels readiness and still terminates the prepared main runtime.
                }
                participant.awaitMainTermination()
            }
            override fun isStopComplete(): Boolean = synchronized(lock) { state.stopCompleted }
        }
    }

    private fun requestStart(state: ParticipantState) {
        var startupToStart: Job? = null
        var workflowToStart: Job? = null
        var launchCold = false
        var terminalFailure: Throwable? = null
        synchronized(lock) {
            check(!state.startRequested) { "Consumer already started" }
            check(!state.stopRequested) { "Stopped consumer cannot be started" }
            state.startRequested = true
            if (!registrationFrozen) {
                registrationFrozen = true
                try {
                    initializeProtocolLocked()
                } catch (error: Throwable) {
                    if (error.isFatalJvmError()) throw error
                    log.warn("Traffic warm-up protocol initialization failed; using cold start", error)
                }
                val attempt = WarmupRecipientAttempt(
                    configuration = configuration,
                    instanceId = instanceId,
                    namespace = namespace,
                    participants = participants.values.map(ParticipantState::participant),
                    scope = scope,
                    blockingDispatcher = execution.blocking,
                    lock = lock,
                    runtime = if (::runtime.isInitialized) runtime else null,
                    transport = if (::transport.isInitialized) transport else null,
                    brokerLifecycle = brokerLifecycle,
                    protocolStartup = protocolStartup,
                    awaitParticipants = ::awaitAllParticipantsStarted,
                    observe = ::observe,
                    onCompleted = ::launchRequestedMainRuntimes,
                    onCleanupIncomplete = ::failParticipantReadiness
                )
                recipientAttempt = attempt
                startupToStart = protocolStartup
                workflowToStart = attempt.prepareWorkflow()
            } else {
                recipientAttempt?.outcome()?.let { outcome ->
                    terminalFailure = outcome.cleanupFailure
                    launchCold = terminalFailure == null
                }
            }
        }
        observe(TrafficWarmupLifecyclePhase.WAITING_FOR_PARTICIPANTS, state.participant.participantId)
        startSignal.trySend(Unit)
        startupToStart?.start()
        workflowToStart?.start()
        terminalFailure?.let { state.ready.completeExceptionally(it) }
        if (launchCold) scope.launch { startMain(state) }
    }

    private suspend fun requestStop(state: ParticipantState) {
        val attempt: CompletableDeferred<Unit>
        val ownsAttempt: Boolean
        val recipient: WarmupRecipientAttempt?
        val launchFinished: CompletableDeferred<Unit>?
        synchronized(lock) {
            state.stopAttempt?.let {
                attempt = it
                ownsAttempt = false
                recipient = null
                launchFinished = null
                return@synchronized
            }
            attempt = CompletableDeferred()
            state.stopAttempt = attempt
            ownsAttempt = true
            state.stopRequested = true
            recipient = recipientAttempt
            launchFinished = state.mainLaunchFinished
            if (!state.ready.isCompleted) {
                state.ready.completeExceptionally(ParticipantStoppedBeforeMainReady())
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
            recipient?.stopForParticipant()
            launchFinished?.await()
        } catch (error: Throwable) {
            recordFailure(error)
        }
        try {
            if (::donorSessionManager.isInitialized) {
                donorSessionManager.onParticipantUnavailable(state.participant.participantId)
            }
        } catch (error: Throwable) {
            recordFailure(error)
        }
        try {
            synchronized(lock) { recipientAttempt }?.retryIncompleteCleanup()
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
                metadataTimeout = configuration.preparationTimeout,
                preserveSendOrder = configuration.brokerLifecycleConfiguration != null
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
        val newDonorSessionManager = WarmupDonorSessionManager(
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
            drainTimeout = configuration.completionTimeout,
            admissionInitiallyOpen = configuration.brokerLifecycleConfiguration == null
        )
        val newRuntime = WarmupDiscoveryRuntime(
            instanceId,
            namespace,
            newRouter,
            newTransport,
            StatelessWarmupDonorDiscovery(instanceId, namespace, mappings, eligibility),
            newDonorSessionManager,
            configuration.offerWindow,
            configuration.preparationTimeout,
            configuration.completionTimeout,
            configuration.donorLivenessTimeout,
            scope
        )
        transport = newTransport
        donorStore = newDonorStore
        donorSessionManager = newDonorSessionManager
        runtime = newRuntime
        configuration.brokerLifecycleConfiguration?.let { broker ->
            brokerLifecycle = WarmupBrokerLifecycle(
                broker, scope, execution.blocking, ::fenceBrokerAdmission, ::reopenBrokerAdmission
            )
        }
        protocolStartup = scope.async(start = CoroutineStart.LAZY) {
            val validation = WarmupSubscriptionValidation(
                configuration.preparationTimeout, execution.blocking, subscriptionAdminFactory
            )
            validation.failureReason(registered)?.let { return@async it }

            // Participant stop may cancel the recipient attempt while this context still serves donors.
            ensureActive()
            brokerLifecycle?.start()
            if (newRuntime.startControl()) null else "control_unavailable"
        }
    }

    private fun fenceBrokerAdmission(reason: String, deadlineMillis: Long?): Deferred<Set<String>> = synchronized(lock) {
        brokerCleanup?.let { return@synchronized it }
        delegationAdmissionOpen.set(false)
        val recipient = checkNotNull(recipientAttempt)
        recipient.recordBrokerInterruption(reason)
        val drain = deadlineMillis?.let {
            WarmupBrokerDrain(
                it,
                configuration.completionTimeout,
                checkNotNull(configuration.brokerLifecycleConfiguration).operationTimeout
            )
        }
        val sessions = donorSessionManager.closeBrokerAdmission(drain).toMutableSet()
        val fence = recipient.fenceBrokerAdmission(reason, drain)
        fence.sessionId?.let(sessions::add)
        scope.async {
            recipient.awaitBrokerCleanup(fence)
            while (donorStore.admittedSessionCount() != 0) delay(10)
            // Detachment releases admission before the close job can finish or retain a failed resource.
            donorSessionManager.awaitBrokerCloseSends()
            while (!donorSessionManager.awaitCleanupBarrier(configuration.completionTimeout)) delay(100)
            donorSessionManager.awaitBrokerCloseSends()
            sessions.toSet()
        }.also { brokerCleanup = it }
    }

    private fun reopenBrokerAdmission(): Boolean = synchronized(lock) {
        val cleanup = brokerCleanup
        if (sharedClosed || participants.values.none { it.startRequested && !it.stopRequested } ||
            (cleanup != null && (!cleanup.isCompleted || cleanup.isCancelled))
        ) return@synchronized false
        brokerCleanup = null
        recipientAttempt?.recordBrokerInterruption(null)
        donorSessionManager.openBrokerAdmission()
        delegationAdmissionOpen.set(true)
        true
    }

    private suspend fun prepareLogicalDonorSession(acceptance: SessionAccepted): LogicalWarmupDonorSession {
        val quotaPhases = donorQuotaPhases.Session(
            acceptance.mappings.map { it.mapping.quota.quotaIntervalMillis.milliseconds }.toSet()
        )
        val prepared = mutableListOf<WarmupDonorSessionLifecycle>()
        try {
            acceptance.mappings.groupBy { it.mapping.participantId }.forEach { (participantId, _) ->
                val participant = synchronized(lock) { participants[participantId]?.participant }
                    ?: error("Unknown warm-up participant ${participantId.value}")
                prepared += participant.prepareDonorLane(acceptance, quotaPhases)
            }
            return LogicalWarmupDonorSession(
                acceptance.sessionId,
                acceptance.admissionBudgetPerSecond,
                prepared,
                quotaPhases
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

    /**
     * Only schedules the main runtimes. Their readiness is participant-local and must not extend
     * the recipient attempt or another participant's stop.
     */
    private fun launchRequestedMainRuntimes() {
        val requested = synchronized(lock) {
            participants.values.filter { it.startRequested && !it.stopRequested }
        }
        requested.forEach { state -> scope.launch { startMain(state) } }
    }

    private fun failParticipantReadiness(failure: Throwable) = synchronized(lock) {
        participants.values.filter { it.startRequested && !it.ready.isCompleted }
            .forEach { it.ready.completeExceptionally(failure) }
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
                val attempt = synchronized(lock) { checkNotNull(recipientAttempt) }
                observe(attempt.mainReadyEvent(state.participant.participantId))
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
                if (unexpectedlyUnavailable && ::donorSessionManager.isInitialized) {
                    donorSessionManager.onParticipantUnavailable(state.participant.participantId)
                }
            }
        }
    }

    private suspend fun awaitAllParticipantsStarted(): Boolean =
        withTimeoutOrNull(configuration.participantStartTimeout) {
            while (!allParticipantsStarted()) startSignal.receive()
            true
        } == true

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
        try {
            protocolStartup?.cancelAndJoin()
            val donorsDrained = !::donorStore.isInitialized || withTimeoutOrNull(configuration.completionTimeout) {
                while (donorStore.admittedSessionCount() != 0) delay(10)
                true
            } == true
            check(donorsDrained) { "Warm-up donor sessions did not drain before context shutdown" }
            val donorCleanupComplete = !::donorSessionManager.isInitialized ||
                donorSessionManager.awaitCleanupBarrier(configuration.completionTimeout)
            check(donorCleanupComplete) { "Warm-up donor resource cleanup did not complete before context shutdown" }
            if (::runtime.isInitialized) {
                runtime.stop()
                runtime.close()
            }
            brokerLifecycle?.leave(configuration.completionTimeout)
            lifecycleJob.cancelAndJoin()
            check(execution.close(configuration.completionTimeout)) {
                "Warm-up service executors did not terminate before the context shutdown deadline"
            }
        } catch (error: Throwable) {
            synchronized(lock) { sharedClosed = false }
            throw error
        }
    }

    private fun observe(phase: TrafficWarmupLifecyclePhase, participantId: WarmupParticipantId) =
        observe(TrafficWarmupLifecycleEvent(phase, participantId.value))

    private fun observe(event: TrafficWarmupLifecycleEvent) {
        try {
            configuration.lifecycleObserver(event)
        } catch (error: Exception) {
            log.warn("Traffic warm-up lifecycle observer failed", error)
        }
    }
}
