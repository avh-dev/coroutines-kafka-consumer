package avh.ckc.spring

import avh.ckc.core.ConsumerContainer
import avh.ckc.core.ConsumerContainerException
import avh.ckc.core.ConsumerContainerParticipant
import avh.ckc.core.ConsumerContainerPhase
import avh.ckc.core.consumerContainer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.springframework.beans.factory.DisposableBean
import org.springframework.context.ApplicationContext
import org.springframework.context.SmartLifecycle

/**
 * Spring-managed lifecycle and registry implementation for starter-managed CKC consumers.
 *
 * Consumers configured with `auto-startup: true` start with the application context. Manual
 * consumers can be controlled through [CkcConsumerRegistry]. Spring and named stops wait up to
 * the configured shutdown timeout. The core container owns cleanup beyond that wait and retries
 * incomplete participants. Starter-owned dispatchers close after destruction and cleanup.
 */
class CkcConsumersLifecycle internal constructor(
    private val applicationContext: ApplicationContext,
    private val providedConsumerRuntimes: List<NamedConsumerRuntime>? = null
) : SmartLifecycle, CkcConsumerRegistry, DisposableBean {
    private val dispatcherRegistryLazy = lazy {
        CkcDispatcherRegistry(applicationContext, properties)
    }
    private val dispatcherRegistry: CkcDispatcherRegistry by dispatcherRegistryLazy
    private val consumerRuntimes: List<NamedConsumerRuntime> by lazy {
        providedConsumerRuntimes ?: resolveConsumerRuntimes(applicationContext, dispatcherRegistry)
    }
    private val jointContainerLazy = lazy { containerBinding(consumerRuntimes) }
    private val jointContainer: ContainerBinding by jointContainerLazy
    private val runningConsumerNames = linkedSetOf<String>()
    private val terminalFailures = mutableMapOf<String, Throwable>()
    private val lifecycleObserverScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val stopObservers = mutableMapOf<String, Job>()
    private val terminalFailureCoordinator by lazy {
        CkcTerminalFailureCoordinator(
            applicationContext,
            applicationContext.getBean(CkcTerminalFailureExitHandler::class.java),
            applicationContext.getBean(CkcTerminalFailureHaltHandler::class.java)
        )
    }
    private val properties: CkcConsumerProperties
        get() = applicationContext.getBean(CkcConsumerProperties::class.java)

    override val consumerNames: Set<String>
        get() = consumerRuntimes.mapTo(linkedSetOf()) { it.name }

    @Volatile
    private var running = false

    @Volatile
    private var shutdownRequested = false

    @Volatile
    private var destroyRequested = false

    @Volatile
    private var dispatchersClosed = false

    @Volatile
    internal var lifecycleStarted = false
        private set

    override fun start() {
        synchronized(this) {
            if (lifecycleStarted) {
                return
            }
            logStartupBanner()
            shutdownRequested = false
            lifecycleStarted = true
            val autoStartupRuntimes = consumerRuntimes.filter { it.autoStartup }
            logger.info(
                "Starting ${autoStartupRuntimes.size} auto-startup CKC consumer(s); " +
                    "${consumerRuntimes.size - autoStartupRuntimes.size} manual consumer(s) registered"
            )
            try {
                autoStartupRuntimes.forEach { startRuntime(it) }
            } finally {
                running = true
            }
        }
    }

    override fun stop() {
        synchronized(this) {
            shutdownRequested = true
        }
        if (consumerRuntimes.isEmpty()) {
            synchronized(this) {
                running = false
                lifecycleStarted = false
            }
            closeDispatchersIfReleased()
            return
        }
        val result = try {
            stopContainer(jointContainer, properties.lifecycle.shutdownTimeout, participantIndex = null)
        } finally {
            updateAfterStop(jointContainer)
        }
        if (result.timedOut) observeStopCompletion("joint", jointContainer, participantIndex = null)
        result.throwIfIncomplete()
    }

    override fun stop(callback: Runnable) {
        try {
            stop()
        } finally {
            callback.run()
        }
    }

    override fun isRunning(): Boolean = running

    override fun isAutoStartup(): Boolean = true

    override fun getPhase(): Int = properties.lifecycle.phase

    override fun destroy() {
        destroyRequested = true
        closeDispatchersIfReleased()
    }

    override fun isRunning(name: String): Boolean =
        synchronized(this) {
            requireConsumerRuntime(name)
            name in runningConsumerNames
        }

    override fun start(name: String) {
        synchronized(this) {
            check(!shutdownRequested) { "CKC consumers are stopping" }
            startRuntime(requireConsumerRuntime(name))
            running = true
        }
    }

    override fun stop(name: String) {
        val participantIndex = synchronized(this) {
            val runtime = requireConsumerRuntime(name)
            if (runtime.name !in runningConsumerNames) {
                return
            }
            jointContainer.names.indexOf(name)
        }
        val result = try {
            stopContainer(jointContainer, properties.lifecycle.shutdownTimeout, participantIndex)
        } finally {
            updateAfterStop(jointContainer)
        }
        if (result.timedOut) {
            observeStopCompletion("individual:$name", jointContainer, participantIndex)
        }
        result.throwIfIncomplete()
    }

    internal fun consumerStateSnapshots(): List<CkcConsumerStateSnapshot> =
        synchronized(this) {
            consumerRuntimes.map { runtime ->
                CkcConsumerStateSnapshot(
                    name = runtime.name,
                    autoStartup = runtime.autoStartup,
                    running = runtime.name in runningConsumerNames,
                    handler = runtime.handler,
                    cluster = runtime.cluster,
                    topics = runtime.topics,
                    topicPattern = runtime.topicPattern,
                    groupId = runtime.groupId,
                    clientId = runtime.clientId,
                    processingMode = runtime.processingMode.name,
                    workerConcurrency = runtime.workerConcurrency,
                    consumerPollLoopConcurrency = runtime.consumerPollLoopConcurrency,
                    processingDispatcher = runtime.processingDispatcher,
                    retrySchema = runtime.retrySchema,
                    metrics = runtime.metrics,
                    runtime = runtime.consumer.stateSnapshot(),
                    warmup = runtime.warmup?.snapshot(),
                    terminalFailure = terminalFailures[runtime.name]
                )
            }
        }

    private fun startRuntime(runtime: NamedConsumerRuntime) {
        if (runtime.name in runningConsumerNames) {
            return
        }
        logger.info("Starting CKC consumer '${runtime.name}'")
        runtime.consumer.start()
        runningConsumerNames += runtime.name
        observeTerminalFailure(runtime)
        observeShutdownCompletion(runtime)
        logger.info("Started CKC consumer '${runtime.name}'")
    }

    private fun observeTerminalFailure(runtime: NamedConsumerRuntime) {
        lifecycleObserverScope.launch {
            try {
                runtime.consumer.awaitTermination()
            } catch (failure: Throwable) {
                synchronized(this@CkcConsumersLifecycle) {
                    terminalFailures.putIfAbsent(runtime.name, failure)
                }
                if (shutdownRequested) return@launch
                logger.severe("CKC consumer '${runtime.name}' reached terminal failure: ${failure.message}")
                if (properties.lifecycle.terminalFailureAction == CkcConsumerProperties.TerminalFailureAction.EXIT_PROCESS &&
                    !shutdownRequested
                ) {
                    terminalFailureCoordinator.trigger()
                }
            }
        }
    }

    private fun observeShutdownCompletion(runtime: NamedConsumerRuntime) {
        lifecycleObserverScope.launch {
            runtime.consumer.awaitShutdownCompletion()
            synchronized(this@CkcConsumersLifecycle) {
                if (runtime.consumer.isStopComplete()) {
                    runningConsumerNames.remove(runtime.name)
                    refreshRunningStateLocked()
                }
            }
        }
    }

    private fun stopContainer(
        binding: ContainerBinding,
        timeout: java.time.Duration,
        participantIndex: Int?
    ): ContainerShutdownResult {
        require(!timeout.isNegative && !timeout.isZero) {
            "ckc.lifecycle.shutdown-timeout must be > 0"
        }
        logger.info(
            "Stopping ${participantIndex?.let { 1 } ?: binding.names.size} CKC consumer(s) " +
                "with shutdown timeout ${timeout.toMillis()}ms"
        )
        var containerFailure: ConsumerContainerException? = null
        val timedOut = try {
            runBlocking {
                withTimeout(timeout.toMillis()) {
                    if (participantIndex == null) binding.container.stop()
                    else binding.container.stopParticipant(participantIndex)
                }
            }
            false
        } catch (_: TimeoutCancellationException) {
            true
        } catch (error: ConsumerContainerException) {
            containerFailure = error
            false
        }
        val status = binding.container.status()
        val relevantStatuses = status.participants.filter { participant ->
            participantIndex == null || participant.participantIndex == participantIndex
        }
        status.fatalFailure
            ?.takeIf { fatal -> participantIndex == null || relevantStatuses.any { it.failure === fatal } }
            ?.let { throw it }
        val failures = linkedMapOf<String, CkcConsumerLifecycleFailure>()
        containerFailure?.failures?.forEach { failure ->
            val name = binding.names[failure.participantIndex]
            failures[name] = CkcConsumerLifecycleFailure(name, failure.phase, failure.error)
        }
        relevantStatuses.forEach { participant ->
            participant.failure?.let { error ->
                val name = binding.names[participant.participantIndex]
                failures.putIfAbsent(
                    name,
                    CkcConsumerLifecycleFailure(name, ConsumerContainerPhase.STOP, error)
                )
            }
        }
        val unfinished = relevantStatuses
            .filterNot { it.stopComplete }
            .mapTo(linkedSetOf()) { binding.names[it.participantIndex] }
        if (unfinished.isNotEmpty()) {
            logger.warning(
                "Shutdown remains incomplete for CKC consumer(s): " +
                    unfinished.joinToString()
            )
        }
        return ContainerShutdownResult(
            failures = failures.values.toList(),
            unfinishedConsumerNames = unfinished,
            timedOut = timedOut
        )
    }

    private fun containerBinding(runtimes: List<NamedConsumerRuntime>): ContainerBinding =
        ContainerBinding(
            names = runtimes.map(NamedConsumerRuntime::name),
            container = consumerContainer(
                runtimes.map { runtime ->
                    object : ConsumerContainerParticipant {
                        override fun start() = runtime.consumer.start()
                        override suspend fun awaitReady() = Unit
                        override suspend fun stop() = runtime.consumer.stop()
                        override fun isStopComplete(): Boolean = runtime.consumer.isStopComplete()
                    }
                }
            )
        )

    private fun observeStopCompletion(
        key: String,
        binding: ContainerBinding,
        participantIndex: Int?
    ) {
        synchronized(this) {
            if (stopObservers[key]?.isActive == true) return
            stopObservers[key] = lifecycleObserverScope.launch {
                try {
                    if (participantIndex == null) binding.container.stop()
                    else binding.container.stopParticipant(participantIndex)
                } catch (error: Throwable) {
                    // Core retains the indexed result for a later stop call; this observer refreshes state.
                    binding.container.status().fatalFailure?.let { throw it }
                    logger.severe("CKC consumer cleanup ended after the Spring deadline: ${error.message}")
                } finally {
                    updateAfterStop(binding)
                    synchronized(this@CkcConsumersLifecycle) { stopObservers.remove(key) }
                }
            }
        }
    }

    private fun updateAfterStop(binding: ContainerBinding) {
        val status = binding.container.status()
        val completedNames = status.participants
            .filter { it.stopComplete }
            .mapTo(linkedSetOf()) { binding.names[it.participantIndex] }
        synchronized(this) {
            runningConsumerNames.removeAll(completedNames)
            refreshRunningStateLocked()
        }
        closeDispatchersIfReleased()
    }

    private fun refreshRunningStateLocked() {
        val cleanupIncomplete = shutdownRequested && jointContainerLazy.isInitialized() &&
            jointContainer.container.status().participants.any { !it.stopComplete }
        running = runningConsumerNames.isNotEmpty() || cleanupIncomplete
        if (!running && shutdownRequested) lifecycleStarted = false
    }

    private fun closeDispatchersIfReleased() {
        val close = synchronized(this) {
            if (!destroyRequested || dispatchersClosed || !dispatcherRegistryLazy.isInitialized()) {
                false
            } else {
                val released = !jointContainerLazy.isInitialized() ||
                    jointContainer.container.status().participants.all { it.stopComplete }
                if (released) dispatchersClosed = true
                released
            }
        }
        if (close) dispatcherRegistry.close()
    }

    private fun requireConsumerRuntime(name: String): NamedConsumerRuntime =
        consumerRuntimes.firstOrNull { it.name == name }
            ?: error("No CKC consumer named '$name' is registered")

}

/** One named Spring consumer failure translated from the core container's indexed result. */
data class CkcConsumerLifecycleFailure(
    val consumerName: String,
    val phase: ConsumerContainerPhase,
    val error: Throwable
)

/**
 * Reports the outcome of a Spring wait for consumer shutdown. [lifecycleFailures] lists observed
 * participant errors. [unfinishedConsumerNames] identifies consumers whose cleanup was still
 * incomplete. [timedOut] indicates that the Spring waiting budget expired.
 */
class CkcConsumersShutdownException internal constructor(
    val lifecycleFailures: List<CkcConsumerLifecycleFailure>,
    val unfinishedConsumerNames: Set<String>,
    val timedOut: Boolean
) : IllegalStateException(
    buildString {
        append("CKC consumer shutdown did not complete cleanly")
        if (lifecycleFailures.isNotEmpty()) {
            append("; failures=${lifecycleFailures.joinToString { it.consumerName }}")
        }
        if (unfinishedConsumerNames.isNotEmpty()) {
            append("; unfinished=${unfinishedConsumerNames.joinToString()}")
        }
        if (timedOut) append("; Spring shutdown wait timed out")
    }
) {
    /** Observed errors indexed by Spring consumer name. */
    val failures: Map<String, Throwable> = lifecycleFailures.associate { it.consumerName to it.error }

    init {
        lifecycleFailures.forEach { addSuppressed(it.error) }
    }
}

/** Maps each core container index back to its Spring consumer name. */
private data class ContainerBinding(
    val names: List<String>,
    val container: ConsumerContainer
)

/** Snapshot of one Spring stop wait, including unfinished participant cleanup. */
private data class ContainerShutdownResult(
    val failures: List<CkcConsumerLifecycleFailure>,
    val unfinishedConsumerNames: Set<String>,
    val timedOut: Boolean
) {
    fun throwIfIncomplete() {
        if (failures.isNotEmpty() || unfinishedConsumerNames.isNotEmpty() || timedOut) {
            throw CkcConsumersShutdownException(failures, unfinishedConsumerNames, timedOut)
        }
    }
}

internal data class CkcConsumerStateSnapshot(
    val name: String,
    val autoStartup: Boolean,
    val running: Boolean,
    val handler: String,
    val cluster: String?,
    val topics: List<String>,
    val topicPattern: String?,
    val groupId: String?,
    val clientId: String?,
    val processingMode: String,
    val workerConcurrency: Int,
    val consumerPollLoopConcurrency: Int,
    val processingDispatcher: String,
    val retrySchema: String?,
    val metrics: String,
    val runtime: avh.ckc.core.ConsumerStateSnapshot,
    val warmup: CkcWarmupStateSnapshot?,
    val terminalFailure: Throwable?
) {
    val failed: Boolean get() = runtime.failed || terminalFailure != null
}
