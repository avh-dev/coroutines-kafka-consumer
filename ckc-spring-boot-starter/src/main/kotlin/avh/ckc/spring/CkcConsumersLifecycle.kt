package avh.ckc.spring

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.springframework.beans.factory.DisposableBean
import org.springframework.context.ApplicationContext
import org.springframework.context.SmartLifecycle
import java.util.concurrent.ConcurrentHashMap

/**
 * Spring-managed lifecycle and registry implementation for starter-managed CKC consumers.
 *
 * Consumers configured with `auto-startup: true` start with the application context;
 * manual consumers are registered here and can be controlled through [CkcConsumerRegistry].
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
    private val runningConsumerNames = linkedSetOf<String>()
    private val terminalFailureScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
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
    internal var lifecycleStarted = false
        private set

    override fun start() {
        synchronized(this) {
            if (lifecycleStarted) {
                return
            }
            logStartupBanner()
            shutdownRequested = false
            val autoStartupRuntimes = consumerRuntimes.filter { it.autoStartup }
            logger.info(
                "Starting ${autoStartupRuntimes.size} auto-startup CKC consumer(s); " +
                    "${consumerRuntimes.size - autoStartupRuntimes.size} manual consumer(s) registered"
            )
            autoStartupRuntimes.forEach { startRuntime(it) }
            lifecycleStarted = true
            running = true
        }
    }

    override fun stop() {
        synchronized(this) {
            shutdownRequested = true
            if (!lifecycleStarted && runningConsumerNames.isEmpty()) {
                return
            }
            val result = stopRuntimes(
                runtimes = consumerRuntimes.filter { it.name in runningConsumerNames }.asReversed(),
                timeout = properties.lifecycle.shutdownTimeout
            )
            runningConsumerNames.removeAll(result.completedConsumerNames)
            running = runningConsumerNames.isNotEmpty()
            if (!running) {
                lifecycleStarted = false
            }
            result.throwIfIncomplete()
        }
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
        if (dispatcherRegistryLazy.isInitialized()) {
            dispatcherRegistry.close()
        }
    }

    override fun isRunning(name: String): Boolean =
        synchronized(this) {
            requireConsumerRuntime(name)
            name in runningConsumerNames
        }

    override fun start(name: String) {
        synchronized(this) {
            startRuntime(requireConsumerRuntime(name))
            running = true
        }
    }

    override fun stop(name: String) {
        synchronized(this) {
            val runtime = requireConsumerRuntime(name)
            if (runtime.name !in runningConsumerNames) {
                return
            }
            val result = stopRuntimes(listOf(runtime), properties.lifecycle.shutdownTimeout)
            runningConsumerNames.removeAll(result.completedConsumerNames)
            running = runningConsumerNames.isNotEmpty()
            result.throwIfIncomplete()
        }
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
                    runtime = runtime.consumer.stateSnapshot()
                )
            }
        }

    private fun startRuntime(runtime: NamedConsumerRuntime) {
        if (runtime.name in runningConsumerNames) {
            return
        }
        logger.info("Starting CKC consumer '${runtime.name}'")
        runtime.consumer.start()
        observeTerminalFailure(runtime)
        runningConsumerNames += runtime.name
        logger.info("Started CKC consumer '${runtime.name}'")
    }

    private fun observeTerminalFailure(runtime: NamedConsumerRuntime) {
        terminalFailureScope.launch {
            try {
                runtime.consumer.awaitTermination()
            } catch (failure: Throwable) {
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

    private fun stopRuntimes(
        runtimes: List<NamedConsumerRuntime>,
        timeout: java.time.Duration
    ): ConsumerShutdownResult {
        if (runtimes.isEmpty()) {
            return ConsumerShutdownResult.EMPTY
        }
        require(!timeout.isNegative && !timeout.isZero) {
            "ckc.lifecycle.shutdown-timeout must be > 0"
        }
        logger.info(
            "Stopping ${runtimes.size} CKC consumer(s) with shutdown timeout ${timeout.toMillis()}ms"
        )
        val outcomes = ConcurrentHashMap<String, Result<Unit>>()
        runBlocking {
            withTimeoutOrNull(timeout.toMillis()) {
                coroutineScope {
                    runtimes.map { runtime ->
                        async(Dispatchers.Default) {
                            logger.info("Stopping CKC consumer '${runtime.name}'")
                            val outcome = try {
                                runtime.consumer.stop()
                                logger.info("Stopped CKC consumer '${runtime.name}'")
                                Result.success(Unit)
                            } catch (error: Throwable) {
                                if (error is CancellationException && !currentCoroutineContext().isActive) {
                                    throw error
                                }
                                logger.severe(
                                    "Failed to stop CKC consumer '${runtime.name}': ${error.message}"
                                )
                                Result.failure(error)
                            }
                            outcomes[runtime.name] = outcome
                        }
                    }
                        .awaitAll()
                }
            }
        }
        val unfinished = runtimes.mapTo(linkedSetOf()) { it.name } - outcomes.keys
        if (unfinished.isNotEmpty()) {
            logger.warning(
                "Timed out after ${timeout.toMillis()}ms while stopping CKC consumer(s): " +
                    unfinished.joinToString()
            )
        }
        return ConsumerShutdownResult(
            completedConsumerNames = outcomes.keys.toSet(),
            failures = outcomes.mapNotNull { (name, outcome) ->
                outcome.exceptionOrNull()?.let { name to it }
            }.toMap(),
            unfinishedConsumerNames = unfinished
        )
    }

    private fun requireConsumerRuntime(name: String): NamedConsumerRuntime =
        consumerRuntimes.firstOrNull { it.name == name }
            ?: error("No CKC consumer named '$name' is registered")

}

/** Aggregates every completed failure and every consumer still stopping at the shared deadline. */
class CkcConsumersShutdownException internal constructor(
    val failures: Map<String, Throwable>,
    val unfinishedConsumerNames: Set<String>
) : IllegalStateException(
    buildString {
        append("CKC consumer shutdown did not complete cleanly")
        if (failures.isNotEmpty()) append("; failures=${failures.keys.joinToString()}")
        if (unfinishedConsumerNames.isNotEmpty()) {
            append("; unfinished=${unfinishedConsumerNames.joinToString()}")
        }
    }
) {
    init {
        failures.values.forEach(::addSuppressed)
    }
}

private data class ConsumerShutdownResult(
    val completedConsumerNames: Set<String>,
    val failures: Map<String, Throwable>,
    val unfinishedConsumerNames: Set<String>
) {
    fun throwIfIncomplete() {
        if (failures.isNotEmpty() || unfinishedConsumerNames.isNotEmpty()) {
            throw CkcConsumersShutdownException(failures, unfinishedConsumerNames)
        }
    }

    companion object {
        val EMPTY = ConsumerShutdownResult(emptySet(), emptyMap(), emptySet())
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
    val runtime: avh.ckc.core.ConsumerStateSnapshot
)
