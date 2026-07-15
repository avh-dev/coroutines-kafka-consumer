package avh.ckc.spring

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.springframework.beans.factory.DisposableBean
import org.springframework.context.ApplicationContext
import org.springframework.context.SmartLifecycle

/**
 * Spring-managed lifecycle and registry implementation for starter-managed CKC consumers.
 *
 * Consumers configured with `auto-startup: true` start with the application context;
 * manual consumers are registered here and can be controlled through [CkcConsumerRegistry].
 */
class CkcConsumersLifecycle internal constructor(
    private val applicationContext: ApplicationContext
) : SmartLifecycle, CkcConsumerRegistry, DisposableBean {
    private val dispatcherRegistryLazy = lazy {
        CkcDispatcherRegistry(applicationContext, properties)
    }
    private val dispatcherRegistry: CkcDispatcherRegistry by dispatcherRegistryLazy
    private val consumerRuntimes: List<NamedConsumerRuntime> by lazy {
        resolveConsumerRuntimes(applicationContext, dispatcherRegistry)
    }
    private val runningConsumerNames = linkedSetOf<String>()
    private val properties: CkcConsumerProperties
        get() = applicationContext.getBean(CkcConsumerProperties::class.java)

    override val consumerNames: Set<String>
        get() = consumerRuntimes.mapTo(linkedSetOf()) { it.name }

    @Volatile
    private var running = false

    @Volatile
    internal var lifecycleStarted = false
        private set

    override fun start() {
        synchronized(this) {
            if (lifecycleStarted) {
                return
            }
            logStartupBanner()
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
            if (!lifecycleStarted && runningConsumerNames.isEmpty()) {
                return
            }
            try {
                stopRuntimes(
                    runtimes = consumerRuntimes.filter { it.name in runningConsumerNames }.asReversed(),
                    timeout = properties.lifecycle.shutdownTimeout
                )
            } finally {
                runningConsumerNames.clear()
                running = false
                lifecycleStarted = false
            }
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
            try {
                stopRuntimes(listOf(runtime), properties.lifecycle.shutdownTimeout)
            } finally {
                runningConsumerNames.remove(runtime.name)
                running = runningConsumerNames.isNotEmpty()
            }
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
        runningConsumerNames += runtime.name
        logger.info("Started CKC consumer '${runtime.name}'")
    }

    private fun stopRuntimes(
        runtimes: List<NamedConsumerRuntime>,
        timeout: java.time.Duration
    ) {
        if (runtimes.isEmpty()) {
            return
        }
        require(!timeout.isNegative && !timeout.isZero) {
            "ckc.lifecycle.shutdown-timeout must be > 0"
        }
        logger.info(
            "Stopping ${runtimes.size} CKC consumer(s) with shutdown timeout ${timeout.toMillis()}ms"
        )
        runBlocking {
            try {
                withTimeout(timeout.toMillis()) {
                    runtimes.forEach { runtime ->
                        logger.info("Stopping CKC consumer '${runtime.name}'")
                        runtime.consumer.stop()
                        logger.info("Stopped CKC consumer '${runtime.name}'")
                    }
                }
            } catch (error: kotlinx.coroutines.TimeoutCancellationException) {
                logger.warning(
                    "Timed out after ${timeout.toMillis()}ms while stopping CKC consumer(s): " +
                        runtimes.joinToString { it.name }
                )
            }
        }
    }

    private fun requireConsumerRuntime(name: String): NamedConsumerRuntime =
        consumerRuntimes.firstOrNull { it.name == name }
            ?: error("No CKC consumer named '$name' is registered")
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
