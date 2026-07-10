package avh.ckc.spring

import avh.ckc.core.RetryPolicy
import avh.ckc.core.RetryRule
import avh.ckc.core.coroutinesKafkaConsumer
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.micrometer.MicrometerConsumerMetricsSchema
import avh.ckc.micrometer.RecordDrivenTagExtractors
import avh.ckc.micrometer.RecordMetricTagDefinition
import avh.ckc.micrometer.micrometerConsumerMetrics
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.springframework.beans.factory.DisposableBean
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.ApplicationContext
import org.springframework.context.SmartLifecycle
import org.springframework.context.annotation.Bean
import org.springframework.core.annotation.AnnotationUtils
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.logging.Logger
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern
import kotlin.reflect.KClass
import kotlin.time.toKotlinDuration

@AutoConfiguration
@ConditionalOnClass(CkcConsumer::class)
@ConditionalOnProperty(prefix = "ckc", name = ["enabled"], havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(CkcConsumerProperties::class)
class CkcSpringBootAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    fun ckcConsumersLifecycle(applicationContext: ApplicationContext): CkcConsumersLifecycle =
        CkcConsumersLifecycle(applicationContext)
}

class CkcConsumersLifecycle internal constructor(
    private val applicationContext: ApplicationContext
) : SmartLifecycle, CkcConsumerRegistry, DisposableBean {
    private val dispatcherRegistryLazy = lazy {
        CkcDispatcherRegistry(applicationContext, properties)
    }
    private val dispatcherRegistry: CkcDispatcherRegistry by dispatcherRegistryLazy
    private val consumerRuntimes: List<NamedConsumerRuntime> by lazy { resolveConsumerRuntimes() }
    private val runningConsumerNames = linkedSetOf<String>()
    private val properties: CkcConsumerProperties
        get() = applicationContext.getBean(CkcConsumerProperties::class.java)

    override val consumerNames: Set<String>
        get() = consumerRuntimes.mapTo(linkedSetOf()) { it.name }

    @Volatile
    private var running = false

    override fun start() {
        synchronized(this) {
            logStartupBanner()
            val autoStartupRuntimes = consumerRuntimes.filter { it.autoStartup }
            logger.info(
                "Starting ${autoStartupRuntimes.size} auto-startup CKC consumer(s); " +
                    "${consumerRuntimes.size - autoStartupRuntimes.size} manual consumer(s) registered"
            )
            autoStartupRuntimes.forEach { startRuntime(it) }
            running = runningConsumerNames.isNotEmpty()
        }
    }

    override fun stop() {
        synchronized(this) {
            if (!running) {
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
            }
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
            running = runningConsumerNames.isNotEmpty()
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

    private fun startRuntime(runtime: NamedConsumerRuntime) {
        if (runtime.name in runningConsumerNames) {
            return
        }
        logger.info("Starting CKC consumer '${runtime.name}'")
        runtime.consumer.start()
        runningConsumerNames += runtime.name
        logger.info("Started CKC consumer '${runtime.name}'")
    }

    private fun logStartupBanner() {
        logger.info(
            "\n" +
                "  ___ _  __ ___\n" +
                " / __| |/ // __|  v${ckcStarterVersion()}\n" +
                "| (__| ' <| (__   Coroutines Kafka Consumer\n" +
                " \\___|_|\\_\\\\___|\n"
        )
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

    private fun resolveConsumerRuntimes(): List<NamedConsumerRuntime> {
        val properties = applicationContext.getBean(CkcConsumerProperties::class.java)
        val annotatedConsumers = applicationContext.getBeansOfType(CkcConsumer::class.java)
            .map { (beanName, bean) ->
                val annotation = AnnotationUtils.findAnnotation(bean.javaClass, CkcKafkaConsumer::class.java)
                    ?: error("CKC consumer bean '$beanName' is missing @CkcKafkaConsumer")
                require(annotation.name.isNotBlank()) {
                    "CKC consumer bean '$beanName' declares @CkcKafkaConsumer with a blank name"
                }
                AnnotatedConsumer(beanName, annotation.name, bean)
            }

        validateConsumerSet(properties, annotatedConsumers)

        return annotatedConsumers.map { annotatedConsumer ->
            val consumerName = annotatedConsumer.consumerName
            @Suppress("UNCHECKED_CAST")
            val consumerBean = annotatedConsumer.bean as CkcConsumer<Any?, Any?>
            val consumerProperties = properties.consumers.getValue(consumerName)
            val resolvedCluster = resolveCluster(properties, consumerName, consumerProperties)
            val kafkaProperties = consumerProperties.kafkaProperties(resolvedCluster.kafkaProperties)
            validateConsumerProperties(properties, consumerName, consumerProperties, kafkaProperties)
            NamedConsumerRuntime(
                name = consumerName,
                autoStartup = consumerProperties.autoStartup,
                handler = annotatedConsumer.bean.javaClass.name,
                cluster = resolvedCluster.name,
                topics = consumerProperties.topics,
                topicPattern = consumerProperties.topicPattern,
                groupId = kafkaProperties[ConsumerConfig.GROUP_ID_CONFIG]?.toString(),
                clientId = kafkaProperties[ConsumerConfig.CLIENT_ID_CONFIG]?.toString(),
                processingMode = consumerProperties.processingMode,
                workerConcurrency = consumerProperties.workerConcurrency,
                consumerPollLoopConcurrency = consumerProperties.consumerPollLoopConcurrency,
                processingDispatcher = resolvedProcessingDispatcherName(properties, consumerProperties),
                retrySchema = resolvedRetrySchemaName(properties, consumerProperties.retrySchema),
                metrics = resolvedMetricsDescription(properties, consumerName),
                consumer = buildConsumer(
                    applicationContext,
                    dispatcherRegistry,
                    properties,
                    consumerName,
                    consumerBean,
                    consumerProperties,
                    resolvedCluster.kafkaProperties
                )
            )
        }
            .also { runtimes ->
                runtimes.forEach { runtime ->
                    logger.info(
                        "Resolved CKC consumer '${runtime.name}': autoStartup=${runtime.autoStartup}, " +
                            "handler=${runtime.handler}, cluster=${runtime.cluster}, topics=${runtime.topics}, " +
                            "topicPattern=${runtime.topicPattern}, groupId=${runtime.groupId}, " +
                            "clientId=${runtime.clientId ?: "<none>"}, processingMode=${runtime.processingMode}, " +
                            "workerConcurrency=${runtime.workerConcurrency}, " +
                            "consumerPollLoopConcurrency=${runtime.consumerPollLoopConcurrency}, " +
                            "processingDispatcher=${runtime.processingDispatcher}, " +
                            "retrySchema=${runtime.retrySchema ?: "<none>"}, metrics=${runtime.metrics}"
                    )
                }
            }
    }
}

private data class NamedConsumerRuntime(
    val name: String,
    val autoStartup: Boolean,
    val handler: String,
    val cluster: String?,
    val topics: List<String>,
    val topicPattern: String?,
    val groupId: String?,
    val clientId: String?,
    val processingMode: avh.ckc.core.ProcessingMode,
    val workerConcurrency: Int,
    val consumerPollLoopConcurrency: Int,
    val processingDispatcher: String,
    val retrySchema: String?,
    val metrics: String,
    val consumer: avh.ckc.core.CoroutinesKafkaConsumer<Any?, Any?>
)

private data class AnnotatedConsumer(
    val beanName: String,
    val consumerName: String,
    val bean: CkcConsumer<*, *>
)

private data class ResolvedCluster(
    val name: String,
    val kafkaProperties: Map<String, String>
)

private class CkcDispatcherRegistry(
    private val applicationContext: ApplicationContext,
    private val properties: CkcConsumerProperties
) : AutoCloseable {
    private val resolvedDispatchers = linkedMapOf<String, CoroutineDispatcher>()
    private val ownedDispatchers = mutableListOf<ExecutorCoroutineDispatcher>()

    fun processingDispatcher(
        consumerName: String,
        consumerProperties: CkcConsumerProperties.Consumer
    ): CoroutineDispatcher {
        val dispatcherName = resolvedProcessingDispatcherName(properties, consumerProperties)
        return dispatcher(dispatcherName, consumerName)
    }

    private fun dispatcher(name: String, consumerName: String): CoroutineDispatcher =
        resolvedDispatchers.getOrPut(name) {
            when (name) {
                DISPATCHERS_DEFAULT_NAME -> Dispatchers.Default
                DISPATCHERS_IO_NAME -> Dispatchers.IO
                else -> configuredDispatcher(name, consumerName)
            }
        }

    private fun configuredDispatcher(name: String, consumerName: String): CoroutineDispatcher {
        val dispatcher = properties.dispatchers[name]
            ?: error("CKC consumer '$consumerName' references unknown processing dispatcher '$name'")
        return when (dispatcher.type) {
            CkcConsumerProperties.DispatcherType.DISPATCHERS_DEFAULT -> Dispatchers.Default
            CkcConsumerProperties.DispatcherType.DISPATCHERS_IO -> Dispatchers.IO
            CkcConsumerProperties.DispatcherType.FIXED_THREAD_POOL -> fixedThreadPoolDispatcher(name, dispatcher)
            CkcConsumerProperties.DispatcherType.VIRTUAL_THREAD_PER_TASK -> virtualThreadPerTaskDispatcher(name, dispatcher)
            CkcConsumerProperties.DispatcherType.BEAN -> beanDispatcher(name, consumerName, dispatcher)
        }
    }

    private fun fixedThreadPoolDispatcher(
        name: String,
        dispatcher: CkcConsumerProperties.Dispatcher
    ): ExecutorCoroutineDispatcher {
        require(dispatcher.threads > 0) { "ckc.dispatchers.$name.threads must be > 0" }
        val threadNumber = AtomicInteger()
        val prefix = dispatcher.threadNamePrefix ?: "ckc-$name-worker-"
        return Executors.newFixedThreadPool(dispatcher.threads) { runnable ->
            Thread(runnable, "$prefix${threadNumber.incrementAndGet()}").apply {
                isDaemon = true
            }
        }.asCoroutineDispatcher().also {
            ownedDispatchers += it
            logger.info(
                "Resolved CKC dispatcher '$name': type=${dispatcher.type}, threads=${dispatcher.threads}, " +
                    "threadNamePrefix=$prefix"
            )
        }
    }

    private fun virtualThreadPerTaskDispatcher(
        name: String,
        dispatcher: CkcConsumerProperties.Dispatcher
    ): ExecutorCoroutineDispatcher {
        val threadNumber = AtomicInteger()
        val prefix = dispatcher.threadNamePrefix ?: "ckc-$name-virtual-"
        return Executors.newThreadPerTaskExecutor { runnable ->
            Thread.ofVirtual()
                .name(prefix, threadNumber.incrementAndGet().toLong())
                .factory()
                .newThread(runnable)
        }.asCoroutineDispatcher().also {
            ownedDispatchers += it
            logger.info(
                "Resolved CKC dispatcher '$name': type=${dispatcher.type}, threadNamePrefix=$prefix"
            )
        }
    }

    private fun beanDispatcher(
        name: String,
        consumerName: String,
        dispatcher: CkcConsumerProperties.Dispatcher
    ): CoroutineDispatcher {
        val beanName = requireNotNull(dispatcher.beanName?.takeIf { it.isNotBlank() }) {
            "ckc.dispatchers.$name.bean-name must be specified for BEAN dispatchers"
        }
        val bean = runCatching {
            applicationContext.getBean(beanName, CoroutineDispatcher::class.java)
        }.getOrElse { reason ->
            error(
                "CKC consumer '$consumerName' references dispatcher '$name' backed by bean '$beanName', " +
                    "but no CoroutineDispatcher bean with that name is available: ${reason.message}"
            )
        }
        logger.info("Resolved CKC dispatcher '$name': type=${dispatcher.type}, beanName=$beanName")
        return bean
    }

    override fun close() {
        ownedDispatchers.asReversed().forEach { dispatcher ->
            runCatching { dispatcher.close() }
        }
        ownedDispatchers.clear()
        resolvedDispatchers.clear()
    }

    companion object {
        const val DISPATCHERS_DEFAULT_NAME = "dispatchers-default"
        const val DISPATCHERS_IO_NAME = "dispatchers-io"
        val BUILT_IN_DISPATCHER_NAMES = setOf(DISPATCHERS_DEFAULT_NAME, DISPATCHERS_IO_NAME)
    }
}

private fun buildConsumer(
    applicationContext: ApplicationContext,
    dispatcherRegistry: CkcDispatcherRegistry,
    properties: CkcConsumerProperties,
    consumerName: String,
    consumerBean: CkcConsumer<Any?, Any?>,
    consumerProperties: CkcConsumerProperties.Consumer,
    clusterProperties: Map<String, String>
): avh.ckc.core.CoroutinesKafkaConsumer<Any?, Any?> {

    val metrics = consumerMetrics(applicationContext, properties, consumerName)
    return coroutinesKafkaConsumer(
        consumerProperties = consumerProperties.kafkaProperties(clusterProperties)
    ) {
        processingMode = consumerProperties.processingMode
        workerConcurrency = consumerProperties.workerConcurrency
        consumerPollLoopConcurrency = consumerProperties.consumerPollLoopConcurrency
        commitIntervalMs = consumerProperties.commitInterval.toMillis()
        workChannelCapacity = consumerProperties.workChannelCapacity
        processingDispatcher = dispatcherRegistry.processingDispatcher(consumerName, consumerProperties)
        retryPolicy = retryPolicy(properties, consumerName, consumerProperties.retrySchema)
        this.metrics = metrics
        onProcessingFailure { record, reason -> consumerBean.handleFailure(record, reason) }
        configureSubscription(consumerName, consumerProperties)
        handle { record -> consumerBean.process(record) }
    }
}

private fun resolveCluster(
    properties: CkcConsumerProperties,
    consumerName: String,
    consumerProperties: CkcConsumerProperties.Consumer
): ResolvedCluster {
    val clusterName = consumerProperties.cluster
        ?: properties.defaultCluster
        ?: properties.clusters.singleClusterNameOrNull()
        ?: error(
            "Missing CKC cluster for consumer '$consumerName'. Set ckc.consumers.$consumerName.cluster, " +
                "ckc.default-cluster, or define exactly one ckc.clusters entry."
        )

    val clusterProperties = properties.clusters[clusterName]?.kafkaProperties
        ?: error("CKC consumer '$consumerName' references unknown cluster '$clusterName'")
    return ResolvedCluster(clusterName, clusterProperties)
}

private fun Map<String, CkcConsumerProperties.Cluster>.singleClusterNameOrNull(): String? =
    if (size == 1) keys.single() else null

private fun validateConsumerSet(
    properties: CkcConsumerProperties,
    annotatedConsumers: List<AnnotatedConsumer>
) {
    validateDispatcherSet(properties)

    val duplicateConsumers = annotatedConsumers
        .groupBy { it.consumerName }
        .filterValues { it.size > 1 }
    require(duplicateConsumers.isEmpty()) {
        "Multiple CKC consumer beans declare the same consumer name: " +
            duplicateConsumers.entries.joinToString { (consumerName, consumers) ->
                "$consumerName=${consumers.joinToString { it.beanName }}"
            }
    }

    val handlerNames = annotatedConsumers.mapTo(linkedSetOf()) { it.consumerName }
    val configuredNames = properties.consumers.keys
    val missingConfigs = handlerNames - configuredNames
    require(missingConfigs.isEmpty()) {
        "Missing CKC configuration properties for consumer(s): ${missingConfigs.joinToString()}"
    }
    val missingHandlers = configuredNames - handlerNames
    require(missingHandlers.isEmpty()) {
        "Missing @CkcKafkaConsumer bean(s) for configured consumer(s): ${missingHandlers.joinToString()}"
    }
}

private fun validateConsumerProperties(
    properties: CkcConsumerProperties,
    consumerName: String,
    consumerProperties: CkcConsumerProperties.Consumer,
    kafkaProperties: Map<String, Any?>
) {
    validateSubscription(consumerName, consumerProperties)
    validatePositive(consumerName, "worker-concurrency", consumerProperties.workerConcurrency)
    validatePositive(consumerName, "consumer-poll-loop-concurrency", consumerProperties.consumerPollLoopConcurrency)
    validatePositive(consumerName, "work-channel-capacity", consumerProperties.workChannelCapacity)
    require(!consumerProperties.commitInterval.isNegative && !consumerProperties.commitInterval.isZero) {
        "ckc.consumers.$consumerName.commit-interval must be > 0"
    }
    requireKafkaProperty(consumerName, kafkaProperties, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)
    requireKafkaProperty(consumerName, kafkaProperties, ConsumerConfig.GROUP_ID_CONFIG)
    requireKafkaProperty(consumerName, kafkaProperties, ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)
    requireKafkaProperty(consumerName, kafkaProperties, ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)
    consumerProperties.retrySchema?.takeIf { it.isNotBlank() }?.let { schemaName ->
        require(schemaName in properties.retrySchemas) {
            "CKC consumer '$consumerName' references unknown retry schema '$schemaName'"
        }
    }
    properties.defaultRetrySchema?.takeIf { it.isNotBlank() }?.let { schemaName ->
        require(schemaName in properties.retrySchemas) {
            "CKC default retry schema references unknown retry schema '$schemaName'"
        }
    }
    validateMetricsSchema(properties, consumerName)
    validateProcessingDispatcher(properties, consumerName, consumerProperties)
}

private fun validateDispatcherSet(properties: CkcConsumerProperties) {
    val reservedNames = CkcDispatcherRegistry.BUILT_IN_DISPATCHER_NAMES
    val reservedConfiguredNames = properties.dispatchers.keys intersect reservedNames
    require(reservedConfiguredNames.isEmpty()) {
        "CKC dispatcher names are reserved and cannot be configured: ${reservedConfiguredNames.joinToString()}"
    }
    properties.defaultProcessingDispatcher?.takeIf { it.isNotBlank() }?.let { dispatcherName ->
        require(dispatcherName in reservedNames || dispatcherName in properties.dispatchers) {
            "CKC default processing dispatcher references unknown dispatcher '$dispatcherName'"
        }
    }
    properties.dispatchers.forEach { (name, dispatcher) ->
        require(name.isNotBlank()) { "CKC dispatcher name must not be blank" }
        when (dispatcher.type) {
            CkcConsumerProperties.DispatcherType.FIXED_THREAD_POOL -> {
                require(dispatcher.threads > 0) { "ckc.dispatchers.$name.threads must be > 0" }
            }

            CkcConsumerProperties.DispatcherType.BEAN -> {
                require(!dispatcher.beanName.isNullOrBlank()) {
                    "ckc.dispatchers.$name.bean-name must be specified for BEAN dispatchers"
                }
            }

            CkcConsumerProperties.DispatcherType.DISPATCHERS_DEFAULT,
            CkcConsumerProperties.DispatcherType.DISPATCHERS_IO,
            CkcConsumerProperties.DispatcherType.VIRTUAL_THREAD_PER_TASK -> Unit
        }
    }
}

private fun validateProcessingDispatcher(
    properties: CkcConsumerProperties,
    consumerName: String,
    consumerProperties: CkcConsumerProperties.Consumer
) {
    val dispatcherName = resolvedProcessingDispatcherName(properties, consumerProperties)
    require(dispatcherName in CkcDispatcherRegistry.BUILT_IN_DISPATCHER_NAMES || dispatcherName in properties.dispatchers) {
        "CKC consumer '$consumerName' references unknown processing dispatcher '$dispatcherName'"
    }
}

private fun resolvedProcessingDispatcherName(
    properties: CkcConsumerProperties,
    consumerProperties: CkcConsumerProperties.Consumer
): String =
    consumerProperties.processingDispatcher?.takeIf { it.isNotBlank() }
        ?: properties.defaultProcessingDispatcher?.takeIf { it.isNotBlank() }
        ?: CkcDispatcherRegistry.DISPATCHERS_DEFAULT_NAME

private fun validateSubscription(
    consumerName: String,
    properties: CkcConsumerProperties.Consumer
) {
    val hasTopics = properties.topics.isNotEmpty()
    val hasPattern = !properties.topicPattern.isNullOrBlank()
    require(hasTopics xor hasPattern) {
        "Exactly one of ckc.consumers.$consumerName.topics or ckc.consumers.$consumerName.topic-pattern must be specified"
    }
    require(properties.topics.none { it.isBlank() }) {
        "ckc.consumers.$consumerName.topics must not contain blank topic names"
    }
}

private fun validatePositive(consumerName: String, propertyName: String, value: Int) {
    require(value > 0) { "ckc.consumers.$consumerName.$propertyName must be > 0" }
}

private fun requireKafkaProperty(
    consumerName: String,
    kafkaProperties: Map<String, Any?>,
    propertyName: String
) {
    require(!kafkaProperties[propertyName]?.toString().isNullOrBlank()) {
        "Missing Kafka property '$propertyName' for CKC consumer '$consumerName'"
    }
}

private fun validateMetricsSchema(properties: CkcConsumerProperties, consumerName: String) {
    if (!properties.metrics.enabled || properties.metrics.implementation != CkcConsumerProperties.MetricsImplementation.MICROMETER) {
        return
    }
    resolveMicrometerSchemaProperties(properties, consumerName)
}

private fun resolvedRetrySchemaName(
    properties: CkcConsumerProperties,
    consumerRetrySchema: String?
): String? =
    consumerRetrySchema?.takeIf { it.isNotBlank() }
        ?: properties.defaultRetrySchema?.takeIf { it.isNotBlank() }

private fun resolvedMetricsDescription(
    properties: CkcConsumerProperties,
    consumerName: String
): String {
    if (!properties.metrics.enabled) {
        return "disabled"
    }
    return when (properties.metrics.implementation) {
        CkcConsumerProperties.MetricsImplementation.MICROMETER -> {
            val schema = resolveMicrometerSchemaName(properties, consumerName)
            "micrometer(schema=${schema ?: "<legacy>"})"
        }

        CkcConsumerProperties.MetricsImplementation.CUSTOM -> "custom"
        CkcConsumerProperties.MetricsImplementation.NONE -> "none"
    }
}

private fun consumerMetrics(
    applicationContext: ApplicationContext,
    properties: CkcConsumerProperties,
    consumerName: String
): ConsumerMetrics<Any?, Any?> {
    if (!properties.metrics.enabled || properties.metrics.implementation == CkcConsumerProperties.MetricsImplementation.NONE) {
        return ConsumerMetrics.NOOP
    }

    return when (properties.metrics.implementation) {
        CkcConsumerProperties.MetricsImplementation.MICROMETER ->
            micrometerMetrics(applicationContext, properties, consumerName)

        CkcConsumerProperties.MetricsImplementation.CUSTOM ->
            customMetrics(applicationContext, consumerName)

        CkcConsumerProperties.MetricsImplementation.NONE ->
            ConsumerMetrics.NOOP
    }
}

private fun micrometerMetrics(
    applicationContext: ApplicationContext,
    properties: CkcConsumerProperties,
    consumerName: String
): ConsumerMetrics<Any?, Any?> {
    val meterRegistry = runCatching {
        applicationContext.getBean(MeterRegistry::class.java)
    }.getOrNull() ?: run {
        logger.warning("CKC Micrometer metrics are enabled but no MeterRegistry bean is available; using NOOP metrics")
        return ConsumerMetrics.NOOP
    }

    val schema = buildMicrometerSchema(
        meterRegistry = meterRegistry,
        properties = properties,
        consumerName = consumerName
    )
    @Suppress("UNCHECKED_CAST")
    val recordDrivenTagExtractors = resolveAnnotatedBean(
        applicationContext = applicationContext,
        beanType = RecordDrivenTagExtractors::class.java,
        annotationType = CkcMicrometerRecordTags::class.java,
        consumerName = consumerName,
        annotationConsumer = CkcMicrometerRecordTags::consumer,
        missingDefault = RecordDrivenTagExtractors.none<Any?, Any?>(),
        useSingleUnannotatedBean = false
    ) as RecordDrivenTagExtractors<Any?, Any?>

    return micrometerConsumerMetrics(schema) {
        consumerId = consumerName
        this.recordDrivenTagExtractors = recordDrivenTagExtractors
    }
}

private fun buildMicrometerSchema(
    meterRegistry: MeterRegistry,
    properties: CkcConsumerProperties,
    consumerName: String
): MicrometerConsumerMetricsSchema {
    val schemaProperties = resolveMicrometerSchemaProperties(properties, consumerName)
    return MicrometerConsumerMetricsSchema(
        meterRegistry = meterRegistry,
        metricPrefix = schemaProperties.metricPrefix,
        staticTags = schemaProperties.staticTags.map { tag ->
            require(tag.name.isNotBlank()) { "CKC Micrometer static tag name must not be blank" }
            Tag.of(tag.name, tag.value)
        },
        recordDrivenTags = schemaProperties.recordDrivenTags.map { tag ->
            RecordMetricTagDefinition(tag.name, tag.default)
        }
    )
}

private fun resolveMicrometerSchemaProperties(
    properties: CkcConsumerProperties,
    consumerName: String
): CkcConsumerProperties.MicrometerSchema {
    val configuredSchemas = properties.metrics.micrometer.schemas
    if (configuredSchemas.isEmpty()) {
        return CkcConsumerProperties.MicrometerSchema(metricPrefix = properties.metrics.prefix)
    }

    val schemaName = properties.consumers[consumerName]?.metrics?.schema
        ?: properties.metrics.micrometer.defaultSchema
        ?: "default".takeIf { it in configuredSchemas }
        ?: configuredSchemas.keys.singleOrNull()
        ?: error(
            "Missing CKC Micrometer schema for consumer '$consumerName'. Set " +
                "ckc.consumers.$consumerName.metrics.schema or ckc.metrics.micrometer.default-schema."
        )

    return configuredSchemas[schemaName]
        ?: error("CKC consumer '$consumerName' references unknown Micrometer schema '$schemaName'")
}

private fun resolveMicrometerSchemaName(
    properties: CkcConsumerProperties,
    consumerName: String
): String? {
    val configuredSchemas = properties.metrics.micrometer.schemas
    if (configuredSchemas.isEmpty()) {
        return null
    }
    return properties.consumers[consumerName]?.metrics?.schema
        ?: properties.metrics.micrometer.defaultSchema
        ?: "default".takeIf { it in configuredSchemas }
        ?: configuredSchemas.keys.singleOrNull()
        ?: error(
            "Missing CKC Micrometer schema for consumer '$consumerName'. Set " +
                "ckc.consumers.$consumerName.metrics.schema or ckc.metrics.micrometer.default-schema."
        )
}

private fun customMetrics(
    applicationContext: ApplicationContext,
    consumerName: String
): ConsumerMetrics<Any?, Any?> {
    @Suppress("UNCHECKED_CAST")
    return resolveAnnotatedBean(
        applicationContext = applicationContext,
        beanType = ConsumerMetrics::class.java,
        annotationType = CkcConsumerMetrics::class.java,
        consumerName = consumerName,
        annotationConsumer = CkcConsumerMetrics::consumer,
        missingDefault = null,
        useSingleUnannotatedBean = true
    ) as ConsumerMetrics<Any?, Any?>? ?: error(
        "CKC custom metrics are enabled but no ConsumerMetrics bean is available for consumer '$consumerName'. " +
            "Declare a ConsumerMetrics bean annotated with @CkcConsumerMetrics."
    )
}

private fun <T : Any, A : Annotation> resolveAnnotatedBean(
    applicationContext: ApplicationContext,
    beanType: Class<T>,
    annotationType: Class<A>,
    consumerName: String,
    annotationConsumer: (A) -> String,
    missingDefault: T?,
    useSingleUnannotatedBean: Boolean
): T? {
    val beans = applicationContext.getBeansOfType(beanType)
    val annotatedBeans = beans.mapNotNull { (beanName, bean) ->
        val annotation = applicationContext.findAnnotationOnBean(beanName, annotationType)
            ?: AnnotationUtils.findAnnotation(bean.javaClass, annotationType)
            ?: return@mapNotNull null
        AnnotatedBean(beanName, bean, annotationConsumer(annotation))
    }

    val exactMatches = annotatedBeans.filter { it.consumer == consumerName }
    if (exactMatches.size > 1) {
        error(
            "Multiple ${beanType.simpleName} beans are annotated for CKC consumer '$consumerName': " +
                exactMatches.joinToString { it.name }
        )
    }
    exactMatches.singleOrNull()?.let { return it.bean }

    val defaultMatches = annotatedBeans.filter { it.consumer.isBlank() }
    if (defaultMatches.size > 1) {
        error(
            "Multiple default ${beanType.simpleName} beans are annotated for CKC metrics: " +
                defaultMatches.joinToString { it.name }
        )
    }
    defaultMatches.singleOrNull()?.let { return it.bean }

    if (useSingleUnannotatedBean && beans.size == 1) {
        return beans.values.single()
    }

    return missingDefault
}

private data class AnnotatedBean<T>(
    val name: String,
    val bean: T,
    val consumer: String
)

private fun avh.ckc.core.CoroutinesKafkaConsumerBuilder<Any?, Any?>.configureSubscription(
    consumerName: String,
    properties: CkcConsumerProperties.Consumer
) {
    val hasTopics = properties.topics.isNotEmpty()
    val hasPattern = !properties.topicPattern.isNullOrBlank()
    require(hasTopics xor hasPattern) {
        "Exactly one of ckc.consumers.$consumerName.topics or ckc.consumers.$consumerName.topic-pattern must be specified"
    }
    if (hasTopics) {
        topics(properties.topics)
    } else {
        topicsPattern(Pattern.compile(properties.topicPattern))
    }
}

private fun retryPolicy(
    properties: CkcConsumerProperties,
    consumerName: String,
    consumerRetrySchema: String?
): RetryPolicy {
    val schemaName = consumerRetrySchema?.takeIf { it.isNotBlank() }
        ?: properties.defaultRetrySchema?.takeIf { it.isNotBlank() }
        ?: return RetryPolicy.none()
    val schema = properties.retrySchemas[schemaName]
        ?: error("CKC consumer '$consumerName' references unknown retry schema '$schemaName'")
    return retryPolicyFromSchema(consumerName, schemaName, schema)
}

private fun retryPolicyFromSchema(
    consumerName: String,
    schemaName: String,
    schema: CkcConsumerProperties.RetrySchema
): RetryPolicy {
    require(schema.rules.isNotEmpty()) { "ckc.retry-schemas.$schemaName.rules must not be empty" }
    return RetryPolicy.of(
        *schema.rules.mapIndexed { index, rule ->
            val path = "ckc.retry-schemas.$schemaName.rules[$index]"
            require(rule.maxRetries >= 0) { "$path.max-retries must be >= 0" }
            require(!rule.delay.isNegative) { "$path.delay must be >= 0" }
            RetryRule.of(
                exceptionTypes = resolveExceptionTypes(consumerName, path, rule.exceptions),
                maxRetries = rule.maxRetries,
                delay = rule.delay.toKotlinDuration()
            )
        }.toTypedArray()
    )
}

private fun resolveExceptionTypes(
    consumerName: String,
    path: String,
    exceptionClassNames: List<String>
): List<KClass<out Throwable>> {
    require(exceptionClassNames.isNotEmpty()) {
        "$path.exceptions must not be empty for CKC consumer '$consumerName'"
    }
    return exceptionClassNames.map { className ->
        val trimmedClassName = className.trim()
        require(trimmedClassName.isNotEmpty()) {
            "$path.exceptions must not contain blank class names for CKC consumer '$consumerName'"
        }
        val exceptionClass = runCatching {
            Class.forName(trimmedClassName)
        }.getOrElse { reason ->
            error(
                "$path references exception class '$trimmedClassName' for CKC consumer '$consumerName', " +
                    "but the class could not be loaded: ${reason.message}"
            )
        }
        require(Throwable::class.java.isAssignableFrom(exceptionClass)) {
            "$path references '$trimmedClassName' for CKC consumer '$consumerName', but it is not a Throwable"
        }
        @Suppress("UNCHECKED_CAST")
        (exceptionClass as Class<out Throwable>).kotlin
    }
}

internal fun ckcStarterVersion(): String =
    CkcSpringBootAutoConfiguration::class.java.`package`?.implementationVersion
        ?: "dev"

private val logger: Logger = Logger.getLogger(CkcSpringBootAutoConfiguration::class.java.name)
