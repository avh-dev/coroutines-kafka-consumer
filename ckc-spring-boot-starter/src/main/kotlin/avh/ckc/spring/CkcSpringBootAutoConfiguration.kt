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
import java.util.logging.Logger
import java.util.regex.Pattern
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
) : SmartLifecycle, CkcConsumerRegistry {
    private val consumerRuntimes: List<NamedConsumerRuntime> by lazy { resolveConsumerRuntimes() }
    private val runningConsumerNames = linkedSetOf<String>()

    override val consumerNames: Set<String>
        get() = consumerRuntimes.mapTo(linkedSetOf()) { it.name }

    @Volatile
    private var running = false

    override fun start() {
        synchronized(this) {
            consumerRuntimes
                .filter { it.autoStartup }
                .forEach { startRuntime(it) }
            running = runningConsumerNames.isNotEmpty()
        }
    }

    override fun stop() {
        synchronized(this) {
            if (!running) {
                return
            }
            try {
                runBlocking {
                    consumerRuntimes
                        .filter { it.name in runningConsumerNames }
                        .asReversed()
                        .forEach { it.consumer.stop() }
                }
            } finally {
                runningConsumerNames.clear()
                running = false
            }
        }
    }

    override fun isRunning(): Boolean = running

    override fun isAutoStartup(): Boolean = true

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
                runBlocking {
                    runtime.consumer.stop()
                }
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
        runtime.consumer.start()
        runningConsumerNames += runtime.name
    }

    private fun requireConsumerRuntime(name: String): NamedConsumerRuntime =
        consumerRuntimes.firstOrNull { it.name == name }
            ?: error("No CKC consumer named '$name' is registered")

    private fun resolveConsumerRuntimes(): List<NamedConsumerRuntime> {
        val properties = applicationContext.getBean(CkcConsumerProperties::class.java)
        val annotatedConsumers = applicationContext.getBeansOfType(CkcConsumer::class.java)
            .mapNotNull { (beanName, bean) ->
                val annotation = AnnotationUtils.findAnnotation(bean.javaClass, CkcKafkaConsumer::class.java)
                    ?: return@mapNotNull null
                annotation.name.takeIf { it.isNotBlank() }?.let { consumerName ->
                    beanName to (consumerName to bean)
                }
            }

        return annotatedConsumers.map { (_, namedBean) ->
            val consumerName = namedBean.first
            @Suppress("UNCHECKED_CAST")
            val consumerBean = namedBean.second as CkcConsumer<Any?, Any?>
            val consumerProperties = properties.consumers[consumerName]
                ?: error("Missing CKC configuration properties for consumer '$consumerName'")
            NamedConsumerRuntime(
                name = consumerName,
                autoStartup = consumerProperties.autoStartup,
                consumer = buildConsumer(
                    applicationContext,
                    properties,
                    consumerName,
                    consumerBean,
                    consumerProperties,
                    resolveClusterProperties(properties, consumerName, consumerProperties)
                )
            )
        }
    }
}

private data class NamedConsumerRuntime(
    val name: String,
    val autoStartup: Boolean,
    val consumer: avh.ckc.core.CoroutinesKafkaConsumer<Any?, Any?>
)

private fun buildConsumer(
    applicationContext: ApplicationContext,
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
        retryPolicy = retryPolicy(consumerProperties.retry)
        this.metrics = metrics
        onProcessingFailure { record, reason -> consumerBean.handleFailure(record, reason) }
        configureSubscription(consumerName, consumerProperties)
        handle { record -> consumerBean.process(record) }
    }
}

private fun resolveClusterProperties(
    properties: CkcConsumerProperties,
    consumerName: String,
    consumerProperties: CkcConsumerProperties.Consumer
): Map<String, String> {
    val clusterName = consumerProperties.cluster
        ?: properties.defaultCluster
        ?: properties.clusters.singleClusterNameOrNull()
        ?: error(
            "Missing CKC cluster for consumer '$consumerName'. Set ckc.consumers.$consumerName.cluster, " +
                "ckc.default-cluster, or define exactly one ckc.clusters entry."
        )

    return properties.clusters[clusterName]?.kafkaProperties
        ?: error("CKC consumer '$consumerName' references unknown cluster '$clusterName'")
}

private fun Map<String, CkcConsumerProperties.Cluster>.singleClusterNameOrNull(): String? =
    if (size == 1) keys.single() else null

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

private fun retryPolicy(properties: CkcConsumerProperties.Retry): RetryPolicy {
    require(properties.maxRetries >= 0) { "ckc retry max-retries must be >= 0" }
    require(!properties.delay.isNegative) { "ckc retry delay must be >= 0" }
    if (properties.maxRetries == 0) {
        return RetryPolicy.none()
    }
    return RetryPolicy.of(
        RetryRule.of(
            exceptionTypes = listOf(Throwable::class),
            maxRetries = properties.maxRetries,
            delay = properties.delay.toKotlinDuration()
        )
    )
}

private val logger: Logger = Logger.getLogger(CkcSpringBootAutoConfiguration::class.java.name)
