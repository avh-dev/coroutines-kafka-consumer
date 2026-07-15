package avh.ckc.spring

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.springframework.context.ApplicationContext
import org.springframework.core.annotation.AnnotationUtils

internal data class NamedConsumerRuntime(
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

internal data class AnnotatedConsumer(
    val beanName: String,
    val consumerName: String,
    val bean: CkcConsumer<*, *>
)

internal data class ResolvedCluster(
    val name: String,
    val kafkaProperties: Map<String, String>
)

internal fun resolveConsumerRuntimes(
    applicationContext: ApplicationContext,
    dispatcherRegistry: CkcDispatcherRegistry
): List<NamedConsumerRuntime> {
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

internal fun resolveCluster(
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
