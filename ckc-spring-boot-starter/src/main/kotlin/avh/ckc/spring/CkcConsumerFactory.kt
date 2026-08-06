package avh.ckc.spring

import avh.ckc.core.CoroutinesKafkaConsumerBuilder
import avh.ckc.core.coroutinesKafkaConsumer
import org.springframework.context.ApplicationContext
import java.util.regex.Pattern
import kotlin.time.toKotlinDuration

internal fun buildConsumer(
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
        commitRecordsThreshold = consumerProperties.commitRecordsThreshold
        workChannelCapacity = consumerProperties.workChannelCapacity
        freshnessMaxRecordAge = consumerProperties.freshnessMaxRecordAge?.toKotlinDuration()
        processingDispatcher = dispatcherRegistry.processingDispatcher(consumerName, consumerProperties)
        retryPolicy = retryPolicy(properties, consumerName, consumerProperties.retrySchema)
        this.metrics = metrics
        recordProcessingContext = recordProcessingContext(properties, consumerName, consumerProperties)
        onProcessingFailure { record, reason -> consumerBean.handleFailure(record, reason) }
        configureSubscription(consumerName, consumerProperties)
        handle { record -> consumerBean.process(record) }
    }
}

private fun CoroutinesKafkaConsumerBuilder<Any?, Any?>.configureSubscription(
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
