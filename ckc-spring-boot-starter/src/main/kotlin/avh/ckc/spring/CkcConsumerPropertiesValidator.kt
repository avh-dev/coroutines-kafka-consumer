package avh.ckc.spring

import org.apache.kafka.clients.consumer.ConsumerConfig

internal fun validateConsumerSet(
    properties: CkcConsumerProperties,
    annotatedConsumers: List<AnnotatedConsumer>
) {
    validateGlobalProperties(properties)
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

private fun validateGlobalProperties(properties: CkcConsumerProperties) {
    require(!properties.lifecycle.shutdownTimeout.isNegative && !properties.lifecycle.shutdownTimeout.isZero) {
        "ckc.lifecycle.shutdown-timeout must be > 0"
    }
    if (properties.observability.mdc.enabled) {
        require(properties.observability.mdc.maxKeyLength > 0) {
            "ckc.observability.mdc.max-key-length must be > 0"
        }
    }
    properties.defaultCluster?.takeIf { it.isNotBlank() }?.let { clusterName ->
        require(clusterName in properties.clusters) {
            "CKC default cluster references unknown cluster '$clusterName'"
        }
    }
    properties.clusters.forEach { (name, _) ->
        require(name.isNotBlank()) { "CKC cluster name must not be blank" }
    }
    properties.defaultRetrySchema?.takeIf { it.isNotBlank() }?.let { schemaName ->
        require(schemaName in properties.retrySchemas) {
            "CKC default retry schema references unknown retry schema '$schemaName'"
        }
    }
    properties.retrySchemas.forEach { (schemaName, schema) ->
        require(schemaName.isNotBlank()) { "CKC retry schema name must not be blank" }
        retryPolicyFromSchema("startup validation", schemaName, schema)
    }
    validateMicrometerSchemas(properties)
}

internal fun validateConsumerProperties(
    properties: CkcConsumerProperties,
    consumerName: String,
    consumerProperties: CkcConsumerProperties.Consumer,
    kafkaProperties: Map<String, Any?>
) {
    validateSubscription(consumerName, consumerProperties)
    validatePositive(consumerName, "worker-concurrency", consumerProperties.workerConcurrency)
    validatePositive(consumerName, "consumer-poll-loop-concurrency", consumerProperties.consumerPollLoopConcurrency)
    validatePositive(consumerName, "commit-records-threshold", consumerProperties.commitRecordsThreshold)
    validatePositive(consumerName, "work-channel-capacity", consumerProperties.workChannelCapacity)
    consumerProperties.freshnessMaxRecordAge?.let { maxRecordAge ->
        require(!maxRecordAge.isNegative && !maxRecordAge.isZero) {
            "ckc.consumers.$consumerName.freshness-max-record-age must be > 0"
        }
        require(consumerProperties.processingMode.isFreshnessFirstMode()) {
            "ckc.consumers.$consumerName.freshness-max-record-age is supported only for freshness-first processing modes"
        }
    }
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

private fun avh.ckc.core.ProcessingMode.isFreshnessFirstMode(): Boolean =
    when (this) {
        avh.ckc.core.ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
        avh.ckc.core.ProcessingMode.AT_LEAST_ONCE_KEY_ORDERING,
        avh.ckc.core.ProcessingMode.AT_LEAST_ONCE_PARTITION_ORDERING -> false
        avh.ckc.core.ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST,
        avh.ckc.core.ProcessingMode.FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY -> true
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

internal fun resolvedProcessingDispatcherName(
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
