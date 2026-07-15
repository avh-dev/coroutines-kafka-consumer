package avh.ckc.spring

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.micrometer.MicrometerConsumerMetricsSchema
import avh.ckc.micrometer.RecordDrivenTagExtractors
import avh.ckc.micrometer.RecordMetricTagDefinition
import avh.ckc.micrometer.micrometerConsumerMetrics
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import org.springframework.context.ApplicationContext
import org.springframework.core.annotation.AnnotationUtils

internal fun validateMetricsSchema(properties: CkcConsumerProperties, consumerName: String) {
    if (!properties.metrics.enabled || properties.metrics.implementation != CkcConsumerProperties.MetricsImplementation.MICROMETER) {
        return
    }
    resolveMicrometerSchemaProperties(properties, consumerName)
}

internal fun validateMicrometerSchemas(properties: CkcConsumerProperties) {
    if (!properties.metrics.enabled || properties.metrics.implementation != CkcConsumerProperties.MetricsImplementation.MICROMETER) {
        return
    }
    properties.metrics.micrometer.defaultSchema?.takeIf { it.isNotBlank() }?.let { schemaName ->
        require(schemaName in properties.metrics.micrometer.schemas) {
            "CKC default Micrometer schema references unknown schema '$schemaName'"
        }
    }
    properties.metrics.micrometer.schemas.forEach { (schemaName, schema) ->
        require(schemaName.isNotBlank()) { "CKC Micrometer schema name must not be blank" }
        require(schema.metricPrefix.isNotBlank()) {
            "ckc.metrics.micrometer.schemas.$schemaName.metric-prefix must not be blank"
        }
        schema.staticTags.forEachIndexed { index, tag ->
            require(tag.name.isNotBlank()) {
                "ckc.metrics.micrometer.schemas.$schemaName.static-tags[$index].name must not be blank"
            }
        }
        schema.recordDrivenTags.forEachIndexed { index, tag ->
            require(tag.name.isNotBlank()) {
                "ckc.metrics.micrometer.schemas.$schemaName.record-driven-tags[$index].name must not be blank"
            }
        }
    }
}

internal fun resolvedMetricsDescription(
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

internal fun consumerMetrics(
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
