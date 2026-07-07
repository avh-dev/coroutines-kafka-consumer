package avh.ckc.micrometer

import io.micrometer.core.instrument.Tag
import org.apache.kafka.clients.consumer.ConsumerRecord

private val reservedRecordTagKeys = setOf("consumer_id", "topic", "error", "attempt", "success", "reason")

/**
 * Schema for record-driven custom tags.
 *
 * Custom tags are schema, not ad-hoc labels: all record metrics created by a
 * single [MicrometerConsumerMetricsFactory] instance use the same declared tag keys.
 * This keeps Prometheus/OpenMetrics output valid when different consumers emit
 * the same metric family.
 */
class RecordDrivenTagSchema internal constructor(
    private val tags: List<RecordMetricTagDefinition>
) {
    internal val keys: Set<String> = tags.mapTo(LinkedHashSet()) { it.key }

    init {
        val duplicateKeys = tags.groupBy { it.key }.filterValues { it.size > 1 }.keys
        require(duplicateKeys.isEmpty()) { "Record-driven tag schema contains duplicate keys: ${duplicateKeys.joinToString()}" }
    }

    internal fun <K, V> tagsFrom(
        provider: RecordDrivenTagValues<K, V>,
        record: ConsumerRecord<K, V>
    ): List<Tag> =
        tags.map { schemaTag ->
            val value = provider.extractors[schemaTag.key]?.extract(record)
            Tag.of(schemaTag.key, value ?: schemaTag.defaultValue)
        }

    companion object {
        fun empty(): RecordDrivenTagSchema = RecordDrivenTagSchema(emptyList())
    }
}

internal data class RecordMetricTagDefinition(
    val key: String,
    val defaultValue: String
)

/**
 * Builder for [RecordDrivenTagSchema].
 */
class RecordDrivenTagSchemaBuilder internal constructor() {
    private val tags = mutableListOf<RecordMetricTagDefinition>()

    /**
     * Declares a custom record tag key and the value used when no extractor value is available.
     */
    fun tag(key: String, defaultValue: String = "NONE") {
        validateRecordTagKey(key)
        tags += RecordMetricTagDefinition(key, defaultValue)
    }

    internal fun build(): RecordDrivenTagSchema =
        RecordDrivenTagSchema(tags.toList())
}

/**
 * Creates a record-driven tag schema with default value `NONE` for every key.
 */
fun recordDrivenTagSchema(vararg keys: String): RecordDrivenTagSchema =
    recordDrivenTagSchema {
        keys.forEach { tag(it) }
    }

/**
 * Creates a record-driven tag schema from tag key to default value mappings.
 */
fun recordDrivenTagSchema(defaultValuesByKey: Map<String, String>): RecordDrivenTagSchema =
    recordDrivenTagSchema {
        defaultValuesByKey.forEach { (key, defaultValue) ->
            tag(key, defaultValue)
        }
    }

/**
 * Creates a record-driven tag schema with explicit keys and default values.
 */
fun recordDrivenTagSchema(block: RecordDrivenTagSchemaBuilder.() -> Unit): RecordDrivenTagSchema =
    RecordDrivenTagSchemaBuilder().apply(block).build()

private fun validateRecordTagKey(key: String) {
    require(key.isNotBlank()) { "Record metric tag key must not be blank" }
    require(key !in reservedRecordTagKeys) { "Record metric tag key '$key' is reserved" }
}

/**
 * Extracts one record-driven custom tag value from a Kafka record.
 */
fun interface RecordDrivenTagValueExtractor<K, V> {
    fun extract(record: ConsumerRecord<K, V>): String?
}

/**
 * Per-consumer values for tags declared by [RecordDrivenTagSchema].
 */
class RecordDrivenTagValues<K, V> internal constructor(
    internal val extractors: Map<String, RecordDrivenTagValueExtractor<K, V>>
) {
    companion object {
        fun <K, V> none(): RecordDrivenTagValues<K, V> =
            RecordDrivenTagValues(emptyMap())
    }
}

/**
 * Builder for [RecordDrivenTagValues].
 */
class RecordDrivenTagValuesBuilder<K, V> internal constructor() {
    private val extractors = linkedMapOf<String, RecordDrivenTagValueExtractor<K, V>>()

    /**
     * Declares an extractor for a custom record tag key.
     */
    fun tag(key: String, extractor: RecordDrivenTagValueExtractor<K, V>) {
        extractors[key] = extractor
    }

    /**
     * Declares an extractor for a custom record tag key.
     */
    fun tag(key: String, extractor: (ConsumerRecord<K, V>) -> String?) {
        extractors[key] = RecordDrivenTagValueExtractor(extractor)
    }

    internal fun build(): RecordDrivenTagValues<K, V> =
        RecordDrivenTagValues(extractors.toMap())
}

/**
 * Creates record-driven tag values from an extractor map.
 */
fun <K, V> recordDrivenTagValues(
    extractors: Map<String, RecordDrivenTagValueExtractor<K, V>>
): RecordDrivenTagValues<K, V> =
    RecordDrivenTagValues(extractors.toMap())

/**
 * Creates record-driven tag values with a Kotlin builder.
 */
fun <K, V> recordDrivenTagValues(
    block: RecordDrivenTagValuesBuilder<K, V>.() -> Unit
): RecordDrivenTagValues<K, V> =
    RecordDrivenTagValuesBuilder<K, V>().apply(block).build()
