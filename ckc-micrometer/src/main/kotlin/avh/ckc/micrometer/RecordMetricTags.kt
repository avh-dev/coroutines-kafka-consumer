package avh.ckc.micrometer

import io.micrometer.core.instrument.Tag
import org.apache.kafka.clients.consumer.ConsumerRecord

private val reservedRecordTagKeys = setOf("consumer_id", "topic", "error", "attempt", "success", "reason")

/**
 * Definition of one record-driven custom metric tag.
 *
 * Custom tags are schema, not ad-hoc labels: all record metrics created by a
 * single [MicrometerConsumerMetricsSchema] instance use the same declared tag keys.
 * This keeps Prometheus/OpenMetrics output valid when different consumers emit
 * the same metric family.
 */
data class RecordMetricTagDefinition(
    val key: String,
    val defaultValue: String
) {
    init {
        validateRecordTagKey(key)
    }
}

/**
 * Builder for record-driven custom metric tag definitions.
 */
class RecordDrivenTagsBuilder internal constructor() {
    private val tags = mutableListOf<RecordMetricTagDefinition>()

    /**
     * Declares a custom record tag key and the value used when no extractor value is available.
     */
    fun tag(key: String, defaultValue: String = "NONE") {
        tags += RecordMetricTagDefinition(key, defaultValue)
    }

    internal fun build(): List<RecordMetricTagDefinition> =
        tags.toList().also(::validateRecordDrivenTags)
}

/**
 * Creates a record-driven tag schema with default value `NONE` for every key.
 */
fun recordDrivenTags(vararg keys: String): List<RecordMetricTagDefinition> =
    recordDrivenTags {
        keys.forEach { tag(it) }
    }

/**
 * Creates a record-driven tag schema from tag key to default value mappings.
 */
fun recordDrivenTags(defaultValuesByKey: Map<String, String>): List<RecordMetricTagDefinition> =
    recordDrivenTags {
        defaultValuesByKey.forEach { (key, defaultValue) ->
            tag(key, defaultValue)
        }
    }

/**
 * Creates a record-driven tag schema with explicit keys and default values.
 */
fun recordDrivenTags(block: RecordDrivenTagsBuilder.() -> Unit): List<RecordMetricTagDefinition> =
    RecordDrivenTagsBuilder().apply(block).build()

internal fun validateRecordDrivenTags(tags: List<RecordMetricTagDefinition>) {
    val duplicateKeys = tags.groupBy { it.key }.filterValues { it.size > 1 }.keys
    require(duplicateKeys.isEmpty()) { "Record-driven tags contain duplicate keys: ${duplicateKeys.joinToString()}" }
}

internal val List<RecordMetricTagDefinition>.keys: Set<String>
    get() = mapTo(LinkedHashSet()) { it.key }

internal fun <K, V> List<RecordMetricTagDefinition>.tagsFrom(
    provider: RecordDrivenTagExtractors<K, V>,
    record: ConsumerRecord<K, V>
): List<Tag> =
    map { tag ->
        val value = provider.extractors[tag.key]?.extract(record)
        Tag.of(tag.key, value ?: tag.defaultValue)
    }

private fun validateRecordTagKey(key: String) {
    require(key.isNotBlank()) { "Record metric tag key must not be blank" }
    require(key !in reservedRecordTagKeys) { "Record metric tag key '$key' is reserved" }
}

/**
 * Extracts one record-driven custom tag value from a Kafka record.
 */
fun interface RecordDrivenTagExtractor<K, V> {
    fun extract(record: ConsumerRecord<K, V>): String?
}

/**
 * Per-consumer extractors for record-driven custom tags declared by [MicrometerConsumerMetricsSchema].
 */
class RecordDrivenTagExtractors<K, V> internal constructor(
    internal val extractors: Map<String, RecordDrivenTagExtractor<K, V>>
) {
    companion object {
        fun <K, V> none(): RecordDrivenTagExtractors<K, V> =
            RecordDrivenTagExtractors(emptyMap())
    }
}

/**
 * Builder for [RecordDrivenTagExtractors].
 */
class RecordDrivenTagExtractorsBuilder<K, V> internal constructor() {
    private val extractors = linkedMapOf<String, RecordDrivenTagExtractor<K, V>>()

    /**
     * Declares an extractor for a custom record tag key.
     */
    fun tag(key: String, extractor: RecordDrivenTagExtractor<K, V>) {
        extractors[key] = extractor
    }

    /**
     * Declares an extractor for a custom record tag key.
     */
    fun tag(key: String, extractor: (ConsumerRecord<K, V>) -> String?) {
        extractors[key] = RecordDrivenTagExtractor(extractor)
    }

    internal fun build(): RecordDrivenTagExtractors<K, V> =
        RecordDrivenTagExtractors(extractors.toMap())
}

/**
 * Creates record-driven tag extractors from an extractor map.
 */
fun <K, V> recordDrivenTagExtractors(
    extractors: Map<String, RecordDrivenTagExtractor<K, V>>
): RecordDrivenTagExtractors<K, V> =
    RecordDrivenTagExtractors(extractors.toMap())

/**
 * Creates record-driven tag extractors with a Kotlin builder.
 */
fun <K, V> recordDrivenTagExtractors(
    block: RecordDrivenTagExtractorsBuilder<K, V>.() -> Unit
): RecordDrivenTagExtractors<K, V> =
    RecordDrivenTagExtractorsBuilder<K, V>().apply(block).build()
