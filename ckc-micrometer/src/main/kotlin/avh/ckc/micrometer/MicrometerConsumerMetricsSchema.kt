package avh.ckc.micrometer

import avh.ckc.core.metrics.ConsumerMetrics
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.logging.Logger

/**
 * Describes a Micrometer metric family used to create CKC [ConsumerMetrics] instances.
 *
 * In plain Kotlin usage the schema also carries the target [MeterRegistry]. In Spring Boot usage,
 * the registry is usually injected by auto-configuration while users configure schema fields such
 * as [metricPrefix], [staticTags], and [recordDrivenTags].
 *
 * Create one schema for a metric family configuration, then create concrete [ConsumerMetrics]
 * instances with [micrometerConsumerMetrics]. Consumers created from the same schema share the same
 * metric prefix, static tags, and record-driven custom tag definitions.
 *
 * @param meterRegistry registry that receives CKC meters.
 * @param metricPrefix user-defined prefix prepended to the permanent `ckc`
 * namespace. For example, `myapp` produces metrics such as
 * `myapp.ckc.record.process.duration`.
 * @param staticTags tags attached to every meter created by this schema.
 * @param recordDrivenTags record-driven custom tag keys and default values that
 * may be populated by per-consumer extractors.
 */
open class MicrometerConsumerMetricsSchema(
    internal val meterRegistry: MeterRegistry,
    val metricPrefix: String,
    internal val staticTags: List<Tag> = emptyList(),
    internal val recordDrivenTags: List<RecordMetricTagDefinition> = emptyList()
) {
    init {
        require(metricPrefix.isNotBlank()) { "Metric prefix must not be blank" }
        require(!metricPrefix.startsWith(".")) { "Metric prefix must not start with '.'" }
        require(!metricPrefix.endsWith(".")) { "Metric prefix must not end with '.'" }
        require(!metricPrefix.endsWith(".ckc")) {
            "Metric prefix must not include the permanent '.ckc' segment"
        }
        validateRecordDrivenTags(recordDrivenTags)
    }

    private val metricNames = MicrometerMetricNames(metricPrefix)

    internal fun <K, V> createConsumerMetrics(
        consumerId: String,
        recordDrivenTagExtractors: RecordDrivenTagExtractors<K, V>
    ): ConsumerMetrics<K, V> {
        require(consumerId.isNotBlank()) { "Consumer id must not be blank" }
        val unknownKeys = recordDrivenTagExtractors.extractors.keys - recordDrivenTags.keys
        if (unknownKeys.isNotEmpty()) {
            logger.warning(
                "Ignoring record-driven tag extractors not declared in the Micrometer record-driven tag schema: " +
                    unknownKeys.joinToString()
            )
        }
        return BoundMicrometerConsumerMetrics(this, consumerId, recordDrivenTagExtractors)
    }

    internal fun timer(suffix: String, tags: Iterable<Tag> = staticTags): Timer =
        Timer.builder(metricName(suffix))
            .tags(tags)
            .register(meterRegistry)

    internal fun summary(suffix: String, tags: Iterable<Tag> = staticTags): DistributionSummary =
        DistributionSummary.builder(metricName(suffix))
            .tags(tags)
            .register(meterRegistry)

    internal fun counter(suffix: String, tags: Iterable<Tag> = staticTags): Counter =
        Counter.builder(metricName(suffix))
            .tags(tags)
            .register(meterRegistry)

    internal fun metricName(suffix: String): String =
        metricNames.fullName(suffix)

    internal fun tags(baseTags: Iterable<Tag> = staticTags, vararg pairs: Pair<String, String>): Tags =
        Tags.of(baseTags).and(pairs.map { Tag.of(it.first, it.second) })

    internal fun <K, V> recordTags(
        baseTags: Iterable<Tag>,
        recordDrivenTagExtractors: RecordDrivenTagExtractors<K, V>,
        record: ConsumerRecord<K, V>
    ): Tags {
        return tags(
            baseTags,
            "topic" to record.topic()
        ).and(recordDrivenTags.tagsFrom(recordDrivenTagExtractors, record))
    }

    companion object {
        const val DEFAULT_CONSUMER_ID: String = "default"
        private val logger: Logger = Logger.getLogger(MicrometerConsumerMetricsSchema::class.java.name)
    }
}

/**
 * Builder for a Micrometer-backed [ConsumerMetrics] instance bound to one CKC consumer.
 *
 * The builder is used by [micrometerConsumerMetrics]. If [consumerId] is not changed, the
 * bound metrics use `consumer_id=default`. [recordDrivenTagExtractors] may provide extractors for
 * custom tags declared by the schema's [MicrometerConsumerMetricsSchema.recordDrivenTags].
 */
class MicrometerConsumerMetricsBuilder<K, V> internal constructor() {
    /**
     * Logical CKC consumer id written to the `consumer_id` tag.
     *
     * This is not Kafka `group.id` or Kafka `client.id`. Keep it stable and low-cardinality.
     */
    var consumerId: String = MicrometerConsumerMetricsSchema.DEFAULT_CONSUMER_ID

    /**
     * Per-consumer extractors for record-driven custom tags.
     *
     * Missing extractors and `null` extractor results use defaults from the metrics schema.
     */
    var recordDrivenTagExtractors: RecordDrivenTagExtractors<K, V> =
        RecordDrivenTagExtractors.none()
}

/**
 * Creates a Micrometer-backed [ConsumerMetrics] instance for one CKC consumer.
 *
 * The returned metrics instance has a stable `consumer_id` tag and uses the record-driven tag schema from
 * [schema]. Extractors declared in [block] are validated against the schema once at
 * creation time; unknown extractor keys are logged and ignored.
 */
fun <K, V> micrometerConsumerMetrics(
    schema: MicrometerConsumerMetricsSchema,
    block: MicrometerConsumerMetricsBuilder<K, V>.() -> Unit = {}
): ConsumerMetrics<K, V> {
    val builder = MicrometerConsumerMetricsBuilder<K, V>().apply(block)
    return schema.createConsumerMetrics(builder.consumerId, builder.recordDrivenTagExtractors)
}
