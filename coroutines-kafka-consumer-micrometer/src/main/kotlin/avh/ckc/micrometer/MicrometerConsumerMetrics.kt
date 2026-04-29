package avh.ckc.micrometer

import avh.ckc.core.ConsumerMetrics
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.concurrent.TimeUnit

private val reservedRecordTagKeys = setOf("topic", "partition", "error", "attempt", "success")

/**
 * Definition of a custom record tag that may appear on record-level metrics.
 *
 * The metric schema is owned by [MicrometerConsumerMetrics] and must stay stable for Prometheus.
 * Consumer-specific providers may only assign values to tag definitions declared upfront.
 */
class RecordMetricTag internal constructor(
    val key: String,
    val missingValue: String
)

fun recordMetricTag(key: String, missingValue: String = "NONE"): RecordMetricTag {
    require(key.isNotBlank()) { "Record metric tag key must not be blank" }
    require(key !in reservedRecordTagKeys) { "Record metric tag key '$key' is reserved" }
    return RecordMetricTag(key, missingValue)
}

class RecordMetricTagSchema internal constructor(
    internal val tags: List<RecordMetricTag>
) {
    init {
        val duplicateKeys = tags.groupBy { it.key }.filterValues { it.size > 1 }.keys
        require(duplicateKeys.isEmpty()) { "Record metric tag schema contains duplicate keys: ${duplicateKeys.joinToString()}" }
    }

    companion object {
        fun empty(): RecordMetricTagSchema = RecordMetricTagSchema(emptyList())
    }
}

fun recordMetricTagSchema(vararg tags: RecordMetricTag): RecordMetricTagSchema =
    RecordMetricTagSchema(tags.toList())

class RecordMetricTagValueBuilder internal constructor(
    private val schema: RecordMetricTagSchema
) {
    private val values = LinkedHashMap<String, String?>()

    fun set(tag: RecordMetricTag, value: String?) {
        require(schema.tags.any { it.key == tag.key }) {
            "Record metric tag '${tag.key}' is not declared in the metrics schema"
        }
        values[tag.key] = value
    }

    internal fun toTags(): List<Tag> =
        schema.tags.map { schemaTag ->
            Tag.of(schemaTag.key, values[schemaTag.key] ?: schemaTag.missingValue)
        }
}

fun interface ConsumerRecordTagValueProvider<in K, in V> {
    fun populateTags(builder: RecordMetricTagValueBuilder, key: K?, value: V?, record: ConsumerRecord<ByteArray, ByteArray>)

    companion object {
        fun none(): ConsumerRecordTagValueProvider<Any?, Any?> = ConsumerRecordTagValueProvider { _, _, _, _ -> }
    }
}

@Suppress("UNCHECKED_CAST")
fun <K, V> consumerRecordTagValueProvider(
    block: RecordMetricTagValueBuilder.(K?, V?, ConsumerRecord<ByteArray, ByteArray>) -> Unit
): ConsumerRecordTagValueProvider<Any?, Any?> =
    ConsumerRecordTagValueProvider { builder, key, value, record -> builder.block(key as K?, value as V?, record) }

/**
 * Shared Micrometer-backed metrics adapter that owns the metric schema.
 *
 * One instance should be created per metric family configuration. Concrete
 * [ConsumerMetrics] instances are then bound per consumer via [forConsumer].
 */
open class MicrometerConsumerMetrics(
    private val meterRegistry: MeterRegistry,
    private val meterPrefix: String = "ckc",
    private val commonTags: Iterable<Tag> = emptyList(),
    private val recordTagSchema: RecordMetricTagSchema = RecordMetricTagSchema.empty()
) {

    fun <K, V> forConsumer(
        recordTagValueProvider: ConsumerRecordTagValueProvider<K, V> =
            ConsumerRecordTagValueProvider.none() as ConsumerRecordTagValueProvider<K, V>
    ): ConsumerMetrics<K, V> = BoundConsumerMetrics(recordTagValueProvider)

    private inner class BoundConsumerMetrics<K, V>(
        private val recordTagValueProvider: ConsumerRecordTagValueProvider<K, V>
    ) : ConsumerMetrics<K, V> {

        override fun onPoll(recordsCount: Int, durationNanos: Long) {
            timer("poll.duration").record(durationNanos, TimeUnit.NANOSECONDS)
            summary("poll.records").record(recordsCount.toDouble())
        }

        override fun onRecordProcessed(
            key: K?,
            value: V?,
            record: ConsumerRecord<ByteArray, ByteArray>,
            recordAgeMillis: Long,
            durationNanos: Long
        ) {
            val tags = recordTags(recordTagValueProvider, key, value, record)
            timer("record.process.duration", tags).record(durationNanos, TimeUnit.NANOSECONDS)
            summary("record.age", tags.and("error", "none")).record(recordAgeMillis.toDouble())
            counter("record.processed", tags).increment()
        }

        override fun onRecordFailed(
            key: K?,
            value: V?,
            record: ConsumerRecord<ByteArray, ByteArray>,
            recordAgeMillis: Long,
            error: Throwable,
            durationNanos: Long
        ) {
            val tags = recordTags(recordTagValueProvider, key, value, record).and("error", error::class.java.simpleName)
            timer("record.failed.duration", tags).record(durationNanos, TimeUnit.NANOSECONDS)
            summary("record.age", tags).record(recordAgeMillis.toDouble())
            counter("record.failed", tags).increment()
        }

        override fun onRetry(
            key: K?,
            value: V?,
            record: ConsumerRecord<ByteArray, ByteArray>,
            attempt: Int,
            error: Throwable
        ) {
            counter(
                "record.retry",
                recordTags(recordTagValueProvider, key, value, record)
                    .and("attempt", attempt.toString())
                    .and("error", error::class.java.simpleName)
            ).increment()
        }

        override fun onCommit(partitionsCount: Int, durationNanos: Long, success: Boolean) {
            val tags = tags("success" to success.toString())
            timer("commit.duration", tags).record(durationNanos, TimeUnit.NANOSECONDS)
            summary("commit.partitions", tags).record(partitionsCount.toDouble())
            counter("commit", tags).increment()
        }

        override fun onConsumerFailure(error: Throwable) {
            counter("failure", tags("error" to error::class.java.simpleName)).increment()
        }
    }

    private fun timer(name: String, tags: Iterable<Tag> = commonTags): Timer =
        Timer.builder(metricName(name))
            .tags(tags)
            .register(meterRegistry)

    private fun summary(name: String, tags: Iterable<Tag> = commonTags): DistributionSummary =
        DistributionSummary.builder(metricName(name))
            .tags(tags)
            .register(meterRegistry)

    private fun counter(name: String, tags: Iterable<Tag> = commonTags): Counter =
        Counter.builder(metricName(name))
            .tags(tags)
            .register(meterRegistry)

    private fun <K, V> recordTags(
        recordTagValueProvider: ConsumerRecordTagValueProvider<K, V>,
        key: K?,
        value: V?,
        record: ConsumerRecord<ByteArray, ByteArray>
    ): Tags {
        val builder = RecordMetricTagValueBuilder(recordTagSchema)
        recordTagValueProvider.populateTags(builder, key, value, record)
        return tags(
            "topic" to record.topic(),
            "partition" to record.partition().toString()
        ).and(builder.toTags())
    }

    private fun tags(vararg pairs: Pair<String, String>): Tags =
        Tags.of(commonTags).and(pairs.map { Tag.of(it.first, it.second) })

    private fun metricName(suffix: String): String = "$meterPrefix.$suffix"
}
