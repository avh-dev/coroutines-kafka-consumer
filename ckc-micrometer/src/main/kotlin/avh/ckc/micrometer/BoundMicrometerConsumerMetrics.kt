package avh.ckc.micrometer

import avh.ckc.core.metrics.BackpressureAction
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.ConsumerPartitionStats
import avh.ckc.core.metrics.ConsumerRuntimeStats
import avh.ckc.core.metrics.RecordDropReason
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.concurrent.TimeUnit

internal class BoundMicrometerConsumerMetrics<K, V>(
    private val schema: MicrometerConsumerMetricsSchema,
    consumerId: String,
    private val recordDrivenTagExtractors: RecordDrivenTagExtractors<K, V>
) : ConsumerMetrics<K, V> {
    private val consumerTags: Tags = Tags.of(schema.staticTags).and("consumer_id", consumerId)
    private val runtimeMeters = mutableListOf<Meter>()
    private val partitionMeters = mutableMapOf<Pair<String, Int>, Meter>()

    override fun bindRuntimeMetrics(stats: ConsumerRuntimeStats) {
        if (runtimeMeters.isNotEmpty()) {
            return
        }

        runtimeMeters += gauge(WORKERS, stats) { it.workerCount.toDouble() }
        runtimeMeters += gauge(WORKERS_ACTIVE, stats) { it.activeWorkerCount.toDouble() }
        runtimeMeters += gauge(WORK_QUEUE_SIZE, stats) { it.workQueueSize.toDouble() }
        runtimeMeters += gauge(WORK_QUEUE_CAPACITY, stats) { it.workQueueCapacity.toDouble() }
        runtimeMeters += gauge(WORK_QUEUE_MAX, stats) { it.maxObservedWorkQueueSize.toDouble() }
        runtimeMeters += gauge(ORDERING_QUEUE_SIZE, stats) { it.orderingQueueSize.toDouble() }
        runtimeMeters += gauge(ORDERING_QUEUE_MAX, stats) { it.maxObservedOrderingQueueSize.toDouble() }
    }

    override fun unbindRuntimeMetrics() {
        runtimeMeters.forEach(schema.meterRegistry::remove)
        runtimeMeters.clear()
    }

    override fun bindPartitionMetrics(stats: ConsumerPartitionStats) {
        val key = stats.topic to stats.partition
        if (partitionMeters.containsKey(key)) {
            return
        }

        partitionMeters[key] = Gauge.builder(schema.metricName(OFFSETTRACKER_CAPACITY), stats) {
            it.offsetTrackerBitCapacity.toDouble()
        }
            .tags(consumerTags)
            .tag("topic", stats.topic)
            .tag("partition", stats.partition.toString())
            .register(schema.meterRegistry)
    }

    override fun unbindPartitionMetrics(topic: String, partition: Int) {
        val meter = partitionMeters.remove(topic to partition) ?: return
        schema.meterRegistry.remove(meter)
    }

    override fun onPoll(recordsCount: Int, durationNanos: Long) {
        timer(POLL_DURATION).record(durationNanos, TimeUnit.NANOSECONDS)
        summary(POLL_RECORDS).record(recordsCount.toDouble())
    }

    override fun onRecordProcessed(
        key: K?,
        value: V?,
        record: ConsumerRecord<K, V>,
        recordAgeMillis: Long,
        durationNanos: Long
    ) {
        val tags = recordTags(record)
        timer(RECORD_PROCESS_DURATION, tags).record(durationNanos, TimeUnit.NANOSECONDS)
        timer(RECORD_AGE, tags.and("error", "none")).record(recordAgeMillis, TimeUnit.MILLISECONDS)
    }

    override fun onRecordFailed(
        key: K?,
        value: V?,
        record: ConsumerRecord<K, V>,
        recordAgeMillis: Long,
        error: Throwable,
        durationNanos: Long
    ) {
        val tags = recordTags(record).and("error", error::class.java.simpleName)
        timer(RECORD_FAILED_DURATION, tags).record(durationNanos, TimeUnit.NANOSECONDS)
        timer(RECORD_AGE, tags).record(recordAgeMillis, TimeUnit.MILLISECONDS)
    }

    override fun onRecordDropped(record: ConsumerRecord<K, V>, reason: RecordDropReason) {
        counter(
            RECORD_DROPPED,
            tags(
                "topic" to record.topic(),
                "reason" to reason.name.lowercase()
            )
        ).increment()
    }

    override fun onRetry(
        key: K?,
        value: V?,
        record: ConsumerRecord<K, V>,
        attempt: Int,
        error: Throwable
    ) {
        counter(
            RECORD_RETRY,
            recordTags(record)
                .and("attempt", attempt.toString())
                .and("error", error::class.java.simpleName)
        ).increment()
    }

    override fun onCommit(partitionsCount: Int, offsetsCount: Long, durationNanos: Long, success: Boolean) {
        val tags = tags("success" to success.toString())
        timer(COMMIT_DURATION, tags).record(durationNanos, TimeUnit.NANOSECONDS)
        summary(COMMIT_PARTITIONS, tags).record(partitionsCount.toDouble())
        summary(COMMIT_OFFSETS, tags).record(offsetsCount.toDouble())
    }

    override fun onBackpressurePauseResume(action: BackpressureAction, partitionsCount: Int) {
        val tags = tags("action" to action.name.lowercase())
        counter(PAUSE_RESUME, tags).increment()
        summary(PAUSE_RESUME_PARTITIONS, tags).record(partitionsCount.toDouble())
    }

    override fun onConsumerFailure(error: Throwable) {
        counter(FAILURE, tags("error" to error::class.java.simpleName)).increment()
    }

    private fun timer(suffix: String, tags: Iterable<Tag> = consumerTags): Timer =
        schema.timer(suffix, tags)

    private fun summary(suffix: String, tags: Iterable<Tag> = consumerTags): DistributionSummary =
        schema.summary(suffix, tags)

    private fun counter(suffix: String, tags: Iterable<Tag> = consumerTags): Counter =
        schema.counter(suffix, tags)

    private fun gauge(
        suffix: String,
        stats: ConsumerRuntimeStats,
        valueFunction: (ConsumerRuntimeStats) -> Double
    ): Gauge =
        Gauge.builder(schema.metricName(suffix), stats, valueFunction)
            .tags(consumerTags)
            .register(schema.meterRegistry)

    private fun tags(vararg pairs: Pair<String, String>): Tags =
        schema.tags(consumerTags, *pairs)

    private fun recordTags(record: ConsumerRecord<K, V>): Tags =
        schema.recordTags(consumerTags, recordDrivenTagExtractors, record)
}
