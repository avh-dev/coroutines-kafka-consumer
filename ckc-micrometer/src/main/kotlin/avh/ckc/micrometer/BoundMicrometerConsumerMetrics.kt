package avh.ckc.micrometer

import avh.ckc.core.metrics.BackpressureAction
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.ConsumerPartitionStats
import avh.ckc.core.metrics.ConsumerRuntimeStats
import avh.ckc.core.metrics.OffsetCommitMetadataStats
import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.core.metrics.RecordHandoffReason
import avh.ckc.core.metrics.LifecycleOutcome
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

internal class BoundMicrometerConsumerMetrics<K, V>(
    private val schema: MicrometerConsumerMetricsSchema,
    consumerId: String,
    private val recordDrivenTagExtractors: RecordDrivenTagExtractors<K, V>
) : ConsumerMetrics<K, V> {
    private val consumerTags: Tags = Tags.of(schema.staticTags).and("consumer_id", consumerId)
    private val kafkaClientMetrics = mutableMapOf<Int, KafkaClientMetrics>()
    private val runtimeMeters = mutableListOf<Meter>()
    private val partitionMeters = mutableMapOf<Pair<String, Int>, PartitionMeterBinding>()
    private val recoveringPollLoops = AtomicInteger(0)
    private val recoveryActiveGauge: Gauge = Gauge.builder(
        schema.metricName(RECOVERY_ACTIVE),
        recoveringPollLoops
    ) { if (it.get() > 0) 1.0 else 0.0 }
        .tags(consumerTags)
        .register(schema.meterRegistry)

    override fun bindKafkaClientMetrics(pollLoopId: Int, consumer: Consumer<K, V>) {
        if (!schema.kafkaClientMetricsEnabled) {
            return
        }
        synchronized(kafkaClientMetrics) {
            check(pollLoopId !in kafkaClientMetrics) {
                "Kafka consumer metrics are already bound for poll loop $pollLoopId"
            }
            val clientMetrics = KafkaClientMetrics(
                consumer,
                consumerTags.and("poll_loop", pollLoopId.toString())
            )
            try {
                clientMetrics.bindTo(schema.meterRegistry)
            } catch (failure: Exception) {
                clientMetrics.close()
                throw failure
            }
            kafkaClientMetrics[pollLoopId] = clientMetrics
        }
    }

    override fun unbindKafkaClientMetrics(pollLoopId: Int) {
        synchronized(kafkaClientMetrics) {
            kafkaClientMetrics.remove(pollLoopId)?.close()
        }
    }

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

    override fun bindPartitionMetrics(pollLoopId: Int, stats: ConsumerPartitionStats) {
        val key = stats.topic to stats.partition
        synchronized(partitionMeters) {
            val current = partitionMeters[key]
            if (current?.pollLoopId == pollLoopId && current.stats === stats) return
            if (current != null) {
                partitionMeters.remove(key)
                schema.meterRegistry.remove(current.meter)
            }

            val meter = Gauge.builder(schema.metricName(OFFSETTRACKER_CAPACITY), stats) {
                it.offsetTrackerBitCapacity.toDouble()
            }
                .tags(consumerTags)
                .tag("topic", stats.topic)
                .tag("partition", stats.partition.toString())
                .register(schema.meterRegistry)
            partitionMeters[key] = PartitionMeterBinding(pollLoopId, stats, meter)
        }
    }

    override fun unbindPartitionMetrics(pollLoopId: Int, stats: ConsumerPartitionStats) {
        val key = stats.topic to stats.partition
        synchronized(partitionMeters) {
            val current = partitionMeters[key] ?: return
            if (current.pollLoopId != pollLoopId || current.stats !== stats) return
            partitionMeters.remove(key)
            schema.meterRegistry.remove(current.meter)
        }
    }

    override fun onPoll(recordsCount: Int, durationNanos: Long) {
        timer(POLL_DURATION).record(durationNanos, TimeUnit.NANOSECONDS)
        summary(POLL_RECORDS).record(recordsCount.toDouble())
    }

    override fun onRecordProcessed(
        key: K?,
        value: V?,
        record: ConsumerRecord<K, V>,
        endToEndLatencyMillis: Long,
        durationNanos: Long
    ) {
        val tags = recordTags(record)
        timer(RECORD_PROCESS_DURATION, tags).record(durationNanos, TimeUnit.NANOSECONDS)
        timer(RECORD_END_TO_END_DURATION, tags).record(endToEndLatencyMillis, TimeUnit.MILLISECONDS)
    }

    override fun onRecordFailed(
        key: K?,
        value: V?,
        record: ConsumerRecord<K, V>,
        error: Throwable,
        durationNanos: Long
    ) {
        val tags = recordTags(record).and("error", error::class.java.simpleName)
        timer(RECORD_FAILED_DURATION, tags).record(durationNanos, TimeUnit.NANOSECONDS)
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

    override fun onRecordHandedOff(record: ConsumerRecord<K, V>, reason: RecordHandoffReason) {
        counter(
            RECORD_HANDED_OFF,
            tags("topic" to record.topic(), "reason" to reason.name.lowercase())
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

    override fun onOffsetCommitMetadataEncoded(stats: OffsetCommitMetadataStats) {
        val tags = tags(
            "topic" to stats.topic,
            "compression" to stats.compression.name.lowercase(),
            "included" to stats.includedInCommit.toString()
        )
        summary(COMMIT_METADATA_SIZE, tags).record(stats.candidateSizeBytes.toDouble())
        summary(COMMIT_METADATA_LIMIT_UTILIZATION, tags).record(stats.limitUtilization)
        summary(COMMIT_METADATA_COMPRESSION_RATIO, tags).record(stats.compressionRatio)
        summary(COMMIT_METADATA_PAYLOAD_RAW_SIZE, tags).record(stats.rawPayloadSizeBytes.toDouble())
        summary(COMMIT_METADATA_PAYLOAD_ENCODED_SIZE, tags).record(stats.encodedPayloadSizeBytes.toDouble())
    }

    override fun onBackpressurePauseResume(action: BackpressureAction, partitionsCount: Int) {
        val tags = tags("action" to action.name.lowercase())
        counter(PAUSE_RESUME, tags).increment()
        summary(PAUSE_RESUME_PARTITIONS, tags).record(partitionsCount.toDouble())
    }

    override fun onConsumerFailure(error: Throwable) {
        counter(FAILURE, tags("error" to error::class.java.simpleName)).increment()
        counter(TERMINAL_FAILURE, tags("error" to error::class.java.simpleName)).increment()
    }

    override fun onPartitionHandoff(durationNanos: Long, outcome: LifecycleOutcome) {
        timer(PARTITION_HANDOFF_DURATION, lifecycleTags(outcome)).record(durationNanos, TimeUnit.NANOSECONDS)
    }

    override fun onShutdown(durationNanos: Long, outcome: LifecycleOutcome) {
        timer(SHUTDOWN_DURATION, lifecycleTags(outcome)).record(durationNanos, TimeUnit.NANOSECONDS)
    }

    override fun onRecoveryAttempt() {
        counter(RECOVERY_ATTEMPT).increment()
    }

    override fun onRecoveryCompleted(durationNanos: Long, outcome: LifecycleOutcome) {
        timer(RECOVERY_DURATION, lifecycleTags(outcome)).record(durationNanos, TimeUnit.NANOSECONDS)
    }

    override fun onRecoveryStateChanged(active: Boolean) {
        if (active) {
            recoveringPollLoops.incrementAndGet()
        } else {
            recoveringPollLoops.getAndUpdate { current -> if (current > 0) current - 1 else 0 }
        }
    }

    private fun lifecycleTags(outcome: LifecycleOutcome): Tags =
        tags("outcome" to outcome.name.lowercase())

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

    private data class PartitionMeterBinding(
        val pollLoopId: Int,
        val stats: ConsumerPartitionStats,
        val meter: Meter
    )
}
