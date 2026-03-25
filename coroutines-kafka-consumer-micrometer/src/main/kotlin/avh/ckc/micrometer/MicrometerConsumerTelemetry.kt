package avh.ckc.micrometer

import avh.ckc.core.ConsumerTelemetry
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import java.util.concurrent.TimeUnit

class MicrometerConsumerTelemetry(
    private val meterRegistry: MeterRegistry,
    private val meterPrefix: String = "ckc",
    private val commonTags: Iterable<Tag> = emptyList()
) : ConsumerTelemetry {

    override fun onPoll(recordsCount: Int, durationNanos: Long) {
        timer("poll.duration").record(durationNanos, TimeUnit.NANOSECONDS)
        summary("poll.records").record(recordsCount.toDouble())
    }

    override fun onRecordProcessed(topic: String, partition: Int, recordAgeMillis: Long, durationNanos: Long) {
        val tags = recordTags(topic, partition)
        timer("record.process.duration", tags).record(durationNanos, TimeUnit.NANOSECONDS)
        summary("record.age", tags).record(recordAgeMillis.toDouble())
        counter("record.processed", tags).increment()
    }

    override fun onRecordFailed(
        topic: String,
        partition: Int,
        recordAgeMillis: Long,
        error: Throwable,
        durationNanos: Long
    ) {
        val tags = recordTags(topic, partition).and("error", error::class.java.simpleName)
        timer("record.failed.duration", tags).record(durationNanos, TimeUnit.NANOSECONDS)
        summary("record.age", tags).record(recordAgeMillis.toDouble())
        counter("record.failed", tags).increment()
    }

    override fun onRetry(topic: String, partition: Int, attempt: Int, error: Throwable) {
        counter(
            "record.retry",
            recordTags(topic, partition)
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

    private fun recordTags(topic: String, partition: Int): Tags =
        tags(
            "topic" to topic,
            "partition" to partition.toString()
        )

    private fun tags(vararg pairs: Pair<String, String>): Tags =
        Tags.of(commonTags).and(pairs.map { Tag.of(it.first, it.second) })

    private fun metricName(suffix: String): String = "$meterPrefix.$suffix"
}
