package avh.ckc.micrometer

internal class MicrometerMetricNames(metricPrefix: String) {
    private val metricPrefix = "$metricPrefix.ckc"

    fun fullName(suffix: String): String = "$metricPrefix.$suffix"
}

internal const val RECORD_PROCESS_DURATION = "record.process.duration"
internal const val RECORD_FAILED_DURATION = "record.failed.duration"
internal const val RECORD_AGE = "record.age"
internal const val RECORD_DROPPED = "record.dropped"
internal const val RECORD_RETRY = "record.retry"

internal const val POLL_DURATION = "poll.duration"
internal const val POLL_RECORDS = "poll.records"

internal const val COMMIT_DURATION = "commit.duration"
internal const val COMMIT_PARTITIONS = "commit.partitions"
internal const val COMMIT_OFFSETS = "commit.offsets"

internal const val PAUSE_RESUME = "pause.resume"
internal const val PAUSE_RESUME_PARTITIONS = "pause.resume.partitions"

internal const val FAILURE = "failure"

internal const val WORKERS = "workers"
internal const val WORKERS_ACTIVE = "workers.active"
internal const val WORK_QUEUE_SIZE = "work.queue.size"
internal const val WORK_QUEUE_CAPACITY = "work.queue.capacity"
internal const val WORK_QUEUE_MAX = "work.queue.max"
internal const val ORDERING_QUEUE_SIZE = "ordering.queue.size"
internal const val ORDERING_QUEUE_MAX = "ordering.queue.max"

internal const val OFFSETTRACKER_CAPACITY = "offsettracker.capacity"
