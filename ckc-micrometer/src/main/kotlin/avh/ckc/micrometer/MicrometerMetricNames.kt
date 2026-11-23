package avh.ckc.micrometer

internal class MicrometerMetricNames(metricPrefix: String) {
    private val metricPrefix = "$metricPrefix.ckc"

    fun fullName(suffix: String): String = "$metricPrefix.$suffix"
}

internal const val RECORD_PROCESS_DURATION = "record.process.duration"
internal const val RECORD_FAILED_DURATION = "record.failed.duration"
internal const val RECORD_END_TO_END_DURATION = "record.end.to.end.duration"
internal const val RECORD_DROPPED = "record.dropped"
internal const val RECORD_HANDED_OFF = "record.handed.off"
internal const val RECORD_RETRY = "record.retry"

internal const val POLL_DURATION = "poll.duration"
internal const val POLL_RECORDS = "poll.records"

internal const val COMMIT_DURATION = "commit.duration"
internal const val COMMIT_PARTITIONS = "commit.partitions"
internal const val COMMIT_OFFSETS = "commit.offsets"
internal const val COMMIT_METADATA_SIZE = "commit.metadata.size"
internal const val COMMIT_METADATA_LIMIT_UTILIZATION = "commit.metadata.limit.utilization"
internal const val COMMIT_METADATA_COMPRESSION_RATIO = "commit.metadata.payload.compression.ratio"
internal const val COMMIT_METADATA_PAYLOAD_RAW_SIZE = "commit.metadata.payload.raw.size"
internal const val COMMIT_METADATA_PAYLOAD_ENCODED_SIZE = "commit.metadata.payload.encoded.size"

internal const val PAUSE_RESUME = "pause.resume"
internal const val PAUSE_RESUME_PARTITIONS = "pause.resume.partitions"

internal const val FAILURE = "failure"
internal const val TERMINAL_FAILURE = "terminal.failure"
internal const val PARTITION_HANDOFF_DURATION = "partition.handoff.duration"
internal const val SHUTDOWN_DURATION = "shutdown.duration"
internal const val RECOVERY_ATTEMPT = "recovery.attempt"
internal const val RECOVERY_DURATION = "recovery.duration"
internal const val RECOVERY_ACTIVE = "recovery.active"

internal const val WORKERS = "workers"
internal const val WORKERS_ACTIVE = "workers.active"
internal const val WORK_QUEUE_SIZE = "work.queue.size"
internal const val WORK_QUEUE_CAPACITY = "work.queue.capacity"
internal const val WORK_QUEUE_MAX = "work.queue.max"
internal const val ORDERING_QUEUE_SIZE = "ordering.queue.size"
internal const val ORDERING_QUEUE_MAX = "ordering.queue.max"

internal const val OFFSETTRACKER_CAPACITY = "offsettracker.capacity"
