package avh.ckc.core

/**
 * Lightweight point-in-time state for a CKC consumer.
 *
 * The snapshot is intended for diagnostics and health checks. Values are sampled without blocking
 * Kafka poll threads, so they can be slightly stale while the consumer is processing records.
 */
data class ConsumerStateSnapshot(
    val started: Boolean,
    val stopped: Boolean,
    val failed: Boolean,
    val failureClass: String?,
    val failureMessage: String?,
    val processingMode: ProcessingMode,
    val workerConcurrency: Int,
    val consumerPollLoopConcurrency: Int,
    val workChannelCapacity: Int,
    val processing: ProcessingRuntimeStateSnapshot,
    val pollLoops: List<PollLoopStateSnapshot>
) {
    val assignedPartitionCount: Int
        get() = pollLoops.sumOf { it.assignedPartitions.size }
}

data class ProcessingRuntimeStateSnapshot(
    val workerCount: Int,
    val activeWorkerCount: Int,
    val workQueueSize: Int,
    val workQueueCapacity: Int,
    val maxObservedWorkQueueSize: Int,
    val orderingQueueSize: Int,
    val maxObservedOrderingQueueSize: Int
)

data class PollLoopStateSnapshot(
    val id: Int,
    val started: Boolean,
    val running: Boolean,
    val shutdownRequested: Boolean,
    val assignedPartitions: List<AssignedPartitionSnapshot>,
    val lastPollEpochMillis: Long?,
    val lastPollRecordCount: Int?,
    val lastCommitAttemptEpochMillis: Long?,
    val lastCommitSucceeded: Boolean?
)

data class AssignedPartitionSnapshot(
    val topic: String,
    val partition: Int
)
