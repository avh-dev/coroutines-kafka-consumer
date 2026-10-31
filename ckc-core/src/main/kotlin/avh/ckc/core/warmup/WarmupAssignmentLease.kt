package avh.ckc.core.warmup

/**
 * A globally exclusive assignment reservation. Implementations must be linearizable across every
 * application replica using the same warm-up partitions; an in-process lock is not sufficient.
 */
interface WarmupAssignmentLease {
    val assignments: List<WarmupPartitionAssignment>
    suspend fun release()
}

fun interface WarmupAssignmentLeaseProvider {
    /** Returns null when another session currently owns the required global warm-up capacity. */
    suspend fun tryAcquire(
        sessionId: WarmupSessionId,
        donorInstanceId: WarmupInstanceId,
        recipientInstanceId: WarmupInstanceId,
        participants: List<WarmupParticipantConfiguration>
    ): WarmupAssignmentLease?
}
