package avh.ckc.core.polling

import avh.ckc.core.PollLoopStateSnapshot
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlin.time.Duration

internal interface ConsumerPollLoopControl {
    fun start(): Job

    fun prepareForShutdown(): Deferred<Unit>

    fun prepareForShutdown(deadlineNanos: Long): Deferred<Unit> = prepareForShutdown()

    /** Requests final poll-thread commit and Kafka client closure after workers drained. */
    fun finalizeShutdown(): Deferred<Unit> = prepareForShutdown()

    fun stateSnapshot(): PollLoopStateSnapshot

    /** Allows the owner to apply lifecycle policy without widening test factory signatures. */
    fun configurePartitionHandoffTimeout(timeout: Duration) = Unit

    fun configureShutdownDrainTimeout(timeout: Duration) = Unit

    fun configureProcessingCancellationTimeout(timeout: Duration) = Unit

    fun configureRecovery(timeout: Duration, retryBackoff: Duration) = Unit

    /** Releases resources owned by this poll loop after its job has terminated. */
    suspend fun releaseResources() = Unit
}
