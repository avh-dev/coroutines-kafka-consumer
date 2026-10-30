package avh.ckc.core.polling

import avh.ckc.core.PollLoopStateSnapshot
import avh.ckc.core.warmup.WarmupForwardingBoundary
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlin.time.Duration

internal interface ConsumerPollLoopControl {
    fun start(): Job

    /** Completes after Kafka assignment and initial positions are established. */
    fun readiness(): Deferred<Unit> = kotlinx.coroutines.CompletableDeferred(Unit)

    fun drainWarmup(boundary: WarmupForwardingBoundary): Deferred<Unit> =
        error("Warm-up drain is not supported by this poll loop")

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
