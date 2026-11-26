package avh.ckc.warmup.broker

import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.TimeMark
import kotlin.time.TimeSource
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.withTimeoutOrNull

internal const val BROKER_DRAINING = "broker_draining"

/** Local monotonic cutoffs derived from the broker's shared wall-clock deadline. */
internal class WarmupBrokerDrain(
    deadlineMillis: Long,
    cleanupTimeout: Duration,
    operationTimeout: Duration
) {
    private val start = TimeSource.Monotonic.markNow()
    private val budget = (deadlineMillis - System.currentTimeMillis()).coerceAtLeast(0).milliseconds
    // Reserve time for the recipient fence, then for donor fallback and lifecycle confirmations.
    private val publication = minOf(operationTimeout, budget / 8)
    private val confirmation = minOf(cleanupTimeout + operationTimeout, budget / 4)
    private val cleanup = minOf(cleanupTimeout, budget / 3)
    private val deadline: TimeMark = start + budget
    val fenceDeadline: TimeMark = deadline - confirmation
    val forwardingAckDeadline: TimeMark = fenceDeadline + confirmation / 2
    val cleanupDeadline: TimeMark = fenceDeadline - publication
    val processingDeadline: TimeMark = cleanupDeadline - cleanup
    val consumerStopDeadline: TimeMark = processingDeadline + cleanup / 2
    val resultFlushDeadline: TimeMark = cleanupDeadline - cleanup / 4
}

internal fun TimeMark.remaining(): Duration = -elapsedNow()

/** Switches to the broker cutoff without cancelling work already in progress. */
internal suspend fun <T : Any> Deferred<T>.awaitDuringDrain(
    normalTimeout: Duration,
    drain: Deferred<WarmupBrokerDrain>,
    remaining: (WarmupBrokerDrain) -> Duration
): T? {
    val request = withTimeoutOrNull(normalTimeout) {
        select {
            onAwait { null }
            drain.onAwait { it }
        }
    }
    if (isCompleted) return await()
    val planned = request ?: if (drain.isCompleted) drain.await() else return null
    return withTimeoutOrNull(remaining(planned)) { await() }
}
