package avh.ckc.core.warmup

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.time.Duration
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.TimeSource

/** Checks the round trip to every required donor, including when no records are being delegated. */
internal class WarmupDonorLiveness(
    private val request: WarmupRequested,
    donors: Set<WarmupInstanceId>,
    private val channel: WarmupControlChannel,
    private val timeout: Duration,
    private val interval: Duration,
    timeSource: TimeSource = TimeSource.Monotonic
) {
    private val started = timeSource.markNow()
    private val replies = donors.associateWith { MutableStateFlow(0L) }

    fun onReply(reply: WarmupLivenessProbe) {
        val latest = replies[reply.senderInstanceId] ?: return
        if (reply.reply && reply.sessionId == request.sessionId && reply.namespace == request.namespace &&
            reply.targetInstanceId == request.senderInstanceId && reply.protocolVersion == request.protocolVersion &&
            reply.configurationFingerprint == request.configurationFingerprint &&
            reply.expiresAtEpochMillis == request.expiresAtEpochMillis && reply.sentAtNanos <= elapsedNanos().coerceAtLeast(1)
        ) {
            latest.update { maxOf(it, reply.sentAtNanos) }
        }
    }

    suspend fun awaitTimeout() = coroutineScope {
        val missingReply = CompletableDeferred<Unit>()
        val monitors = replies.map { (donor, latest) ->
            launch {
                val sender = launch { sendProbes(donor) }
                try {
                    awaitMissingReply(latest)
                    missingReply.complete(Unit)
                } finally {
                    sender.cancel()
                }
            }
        }
        try {
            missingReply.await()
        } finally {
            monitors.forEach { it.cancel() }
        }
    }

    private suspend fun awaitMissingReply(latest: MutableStateFlow<Long>) {
        var confirmedSend = 0L
        while (true) {
            val remaining = timeout - (elapsedNanos() - confirmedSend).nanoseconds
            if (remaining <= Duration.ZERO) return
            confirmedSend = withTimeoutOrNull(remaining) { latest.first { it > confirmedSend } } ?: return
        }
    }

    private suspend fun sendProbes(donor: WarmupInstanceId) {
        while (true) {
            val probe = WarmupLivenessProbe(
                request.namespace, request.sessionId, request.senderInstanceId, donor,
                elapsedNanos().coerceAtLeast(1), false, request.configurationFingerprint, request.expiresAtEpochMillis
            )
            // Submission may itself suspend while Kafka is unavailable. The deadline watcher runs independently.
            try {
                withTimeoutOrNull(interval) { channel.send(probe) }
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Exception) {
                // A failed attempt can recover on the next probe within the same deadline.
            }
            val untilNext = interval - (elapsedNanos() - probe.sentAtNanos).nanoseconds
            if (untilNext > Duration.ZERO) delay(untilNext)
        }
    }

    private fun elapsedNanos(): Long = started.elapsedNow().inWholeNanoseconds
}
