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
    private val readiness: List<RecipientReady>,
    private val channel: WarmupControlChannel,
    private val timeout: Duration,
    private val interval: Duration,
    timeSource: TimeSource = TimeSource.Monotonic
) {
    private val started = timeSource.markNow()
    private val replies = readiness.associate { it.targetInstanceId to MutableStateFlow<Long?>(0) }

    fun onDonorClosed(donor: WarmupInstanceId) {
        replies[donor]?.value = null
    }

    fun onReply(reply: WarmupLivenessProbe) {
        val latest = replies[reply.senderInstanceId] ?: return
        if (reply.reply && reply.sessionId == request.sessionId && reply.namespace == request.namespace &&
            reply.targetInstanceId == request.senderInstanceId && reply.protocolVersion == request.protocolVersion &&
            reply.configurationFingerprint == request.configurationFingerprint &&
            reply.expiresAtEpochMillis == request.expiresAtEpochMillis && reply.sentAtNanos <= elapsedNanos().coerceAtLeast(1)
        ) {
            latest.update { it?.let { confirmed -> maxOf(confirmed, reply.sentAtNanos) } }
        }
    }

    suspend fun awaitTimeout() = coroutineScope {
        val missingReply = CompletableDeferred<Unit>()
        val monitors = readiness.map { ready ->
            launch {
                val sender = launch { sendProbes(ready) }
                try {
                    if (replyTimedOut(replies.getValue(ready.targetInstanceId))) missingReply.complete(Unit)
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

    private suspend fun replyTimedOut(latest: MutableStateFlow<Long?>): Boolean {
        var confirmedSend = 0L
        while (true) {
            val remaining = timeout - (elapsedNanos() - confirmedSend).nanoseconds
            if (latest.value == null) return false
            if (remaining <= Duration.ZERO) return true
            val reply = withTimeoutOrNull(remaining) {
                // Wrap the value so donor completion and expiry of the local wait stay distinct.
                latest.first { it == null || it > confirmedSend }.let(::ProbeConfirmation)
            } ?: return true
            confirmedSend = reply.sentAtNanos ?: return false
        }
    }

    private data class ProbeConfirmation(val sentAtNanos: Long?)

    private suspend fun sendProbes(ready: RecipientReady) {
        while (true) {
            val probe = WarmupLivenessProbe(ready, elapsedNanos().coerceAtLeast(1))
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
