package avh.ckc.core.warmup

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.CoroutinesKafkaConsumerBuilder
import avh.ckc.core.isFatalJvmError
import kotlinx.coroutines.CancellationException

internal fun interface WarmupRecipientParticipantRuntime {
    fun create(sessionId: WarmupSessionId, start: WarmupDestinationStart): CoroutinesKafkaConsumer<*, *>
}

/** Creates a typed CKC runtime whose poll loop owns exactly warm-up partition 0 from [start]. */
internal fun <K, V> warmupRecipientParticipantRuntime(
    consumerProperties: Map<String, Any?>,
    configure: CoroutinesKafkaConsumerBuilder<K, V>.() -> Unit
): WarmupRecipientParticipantRuntime = WarmupRecipientParticipantRuntime { sessionId, start ->
    CoroutinesKafkaConsumerBuilder<K, V>()
        .apply(configure)
        .buildWarmupRecipient(consumerProperties, sessionId, start.assignment.destination, start.offset)
}

/** Production preparation for heterogeneous participants and independently assigned consumers. */
internal class KafkaWarmupRecipientConsumerPreparation(
    participantRuntimes: Map<WarmupParticipantId, WarmupRecipientParticipantRuntime>
) : WarmupRecipientConsumerPreparation {
    private val participantRuntimes = participantRuntimes.toMap()

    override suspend fun prepare(
        sessionId: WarmupSessionId,
        start: WarmupDestinationStart
    ): PreparedWarmupRecipientConsumer {
        val participantId = start.assignment.mapping.participantId
        val runtime = requireNotNull(participantRuntimes[participantId]) {
            "No warm-up consumer runtime configured for participant ${participantId.value}"
        }.create(sessionId, start)
        return CoroutinesPreparedWarmupRecipientConsumer(start, runtime)
    }
}

private class CoroutinesPreparedWarmupRecipientConsumer(
    start: WarmupDestinationStart,
    private val runtime: CoroutinesKafkaConsumer<*, *>
) : PreparedWarmupRecipientConsumer {
    override val assignment = start.assignment
    override val startOffset = start.offset

    override fun start() = runtime.start()
    override suspend fun awaitReady() = runtime.awaitReady()
    override suspend fun stop() {
        try {
            runtime.stop()
        } catch (error: CancellationException) {
            if (!runtime.isShutdownComplete()) throw error
            throw WarmupRecipientConsumerStoppedWithFailureException(error)
        } catch (error: Throwable) {
            if (error.isFatalJvmError()) throw error
            if (!runtime.isShutdownComplete()) throw error
            throw WarmupRecipientConsumerStoppedWithFailureException(error)
        }
    }
}

internal class WarmupRecipientConsumerStoppedWithFailureException(cause: Throwable) :
    IllegalStateException("Warm-up recipient consumer stopped with a terminal failure", cause)
