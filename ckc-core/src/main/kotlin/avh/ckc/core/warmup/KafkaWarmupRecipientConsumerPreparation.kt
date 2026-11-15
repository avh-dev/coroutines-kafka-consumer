package avh.ckc.core.warmup

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.CoroutinesKafkaConsumerBuilder
import avh.ckc.core.isFatalJvmError
import kotlinx.coroutines.CancellationException
import org.apache.kafka.clients.consumer.ConsumerConfig
import java.util.UUID

internal fun interface WarmupRecipientParticipantRuntime {
    fun create(sessionId: WarmupSessionId, start: WarmupDestinationStart): CoroutinesKafkaConsumer<*, *>
}

/** Creates a typed CKC runtime whose poll loop owns exactly warm-up partition 0 from [start]. */
internal fun <K, V> warmupRecipientParticipantRuntime(
    consumerProperties: Map<String, Any?>,
    completionPublisher: ((WarmupSessionId) -> WarmupCompletionPublisher)? = null,
    configure: CoroutinesKafkaConsumerBuilder<K, V>.() -> Unit
): WarmupRecipientParticipantRuntime = WarmupRecipientParticipantRuntime { sessionId, start ->
    val clientId = warmupRecipientClientId(sessionId, start)
    val effectiveProperties = buildWarmupRecipientConsumerProperties(
        consumerProperties,
        mapOf(
            ConsumerConfig.GROUP_ID_CONFIG to clientId,
            ConsumerConfig.CLIENT_ID_CONFIG to clientId,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "none"
        )
    )
    CoroutinesKafkaConsumerBuilder<K, V>()
        .apply(configure)
        .buildWarmupRecipient(
            effectiveProperties,
            sessionId,
            start.assignment.destination,
            start.offset,
            completionPublisher?.invoke(sessionId)
        )
}

internal fun warmupRecipientClientId(sessionId: WarmupSessionId, start: WarmupDestinationStart): String {
    val assignment = start.assignment
    val destination = assignment.destination
    val connectionIdentity = listOf(
        assignment.mapping.participantId.value,
        sessionId.value,
        destination.topic(),
        destination.partition().toString()
    ).joinToString("\u0000")
    return "ckc.traffic-warmup.v6.recipient." +
        UUID.nameUUIDFromBytes(connectionIdentity.toByteArray(Charsets.UTF_8))
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
