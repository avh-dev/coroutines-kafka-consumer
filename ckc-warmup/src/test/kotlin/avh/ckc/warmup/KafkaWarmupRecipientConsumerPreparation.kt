package avh.ckc.warmup

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.CoroutinesKafkaConsumerBuilder
import avh.ckc.warmup.protocol.WarmupParticipantId
import avh.ckc.warmup.protocol.WarmupSessionId
import org.apache.kafka.clients.consumer.ConsumerConfig

internal fun interface WarmupRecipientParticipantRuntime {
    fun create(sessionId: WarmupSessionId, start: WarmupDestinationStart): CoroutinesKafkaConsumer<*, *>
}

/** Creates a typed CKC runtime whose poll loop owns exactly warm-up partition 0 from [start]. */
internal fun <K, V> warmupRecipientParticipantRuntime(
    consumerProperties: Map<String, Any?>,
    completionPublisher: ((WarmupSessionId) -> WarmupCompletionPublisher)? = null,
    resultAdmission: ((WarmupSessionId) -> WarmupResultAdmission)? = null,
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
        .runtimeFactory()
        .buildWarmupRecipient(
            effectiveProperties,
            sessionId,
            start.assignment.destination,
            start.offset,
            completionPublisher?.invoke(sessionId),
            resultAdmission?.invoke(sessionId)
        )
}

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
