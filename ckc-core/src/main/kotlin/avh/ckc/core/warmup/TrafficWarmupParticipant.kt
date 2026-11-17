package avh.ckc.core.warmup

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.CoroutinesKafkaConsumerBuilder
import avh.ckc.core.isFatalJvmError
import avh.ckc.core.metrics.ConsumerMetrics
import kotlinx.coroutines.CancellationException
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerConfig
import java.util.UUID
import kotlin.time.Duration.Companion.milliseconds

internal interface RegisteredTrafficWarmupParticipant {
    val participantId: WarmupParticipantId
    val mappings: List<WarmupTopicMapping>
    val warmupConsumerProperties: Map<String, Any?>
    val mainConsumer: CoroutinesKafkaConsumer<*, *>

    fun prepareDonorLane(acceptance: SessionAccepted): WarmupDonorSessionLifecycle
    suspend fun prepareRecipient(
        sessionId: WarmupSessionId,
        start: WarmupDestinationStart,
        completionPublisher: WarmupCompletionPublisher
    ): PreparedWarmupRecipientConsumer
    fun startMainRuntime()
    suspend fun awaitMainReady()
    suspend fun stopMain()
    suspend fun awaitMainTermination()
}

internal class TrafficWarmupParticipant<K, V>(
    override val participantId: WarmupParticipantId,
    override val mappings: List<WarmupTopicMapping>,
    override val warmupConsumerProperties: Map<String, Any?>,
    private val forwardingProducerProperties: Map<String, Any?>,
    private val mainProperties: Map<String, Any?>,
    private val recipientBuilder: CoroutinesKafkaConsumerBuilder<K, V>,
    override val mainConsumer: CoroutinesKafkaConsumer<K, V>,
    private val dataPlane: WarmupParticipantDataPlane<K, V>,
    private val metrics: ConsumerMetrics<K, V>,
    private val delegationTimeoutMillis: Long,
    private val recipientShutdownTimeoutMillis: Long,
    private val forwardingObserver: (TrafficWarmupForwardedEvent) -> Unit
) : RegisteredTrafficWarmupParticipant {
    private val metricsObserver = WarmupMetricsObserver(metrics)
    private val observer = object : WarmupDataPlaneObserver<K, V> by metricsObserver {
        override fun onForwarded(
            record: ConsumerRecord<K, V>,
            event: WarmupForwardedEvent,
            durationNanos: Long
        ) {
            metricsObserver.onForwarded(record, event, durationNanos)
            forwardingObserver(event.toPublicEvent())
        }
    }

    override fun prepareDonorLane(acceptance: SessionAccepted): WarmupDonorSessionLifecycle {
        val selected = acceptance.mappings.filter { it.mapping.participantId == participantId }
        require(selected.isNotEmpty()) { "Donor acceptance contains no mappings for ${participantId.value}" }
        val sessions = linkedMapOf<WarmupTopicMapping, WarmupDonorSession<K, V>>()
        try {
            selected.forEach { donorMapping ->
                val mapping = mappings.single { it == donorMapping.mapping }
                val quota = mapping.quota
                sessions[mapping] = WarmupDonorSession(
                    sessionId = acceptance.sessionId,
                    intervalBudget = quota.quotaPerInterval,
                    initialQuota = quota.initialQuotaPerInterval,
                    quotaInterval = quota.quotaIntervalMillis.milliseconds,
                    quotaRampDuration = quota.quotaRampDurationMillis.milliseconds,
                    quotaRampCurve = quota.quotaRampCurve,
                    delegationTimeout = delegationTimeoutMillis.milliseconds,
                    destinationResolver = WarmupDestinationResolver { record ->
                        if (record.topic() == mapping.sourceTopic) WarmupDestination(mapping.warmupTopic) else null
                    },
                    sender = KafkaWarmupRecordSender(forwardingProducerProperties),
                    observer = observer,
                    pendingLaneId = mapping.mappingFingerprint,
                    supportsRemoteDiscard = { true }
                )
            }
            return WarmupParticipantDonorLane(acceptance.sessionId, dataPlane, sessions)
        } catch (error: Throwable) {
            sessions.values.toList().asReversed().forEach { session ->
                try {
                    session.close()
                } catch (closeError: Throwable) {
                    error.addSuppressed(closeError)
                }
            }
            throw error
        }
    }

    override suspend fun prepareRecipient(
        sessionId: WarmupSessionId,
        start: WarmupDestinationStart,
        completionPublisher: WarmupCompletionPublisher
    ): PreparedWarmupRecipientConsumer {
        require(start.assignment.mapping.participantId == participantId) {
            "Warm-up assignment belongs to another participant"
        }
        val clientId = warmupRecipientClientId(sessionId, start)
        val effectiveProperties = buildWarmupRecipientConsumerProperties(
            warmupRecipientUserProperties(mainProperties, warmupConsumerProperties),
            mapOf(
                ConsumerConfig.GROUP_ID_CONFIG to clientId,
                ConsumerConfig.CLIENT_ID_CONFIG to clientId,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "none"
            )
        )
        val runtime = recipientBuilder.buildWarmupRecipient(
            effectiveProperties,
            sessionId,
            start.assignment.destination,
            start.offset,
            completionPublisher,
            warmupRecipientRuntimeMetrics(
                metrics,
                "warmup-${start.assignment.mapping.mappingFingerprint}"
            ),
            recipientShutdownTimeoutMillis.milliseconds
        )
        return object : PreparedWarmupRecipientConsumer {
            override val assignment = start.assignment
            override val startOffset = start.offset
            override fun start() = runtime.start()
            override suspend fun awaitReady() = runtime.awaitReady()
            override suspend fun awaitTermination() = runtime.awaitTermination()
            override suspend fun abort(): Boolean = abortWarmupRecipientRuntime(runtime)
            override suspend fun stop() {
                try {
                    runtime.stop()
                } catch (error: CancellationException) {
                    if (!runtime.isShutdownComplete()) throw error
                    throw WarmupRecipientConsumerStoppedWithFailureException(error)
                } catch (error: Throwable) {
                    if (error.isFatalJvmError() || !runtime.isShutdownComplete()) throw error
                    throw WarmupRecipientConsumerStoppedWithFailureException(error)
                }
            }
        }
    }

    override fun startMainRuntime() = mainConsumer.startRuntime()
    override suspend fun awaitMainReady() = mainConsumer.awaitRuntimeReady()

    override suspend fun stopMain() = mainConsumer.stopRuntime()
    override suspend fun awaitMainTermination() = mainConsumer.awaitRuntimeTermination()

    private fun WarmupForwardedEvent.toPublicEvent() = TrafficWarmupForwardedEvent(
        sessionId = sessionId.value,
        sourceTopic = sourceTopicPartition.topic(),
        sourcePartition = sourceTopicPartition.partition(),
        sourceOffset = sourceOffset,
        destinationTopic = destinationTopicPartition.topic(),
        destinationPartition = destinationTopicPartition.partition(),
        destinationOffset = destinationOffset
    )
}

internal fun warmupRecipientUserProperties(
    mainProperties: Map<String, Any?>,
    warmupProperties: Map<String, Any?>
): Map<String, Any?> =
    listOf(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
    ).mapNotNull { key -> mainProperties[key]?.let { key to it } }.toMap() + warmupProperties

internal fun validateWarmupParticipantConnections(
    consumerProperties: Map<String, Any?>,
    producerProperties: Map<String, Any?>
) {
    val consumerBootstrap = consumerProperties[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG]
    val producerBootstrap = producerProperties[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG]
    require(
        consumerBootstrap != null && producerBootstrap != null &&
            bootstrapIdentity(consumerBootstrap) == bootstrapIdentity(producerBootstrap)
    ) {
        "Warm-up consumer and forwarding producer must use the same destination bootstrap servers"
    }
    listOf(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG
    ).forEach { property ->
        require(producerProperties.containsKey(property)) {
            "Kafka property '$property' must be specified for warm-up forwarding"
        }
    }
}

internal fun mappingFingerprint(
    participantId: WarmupParticipantId,
    mapping: TrafficWarmupConsumerConfiguration.Mapping,
    warmupConsumerProperties: Map<String, Any?>
): String = UUID.nameUUIDFromBytes(
    listOf(
        participantId.value,
        mapping.sourceTopic,
        mapping.warmupTopic,
        bootstrapIdentity(warmupConsumerProperties[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG]),
        mapping.quota.quotaPerInterval,
        mapping.quota.initialQuotaPerInterval,
        mapping.quota.quotaIntervalMillis,
        mapping.quota.quotaRampDurationMillis,
        mapping.quota.quotaRampCurve.name
    ).joinToString("\u0000").toByteArray(Charsets.UTF_8)
).toString()

private fun bootstrapIdentity(value: Any?): String = when (value) {
    is Iterable<*> -> value.map(Any?::toString)
    is Array<*> -> value.map(Any?::toString)
    else -> value.toString().split(',')
}.map(String::trim).filter(String::isNotEmpty).sorted().joinToString(",")
