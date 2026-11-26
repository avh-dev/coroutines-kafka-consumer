package avh.ckc.warmup

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.extension.ConsumerRuntimeFactory
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.warmup.protocol.SessionAccepted
import avh.ckc.warmup.protocol.WarmupParticipantId
import avh.ckc.warmup.protocol.WarmupSessionId
import avh.ckc.warmup.protocol.WarmupTopicMapping
import java.util.UUID
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.sync.Semaphore
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerConfig

/** The payload-independent operations that the context needs from one typed participant. */
internal interface RegisteredTrafficWarmupParticipant {
    val participantId: WarmupParticipantId
    val mappings: List<WarmupTopicMapping>
    val warmupConsumerProperties: Map<String, Any?>
    val mainConsumer: CoroutinesKafkaConsumer<*, *>

    fun prepareDonorLane(
        acceptance: SessionAccepted,
        quotaPhases: WarmupDonorQuotaPhases.Session
    ): WarmupDonorSessionLifecycle
    suspend fun prepareRecipient(
        sessionId: WarmupSessionId,
        start: WarmupDestinationStart,
        completionOutbox: WarmupCompletionOutbox
    ): PreparedWarmupRecipientConsumer
    fun startMainRuntime()
    suspend fun awaitMainReady()
    suspend fun stopMain()
    suspend fun awaitMainTermination()
}

/** Owns the typed main runtime and builds its mapping-local donor and recipient resources. */
internal class TrafficWarmupParticipant<K, V>(
    override val participantId: WarmupParticipantId,
    override val mappings: List<WarmupTopicMapping>,
    override val warmupConsumerProperties: Map<String, Any?>,
    private val forwardingProducerProperties: Map<String, Any?>,
    private val mainProperties: Map<String, Any?>,
    private val recipientFactory: ConsumerRuntimeFactory<K, V>,
    override val mainConsumer: CoroutinesKafkaConsumer<K, V>,
    private val dataPlane: WarmupParticipantDataPlane<K, V>,
    private val metrics: ConsumerMetrics<K, V>,
    warmupMetrics: WarmupMetrics<K, V>,
    private val delegationTimeoutMillis: Long,
    private val inflightDelegations: Semaphore,
    private val canDelegate: () -> Boolean,
    private val recipientShutdownTimeoutMillis: Long,
    private val serviceDispatcher: CoroutineDispatcher,
    private val forwardingObserver: (TrafficWarmupForwardedEvent) -> Unit
) : RegisteredTrafficWarmupParticipant {
    private val metricsObserver = WarmupMetricsObserver(warmupMetrics)
    private val resultDeliveryMetricsObserver = WarmupResultDeliveryMetricsObserver(warmupMetrics)
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

    override fun prepareDonorLane(
        acceptance: SessionAccepted,
        quotaPhases: WarmupDonorQuotaPhases.Session
    ): WarmupDonorSessionLifecycle {
        val selected = acceptance.mappings.filter { it.mapping.participantId == participantId }
        require(selected.isNotEmpty()) { "Donor acceptance contains no mappings for ${participantId.value}" }
        val sessions = linkedMapOf<WarmupTopicMapping, WarmupDonorSession<K, V>>()
        try {
            selected.forEach { donorMapping ->
                val mapping = mappings.single { it == donorMapping.mapping }
                val quota = mapping.quota
                val interval = quota.quotaIntervalMillis.milliseconds
                sessions[mapping] = WarmupDonorSession(
                    sessionId = acceptance.sessionId,
                    quotaProfile = quota.profile,
                    quotaInterval = interval,
                    firstQuotaWindowStart = { quotaPhases.firstWindowStart(interval) },
                    delegationTimeout = delegationTimeoutMillis.milliseconds,
                    destinationResolver = WarmupDestinationResolver { record ->
                        if (record.topic() == mapping.sourceTopic) WarmupDestination(mapping.warmupTopic) else null
                    },
                    sender = KafkaWarmupRecordSender(
                        forwardingProducerProperties,
                        submissionDispatcher = serviceDispatcher
                    ),
                    observer = observer,
                    pendingLaneId = mapping.mappingFingerprint,
                    supportsRemoteDiscard = { true },
                    inflightDelegations = inflightDelegations,
                    canDelegate = canDelegate
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
        completionOutbox: WarmupCompletionOutbox
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
        val laneId = start.assignment.mapping.mappingFingerprint
        val resultLane = completionOutbox.lane(laneId, resultDeliveryMetricsObserver.lane(laneId))
        val runtime = recipientFactory.buildWarmupRecipient(
            effectiveProperties,
            sessionId,
            start.assignment.destination,
            start.offset,
            completionPublisher = resultLane,
            resultAdmission = resultLane,
            runtimeMetrics = warmupRecipientRuntimeMetrics(
                metrics,
                "warmup-${start.assignment.mapping.mappingFingerprint}"
            ),
            runtimeShutdownTimeout = recipientShutdownTimeoutMillis.milliseconds
        )
        return CoroutinesPreparedWarmupRecipientConsumer(start, runtime)
    }

    override fun startMainRuntime() = mainConsumer.runtimeControl.start()
    override suspend fun awaitMainReady() = mainConsumer.runtimeControl.awaitReady()

    override suspend fun stopMain() = mainConsumer.runtimeControl.stop()
    override suspend fun awaitMainTermination() = mainConsumer.runtimeControl.awaitTermination()

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
        mapping.quota.quotaIntervalMillis,
        mapping.quota.profile.initialQuota,
        mapping.quota.profile.phases.joinToString(";") { "${it.durationMillis}:${it.targetQuota}" }
    ).joinToString("\u0000").toByteArray(Charsets.UTF_8)
).toString()

private fun bootstrapIdentity(value: Any?): String = when (value) {
    is Iterable<*> -> value.map(Any?::toString)
    is Array<*> -> value.map(Any?::toString)
    else -> value.toString().split(',')
}.map(String::trim).filter(String::isNotEmpty).sorted().joinToString(",")
