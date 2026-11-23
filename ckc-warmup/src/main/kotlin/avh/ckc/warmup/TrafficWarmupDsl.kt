package avh.ckc.warmup

import avh.ckc.core.CoroutinesKafkaConsumerBuilder
import avh.ckc.warmup.protocol.WarmupMappingQuota
import avh.ckc.warmup.protocol.WarmupParticipantId
import avh.ckc.warmup.protocol.WarmupQuotaProfileParser
import avh.ckc.warmup.protocol.WarmupTopicMapping
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.Dispatchers

/** Warm-up settings shared by every participant in one local context. */
class TrafficWarmupContextConfiguration {
    var instanceId: String = UUID.randomUUID().toString()
    var namespace: String = ""
    var controlTopic: String = ""
    var controlProducerProperties: Map<String, Any?> = emptyMap()
    var controlConsumerProperties: Map<String, Any?> = emptyMap()

    var warmupDuration: Duration = 90.seconds
    var offerWindow: Duration = 2.seconds
    var participantStartTimeout: Duration = 15.seconds
    var preparationTimeout: Duration = 15.seconds
    var completionTimeout: Duration = 30.seconds
    var delegationTimeout: Duration = 10.seconds

    /** Maximum time without a control round trip to a supporting donor during warm-up and donor drain. */
    var donorLivenessTimeout: Duration = 10.seconds
    var maxDonorSessions: Int = 4

    /** Maximum total of coordination and blocking Kafka workers owned by this context. */
    var serviceThreadLimit: Int = 4

    /** Idle service workers are released; an active control consumer can retain one worker. */
    var serviceThreadIdleTimeout: Duration = 30.seconds

    /** Aggregate logical-session admission capacity in normalized records per second. */
    var maxAggregateQuotaPerSecond: Int = Int.MAX_VALUE

    /** Shared donor capacity from admission through ACK, result, and any local fallback. */
    var maxInflightDelegations: Int = Int.MAX_VALUE

    internal var brokerLifecycleConfiguration: TrafficWarmupBrokerLifecycleConfiguration? = null

    /** Enables coordinated stop for the temporary broker hosting this context's warm-up destinations. */
    fun brokerLifecycle(block: TrafficWarmupBrokerLifecycleConfiguration.() -> Unit) {
        brokerLifecycleConfiguration = TrafficWarmupBrokerLifecycleConfiguration().apply(block)
    }

    /** Supplies parent lifecycle context elements; protocol dispatch uses the owned service threads. */
    var parentContext: CoroutineContext = Dispatchers.Default

    internal var lifecycleObserver: (TrafficWarmupLifecycleEvent) -> Unit = {}

    fun onLifecycleEvent(observer: (TrafficWarmupLifecycleEvent) -> Unit) {
        lifecycleObserver = observer
    }
}

class TrafficWarmupBrokerLifecycleConfiguration internal constructor() {
    var topic: String = ""
    var kafkaProperties: Map<String, Any?> = emptyMap()
    var operationTimeout: Duration = 2.seconds
}

/** Per-mapping quota; unused capacity is never transferred to another mapping. */
class TrafficWarmupMappingConfiguration internal constructor() {
    /**
     * Absolute per-[quotaInterval] budget over time, using generator-style phase syntax.
     *
     * Equal endpoints hold a quota, unequal endpoints transition linearly, and a `0s` phase changes
     * it immediately. The last value is held until the donor session ends.
     */
    var quotaProfile: String = "10"
    var quotaInterval: Duration = 1.seconds

    internal fun toProtocolQuota(): WarmupMappingQuota = WarmupMappingQuota(
        profile = WarmupQuotaProfileParser.parse(quotaProfile),
        quotaIntervalMillis = quotaInterval.inWholeMilliseconds
    )
}

/** Kafka connections and topic mappings owned by one typed participant. */
class TrafficWarmupConsumerConfiguration<K, V> internal constructor() {
    internal data class Mapping(
        val sourceTopic: String,
        val warmupTopic: String,
        val quota: WarmupMappingQuota
    )

    internal val mappings = linkedMapOf<String, Mapping>()
    internal var forwardingObserver: (TrafficWarmupForwardedEvent) -> Unit = {}

    /**
     * All mappings of one participant use these recipient-consumer connection properties.
     * They must address the same destination clusters as [forwardingProducerProperties].
     */
    var warmupConsumerProperties: Map<String, Any?> = emptyMap()

    /** All mappings of one participant use this forwarding-producer connection. */
    var forwardingProducerProperties: Map<String, Any?> = emptyMap()

    /** Overrides the warm-up metrics binding for this participant. */
    var metrics: WarmupMetrics<K, V>? = null

    fun mapTopic(
        sourceTopic: String,
        warmupTopic: String,
        block: TrafficWarmupMappingConfiguration.() -> Unit = {}
    ) {
        val quota = TrafficWarmupMappingConfiguration().apply(block).toProtocolQuota()
        require(mappings.putIfAbsent(sourceTopic, Mapping(sourceTopic, warmupTopic, quota)) == null) {
            "Source topic $sourceTopic already has a warm-up mapping"
        }
    }

    fun onForwarded(observer: (TrafficWarmupForwardedEvent) -> Unit) {
        forwardingObserver = observer
    }
}

enum class TrafficWarmupLifecyclePhase {
    WAITING_FOR_PARTICIPANTS,
    DISCOVERING,
    READY,
    WARMING,
    WARMUP_INTERRUPTED,
    COMPLETING,
    COLD_FALLBACK,
    MAIN_READY,
    PARTICIPANT_STOPPED,
    CONTROL_UNAVAILABLE,
    RECIPIENT_FINISHED
}

data class TrafficWarmupLifecycleEvent(
    val phase: TrafficWarmupLifecyclePhase,
    val participantId: String? = null,
    val sessionId: String? = null,
    val reasonCode: String? = null,
    val durationMillis: Long? = null,
    val processedRecords: Long? = null,
    /** Elapsed time between observing a warm-up interruption and this participant becoming MAIN_READY. */
    val recoveryDurationMillis: Long? = null
)

data class TrafficWarmupForwardedEvent(
    val sessionId: String,
    val sourceTopic: String,
    val sourcePartition: Int,
    val sourceOffset: Long,
    val destinationTopic: String,
    val destinationPartition: Int,
    val destinationOffset: Long
)

fun trafficWarmupContext(
    block: TrafficWarmupContextConfiguration.() -> Unit
): TrafficWarmupContext = TrafficWarmupContext(TrafficWarmupContextConfiguration().apply(block))

/** Registers this consumer as one typed participant of a shared protocol-v6 context. */
fun <K, V> CoroutinesKafkaConsumerBuilder<K, V>.trafficWarmup(
    context: TrafficWarmupContext,
    participantId: String,
    block: TrafficWarmupConsumerConfiguration<K, V>.() -> Unit
) {
    val id = WarmupParticipantId(participantId)
    val configuration = TrafficWarmupConsumerConfiguration<K, V>().apply(block)
    extendRuntime { runtimeFactory, consumerProperties ->
        val configuredTopics = requireNotNull(runtimeFactory.subscribedTopics) {
            "Traffic warm-up requires an explicit topic subscription"
        }
        require(configuration.mappings.isNotEmpty()) {
            "At least one warm-up topic mapping is required"
        }
        require(configuration.mappings.keys.all(configuredTopics::contains)) {
            "Every warm-up source topic must belong to the main consumer subscription"
        }
        validateWarmupParticipantConnections(
            configuration.warmupConsumerProperties,
            configuration.forwardingProducerProperties
        )
        val protocolMappings = configuration.mappings.values.map { mapping ->
            WarmupTopicMapping(
                participantId = id,
                sourceTopic = mapping.sourceTopic,
                warmupTopic = mapping.warmupTopic,
                mappingFingerprint = mappingFingerprint(
                    id,
                    mapping,
                    configuration.warmupConsumerProperties
                ),
                quota = mapping.quota
            )
        }
        val dataPlane = WarmupParticipantDataPlane<K, V>()
        val main = runtimeFactory.subscribed(consumerProperties, dataPlane)
        val participant = TrafficWarmupParticipant(
            participantId = id,
            mappings = protocolMappings,
            warmupConsumerProperties = configuration.warmupConsumerProperties,
            forwardingProducerProperties = configuration.forwardingProducerProperties,
            mainProperties = consumerProperties,
            recipientFactory = runtimeFactory,
            mainConsumer = main,
            dataPlane = dataPlane,
            metrics = runtimeFactory.metrics,
            warmupMetrics = configuration.metrics ?: defaultWarmupMetrics(runtimeFactory.metrics),
            delegationTimeoutMillis = context.delegationTimeoutMillis(),
            inflightDelegations = context.inflightDelegations,
            recipientShutdownTimeoutMillis = context.recipientShutdownTimeoutMillis(),
            serviceDispatcher = context.serviceDispatcher(),
            forwardingObserver = configuration.forwardingObserver
        )
        main.manageLifecycle(context.register(participant))
        main
    }
}
