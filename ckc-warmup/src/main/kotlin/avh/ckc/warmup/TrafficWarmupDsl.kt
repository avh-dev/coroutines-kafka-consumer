package avh.ckc.warmup

import avh.ckc.core.CoroutinesKafkaConsumerBuilder
import avh.ckc.warmup.dataplane.WarmupParticipantDataPlane
import avh.ckc.warmup.protocol.WarmupMappingQuota
import avh.ckc.warmup.protocol.WarmupParticipantId
import avh.ckc.warmup.protocol.WarmupQuotaProfileParser
import avh.ckc.warmup.protocol.WarmupTopicMapping
import avh.ckc.warmup.runtime.TrafficWarmupParticipant
import avh.ckc.warmup.runtime.mappingFingerprint
import avh.ckc.warmup.runtime.validateWarmupParticipantConnections
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.Dispatchers

/**
 * Shared settings for one local warm-up context. Set [namespace], [controlTopic], and both
 * control client connections explicitly. Participants in different contexts may cooperate when
 * their namespace, control topic, and mapping configurations match.
 *
 * For each Kafka client CKC applies tunable defaults, then user properties, then the client's
 * required properties. Main consumer properties are configured at the core builder. Control,
 * broker lifecycle, and participant destination clients have independent connections.
 */
class TrafficWarmupContextConfiguration {
    /**
     * Identity of this context, random by default. Each live peer needs a distinct non-blank
     * ID of at most 255 characters, without control characters.
     */
    var instanceId: String = UUID.randomUUID().toString()

    /**
     * Routing namespace shared by compatible peers. Set a non-blank value of at most 255
     * characters, without control characters. The default is empty.
     */
    var namespace: String = ""
    /** Dedicated control topic with exactly partition 0. The empty default requires configuration. */
    var controlTopic: String = ""
    /**
     * Control producer connection, including `bootstrap.servers`. Defaults to an empty map and
     * uses `linger.ms=0` unless overridden. CKC sets the serializers, client ID, `acks=all`, and
     * `max.block.ms` from [preparationTimeout]. Broker lifecycle can constrain in-flight sends
     * per connection to one when idempotent ordering would otherwise be unavailable.
     */
    var controlProducerProperties: Map<String, Any?> = emptyMap()

    /**
     * Control consumer connection to the same topic, including `bootstrap.servers`. Defaults to
     * an empty map. Tunable defaults are `fetch.min.bytes=1` and `fetch.max.wait.ms=3000`.
     * CKC sets the deserializers, group and client IDs, disables auto-commit, and uses
     * `auto.offset.reset=earliest`. The first successful assignment explicitly seeks to the tail.
     */
    var controlConsumerProperties: Map<String, Any?> = emptyMap()

    /** Positive warming period after recipient readiness, 90 seconds by default, before completion begins. */
    var warmupDuration: Duration = 90.seconds

    /**
     * Positive offer collection window, 2 seconds by default, measured from request construction.
     * If offers cannot cover every mapping when it expires, the context starts cold.
     */
    var offerWindow: Duration = 2.seconds

    /**
     * Positive wait for all registered participants to request start, 15 seconds by default.
     * Begins after control readiness and any initial broker admission gate. If protocol setup fails,
     * the cold-start path begins this wait immediately. On expiry, requested participants start
     * cold and later starts also bypass warm-up.
     */
    var participantStartTimeout: Duration = 15.seconds

    /**
     * Per-stage preparation budget, 15 seconds by default and at least 1 millisecond. Applies
     * to individual control sends and metadata operations, initial broker admission, donor
     * acceptance after selection is published, and recipient preparation. Expiry leads to cold
     * fallback, subject to cleanup. Control assignment has its own 15-second deadline per attempt.
     */
    var preparationTimeout: Duration = 15.seconds

    /**
     * Positive completion budget, 30 seconds by default. Bounds individual waits for donor drain,
     * recipient cleanup, and pending result delivery as those operations begin. Incomplete cleanup
     * prevents main startup and can be retried on stop. A broker drain can supply earlier deadlines.
     */
    var completionTimeout: Duration = 30.seconds

    /**
     * Per-record send and result wait, 10 seconds by default and at least 1 millisecond, beginning when the
     * delegated send starts. On expiry the donor stops using that mapping session and tries
     * local processing while it still owns the source record.
     */
    var delegationTimeout: Duration = 10.seconds

    /**
     * Maximum time without a control round trip to a supporting donor during warming and donor
     * drain. Defaults to 10 seconds and must be finite and at least 3 milliseconds.
     * Losing the round trip interrupts the recipient attempt.
     */
    var donorLivenessTimeout: Duration = 10.seconds

    /** Positive cap on admitted logical donor sessions, including preparation. Defaults to 4. */
    var maxDonorSessions: Int = 4

    /**
     * Maximum context-owned service threads, 4 by default and at least 3. One thread handles
     * coordination and the rest share blocking Kafka work. User handlers use the core builder's
     * processing dispatcher in the main and recipient runtimes.
     */
    var serviceThreadLimit: Int = 4

    /**
     * Idle service worker expiry, 30 seconds by default and at least 1 millisecond.
     * Control polling can retain a worker.
     */
    var serviceThreadIdleTimeout: Duration = 30.seconds

    /**
     * Positive context-wide donor admission budget, [Int.MAX_VALUE] by default. Sum the per-mapping
     * peak quotas of each admitted session, normalized to records per second and rounded up.
     */
    var maxAggregateQuotaPerSecond: Int = Int.MAX_VALUE

    /**
     * Positive cap on simultaneous delegations across all donor sessions and mappings in this
     * context. Defaults to [Int.MAX_VALUE]. A slot is held through sending, result handling, and
     * any local fallback. When no slot is available, the record takes the local path.
     */
    var maxInflightDelegations: Int = Int.MAX_VALUE

    internal var brokerLifecycleConfiguration: TrafficWarmupBrokerLifecycleConfiguration? = null

    /**
     * Enables the optional broker supervisor handshake and coordinated drain, disabled by default.
     * Contexts wait for admission before warming and fence delegation when the broker requests drain.
     */
    fun brokerLifecycle(block: TrafficWarmupBrokerLifecycleConfiguration.() -> Unit) {
        brokerLifecycleConfiguration = TrafficWarmupBrokerLifecycleConfiguration().apply(block)
    }

    /**
     * Parent coroutine context, [Dispatchers.Default] by default. Its job parents the warm-up
     * scope and cancellation stops that scope. The context uses its owned coordination and blocking
     * dispatchers. Stop the participants to release the context's resources.
     */
    var parentContext: CoroutineContext = Dispatchers.Default

    internal var lifecycleObserver: (TrafficWarmupLifecycleEvent) -> Unit = {}

    /**
     * Installs one observer for this context, replacing any previous observer. The default is a
     * no-op. Observers run inline on the caller or on warm-up coordination workers, sometimes
     * during a locked transition. Calls can overlap. Keep them thread-safe and non-blocking and
     * avoid reentering lifecycle operations. Exceptions of type [Exception] are logged and ignored.
     */
    fun onLifecycleEvent(observer: (TrafficWarmupLifecycleEvent) -> Unit) {
        lifecycleObserver = observer
    }
}

/** Connection and deadlines for the optional broker supervisor journal. */
class TrafficWarmupBrokerLifecycleConfiguration internal constructor() {
    /** Non-blank single-partition journal topic distinct from the control topic. Required when enabled. */
    var topic: String = ""

    /**
     * Kafka connection to the supervisor journal. Supply `bootstrap.servers` and any security
     * settings. The default map is empty. CKC sets journal serializers, read settings, idempotent
     * writes, and client timeouts derived from [operationTimeout]. Drain replies use the broker
     * address advertised by the journal metadata.
     */
    var kafkaProperties: Map<String, Any?> = emptyMap()

    /**
     * Per-operation budget for journal metadata and I/O, 2 seconds by default. Must be finite
     * and between 1 and [Int.MAX_VALUE] milliseconds. The supervisor sets the overall drain deadline.
     */
    var operationTimeout: Duration = 2.seconds
}

/** Per-mapping quota; unused capacity is never transferred to another mapping. */
class TrafficWarmupMappingConfiguration internal constructor() {
    /**
     * Absolute record budget per [quotaInterval]. The profile clock starts at the first
     * delegation attempt in an open quota window after donor activation. Defaults to `"10"`.
     * For example, `"0 -> (30s, ramp) -> 100"`
     * increases the quota linearly over 30 seconds. Equal endpoints hold the quota, a zero-duration
     * phase changes it immediately, and the final value is held until the session ends. Durations
     * use `ms`, `s`, `m`, or `h`, labels are optional, and at least one quota must be positive.
     */
    var quotaProfile: String = "10"
    /**
     * Repeating budget window for this mapping, 1 second by default and at least 1 millisecond.
     * Donors stagger the windows of concurrent sessions with the same interval.
     */
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
     * Recipient connection for every mapping of this participant. Defaults to an empty map.
     * Set `bootstrap.servers` to match [forwardingProducerProperties]. Key and value deserializers
     * fall back to the main consumer settings. These properties override that fallback and the
     * tunable fetch defaults (`fetch.min.bytes=1`, `fetch.max.wait.ms=3000`). CKC sets the
     * group and client IDs, disables auto-commit, uses `auto.offset.reset=none`, and removes
     * `group.instance.id`. Other participants can target different clusters.
     */
    var warmupConsumerProperties: Map<String, Any?> = emptyMap()

    /**
     * Forwarding connection for every mapping of this participant. Defaults to an empty map.
     * Set matching `bootstrap.servers` and both serializer classes. User properties override the
     * `linger.ms=5` default. These settings are independent of the main and control connections.
     */
    var forwardingProducerProperties: Map<String, Any?> = emptyMap()

    /**
     * Optional metrics override for this participant. With `null`, CKC uses the core binding
     * if it implements [WarmupMetrics], adapts a supported Micrometer binding when available,
     * or uses no-op metrics. Each recipient runtime gets its own core runtime metrics binding.
     */
    var metrics: WarmupMetrics<K, V>? = null

    /**
     * Maps one subscribed source topic to a dedicated warm-up topic with exactly partition 0.
     * Each source topic can be mapped once per participant. Destination topic names must be
     * unique across the context, including across distinct destination connections.
     * If a destination name also appears in a main subscription, startup checks the Kafka cluster IDs.
     * A confirmed overlap or unavailable cluster identity disables warm-up for the context.
     */
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

    /**
     * Replaces the default no-op forwarding observer. It runs inline on the donor processing
     * coroutine once the append is confirmed by a producer acknowledgement or a matching recipient
     * result. Calls can overlap across records and sessions. Keep the observer thread-safe
     * and non-blocking. Exceptions of type [Exception] are
     * logged and ignored by the donor.
     */
    fun onForwarded(observer: (TrafficWarmupForwardedEvent) -> Unit) {
        forwardingObserver = observer
    }
}

/** Observable transitions of the shared recipient attempt and individual participants. */
enum class TrafficWarmupLifecyclePhase {
    /** A participant requested start and can join the shared startup. */
    WAITING_FOR_PARTICIPANTS,
    /** The context opened discovery for a recipient session. */
    DISCOVERING,
    /** Warm-up consumers are ready and the recipient accepted the readiness publication. */
    READY,
    /** The warm-up period has started. */
    WARMING,
    /** The recipient detected an interruption and recorded its first reason. */
    WARMUP_INTERRUPTED,
    /** The context is draining the recipient session and delivering results. */
    COMPLETING,
    /** The recipient selected cold startup after cleanup. */
    COLD_FALLBACK,
    /** A participant's main runtime completed Kafka readiness. */
    MAIN_READY,
    /** A participant completed its stop cleanup. */
    PARTICIPANT_STOPPED,
    /** The context could not establish initial control readiness. */
    CONTROL_UNAVAILABLE,
    /** The recipient attempt recorded its outcome, including any incomplete cleanup. */
    RECIPIENT_FINISHED
}

/**
 * Event emitted by [TrafficWarmupContextConfiguration.onLifecycleEvent].
 *
 * [participantId] is set for participant-scoped events. [sessionId] is set after discovery assigns
 * one, or on [TrafficWarmupLifecyclePhase.MAIN_READY] when reporting recovery. [reasonCode] records
 * an interruption or fallback when available. [durationMillis] measures from warming start to the
 * recipient summary and is absent if warming never started. [processedRecords] counts queued
 * terminal handler outcomes, including failures. It excludes policy discards and is absent on other
 * events. [recoveryDurationMillis] is set when main readiness follows a recorded interruption.
 */
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

/**
 * Kafka coordinates of a confirmed forwarding append. [sessionId] correlates within one
 * logical recipient session. Source and destination offsets refer to their respective partitions.
 */
data class TrafficWarmupForwardedEvent(
    val sessionId: String,
    val sourceTopic: String,
    val sourcePartition: Int,
    val sourceOffset: Long,
    val destinationTopic: String,
    val destinationPartition: Int,
    val destinationOffset: Long
)

/**
 * Creates the context shared by one or more typed consumers. Configure each consumer with
 * [trafficWarmup] before starting any participant. The context lives until all participants stop.
 */
fun trafficWarmupContext(
    block: TrafficWarmupContextConfiguration.() -> Unit
): TrafficWarmupContext = TrafficWarmupContext(TrafficWarmupContextConfiguration().apply(block))

/**
 * Registers a typed consumer under a unique non-blank [participantId] of at most 255 characters
 * in [context] when the consumer is built. Configure an explicit main topic subscription and
 * at least one mapping.
 * Build every participant before the first start. The consumer retains the core lifecycle API
 * and reports readiness only after its main runtime establishes Kafka assignment.
 */
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
        val warmupConsumerProperties = configuration.warmupConsumerProperties.toMap()
        val forwardingProducerProperties = configuration.forwardingProducerProperties.toMap()
        validateWarmupParticipantConnections(warmupConsumerProperties, forwardingProducerProperties)
        val protocolMappings = configuration.mappings.values.map { mapping ->
            WarmupTopicMapping(
                participantId = id,
                sourceTopic = mapping.sourceTopic,
                warmupTopic = mapping.warmupTopic,
                mappingFingerprint = mappingFingerprint(
                    id,
                    mapping,
                    warmupConsumerProperties
                ),
                quota = mapping.quota
            )
        }
        val dataPlane = WarmupParticipantDataPlane<K, V>()
        val main = runtimeFactory.subscribed(consumerProperties, dataPlane)
        val participant = TrafficWarmupParticipant(
            participantId = id,
            mappings = protocolMappings,
            warmupConsumerProperties = warmupConsumerProperties,
            forwardingProducerProperties = forwardingProducerProperties,
            mainProperties = consumerProperties,
            mainTopics = configuredTopics,
            runtimeFactory = runtimeFactory,
            mainConsumer = main,
            dataPlane = dataPlane,
            metrics = runtimeFactory.metrics,
            warmupMetrics = configuration.metrics ?: defaultWarmupMetrics(runtimeFactory.metrics),
            delegationTimeoutMillis = context.delegationTimeoutMillis(),
            inflightDelegations = context.inflightDelegations,
            canDelegate = context::canDelegate,
            recipientShutdownTimeoutMillis = context.recipientShutdownTimeoutMillis(),
            serviceDispatcher = context.serviceDispatcher(),
            forwardingObserver = configuration.forwardingObserver
        )
        main.manageLifecycle(context.register(participant))
        main
    }
}
