package avh.ckc.core.warmup

import kotlinx.coroutines.Dispatchers
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/** Shape used to grow a mapping quota from its initial to maximum value. */
enum class QuotaRampCurve(internal val protocolId: Int) {
    LINEAR(0),
    QUADRATIC(1);

    internal companion object {
        fun fromProtocolId(id: Int): QuotaRampCurve = entries.firstOrNull { it.protocolId == id }
            ?: throw IllegalArgumentException("Unknown warm-up quota ramp curve: $id")
    }
}

/** Public protocol settings shared by every participant in one local context. */
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
    var maxDonorSessions: Int = 4

    /** Aggregate logical-session admission capacity in normalized records per second. */
    var maxAggregateQuotaPerSecond: Int = Int.MAX_VALUE

    var parentContext: CoroutineContext = Dispatchers.Default

    internal var lifecycleObserver: (TrafficWarmupLifecycleEvent) -> Unit = {}

    fun onLifecycleEvent(observer: (TrafficWarmupLifecycleEvent) -> Unit) {
        lifecycleObserver = observer
    }
}

/** Per-mapping quota; unused capacity is never transferred to another mapping. */
class TrafficWarmupMappingConfiguration internal constructor() {
    private var initialQuotaExplicit = false
    var quotaPerInterval: Int = 10
        set(value) {
            field = value
            if (!initialQuotaExplicit) initialQuota = value
        }
    private var initialQuota: Int = quotaPerInterval
    var initialQuotaPerInterval: Int
        get() = initialQuota
        set(value) {
            initialQuotaExplicit = true
            initialQuota = value
        }
    var quotaInterval: Duration = 1.seconds

    /** Curve used to grow the quota. Quadratic growth protects cold recipients by default. */
    var quotaRampCurve: QuotaRampCurve = QuotaRampCurve.QUADRATIC

    /** Duration of the ramp from the initial to maximum quota. */
    var quotaRampDuration: Duration = Duration.ZERO

    internal fun toProtocolQuota(): WarmupMappingQuota = WarmupMappingQuota(
        quotaPerInterval = quotaPerInterval,
        initialQuotaPerInterval = initialQuotaPerInterval,
        quotaIntervalMillis = quotaInterval.inWholeMilliseconds,
        quotaRampDurationMillis = quotaRampDuration.inWholeMilliseconds,
        quotaRampCurve = quotaRampCurve
    )
}

/** Kafka connections and topic mappings owned by one typed participant. */
class TrafficWarmupConsumerConfiguration internal constructor() {
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
    COMPLETING,
    COLD_FALLBACK,
    MAIN_READY,
    PARTICIPANT_STOPPED,
    CONTROL_UNAVAILABLE
}

data class TrafficWarmupLifecycleEvent(
    val phase: TrafficWarmupLifecyclePhase,
    val participantId: String? = null,
    val sessionId: String? = null,
    val reasonCode: String? = null
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
