package avh.ckc.core.warmup

/** Offset-based completion of the recipient tail. */
enum class WarmupDrainStatus {
    NOT_STARTED,
    NO_RECORDS,
    COMPLETED,
    FAILED
}

/** What real traffic and successful business handling were actually observed. */
enum class WarmupTrafficStatus {
    INCOMPATIBLE,
    NOT_CURRENTLY_OWNED,
    NO_REAL_TRAFFIC,
    FORWARDED_WITHOUT_SUCCESSFUL_PROCESSING,
    BUSINESS_PROCESSING_OBSERVED
}

/** Delivery correctness established by correlating source, forwarding, and terminal audit events. */
enum class WarmupDeliveryAuditStatus {
    NOT_ANALYZED,
    VERIFIED,
    VIOLATIONS_DETECTED
}

class WarmupTopicOutcome(
    val consumerId: WarmupConsumerId,
    val sourceTopic: String,
    val warmupTopic: String,
    val compatible: Boolean,
    ownedSourcePartitions: Collection<Int>,
    val observedMessages: Long,
    val forwardedMessages: Long,
    /** Records whose business handler returned successfully; failure-handler outcomes are not included. */
    val successfullyProcessedMessages: Long,
    val forwardedMessagesPerSecond: Double,
    val drainStatus: WarmupDrainStatus,
    val deliveryAuditStatus: WarmupDeliveryAuditStatus = WarmupDeliveryAuditStatus.NOT_ANALYZED
) {
    val ownedSourcePartitions: Set<Int> = immutableSetSnapshot(ownedSourcePartitions)

    init {
        validateTopicName("sourceTopic", sourceTopic)
        validateTopicName("warmupTopic", warmupTopic)
        require(this.ownedSourcePartitions.all { it >= 0 }) {
            "ownedSourcePartitions must not contain negative partitions"
        }
        require(observedMessages >= 0) { "observedMessages must not be negative" }
        require(forwardedMessages >= 0) { "forwardedMessages must not be negative" }
        require(successfullyProcessedMessages >= 0) {
            "successfullyProcessedMessages must not be negative"
        }
        require(forwardedMessagesPerSecond.isFinite() && forwardedMessagesPerSecond >= 0.0) {
            "forwardedMessagesPerSecond must be finite and non-negative"
        }
        require(compatible || forwardedMessages == 0L) {
            "An incompatible topic cannot report forwarded messages"
        }
        require(ownedSourcePartitions.isNotEmpty() || forwardedMessages == 0L) {
            "A topic without owned source partitions cannot report forwarded messages"
        }
        require(forwardedMessages > 0 || forwardedMessagesPerSecond == 0.0) {
            "A topic without forwarded messages must report zero forwarding throughput"
        }
        require(forwardedMessages > 0 || successfullyProcessedMessages == 0L) {
            "A topic without forwarded messages cannot report successful business processing"
        }
        require(
            drainStatus != WarmupDrainStatus.NO_RECORDS ||
                forwardedMessages == 0L && successfullyProcessedMessages == 0L
        ) {
            "NO_RECORDS drain status requires zero forwarded and successfully processed messages"
        }
    }

    val trafficStatus: WarmupTrafficStatus
        get() = when {
            !compatible -> WarmupTrafficStatus.INCOMPATIBLE
            ownedSourcePartitions.isEmpty() -> WarmupTrafficStatus.NOT_CURRENTLY_OWNED
            forwardedMessages == 0L -> WarmupTrafficStatus.NO_REAL_TRAFFIC
            successfullyProcessedMessages == 0L ->
                WarmupTrafficStatus.FORWARDED_WITHOUT_SUCCESSFUL_PROCESSING
            else -> WarmupTrafficStatus.BUSINESS_PROCESSING_OBSERVED
        }

    val drainCompleted: Boolean
        get() = drainStatus == WarmupDrainStatus.NO_RECORDS || drainStatus == WarmupDrainStatus.COMPLETED

    val deliveryVerified: Boolean
        get() = deliveryAuditStatus == WarmupDeliveryAuditStatus.VERIFIED

    override fun equals(other: Any?): Boolean =
        this === other ||
            other is WarmupTopicOutcome &&
            consumerId == other.consumerId &&
            sourceTopic == other.sourceTopic &&
            warmupTopic == other.warmupTopic &&
            compatible == other.compatible &&
            ownedSourcePartitions == other.ownedSourcePartitions &&
            observedMessages == other.observedMessages &&
            forwardedMessages == other.forwardedMessages &&
            successfullyProcessedMessages == other.successfullyProcessedMessages &&
            forwardedMessagesPerSecond.compareTo(other.forwardedMessagesPerSecond) == 0 &&
            drainStatus == other.drainStatus &&
            deliveryAuditStatus == other.deliveryAuditStatus

    override fun hashCode(): Int =
        arrayOf(
            consumerId,
            sourceTopic,
            warmupTopic,
            compatible,
            ownedSourcePartitions,
            observedMessages,
            forwardedMessages,
            successfullyProcessedMessages,
            forwardedMessagesPerSecond,
            drainStatus,
            deliveryAuditStatus
        ).contentHashCode()

    override fun toString(): String =
        "WarmupTopicOutcome(consumerId=$consumerId, sourceTopic=$sourceTopic, warmupTopic=$warmupTopic, " +
            "compatible=$compatible, ownedSourcePartitions=$ownedSourcePartitions, " +
            "observedMessages=$observedMessages, forwardedMessages=$forwardedMessages, " +
            "successfullyProcessedMessages=$successfullyProcessedMessages, " +
            "forwardedMessagesPerSecond=$forwardedMessagesPerSecond, drainStatus=$drainStatus, " +
            "deliveryAuditStatus=$deliveryAuditStatus)"
}
