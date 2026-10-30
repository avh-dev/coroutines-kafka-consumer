package avh.ckc.core.warmup

class WarmupTopicCapability(
    val sourceTopic: String,
    val compatible: Boolean,
    ownedPartitions: Collection<Int> = emptySet(),
    val observedMessages: Long = 0
) {
    val ownedPartitions: Set<Int> = immutableSetSnapshot(ownedPartitions)

    init {
        validateTopicName("sourceTopic", sourceTopic)
        require(ownedPartitions.all { it >= 0 }) { "ownedPartitions must not contain negative partitions" }
        require(observedMessages >= 0) { "observedMessages must not be negative" }
    }

    val ownsCandidatePartitions: Boolean
        get() = ownedPartitions.isNotEmpty()

    val hasObservedTraffic: Boolean
        get() = observedMessages > 0

    override fun equals(other: Any?): Boolean =
        this === other ||
            other is WarmupTopicCapability &&
            sourceTopic == other.sourceTopic &&
            compatible == other.compatible &&
            ownedPartitions == other.ownedPartitions &&
            observedMessages == other.observedMessages

    override fun hashCode(): Int =
        arrayOf(sourceTopic, compatible, ownedPartitions, observedMessages).contentHashCode()

    override fun toString(): String =
        "WarmupTopicCapability(sourceTopic=$sourceTopic, compatible=$compatible, " +
            "ownedPartitions=$ownedPartitions, observedMessages=$observedMessages)"
}

class WarmupParticipantCapability(
    val consumerId: WarmupConsumerId,
    topics: Collection<WarmupTopicCapability>
) {
    val topics: List<WarmupTopicCapability> = immutableListSnapshot(topics)

    init {
        require(topics.isNotEmpty()) { "topics must not be empty" }
        require(topics.map { it.sourceTopic }.toSet().size == topics.size) {
            "A participant capability must contain each source topic at most once"
        }
    }

    override fun equals(other: Any?): Boolean =
        this === other ||
            other is WarmupParticipantCapability &&
            consumerId == other.consumerId &&
            topics == other.topics

    override fun hashCode(): Int = 31 * consumerId.hashCode() + topics.hashCode()

    override fun toString(): String =
        "WarmupParticipantCapability(consumerId=$consumerId, topics=$topics)"
}

class WarmupDonorCapability(
    val instanceId: WarmupInstanceId,
    val available: Boolean,
    participants: Collection<WarmupParticipantCapability>,
    /** Application-supplied load score; offers in one deployment must use the same scale. */
    val loadScore: Double? = null
) {
    val participants: List<WarmupParticipantCapability> = immutableListSnapshot(participants)

    init {
        require(participants.isNotEmpty()) { "participants must not be empty" }
        require(participants.map { it.consumerId }.toSet().size == participants.size) {
            "A donor capability must contain each consumer at most once"
        }
        loadScore?.let {
            require(it.isFinite() && it >= 0.0) { "loadScore must be finite and non-negative" }
        }
    }

    override fun equals(other: Any?): Boolean =
        this === other ||
            other is WarmupDonorCapability &&
            instanceId == other.instanceId &&
            available == other.available &&
            participants == other.participants &&
            loadScore == other.loadScore

    override fun hashCode(): Int =
        arrayOf(instanceId, available, participants, loadScore).contentHashCode()

    override fun toString(): String =
        "WarmupDonorCapability(instanceId=$instanceId, available=$available, " +
            "participants=$participants, loadScore=$loadScore)"
}
