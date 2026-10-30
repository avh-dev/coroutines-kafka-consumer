package avh.ckc.core.warmup

data class WarmupTopicCapability(
    val sourceTopic: String,
    val compatible: Boolean,
    val ownedPartitions: Set<Int> = emptySet(),
    val observedMessages: Long = 0
) {
    init {
        validateTopicName("sourceTopic", sourceTopic)
        require(ownedPartitions.all { it >= 0 }) { "ownedPartitions must not contain negative partitions" }
        require(observedMessages >= 0) { "observedMessages must not be negative" }
    }

    val ownsCandidatePartitions: Boolean
        get() = ownedPartitions.isNotEmpty()

    val hasObservedTraffic: Boolean
        get() = observedMessages > 0
}

data class WarmupParticipantCapability(
    val consumerId: WarmupConsumerId,
    val topics: List<WarmupTopicCapability>
) {
    init {
        require(topics.isNotEmpty()) { "topics must not be empty" }
        require(topics.map { it.sourceTopic }.toSet().size == topics.size) {
            "A participant capability must contain each source topic at most once"
        }
    }
}

data class WarmupDonorCapability(
    val instanceId: WarmupInstanceId,
    val available: Boolean,
    val participants: List<WarmupParticipantCapability>,
    /** Application-supplied load score; offers in one deployment must use the same scale. */
    val loadScore: Double? = null
) {
    init {
        require(participants.isNotEmpty()) { "participants must not be empty" }
        require(participants.map { it.consumerId }.toSet().size == participants.size) {
            "A donor capability must contain each consumer at most once"
        }
        loadScore?.let {
            require(it.isFinite() && it >= 0.0) { "loadScore must be finite and non-negative" }
        }
    }
}
