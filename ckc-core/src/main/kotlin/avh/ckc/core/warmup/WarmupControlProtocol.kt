package avh.ckc.core.warmup

import org.apache.kafka.common.TopicPartition

internal const val WARMUP_CONTROL_PROTOCOL_VERSION = 1

internal sealed interface WarmupControlMessage {
    val protocolVersion: Int
    val sessionId: WarmupSessionId
    val senderInstanceId: WarmupInstanceId
    val targetInstanceId: WarmupInstanceId?
}

internal class WarmupRequest(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    participants: Collection<WarmupParticipantConfiguration>,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    override val targetInstanceId: WarmupInstanceId? = null
    val participants: List<WarmupParticipantConfiguration> = immutableListSnapshot(participants)

    init {
        require(participants.isNotEmpty()) { "A warm-up request must contain participants" }
        require(participants.map { it.consumerId }.toSet().size == participants.size) {
            "A warm-up request must contain each consumer at most once"
        }
    }
}

internal class DonorOffer(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    val capability: WarmupDonorCapability,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    init {
        require(capability.instanceId == senderInstanceId) {
            "Donor capability must describe the sending instance"
        }
        require(senderInstanceId != targetInstanceId) { "A donor cannot offer to itself" }
    }
}

internal data class DonorSelected(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    init {
        require(senderInstanceId != targetInstanceId) { "A recipient cannot select itself" }
    }
}

internal class WarmupAssigned(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    val assignment: WarmupSessionAssignment,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    init {
        require(assignment.sessionId == sessionId) { "Assignment session must match the message session" }
        require(assignment.donorInstanceId == senderInstanceId) { "Assignment donor must match the sender" }
        require(assignment.recipientInstanceId == targetInstanceId) { "Assignment recipient must match the target" }
    }
}

internal class RecipientReady(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    consumerIds: Collection<WarmupConsumerId>,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    val consumerIds: Set<WarmupConsumerId> = immutableSetSnapshot(consumerIds)

    init {
        requireDistinctInstances(senderInstanceId, targetInstanceId)
        require(consumerIds.isNotEmpty()) { "Recipient readiness must contain consumers" }
    }
}

internal data class WarmupFinished(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    init {
        requireDistinctInstances(senderInstanceId, targetInstanceId)
    }
}

internal data class WarmupFinalOffset(
    val consumerId: WarmupConsumerId,
    val topicPartition: TopicPartition,
    /** Inclusive acknowledged offset, or null when the partition received no records. */
    val offset: Long?
) {
    init {
        validateTopicName("warmupTopic", topicPartition.topic())
        require(topicPartition.partition() >= 0) { "warmupPartition must not be negative" }
        require(offset == null || offset >= 0) { "final offset must not be negative" }
    }
}

internal class ForwardingStopped(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    finalOffsets: Collection<WarmupFinalOffset>,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    val finalOffsets: List<WarmupFinalOffset> = immutableListSnapshot(finalOffsets)

    init {
        requireDistinctInstances(senderInstanceId, targetInstanceId)
        require(finalOffsets.isNotEmpty()) { "Forwarding completion must contain assigned partitions" }
        require(finalOffsets.map { it.topicPartition }.toSet().size == finalOffsets.size) {
            "Forwarding completion must contain each warm-up partition at most once"
        }
    }
}

internal class SessionCompleted(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    consumerIds: Collection<WarmupConsumerId>,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    val consumerIds: Set<WarmupConsumerId> = immutableSetSnapshot(consumerIds)

    init {
        requireDistinctInstances(senderInstanceId, targetInstanceId)
        require(consumerIds.isNotEmpty()) { "Session completion must contain consumers" }
    }
}

internal data class SessionFailed(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId?,
    val reason: String,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    init {
        targetInstanceId?.let { requireDistinctInstances(senderInstanceId, it) }
        require(reason.isNotBlank()) { "Failure reason must not be blank" }
        require(reason.none(Char::isISOControl)) { "Failure reason must not contain control characters" }
    }
}

private fun requireDistinctInstances(sender: WarmupInstanceId, target: WarmupInstanceId) {
    require(sender != target) { "Control message sender and target must be different instances" }
}
