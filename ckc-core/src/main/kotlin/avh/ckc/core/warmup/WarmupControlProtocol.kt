package avh.ckc.core.warmup

import org.apache.kafka.common.TopicPartition

internal const val WARMUP_CONTROL_PROTOCOL_VERSION = 3

internal sealed interface WarmupControlMessage {
    val protocolVersion: Int
    val sessionId: WarmupSessionId
    val senderInstanceId: WarmupInstanceId
    val targetInstanceId: WarmupInstanceId?
}

internal sealed interface GeneratedWarmupCommand : WarmupControlMessage { val generation: Long }

internal data class StateSyncRequest(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage { override val targetInstanceId: WarmupInstanceId? = null }

internal class StateSnapshot(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    val generation: Long,
    val activeCoordinator: WarmupInstanceId,
    val coveredOffset: Long,
    val lastHeartbeatOffset: Long,
    readyInstances: Collection<WarmupInstanceId>,
    sessions: Collection<WarmupReplicatedSession>,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    val readyInstances = immutableSetSnapshot(readyInstances)
    val sessions = immutableListSnapshot(sessions)
    init {
        require(generation > 0) { "Snapshot generation must be positive" }
        require(coveredOffset >= 0) { "Snapshot covered offset must not be negative" }
        require(lastHeartbeatOffset in -1..coveredOffset) { "Snapshot heartbeat offset is invalid" }
        require(activeCoordinator == senderInstanceId) { "Snapshot sender must be its active coordinator" }
        require(senderInstanceId != targetInstanceId) { "Snapshot must target another instance" }
    }
}

internal data class InstanceReady(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage { override val targetInstanceId: WarmupInstanceId? = null }

internal data class CoordinatorHeartbeat(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val generation: Long,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : GeneratedWarmupCommand {
    override val targetInstanceId: WarmupInstanceId? = null
    init { require(generation > 0) }
}

internal data class CoordinatorClaim(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    val expectedGeneration: Long,
    val expectedCoordinator: WarmupInstanceId?,
    val observedHeartbeatOffset: Long,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    override val targetInstanceId: WarmupInstanceId? = null
    init {
        require(expectedGeneration >= 0) { "Expected generation must not be negative" }
        require(observedHeartbeatOffset >= -1) { "Observed heartbeat offset is invalid" }
    }
}

internal class WarmupRequest(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    participants: Collection<WarmupParticipantConfiguration>,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    override val targetInstanceId: WarmupInstanceId? = null
    val participants = immutableListSnapshot(participants)
    init {
        require(participants.isNotEmpty()) { "A warm-up request must contain participants" }
        require(participants.map { it.consumerId }.toSet().size == participants.size) {
            "A warm-up request must contain each consumer at most once"
        }
    }
}

internal class DonorProbe(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    override val generation: Long,
    val recipientInstanceId: WarmupInstanceId,
    participants: Collection<WarmupParticipantConfiguration>,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : GeneratedWarmupCommand {
    val participants = immutableListSnapshot(participants)
    init { require(generation > 0); require(participants.isNotEmpty()) }
}

internal class DonorOffer(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    val generation: Long,
    val capability: WarmupDonorCapability,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    init {
        require(generation > 0)
        require(capability.instanceId == senderInstanceId) { "Donor capability must describe the sender" }
    }
}

/** Applying this journal record, not acknowledging its send, owns the reservation. */
internal class AssignmentProposed(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val generation: Long,
    val assignment: WarmupSessionAssignment,
    val warmupDurationMillis: Long,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : GeneratedWarmupCommand {
    override val targetInstanceId: WarmupInstanceId? = null
    init {
        require(generation > 0)
        require(warmupDurationMillis >= 0)
        require(assignment.sessionId == sessionId) { "Assignment session must match the message" }
    }
}

internal class RecipientReady(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    consumerIds: Collection<WarmupConsumerId>,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    val consumerIds = immutableSetSnapshot(consumerIds)
    init { require(consumerIds.isNotEmpty()) }
}

internal data class StartForwarding(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    override val generation: Long,
    val stopAtEpochMillis: Long,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : GeneratedWarmupCommand {
    init { require(generation > 0); require(stopAtEpochMillis > 0) }
}

internal data class StopForwarding(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    override val generation: Long,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : GeneratedWarmupCommand { init { require(generation > 0) } }

internal data class WarmupFinalOffset(
    val consumerId: WarmupConsumerId,
    val topicPartition: TopicPartition,
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
    val finalOffsets = immutableListSnapshot(finalOffsets)
    init {
        require(finalOffsets.isNotEmpty())
        require(finalOffsets.map { it.topicPartition }.toSet().size == finalOffsets.size)
    }
}

internal class DrainRecipient(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    override val generation: Long,
    finalOffsets: Collection<WarmupFinalOffset>,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : GeneratedWarmupCommand {
    val finalOffsets = immutableListSnapshot(finalOffsets)
    init { require(generation > 0); require(finalOffsets.isNotEmpty()) }
}

internal class SessionCompleted(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    consumerIds: Collection<WarmupConsumerId>,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    override val targetInstanceId: WarmupInstanceId? = null
    val consumerIds = immutableSetSnapshot(consumerIds)
    init { require(consumerIds.isNotEmpty()) }
}

internal data class SessionFailed(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId?,
    val reason: String,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    init {
        require(reason.isNotBlank())
        require(reason.none(Char::isISOControl))
    }
}

internal enum class WarmupReplicatedStage { ASSIGNED, FORWARDING, STOPPING, DRAINING, QUARANTINED }

internal data class WarmupReplicatedSession(
    val assignment: WarmupSessionAssignment,
    val stage: WarmupReplicatedStage,
    val warmupDurationMillis: Long,
    val stopAtEpochMillis: Long? = null
) {
    init {
        require(warmupDurationMillis >= 0)
        require(stopAtEpochMillis == null || stopAtEpochMillis > 0)
        require(stage != WarmupReplicatedStage.FORWARDING || stopAtEpochMillis != null)
    }
}
