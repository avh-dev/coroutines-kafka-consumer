package avh.ckc.core.warmup

import org.apache.kafka.common.TopicPartition

internal const val WARMUP_CONTROL_PROTOCOL_VERSION = 5

/** Maximum permitted difference between any two participant wall clocks. */
internal const val WARMUP_MAX_PAIRWISE_CLOCK_SKEW_MILLIS = 5_000L
internal const val WARMUP_MAX_RESERVATION_AUTHORIZATION_MILLIS = 600_000L

internal sealed interface WarmupControlMessage {
    val protocolVersion: Int
    val sessionId: WarmupSessionId
    val senderInstanceId: WarmupInstanceId
    val targetInstanceId: WarmupInstanceId?
    val configurationFingerprint: String
    val expiresAtEpochMillis: Long
}

internal data class WarmupTopicMapping(
    val consumerId: WarmupConsumerId,
    val sourceTopic: String,
    val warmupTopic: String
) {
    init {
        validateWarmupTopic("sourceTopic", sourceTopic)
        validateWarmupTopic("warmupTopic", warmupTopic)
    }
}

internal data class WarmupDonorMapping(
    val mapping: WarmupTopicMapping,
    val sourcePartitions: Set<Int>
) {
    init {
        require(sourcePartitions.isNotEmpty()) { "Donor source partitions must not be empty" }
        require(sourcePartitions.all { it >= 0 }) { "Donor source partitions must not be negative" }
    }
}

internal data class WarmupPartitionAssignment(
    val mapping: WarmupTopicMapping,
    val warmupPartition: Int
) {
    init {
        require(warmupPartition >= 0) { "Warm-up partition must not be negative" }
    }

    val destination: TopicPartition get() = TopicPartition(mapping.warmupTopic, warmupPartition)
}

internal enum class WarmupSessionLifecycle {
    DISCOVERING, RESERVED, NEGOTIATING, READY, CLOSING, CLOSED
}

internal enum class WarmupReservationReleaseKind {
    NORMAL,
    EMERGENCY_TIMEOUT
}

internal data class WarmupSessionSnapshot(
    val sessionId: WarmupSessionId,
    val recipient: WarmupInstanceId,
    val configurationFingerprint: String,
    val reservationId: WarmupReservationId?,
    val assignments: List<WarmupPartitionAssignment>,
    val donors: Set<WarmupInstanceId>,
    val lifecycle: WarmupSessionLifecycle,
    val expiresAtEpochMillis: Long,
    val lastControlOffset: Long
)

internal data class WarmupStateSnapshot(
    val boundaryOffset: Long,
    val sessions: List<WarmupSessionSnapshot>,
    val retiredThroughEpochMillis: Long = 0
) {
    init {
        require(boundaryOffset >= -1) { "Snapshot boundary must be at least -1" }
        require(retiredThroughEpochMillis >= 0) { "Snapshot retirement boundary must not be negative" }
        require(sessions.map { it.sessionId }.distinct().size == sessions.size) {
            "Snapshot sessions must be unique"
        }
        require(sessions.all { it.expiresAtEpochMillis > retiredThroughEpochMillis }) {
            "Snapshot must not contain retired sessions"
        }
    }
}

private val RETIREMENT_SESSION_ID = WarmupSessionId("__warmup_session_retirement__")

internal data class RetireSessionsThrough(
    override val senderInstanceId: WarmupInstanceId,
    val deadlineCutoffEpochMillis: Long,
    override val configurationFingerprint: String,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    override val sessionId: WarmupSessionId = RETIREMENT_SESSION_ID
    override val targetInstanceId: WarmupInstanceId? = null
    override val expiresAtEpochMillis: Long = deadlineCutoffEpochMillis

    init { validateCommonControlFields(configurationFingerprint, deadlineCutoffEpochMillis) }

    companion object {
        fun hasSessionId(sessionId: WarmupSessionId): Boolean = sessionId == RETIREMENT_SESSION_ID
    }
}

internal data class WarmupRequested(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    val mappings: List<WarmupTopicMapping>,
    val offerDeadlineEpochMillis: Long,
    override val configurationFingerprint: String,
    override val expiresAtEpochMillis: Long,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    override val targetInstanceId: WarmupInstanceId? = null

    init {
        validateCommonControlFields(configurationFingerprint, expiresAtEpochMillis)
        require(mappings.isNotEmpty()) { "Warm-up request mappings must not be empty" }
        require(mappings.distinct().size == mappings.size) { "Warm-up request mappings must be unique" }
        require(offerDeadlineEpochMillis > 0 && offerDeadlineEpochMillis <= expiresAtEpochMillis) {
            "Offer deadline must be positive and not exceed the session deadline"
        }
    }
}

internal data class DonorOffered(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    val mappings: List<WarmupDonorMapping>,
    override val configurationFingerprint: String,
    override val expiresAtEpochMillis: Long,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION,
    val stateSnapshot: WarmupStateSnapshot? = null
) : WarmupControlMessage {
    init {
        validateCommonControlFields(configurationFingerprint, expiresAtEpochMillis)
        require(mappings.isNotEmpty() || stateSnapshot != null) {
            "A donor offer must provide capacity or an authoritative state snapshot"
        }
    }
}

internal data class ReservationRequested(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    val reservationId: WarmupReservationId,
    val assignments: List<WarmupPartitionAssignment>,
    override val configurationFingerprint: String,
    override val expiresAtEpochMillis: Long,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    override val targetInstanceId: WarmupInstanceId? = null

    init {
        validateCommonControlFields(configurationFingerprint, expiresAtEpochMillis)
        require(assignments.isNotEmpty()) { "Reservation assignments must not be empty" }
        require(assignments.map { it.mapping }.distinct().size == assignments.size) {
            "A reservation must contain one destination per mapping"
        }
        require(assignments.map { it.destination }.distinct().size == assignments.size) {
            "Reservation destinations must be unique"
        }
    }
}

internal data class ReservationReleased(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    val reservationId: WarmupReservationId,
    val kind: WarmupReservationReleaseKind,
    val releasedAtEpochMillis: Long,
    override val configurationFingerprint: String,
    override val expiresAtEpochMillis: Long,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    override val targetInstanceId: WarmupInstanceId? = null
    init {
        validateCommonControlFields(configurationFingerprint, expiresAtEpochMillis)
        require(releasedAtEpochMillis > 0) { "Reservation release time must be positive" }
    }
}

internal data class SessionAccepted(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    val assignments: List<WarmupPartitionAssignment>,
    override val configurationFingerprint: String,
    override val expiresAtEpochMillis: Long,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    init {
        validateCommonControlFields(configurationFingerprint, expiresAtEpochMillis)
        require(assignments.isNotEmpty()) { "Session assignments must not be empty" }
        require(assignments.map { it.mapping }.distinct().size == assignments.size) {
            "Each mapping must have one warm-up assignment per donor"
        }
        require(assignments.map { it.destination }.distinct().size == assignments.size) {
            "Warm-up destination partitions must be exclusive within an acceptance"
        }
    }
}

internal data class DonorAccepted(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    override val configurationFingerprint: String,
    override val expiresAtEpochMillis: Long,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    init { validateCommonControlFields(configurationFingerprint, expiresAtEpochMillis) }
}

internal data class SessionRejected(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    val reason: String,
    override val configurationFingerprint: String,
    override val expiresAtEpochMillis: Long,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    init {
        validateCommonControlFields(configurationFingerprint, expiresAtEpochMillis)
        validateWarmupReason(reason)
    }
}

internal data class RecipientReady(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    val assignments: List<WarmupPartitionAssignment>,
    override val configurationFingerprint: String,
    override val expiresAtEpochMillis: Long,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    init {
        validateCommonControlFields(configurationFingerprint, expiresAtEpochMillis)
        require(assignments.isNotEmpty()) { "Ready assignments must not be empty" }
    }
}

internal data class ControlRecordCompleted(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    val completed: WarmupRecordCompleted,
    override val configurationFingerprint: String,
    override val expiresAtEpochMillis: Long,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    init {
        validateCommonControlFields(configurationFingerprint, expiresAtEpochMillis)
        require(completed.recordId.sessionId == sessionId) { "Completed record must belong to the control session" }
    }
}

internal data class WarmupCompletionRequested(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    override val configurationFingerprint: String,
    override val expiresAtEpochMillis: Long,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    init { validateCommonControlFields(configurationFingerprint, expiresAtEpochMillis) }
}

internal data class DonorClosed(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    override val configurationFingerprint: String,
    override val expiresAtEpochMillis: Long,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    init { validateCommonControlFields(configurationFingerprint, expiresAtEpochMillis) }
}

internal enum class WarmupSessionEndState { COMPLETED, FAILED, COLD_FALLBACK }

internal data class SessionEnded(
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    val state: WarmupSessionEndState,
    val reason: String?,
    override val configurationFingerprint: String,
    override val expiresAtEpochMillis: Long,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    override val targetInstanceId: WarmupInstanceId? = null

    init {
        validateCommonControlFields(configurationFingerprint, expiresAtEpochMillis)
        reason?.let(::validateWarmupReason)
        require(state == WarmupSessionEndState.COMPLETED || reason != null) {
            "Failed or cold-fallback session end must include a reason"
        }
    }
}

private fun validateCommonControlFields(fingerprint: String, expiresAtEpochMillis: Long) {
    require(fingerprint.isNotBlank()) { "Configuration fingerprint must not be blank" }
    require(fingerprint.length <= 255) { "Configuration fingerprint must be at most 255 characters" }
    require(fingerprint.none(Char::isISOControl)) { "Configuration fingerprint must not contain control characters" }
    require(expiresAtEpochMillis > 0) { "Session deadline must be positive" }
}

private fun validateWarmupTopic(name: String, topic: String) {
    require(topic.isNotBlank()) { "$name must not be blank" }
    require(topic.length <= 249) { "$name must be at most 249 characters" }
    require(topic.none(Char::isISOControl)) { "$name must not contain control characters" }
}

private fun validateWarmupReason(reason: String) {
    require(reason.isNotBlank()) { "Reason must not be blank" }
    require(reason.length <= 4_096) { "Reason must be at most 4096 characters" }
    require(reason.none(Char::isISOControl)) { "Reason must not contain control characters" }
}
