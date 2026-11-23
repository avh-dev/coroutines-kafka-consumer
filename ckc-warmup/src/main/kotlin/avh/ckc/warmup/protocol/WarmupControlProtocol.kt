package avh.ckc.warmup.protocol

import org.apache.kafka.common.TopicPartition

const val WARMUP_CONTROL_PROTOCOL_VERSION = 6

@JvmInline
value class WarmupControlNamespace(val value: String) {
    init {
        require(value.isNotBlank()) { "Control namespace must not be blank" }
        require(value.length <= 255) { "Control namespace must be at most 255 characters" }
        require(value.none(Char::isISOControl)) { "Control namespace must not contain control characters" }
    }
}

sealed interface WarmupControlMessage {
    val protocolVersion: Int
    val namespace: WarmupControlNamespace
    val sessionId: WarmupSessionId
    val senderInstanceId: WarmupInstanceId
    val targetInstanceId: WarmupInstanceId?
    val configurationFingerprint: String
    val expiresAtEpochMillis: Long
}

/** Immutable per-mapping quota carried by every offer and acceptance. */
data class WarmupMappingQuota(
    val profile: WarmupQuotaProfile,
    val quotaIntervalMillis: Long = 1_000
) {
    constructor(quota: Int, quotaIntervalMillis: Long = 1_000) :
        this(WarmupQuotaProfile.constant(quota), quotaIntervalMillis)

    init {
        require(quotaIntervalMillis > 0) { "Mapping quota interval must be positive" }
    }

    /** Admission units are records per second, rounded up so capacity is never under-accounted. */
    fun admissionBudgetPerSecond(): Int {
        return quotaPerSecond(profile.maximumQuota, quotaIntervalMillis)
    }
}

/** Both inputs are positive. Round up without adding to the potentially large interval. */
internal fun quotaPerSecond(quota: Int, quotaIntervalMillis: Long): Int {
    val numerator = quota.toLong() * 1_000L
    val rounded = 1 + (numerator - 1) / quotaIntervalMillis
    return rounded.coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
}

internal fun normalizedAdmissionBudget(mappings: Collection<WarmupTopicMapping>): Int {
    val total = mappings.sumOf { it.quota.admissionBudgetPerSecond().toLong() }
    require(total in 1..Int.MAX_VALUE.toLong()) { "Aggregate mapping quota exceeds the protocol admission range" }
    return total.toInt()
}

data class WarmupTopicMapping(
    val participantId: WarmupParticipantId,
    val sourceTopic: String,
    val warmupTopic: String,
    val mappingFingerprint: String,
    val quota: WarmupMappingQuota = WarmupMappingQuota(10)
) {
    init {
        validateTopic("sourceTopic", sourceTopic)
        validateTopic("warmupTopic", warmupTopic)
        validateFingerprint("Mapping fingerprint", mappingFingerprint)
    }
}

data class WarmupSourcePartition(val partition: Int, val ownershipGeneration: Long) {
    init {
        require(partition >= 0) { "Source partition must not be negative" }
        require(ownershipGeneration > 0) { "Ownership generation must be positive" }
    }
}

data class WarmupDonorMapping(
    val mapping: WarmupTopicMapping,
    val sourcePartitions: Set<WarmupSourcePartition>
) {
    init {
        require(sourcePartitions.isNotEmpty()) { "Donor source partitions must not be empty" }
        require(sourcePartitions.map { it.partition }.distinct().size == sourcePartitions.size) {
            "Donor source partitions must be unique"
        }
    }
}

data class WarmupPartitionAssignment(
    val mapping: WarmupTopicMapping,
    val warmupPartition: Int = 0
) {
    init {
        require(warmupPartition == 0) { "GLOBAL-5 warm-up topics must use partition 0" }
    }

    val destination: TopicPartition get() = TopicPartition(mapping.warmupTopic, warmupPartition)
}

data class WarmupRequested(
    override val namespace: WarmupControlNamespace,
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    val mappings: List<WarmupTopicMapping>,
    val offerDeadlineEpochMillis: Long,
    val admissionBudgetPerSecond: Int,
    override val configurationFingerprint: String,
    override val expiresAtEpochMillis: Long,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    override val targetInstanceId: WarmupInstanceId? = null

    init {
        validateCommonControlFields(configurationFingerprint, expiresAtEpochMillis)
        validateMappings(mappings)
        require(offerDeadlineEpochMillis > 0 && offerDeadlineEpochMillis <= expiresAtEpochMillis) {
            "Offer deadline must be positive and not exceed the session deadline"
        }
        validateAdmissionBudget(admissionBudgetPerSecond, mappings)
    }

}

data class DonorOffered(
    override val namespace: WarmupControlNamespace,
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    val mappings: List<WarmupDonorMapping>,
    override val configurationFingerprint: String,
    override val expiresAtEpochMillis: Long,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    init {
        validateCommonControlFields(configurationFingerprint, expiresAtEpochMillis)
        validateDonorMappings(mappings)
    }
}

data class SessionAccepted(
    override val namespace: WarmupControlNamespace,
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    val mappings: List<WarmupDonorMapping>,
    val admissionBudgetPerSecond: Int,
    override val configurationFingerprint: String,
    override val expiresAtEpochMillis: Long,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    init {
        validateCommonControlFields(configurationFingerprint, expiresAtEpochMillis)
        validateDonorMappings(mappings)
        validateAdmissionBudget(admissionBudgetPerSecond, mappings.map(WarmupDonorMapping::mapping))
    }

}

data class DonorAccepted(
    override val namespace: WarmupControlNamespace,
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    val mappings: List<WarmupDonorMapping>,
    val admissionBudgetPerSecond: Int,
    override val configurationFingerprint: String,
    override val expiresAtEpochMillis: Long,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION,
    val supportsLiveness: Boolean = false
) : WarmupControlMessage {
    init {
        validateCommonControlFields(configurationFingerprint, expiresAtEpochMillis)
        validateDonorMappings(mappings)
        validateAdmissionBudget(admissionBudgetPerSecond, mappings.map(WarmupDonorMapping::mapping))
    }

}

data class SessionRejected(
    override val namespace: WarmupControlNamespace,
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    val reasonCode: String,
    override val configurationFingerprint: String,
    override val expiresAtEpochMillis: Long,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    init {
        validateCommonControlFields(configurationFingerprint, expiresAtEpochMillis)
        validateReasonCode(reasonCode)
    }
}

data class RecipientReady(
    override val namespace: WarmupControlNamespace,
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
        validateAssignments(assignments)
    }
}

data class ControlRecordCompleted(
    override val namespace: WarmupControlNamespace,
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

data class WarmupCompletionRequested(
    override val namespace: WarmupControlNamespace,
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    override val configurationFingerprint: String,
    override val expiresAtEpochMillis: Long,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    init {
        validateCommonControlFields(configurationFingerprint, expiresAtEpochMillis)
    }
}

data class DonorClosed(
    override val namespace: WarmupControlNamespace,
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    override val targetInstanceId: WarmupInstanceId,
    override val configurationFingerprint: String,
    override val expiresAtEpochMillis: Long,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    init {
        validateCommonControlFields(configurationFingerprint, expiresAtEpochMillis)
    }
}

/** The matching v6 readiness or admission envelope carries the probe timestamp in a Kafka header. */
data class WarmupLivenessProbe(
    val envelope: WarmupControlMessage,
    val sentAtNanos: Long
) : WarmupControlMessage by envelope {
    val reply: Boolean get() = envelope is DonorAccepted

    init {
        require(envelope is RecipientReady || envelope is DonorAccepted)
        require(sentAtNanos > 0) { "Liveness probe timestamp must be positive" }
    }
}

enum class WarmupSessionEndState { COMPLETED, FAILED, COLD_FALLBACK }

data class SessionEnded(
    override val namespace: WarmupControlNamespace,
    override val sessionId: WarmupSessionId,
    override val senderInstanceId: WarmupInstanceId,
    val state: WarmupSessionEndState,
    val reasonCode: String?,
    override val configurationFingerprint: String,
    override val expiresAtEpochMillis: Long,
    override val protocolVersion: Int = WARMUP_CONTROL_PROTOCOL_VERSION
) : WarmupControlMessage {
    override val targetInstanceId: WarmupInstanceId? = null

    init {
        validateCommonControlFields(configurationFingerprint, expiresAtEpochMillis)
        reasonCode?.let(::validateReasonCode)
        require(state == WarmupSessionEndState.COMPLETED || reasonCode != null) {
            "Failed or cold-fallback session end must include a reason code"
        }
    }
}

private fun validateCommonControlFields(fingerprint: String, expiresAtEpochMillis: Long) {
    validateFingerprint("Configuration fingerprint", fingerprint)
    require(expiresAtEpochMillis > 0) { "Session deadline must be positive" }
}

private fun validateMappings(mappings: List<WarmupTopicMapping>) {
    require(mappings.isNotEmpty()) { "Warm-up mappings must not be empty" }
    require(mappings.distinct().size == mappings.size) { "Warm-up mappings must be unique" }
    require(mappings.map { it.warmupTopic }.distinct().size == mappings.size) {
        "Warm-up destination topics must be unique within a namespace"
    }
}

private fun validateDonorMappings(mappings: List<WarmupDonorMapping>) {
    require(mappings.isNotEmpty()) { "Donor mappings must not be empty" }
    validateMappings(mappings.map { it.mapping })
}

private fun validateAssignments(assignments: List<WarmupPartitionAssignment>) {
    require(assignments.isNotEmpty()) { "Warm-up assignments must not be empty" }
    validateMappings(assignments.map { it.mapping })
    require(assignments.map { it.destination }.distinct().size == assignments.size) {
        "Warm-up destinations must be unique"
    }
}

private fun validateAdmissionBudget(budgetPerSecond: Int, mappings: List<WarmupTopicMapping>) {
    require(budgetPerSecond > 0) { "Admission budget per second must be positive" }
    val expected = mappings.sumOf { it.quota.admissionBudgetPerSecond().toLong() }
    require(expected <= Int.MAX_VALUE && budgetPerSecond == expected.toInt()) {
        "Admission budget per second must equal the normalized sum of mapping quotas"
    }
}

private fun validateTopic(name: String, topic: String) {
    require(topic.isNotBlank()) { "$name must not be blank" }
    require(topic.length <= 249) { "$name must be at most 249 characters" }
    require(topic.none(Char::isISOControl)) { "$name must not contain control characters" }
}

private fun validateFingerprint(name: String, fingerprint: String) {
    require(fingerprint.isNotBlank()) { "$name must not be blank" }
    require(fingerprint.length <= 255) { "$name must be at most 255 characters" }
    require(fingerprint.none(Char::isISOControl)) { "$name must not contain control characters" }
}

private fun validateReasonCode(reasonCode: String) {
    require(reasonCode.isNotBlank()) { "Reason code must not be blank" }
    require(reasonCode.length <= 255) { "Reason code must be at most 255 characters" }
    require(reasonCode.all { it.isLetterOrDigit() || it == '_' || it == '-' || it == '.' }) {
        "Reason code contains unsupported characters"
    }
}
