package avh.ckc.core.warmup

import avh.ckc.core.ProcessingMode
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

data class WarmupTopicMapping(
    val sourceTopic: String,
    val warmupTopic: String
) {
    init {
        validateTopicName("sourceTopic", sourceTopic)
        validateTopicName("warmupTopic", warmupTopic)
    }
}

data class WarmupForwardingProfile(
    val initialFraction: Double = 0.0,
    val targetFraction: Double = 0.1,
    val rampUpDuration: Duration = 2.minutes,
    val holdDuration: Duration? = null
) {
    init {
        require(initialFraction.isFinite() && initialFraction in 0.0..1.0) {
            "initialFraction must be finite and between 0.0 and 1.0"
        }
        require(targetFraction.isFinite() && targetFraction in 0.0..1.0) {
            "targetFraction must be finite and between 0.0 and 1.0"
        }
        require(targetFraction >= initialFraction) {
            "targetFraction must be greater than or equal to initialFraction"
        }
        require(!rampUpDuration.isNegative()) { "rampUpDuration must not be negative" }
        holdDuration?.let {
            require(!it.isNegative()) { "holdDuration must not be negative" }
        }
    }
}

data class WarmupParticipantConfiguration(
    val consumerId: WarmupConsumerId,
    val topicMappings: List<WarmupTopicMapping>,
    val forwardingProfile: WarmupForwardingProfile = WarmupForwardingProfile(),
    val processingMode: ProcessingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
) {
    init {
        require(processingMode == ProcessingMode.AT_LEAST_ONCE_NO_ORDERING) {
            "Real-traffic warm-up MVP supports only AT_LEAST_ONCE_NO_ORDERING"
        }
        require(topicMappings.isNotEmpty()) { "topicMappings must not be empty" }
        val duplicateSourceTopics = topicMappings
            .groupingBy(WarmupTopicMapping::sourceTopic)
            .eachCount()
            .filterValues { it > 1 }
            .keys
        require(duplicateSourceTopics.isEmpty()) {
            "Each source topic must have exactly one warm-up mapping; duplicates: ${duplicateSourceTopics.sorted()}"
        }
    }
}

/** A locally registered CKC consumer that can participate in real-traffic warm-up. */
interface WarmupParticipant {
    val warmupConfiguration: WarmupParticipantConfiguration
}

internal fun validateTopicName(name: String, value: String) {
    require(value.isNotBlank()) { "$name must not be blank" }
    require(value.none(Char::isISOControl)) { "$name must not contain control characters" }
}
