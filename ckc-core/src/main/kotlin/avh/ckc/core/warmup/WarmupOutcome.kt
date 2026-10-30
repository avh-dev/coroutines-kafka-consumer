package avh.ckc.core.warmup

enum class WarmupDrainStatus {
    NOT_STARTED,
    NO_RECORDS,
    COMPLETED,
    FAILED
}

enum class WarmupTopicResult {
    INCOMPATIBLE,
    NOT_CURRENTLY_OWNED,
    NO_REAL_TRAFFIC,
    INCOMPLETE,
    WARMED_WITH_REAL_TRAFFIC,
    FAILED
}

data class WarmupTopicOutcome(
    val consumerId: WarmupConsumerId,
    val sourceTopic: String,
    val warmupTopic: String,
    val compatible: Boolean,
    val ownedSourcePartitions: Set<Int>,
    val observedMessages: Long,
    val forwardedMessages: Long,
    val processedMessages: Long,
    val forwardedMessagesPerSecond: Double,
    val drainStatus: WarmupDrainStatus
) {
    init {
        validateTopicName("sourceTopic", sourceTopic)
        validateTopicName("warmupTopic", warmupTopic)
        require(ownedSourcePartitions.all { it >= 0 }) {
            "ownedSourcePartitions must not contain negative partitions"
        }
        require(observedMessages >= 0) { "observedMessages must not be negative" }
        require(forwardedMessages >= 0) { "forwardedMessages must not be negative" }
        require(processedMessages >= 0) { "processedMessages must not be negative" }
        require(forwardedMessagesPerSecond.isFinite() && forwardedMessagesPerSecond >= 0.0) {
            "forwardedMessagesPerSecond must be finite and non-negative"
        }
        require(compatible || forwardedMessages == 0L) {
            "An incompatible topic cannot report forwarded messages"
        }
        require(forwardedMessages > 0 || forwardedMessagesPerSecond == 0.0) {
            "A topic without forwarded messages must report zero forwarding throughput"
        }
        require(drainStatus != WarmupDrainStatus.NO_RECORDS || forwardedMessages == 0L) {
            "NO_RECORDS drain status requires zero forwarded messages"
        }
    }

    val result: WarmupTopicResult
        get() = when {
            !compatible -> WarmupTopicResult.INCOMPATIBLE
            ownedSourcePartitions.isEmpty() -> WarmupTopicResult.NOT_CURRENTLY_OWNED
            drainStatus == WarmupDrainStatus.FAILED -> WarmupTopicResult.FAILED
            forwardedMessages == 0L -> WarmupTopicResult.NO_REAL_TRAFFIC
            drainStatus == WarmupDrainStatus.COMPLETED && processedMessages >= forwardedMessages ->
                WarmupTopicResult.WARMED_WITH_REAL_TRAFFIC
            else -> WarmupTopicResult.INCOMPLETE
        }
}
