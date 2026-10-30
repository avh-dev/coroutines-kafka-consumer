package avh.ckc.core.warmup

import org.apache.kafka.common.TopicPartition

data class WarmupPartitionAssignment(
    val consumerId: WarmupConsumerId,
    val sourceTopic: String,
    val warmupTopic: String,
    val warmupPartition: Int
) {
    init {
        validateTopicName("sourceTopic", sourceTopic)
        validateTopicName("warmupTopic", warmupTopic)
        require(warmupPartition >= 0) { "warmupPartition must not be negative" }
    }

    val topicPartition: TopicPartition
        get() = TopicPartition(warmupTopic, warmupPartition)
}

data class WarmupSessionAssignment(
    val sessionId: WarmupSessionId,
    val donorInstanceId: WarmupInstanceId,
    val recipientInstanceId: WarmupInstanceId,
    val partitions: List<WarmupPartitionAssignment>
) {
    init {
        require(donorInstanceId != recipientInstanceId) {
            "donorInstanceId and recipientInstanceId must identify different application instances"
        }
        require(partitions.isNotEmpty()) { "partitions must not be empty" }

        val duplicateSources = partitions
            .groupingBy { it.consumerId to it.sourceTopic }
            .eachCount()
            .filterValues { it > 1 }
            .keys
        require(duplicateSources.isEmpty()) {
            "Each consumer and source topic must have one warm-up partition assignment"
        }

        val duplicateDestinations = partitions
            .groupingBy(WarmupPartitionAssignment::topicPartition)
            .eachCount()
            .filterValues { it > 1 }
            .keys
        require(duplicateDestinations.isEmpty()) {
            "A warm-up partition can be assigned only once in a session: ${duplicateDestinations.sortedBy { it.toString() }}"
        }
    }

    fun assignmentsFor(consumerId: WarmupConsumerId): List<WarmupPartitionAssignment> =
        partitions.filter { it.consumerId == consumerId }
}
