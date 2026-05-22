package avh.ckc.core.polling.partition

import avh.ckc.core.VisibleForTesting
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import kotlin.arrayOfNulls
import kotlin.math.max

/**
 * Lock-free registry for mapping Kafka (topic, partition) to a [PartitionState].
 *
 * ## Purpose
 * The registry is optimized for the common Kafka consumer workload:
 *  - extremely frequent lookups per record (hot path)
 *  - very rare structural updates (partition rebalance callbacks)
 *
 * It stores per-topic partition states in arrays for O(1) access by partition index:
 *
 * `topic -> Array<PartitionState?>`
 *
 * ## Concurrency model
 * - Lookups are **lock-free** and read from a single volatile snapshot.
 * - Updates (assignment / revocation) are performed under a monitor lock and published by
 *   **replacing the whole snapshot**.
 * - Arrays in the published snapshot are treated as immutable: updates use copy-on-write
 *   to avoid mutating data observed by readers.
 *
 * This provides wait-free reads with predictable latency, which is important for high-throughput
 * pipelines.
 *
 * ## Notes
 * - The registry does not require knowledge of topic partition counts ahead of time.
 *   Capacity is derived from assignments and grown as needed.
 * - If a topic has a very sparse or extremely large partition id range, an array-based layout
 *   may allocate extra space; this is typically acceptable for Kafka partitioning patterns.
 */
internal class PartitionRegistry {

    /**
     * Volatile snapshot: topic -> partition state array.
     *
     * Readers access this snapshot without synchronization.
     * Writers replace the reference under synchronization (copy-on-write).
     */
    @Volatile
    private var topicPartitionStateMap: Map<String, Array<PartitionState?>> = emptyMap()

    /**
     * Applies a new partition assignment.
     *
     * This method is called from the consumer rebalance callback (onPartitionsAssigned)
     *
     * Behavior:
     * - Ensures per-topic arrays are present and have enough capacity.
     * - Creates a new [PartitionState] for partitions that do not have one yet.
     * - Publishes a new snapshot atomically.
     *
     * Thread-safety:
     * - Synchronized to serialize snapshot publication.
     * - Does not block readers.
     *
     * @return partition states corresponding to the assigned partitions (existing or newly created).
     */
    fun onPartitionsAssigned(partitions: Collection<TopicPartition>): List<PartitionState> {
        if (partitions.isEmpty()) return emptyList()

        val assignedPartitionStates = mutableListOf<PartitionState>()
        val maxPartitionsByTopic = calcMaxPartitionByTopic(partitions)

        synchronized(this) {
            val newTopicPartitionStateMap = copyAndEnsureCapacity(maxPartitionsByTopic)

            for (tp in partitions) {
                val partitionStates = newTopicPartitionStateMap[tp.topic()]!!
                val partition = tp.partition()
                var partitionState = partitionStates[partition]
                if (partitionState == null) {
                   partitionState = PartitionState(tp)
                   partitionStates[partition] = partitionState
                }
                assignedPartitionStates += partitionState
            }
            topicPartitionStateMap = newTopicPartitionStateMap
        }
        return assignedPartitionStates
    }

    /**
     * Hot-path lookup by [ConsumerRecord].
     *
     * Lock-free: reads a single volatile snapshot and performs O(1) access.
     */
    fun partitionStateFor(record: ConsumerRecord<*, *>): PartitionState? =
        partitionStateFor(record.topic(), record.partition());

    /**
     * Hot-path lookup by [TopicPartition].
     *
     * Lock-free: reads a single volatile snapshot and performs O(1) access.
     */
    fun partitionStateFor(topicPartition: TopicPartition): PartitionState? =
        partitionStateFor(topicPartition.topic(), topicPartition.partition());

    /**
     * Builds a new snapshot based on the current one and ensures capacity for the given topics.
     *
     * Copy-on-write rules:
     * - If a topic is new -> allocate a new array.
     * - If a topic exists -> copy the existing array (and resize if needed).
     *
     * This guarantees that arrays visible to readers are never mutated.
     */
    private fun copyAndEnsureCapacity(
        maxPartitionByTopic: Map<String, Int>
    ) : Map<String, Array<PartitionState?>> {
        val newTopicPartitionStatesMap = topicPartitionStateMap.toMutableMap()
        for (e in maxPartitionByTopic) {
            val topic = e.key
            val reqSize = e.value + 1
            val partitionStates= newTopicPartitionStatesMap[topic]
            newTopicPartitionStatesMap[topic] = when {
                partitionStates == null -> arrayOfNulls<PartitionState?>(reqSize)
                else -> partitionStates.copyOf(max(reqSize, partitionStates.size))
            }
        }
        return newTopicPartitionStatesMap
    }

    /**
     * Computes maximum partition index per topic for the given assignment.
     *
     * Used to pre-size per-topic arrays before inserting partition states.
     */
    private fun calcMaxPartitionByTopic(partitions: Collection<TopicPartition>): Map<String, Int> {
        val map = mutableMapOf<String, Int>()
        for (tp in partitions) {
            val topic = tp.topic()
            val partition = tp.partition()
            val currentMaxPartition = map[topic]
            if (currentMaxPartition == null || currentMaxPartition < partition) {
                map[topic] = partition
            }
        }
        return map
    }

    /**
     * Internal lock-free lookup.
     *
     * @return the [PartitionState] for the given (topic, partition) or null if missing/out of range.
     */
    private fun partitionStateFor(topic: String, partition: Int): PartitionState? {
        val states = topicPartitionStateMap[topic] ?: return null
        if (states.size <= partition) {
            return null
        }
        return states[partition]
    }

    @VisibleForTesting
    internal fun snapshotForTest() = topicPartitionStateMap
}
