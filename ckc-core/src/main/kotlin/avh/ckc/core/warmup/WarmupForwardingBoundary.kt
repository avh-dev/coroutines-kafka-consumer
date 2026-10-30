package avh.ckc.core.warmup

import org.apache.kafka.common.TopicPartition

/** Inclusive acknowledged offsets. A null offset explicitly denotes a partition with no sends. */
class WarmupForwardingBoundary(offsets: Map<TopicPartition, Long?>) {
    val offsets: Map<TopicPartition, Long?> = immutableMapSnapshot(offsets)

    init {
        require(this.offsets.isNotEmpty()) { "A boundary must include every assigned partition" }
        require(this.offsets.all { (partition, offset) ->
            partition.partition() >= 0 && partition.topic().isNotBlank() && (offset == null || offset >= 0)
        }) { "Invalid warm-up partition or final offset" }
    }
}
