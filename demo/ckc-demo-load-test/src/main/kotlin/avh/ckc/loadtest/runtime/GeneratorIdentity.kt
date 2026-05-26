package avh.ckc.loadtest.runtime

import java.time.Instant

data class GeneratorIdentity(
    val externalShardIndex: Int,
    val totalExternalShards: Int,
    val workerIndex: Int,
    val totalWorkers: Int
) {
    init {
        require(externalShardIndex >= 0) { "externalShardIndex must be non-negative" }
        require(totalExternalShards > 0) { "totalExternalShards must be positive" }
        require(externalShardIndex < totalExternalShards) { "externalShardIndex must be less than totalExternalShards" }
        require(workerIndex >= 0) { "workerIndex must be non-negative" }
        require(totalWorkers > 0) { "totalWorkers must be positive" }
        require(workerIndex < totalWorkers) { "workerIndex must be less than totalWorkers" }
    }

    fun entityId(prefix: String, sequence: Long, width: Int): String =
        "$prefix-$externalShardIndex-$workerIndex-${sequence.toString().padStart(width, '0')}"

    fun regulatoryTraceId(now: Instant): String = "mrb-$externalShardIndex-$workerIndex-${now.epochSecond}"

    fun label(): String =
        "externalShard=$externalShardIndex/$totalExternalShards worker=$workerIndex/$totalWorkers"

    companion object {
        fun from(shardContext: ShardContext, workerIndex: Int, totalWorkers: Int): GeneratorIdentity =
            GeneratorIdentity(
                externalShardIndex = shardContext.shardIndex,
                totalExternalShards = shardContext.totalShards,
                workerIndex = workerIndex,
                totalWorkers = totalWorkers
            )
    }
}
