package avh.ckc.loadtest.runtime

import java.time.Instant

data class ShardContext(
    val shardIndex: Int,
    val totalShards: Int,
    val testRunId: String?,
    val testRunStartedAt: Instant?
) {
    init {
        require(shardIndex >= 0) { "shardIndex must be non-negative" }
        require(totalShards > 0) { "totalShards must be positive" }
        require(shardIndex < totalShards) { "shardIndex must be less than totalShards" }
    }

    companion object {
        fun fromEnvironment(environment: Map<String, String> = System.getenv()): ShardContext =
            ShardContext(
                shardIndex = environment["JOB_COMPLETION_INDEX"]?.toIntOrNull() ?: 0,
                totalShards = environment["TOTAL_SHARDS"]?.toIntOrNull() ?: 1,
                testRunId = environment["TEST_RUN_ID"],
                testRunStartedAt = environment["TEST_RUN_STARTED_AT"]?.let(Instant::parse)
            )
    }
}
