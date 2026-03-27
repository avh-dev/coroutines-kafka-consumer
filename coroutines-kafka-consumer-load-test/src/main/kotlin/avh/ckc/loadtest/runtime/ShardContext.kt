package avh.ckc.loadtest.runtime

import java.time.Instant

data class ShardContext(
    val shardIndex: Int,
    val totalShards: Int,
    val testRunId: String?,
    val testRunStartedAt: Instant?
) {
    companion object {
        fun fromEnvironment(environment: Map<String, String> = System.getenv()): ShardContext =
            ShardContext(
                shardIndex = environment["JOB_COMPLETION_INDEX"]?.toIntOrNull() ?: 0,
                totalShards = environment["TOTAL_SHARDS"]?.toIntOrNull() ?: 1,
                testRunId = environment["TEST_RUN_ID"],
                testRunStartedAt = environment["TEST_RUN_STARTED_AT"]?.let(Instant::parse)
            )
    }

    fun shardToken(): String = "shard-${shardIndex.toString().padStart(3, '0')}"
}
