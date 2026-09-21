package avh.ckc.demostubs

data class DemoStubsConfig(
    val port: Int,
    val workers: Int,
    val requestTimeoutMillis: Long,
    val redisHost: String,
    val redisPort: Int
) {
    init {
        require(port > 0) { "port must be positive" }
        require(workers > 0) { "workers must be positive" }
        require(requestTimeoutMillis >= 0) { "requestTimeoutMillis must be non-negative" }
        require(redisHost.isNotBlank()) { "redisHost must not be blank" }
        require(redisPort > 0) { "redisPort must be positive" }
    }

    companion object {
        fun fromEnvironment(environment: Map<String, String> = System.getenv()): DemoStubsConfig =
            DemoStubsConfig(
                port = environment["PORT"]?.toIntOrNull() ?: 8080,
                workers = environment["STUB_WORKERS"]?.toIntOrNull() ?: 4,
                requestTimeoutMillis = environment["STUB_REQUEST_TIMEOUT_MS"]?.toLongOrNull() ?: 0,
                redisHost = environment["REDIS_HOST"] ?: "localhost",
                redisPort = environment["REDIS_PORT"]?.toIntOrNull() ?: 6379
            )
    }
}

@kotlinx.serialization.Serializable
data class DemoStubsSettings(
    val eta: ModelLatencySettings,
    val flavour: ModelLatencySettings,
    val registry: ModelLatencySettings = ModelLatencySettings.registryBaseline(),
    val errorRatePercent: Int
) {
    init {
        require(errorRatePercent in 0..100) { "errorRatePercent must be in 0..100" }
    }

    companion object {
        fun baseline(): DemoStubsSettings =
            DemoStubsSettings(
                eta = ModelLatencySettings.baseline(),
                flavour = ModelLatencySettings.baseline(),
                registry = ModelLatencySettings.registryBaseline(),
                errorRatePercent = 0
            )
    }
}

@kotlinx.serialization.Serializable
data class ModelLatencySettings(
    val percentiles: Map<String, Long>
) {
    @kotlinx.serialization.Transient
    internal val buckets: List<LatencyBucket> = percentiles.map { (name, delayMillis) ->
        require(delayMillis >= 0) { "$name delay must be >= 0" }
        LatencyBucket(name, percentileQuantile(name), delayMillis)
    }.sortedBy(LatencyBucket::quantile)

    init {
        require(percentiles.isNotEmpty()) { "percentiles must not be empty" }
        require(buckets.last().quantile == 1.0) { "percentiles must end with p100" }
        require(buckets.map { it.quantile }.distinct().size == buckets.size) {
            "percentile keys must identify distinct quantiles"
        }
        buckets.zipWithNext().forEach { (previous, current) ->
            require(current.delayMillis >= previous.delayMillis) {
                "${current.name} delay must be >= ${previous.name} delay"
            }
        }
    }

    companion object {
        fun baseline(): ModelLatencySettings =
            ModelLatencySettings(
                linkedMapOf("p90" to 40, "p95" to 80, "p99" to 160, "p100" to 300)
            )

        fun registryBaseline(): ModelLatencySettings =
            ModelLatencySettings(
                linkedMapOf("p90" to 2, "p95" to 3, "p99" to 4, "p100" to 5)
            )
    }
}

internal data class LatencyBucket(
    val name: String,
    val quantile: Double,
    val delayMillis: Long
)

internal fun percentileQuantile(name: String): Double {
    require(name.matches(Regex("p[1-9][0-9]*"))) {
        "percentile key must match p<digits>: $name"
    }
    if (name == "p100") return 1.0
    val digits = name.substring(1)
    val quantile = "0.$digits".toDouble()
    require(quantile > 0.0 && quantile < 1.0) { "percentile must be between 0 and p100: $name" }
    return quantile
}
