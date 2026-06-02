package avh.ckc.demostubs

data class DemoStubsConfig(
    val port: Int,
    val workers: Int,
    val redisHost: String,
    val redisPort: Int
) {
    init {
        require(port > 0) { "port must be positive" }
        require(workers > 0) { "workers must be positive" }
        require(redisHost.isNotBlank()) { "redisHost must not be blank" }
        require(redisPort > 0) { "redisPort must be positive" }
    }

    companion object {
        fun fromEnvironment(environment: Map<String, String> = System.getenv()): DemoStubsConfig =
            DemoStubsConfig(
                port = environment["PORT"]?.toIntOrNull() ?: 8080,
                workers = environment["STUB_WORKERS"]?.toIntOrNull() ?: 4,
                redisHost = environment["REDIS_HOST"] ?: "localhost",
                redisPort = environment["REDIS_PORT"]?.toIntOrNull() ?: 6379
            )
    }
}

@kotlinx.serialization.Serializable
data class DemoStubsSettings(
    val eta: ModelLatencySettings,
    val flavour: ModelLatencySettings,
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
                errorRatePercent = 0
            )
    }
}

@kotlinx.serialization.Serializable
data class ModelLatencySettings(
    val delayP90Ms: Long,
    val delayP95Ms: Long,
    val delayP99Ms: Long,
    val delayP100Ms: Long
) {
    init {
        require(delayP90Ms >= 0) { "delayP90Ms must be >= 0" }
        require(delayP95Ms >= delayP90Ms) { "delayP95Ms must be >= delayP90Ms" }
        require(delayP99Ms >= delayP95Ms) { "delayP99Ms must be >= delayP95Ms" }
        require(delayP100Ms >= delayP99Ms) { "delayP100Ms must be >= delayP99Ms" }
    }

    companion object {
        fun baseline(): ModelLatencySettings =
            ModelLatencySettings(
                delayP90Ms = 40,
                delayP95Ms = 80,
                delayP99Ms = 160,
                delayP100Ms = 300
            )
    }
}
