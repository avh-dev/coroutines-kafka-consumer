package avh.ckc.demostubs

data class DemoStubsConfig(
    val port: Int,
    val workers: Int,
    val etaLatency: ModelLatencySettings,
    val flavourLatency: ModelLatencySettings,
    val errorRatePercent: Int
) {
    init {
        require(port > 0) { "port must be positive" }
        require(workers > 0) { "workers must be positive" }
        require(errorRatePercent in 0..100) { "errorRatePercent must be in 0..100" }
    }

    companion object {
        fun fromEnvironment(environment: Map<String, String> = System.getenv()): DemoStubsConfig =
            DemoStubsConfig(
                port = environment["PORT"]?.toIntOrNull() ?: 8080,
                workers = environment["STUB_WORKERS"]?.toIntOrNull() ?: 4,
                etaLatency = ModelLatencySettings.fromEnvironment(environment, "ETA", fallbackPrefix = ""),
                flavourLatency = ModelLatencySettings.fromEnvironment(environment, "FLAVOUR", fallbackPrefix = ""),
                errorRatePercent = environment["ERROR_RATE_PERCENT"]?.toIntOrNull() ?: 0
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
        fun fromEnvironment(
            environment: Map<String, String>,
            prefix: String,
            fallbackPrefix: String
        ): ModelLatencySettings =
            ModelLatencySettings(
                delayP90Ms = latency(environment, prefix, fallbackPrefix, "P90", 10L),
                delayP95Ms = latency(environment, prefix, fallbackPrefix, "P95", 50L),
                delayP99Ms = latency(environment, prefix, fallbackPrefix, "P99", 150L),
                delayP100Ms = latency(environment, prefix, fallbackPrefix, "P100", 300L)
            )

        private fun latency(
            environment: Map<String, String>,
            prefix: String,
            fallbackPrefix: String,
            percentile: String,
            default: Long
        ): Long =
            environment["${prefix}_DELAY_${percentile}_MS"]?.toLongOrNull()
                ?: environment["${fallbackPrefix}DELAY_${percentile}_MS"]?.toLongOrNull()
                ?: default
    }
}
