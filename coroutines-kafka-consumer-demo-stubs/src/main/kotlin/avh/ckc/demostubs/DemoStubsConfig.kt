package avh.ckc.demostubs

data class DemoStubsConfig(
    val port: Int,
    val workers: Int,
    val delayP90Ms: Long,
    val delayP95Ms: Long,
    val delayP99Ms: Long,
    val delayP100Ms: Long,
    val errorRatePercent: Int
) {
    init {
        require(port > 0) { "port must be positive" }
        require(workers > 0) { "workers must be positive" }
        require(delayP90Ms >= 0) { "delayP90Ms must be >= 0" }
        require(delayP95Ms >= delayP90Ms) { "delayP95Ms must be >= delayP90Ms" }
        require(delayP99Ms >= delayP95Ms) { "delayP99Ms must be >= delayP95Ms" }
        require(delayP100Ms >= delayP99Ms) { "delayP100Ms must be >= delayP99Ms" }
        require(errorRatePercent in 0..100) { "errorRatePercent must be in 0..100" }
    }

    companion object {
        fun fromEnvironment(environment: Map<String, String> = System.getenv()): DemoStubsConfig =
            DemoStubsConfig(
                port = environment["PORT"]?.toIntOrNull() ?: 8080,
                workers = environment["STUB_WORKERS"]?.toIntOrNull() ?: 64,
                delayP90Ms = environment["DELAY_P90_MS"]?.toLongOrNull() ?: 10L,
                delayP95Ms = environment["DELAY_P95_MS"]?.toLongOrNull() ?: 50L,
                delayP99Ms = environment["DELAY_P99_MS"]?.toLongOrNull() ?: 150L,
                delayP100Ms = environment["DELAY_P100_MS"]?.toLongOrNull() ?: 300L,
                errorRatePercent = environment["ERROR_RATE_PERCENT"]?.toIntOrNull() ?: 0
            )
    }
}
