package avh.ckc.loadtest.runtime

fun defaultGeneratorWorkers(): Int = Runtime.getRuntime().availableProcessors().coerceAtLeast(1)

fun effectiveGeneratorWorkers(baseTps: Int, configuredWorkers: Int): Int {
    require(baseTps > 0) { "baseTps must be positive" }
    require(configuredWorkers > 0) { "configuredWorkers must be positive" }
    return configuredWorkers.coerceAtMost(baseTps)
}

fun workerBaseTps(baseTps: Int, workerIndex: Int, totalWorkers: Int): Int {
    require(baseTps > 0) { "baseTps must be positive" }
    require(workerIndex >= 0) { "workerIndex must be non-negative" }
    require(totalWorkers > 0) { "totalWorkers must be positive" }
    require(workerIndex < totalWorkers) { "workerIndex must be less than totalWorkers" }

    val floor = baseTps / totalWorkers
    val remainder = baseTps % totalWorkers
    return floor + if (workerIndex < remainder) 1 else 0
}
