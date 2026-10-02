package avh.ckc.loadtest.runtime

fun defaultGeneratorWorkers(): Int = Runtime.getRuntime().availableProcessors().coerceAtLeast(1)

fun effectiveGeneratorWorkers(baseTps: Int, configuredWorkers: Int): Int {
    require(baseTps > 0) { "baseTps must be positive" }
    require(configuredWorkers > 0) { "configuredWorkers must be positive" }
    return configuredWorkers.coerceAtMost(baseTps)
}

fun effectiveGeneratorDispatcherThreads(workerCount: Int, configuredThreads: Int): Int {
    require(workerCount > 0) { "workerCount must be positive" }
    require(configuredThreads > 0) { "configuredThreads must be positive" }
    return configuredThreads.coerceAtMost(workerCount)
}

fun workerBaseTps(baseTps: Int, workerIndex: Int, totalWorkers: Int): Int {
    return distributedBaseTps(baseTps, workerIndex, totalWorkers)
}

fun shardBaseTps(baseTps: Int, shardIndex: Int, totalShards: Int): Int {
    require(baseTps >= totalShards) { "baseTps must be at least totalShards" }
    return distributedBaseTps(baseTps, shardIndex, totalShards)
}

private fun distributedBaseTps(baseTps: Int, index: Int, count: Int): Int {
    require(baseTps > 0) { "baseTps must be positive" }
    require(index >= 0) { "index must be non-negative" }
    require(count > 0) { "count must be positive" }
    require(index < count) { "index must be less than count" }

    val floor = baseTps / count
    val remainder = baseTps % count
    return floor + if (index < remainder) 1 else 0
}
