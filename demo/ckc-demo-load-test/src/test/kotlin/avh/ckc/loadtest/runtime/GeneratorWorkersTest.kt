package avh.ckc.loadtest.runtime

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class GeneratorWorkersTest {
    @Test
    fun `splits process tps across workers and keeps the remainder stable`() {
        val workerRates = (0 until 4).map { workerBaseTps(baseTps = 10_003, workerIndex = it, totalWorkers = 4) }

        assertEquals(listOf(2501, 2501, 2501, 2500), workerRates)
        assertEquals(10_003, workerRates.sum())
    }

    @Test
    fun `does not start more active workers than integer process tps`() {
        assertEquals(3, effectiveGeneratorWorkers(baseTps = 3, configuredWorkers = 8))
    }

    @Test
    fun `keeps state shard count independent from dispatcher thread count`() {
        assertEquals(4, effectiveGeneratorDispatcherThreads(workerCount = 100, configuredThreads = 4))
        assertEquals(3, effectiveGeneratorDispatcherThreads(workerCount = 3, configuredThreads = 8))
    }

    @Test
    fun `distributes aggregate tps across physical shards`() {
        val shardRates = (0 until 5).map { shardBaseTps(baseTps = 50_003, shardIndex = it, totalShards = 5) }

        assertEquals(listOf(10_001, 10_001, 10_001, 10_000, 10_000), shardRates)
        assertEquals(50_003, shardRates.sum())
    }

    @Test
    fun `rejects more physical shards than aggregate tps`() {
        assertFailsWith<IllegalArgumentException> {
            shardBaseTps(baseTps = 2, shardIndex = 0, totalShards = 3)
        }
    }
}
