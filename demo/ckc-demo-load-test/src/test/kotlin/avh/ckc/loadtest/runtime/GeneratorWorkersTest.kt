package avh.ckc.loadtest.runtime

import kotlin.test.Test
import kotlin.test.assertEquals

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
}
