package avh.ckc.demostubs

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.random.Random

class DelaySamplerTest {
    @Test
    fun `sampler uses configured buckets`() {
        val settings = ModelLatencySettings(
            delayP90Ms = 10,
            delayP95Ms = 50,
            delayP99Ms = 150,
            delayP100Ms = 300
        )
        val sampler = DelaySampler(SequenceRandom(0, 89, 90, 94, 95, 98, 99))

        assertEquals(10, sampler.sampleDelayMillis(settings))
        assertEquals(10, sampler.sampleDelayMillis(settings))
        assertEquals(50, sampler.sampleDelayMillis(settings))
        assertEquals(50, sampler.sampleDelayMillis(settings))
        assertEquals(150, sampler.sampleDelayMillis(settings))
        assertEquals(150, sampler.sampleDelayMillis(settings))
        assertEquals(300, sampler.sampleDelayMillis(settings))
    }
}

private class SequenceRandom(vararg values: Int) : Random() {
    private val iterator = values.iterator()

    override fun nextBits(bitCount: Int): Int = error("unused")

    override fun nextInt(until: Int): Int = iterator.next()
}
