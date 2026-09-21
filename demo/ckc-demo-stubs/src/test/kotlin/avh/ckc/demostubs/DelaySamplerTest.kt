package avh.ckc.demostubs

import kotlin.test.Test
import kotlin.test.assertEquals

class DelaySamplerTest {
    @Test
    fun `sampler uses configured buckets`() {
        val settings = ModelLatencySettings(
            linkedMapOf("p50" to 10, "p75" to 50, "p999" to 150, "p100" to 300)
        )
        val quantiles = listOf(0.0, 0.499, 0.5, 0.749, 0.75, 0.9989, 0.999).iterator()
        val sampler = DelaySampler(quantiles::next)

        assertEquals(10, sampler.sampleDelayMillis(settings))
        assertEquals(10, sampler.sampleDelayMillis(settings))
        assertEquals(50, sampler.sampleDelayMillis(settings))
        assertEquals(50, sampler.sampleDelayMillis(settings))
        assertEquals(150, sampler.sampleDelayMillis(settings))
        assertEquals(150, sampler.sampleDelayMillis(settings))
        assertEquals(300, sampler.sampleDelayMillis(settings))
    }
}
