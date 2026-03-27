package avh.ckc.demostubs

import kotlin.random.Random

class DelaySampler(
    private val config: DemoStubsConfig,
    private val random: Random
) {
    fun sampleDelayMillis(): Long {
        val percentile = random.nextInt(100)
        return when {
            percentile < 90 -> config.delayP90Ms
            percentile < 95 -> config.delayP95Ms
            percentile < 99 -> config.delayP99Ms
            else -> config.delayP100Ms
        }
    }
}
