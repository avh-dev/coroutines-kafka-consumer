package avh.ckc.demostubs

import kotlin.random.Random

class DelaySampler(
    private val random: Random
) {
    fun sampleDelayMillis(settings: ModelLatencySettings): Long {
        val percentile = random.nextInt(100)
        return when {
            percentile < 90 -> settings.delayP90Ms
            percentile < 95 -> settings.delayP95Ms
            percentile < 99 -> settings.delayP99Ms
            else -> settings.delayP100Ms
        }
    }
}
