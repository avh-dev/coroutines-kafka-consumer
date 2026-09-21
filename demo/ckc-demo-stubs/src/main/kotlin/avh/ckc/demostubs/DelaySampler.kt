package avh.ckc.demostubs

import kotlin.random.Random

class DelaySampler(
    private val nextQuantile: () -> Double
) {
    constructor(random: Random) : this(random::nextDouble)

    fun sampleDelayMillis(settings: ModelLatencySettings): Long {
        val quantile = nextQuantile()
        require(quantile >= 0.0 && quantile < 1.0) { "random quantile must be in [0, 1)" }
        return settings.buckets.first { quantile < it.quantile }.delayMillis
    }
}
