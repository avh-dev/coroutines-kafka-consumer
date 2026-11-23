package avh.ckc.warmup.micrometer

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.warmup.WarmupMetrics

private val micrometerBindingAvailable: Boolean by lazy {
    try {
        Class.forName(
            "avh.ckc.micrometer.MicrometerConsumerMeterBinding",
            false,
            WarmupMetrics::class.java.classLoader
        )
        true
    } catch (_: ClassNotFoundException) {
        false
    }
}

internal fun <K, V> micrometerWarmupMetrics(metrics: ConsumerMetrics<K, V>): WarmupMetrics<K, V>? =
    if (micrometerBindingAvailable) MicrometerWarmupMetrics.bind(metrics) else null
