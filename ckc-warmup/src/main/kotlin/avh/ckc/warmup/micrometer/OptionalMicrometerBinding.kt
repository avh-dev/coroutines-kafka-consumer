package avh.ckc.warmup.micrometer

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.warmup.WarmupMetrics

private val micrometerAvailable: Boolean by lazy {
    try {
        Class.forName(
            "io.micrometer.core.instrument.MeterRegistry",
            false,
            WarmupMetrics::class.java.classLoader
        )
        true
    } catch (_: ClassNotFoundException) {
        false
    }
}

internal fun <K, V> micrometerWarmupMetrics(metrics: ConsumerMetrics<K, V>): WarmupMetrics<K, V>? =
    if (micrometerAvailable) MicrometerWarmupMetrics.bind(metrics) else null
