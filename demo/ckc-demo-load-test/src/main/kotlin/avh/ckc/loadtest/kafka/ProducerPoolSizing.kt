package avh.ckc.loadtest.kafka

import avh.ckc.loadtest.config.LoadTestConfig
import avh.ckc.loadtest.scenario.LoadScenario
import kotlin.math.ceil

data class ProducerPoolSizes(
    val order: Int,
    val batch: Int,
    val cauldronTelemetry: Int
) {
    init {
        require(order > 0) { "order producer pool size must be positive" }
        require(batch > 0) { "batch producer pool size must be positive" }
        require(cauldronTelemetry > 0) { "cauldron telemetry producer pool size must be positive" }
    }

    companion object {
        fun from(config: LoadTestConfig, scenario: LoadScenario): ProducerPoolSizes {
            val peakRatePercent = scenario.peakRatePercent()
            return ProducerPoolSizes(
                order = producerCount(config.baseTps, config.orderEventPercent, peakRatePercent, config.producerCapacity.orderTps),
                batch = producerCount(config.baseTps, config.batchEventPercent, peakRatePercent, config.producerCapacity.batchTps),
                cauldronTelemetry = producerCount(
                    config.baseTps,
                    config.cauldronTelemetryPercent,
                    peakRatePercent,
                    config.producerCapacity.cauldronTelemetryTps
                )
            )
        }
    }
}

internal fun producerCount(
    baseTps: Int,
    topicPercent: Int,
    peakRatePercent: Int,
    tpsPerProducer: Int
): Int {
    require(baseTps > 0) { "baseTps must be positive" }
    require(topicPercent >= 0) { "topicPercent must be non-negative" }
    require(peakRatePercent >= 0) { "peakRatePercent must be non-negative" }
    require(tpsPerProducer > 0) { "tpsPerProducer must be positive" }

    val peakTopicTps = baseTps.toDouble() * topicPercent / 100.0 * peakRatePercent / 100.0
    return ceil(peakTopicTps / tpsPerProducer).toInt().coerceAtLeast(1)
}
