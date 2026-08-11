package avh.ckc.loadtest.kafka

import avh.ckc.loadtest.config.LoadTestConfig
import avh.ckc.loadtest.config.TopicProducerCapacity
import avh.ckc.loadtest.scenario.LoadScenario
import kotlin.test.Test
import kotlin.test.assertEquals

class ProducerPoolSizingTest {
    @Test
    fun `sizes each topic pool from its peak process throughput`() {
        val config = testConfig(
            baseTps = 10_000,
            orderEventPercent = 35,
            batchEventPercent = 25,
            cauldronTelemetryPercent = 40,
            producerCapacity = TopicProducerCapacity(orderTps = 1_000, batchTps = 800, cauldronTelemetryTps = 1_500)
        )

        val sizes = ProducerPoolSizes.from(
            config,
            LoadScenario.parse("0 -> (1m, warmup) -> 120 -> (2m, peak) -> 80")
        )

        assertEquals(5, sizes.order)
        assertEquals(4, sizes.batch)
        assertEquals(4, sizes.cauldronTelemetry)
    }

    @Test
    fun `keeps one producer for a topic with no scheduled traffic`() {
        assertEquals(1, producerCount(baseTps = 10_000, topicPercent = 0, peakRatePercent = 100, tpsPerProducer = 1_000))
    }

    private fun testConfig(
        baseTps: Int,
        orderEventPercent: Int,
        batchEventPercent: Int,
        cauldronTelemetryPercent: Int,
        producerCapacity: TopicProducerCapacity
    ): LoadTestConfig = LoadTestConfig.fromEnvironment(emptyMap()).copy(
        baseTps = baseTps,
        orderEventPercent = orderEventPercent,
        batchEventPercent = batchEventPercent,
        cauldronTelemetryPercent = cauldronTelemetryPercent,
        producerCapacity = producerCapacity
    )
}
