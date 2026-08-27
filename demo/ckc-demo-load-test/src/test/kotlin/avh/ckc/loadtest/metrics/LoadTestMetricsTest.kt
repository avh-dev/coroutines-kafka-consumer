package avh.ckc.loadtest.metrics

import avh.ckc.loadtest.kafka.ProducerPoolSizes
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import kotlin.test.Test
import kotlin.test.assertContains

class LoadTestMetricsTest {
    @Test
    fun `exposes topic pool sizes and publisher counters`() {
        LoadTestMetrics(
            port = 0,
            shardIndex = 2,
            poolSizes = ProducerPoolSizes(order = 4, batch = 3, cauldronTelemetry = 2)
        ).use { metrics ->
            val stats = ProducerTopicStats().apply {
                sent.addAndGet(12)
                acked.addAndGet(11)
                failed.incrementAndGet()
            }
            metrics.registerTopicStats("order", stats)

            val scrape = metrics.registry.scrape()

            assertContains(scrape, "ckc_load_test_producer_pool_size{shard=\"2\",traffic_topic=\"order\"} 4.0")
            assertContains(scrape, "ckc_load_test_producer_records_sent_total{shard=\"2\",traffic_topic=\"order\"} 12.0")
            assertContains(scrape, "ckc_load_test_producer_records_acked_total{shard=\"2\",traffic_topic=\"order\"} 11.0")
            assertContains(scrape, "ckc_load_test_producer_records_failed_total{shard=\"2\",traffic_topic=\"order\"} 1.0")
        }
    }

    @Test
    fun `binds native Kafka producer batching metrics with pool dimensions`() {
        val producer = MockProducer<String, String>()
        val metricName = MetricName(
            "batch-size-avg",
            "producer-metrics",
            "Average batch size",
            mapOf("client-id" to "test-order")
        )
        producer.setMockMetrics(metricName, fixedMetric(metricName, 64_000.0))

        LoadTestMetrics(
            port = 0,
            shardIndex = 2,
            poolSizes = ProducerPoolSizes(order = 1, batch = 1, cauldronTelemetry = 1)
        ).use { metrics ->
            metrics.bindKafkaProducer("order", generation = 0, producerIndex = 0, producer = producer)

            val scrape = metrics.registry.scrape()

            assertContains(scrape, "kafka_producer_batch_size_avg{")
            assertContains(scrape, "client_id=\"test-order\"")
            assertContains(scrape, "producer_index=\"0\"")
            assertContains(scrape, "producer_generation=\"0\"")
            assertContains(scrape, "traffic_topic=\"order\"")
            assertContains(scrape, "} 64000.0")

            metrics.unbindKafkaProducers("order", 0)
            kotlin.test.assertFalse(metrics.registry.scrape().contains("kafka_producer_batch_size_avg{"))
        }
    }

    private fun fixedMetric(name: MetricName, value: Double): Metric = object : Metric {
        override fun metricName(): MetricName = name

        override fun metricValue(): Any = value
    }
}
