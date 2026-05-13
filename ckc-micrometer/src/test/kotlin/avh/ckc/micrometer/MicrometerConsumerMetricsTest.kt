package avh.ckc.micrometer

import avh.ckc.core.ConsumerPartitionStats
import avh.ckc.core.ConsumerRuntimeStats
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.concurrent.TimeUnit

class MicrometerConsumerMetricsTest {

    private data class TestLifecycleEvent(
        val eventType: String
    )

    @Test
    fun `when record is processed then counters timer and age summary are recorded`() {
        val registry = SimpleMeterRegistry()
        val metrics = MicrometerConsumerMetrics(
            meterRegistry = registry,
            commonTags = listOf(Tag.of("app", "test"))
        ).forConsumer<String, TestLifecycleEvent>(consumerId = "orders")

        metrics.onRecordProcessed(
            key = "key",
            value = TestLifecycleEvent("ORDER_CREATED"),
            record = testRecord(partition = 2),
            recordAgeMillis = 123,
            durationNanos = TimeUnit.MILLISECONDS.toNanos(25)
        )

        assertEquals(
            1.0,
            registry.get("ckc.record.processed")
                .tag("topic", "orders")
                .tag("app", "test")
                .tag("consumer_id", "orders")
                .counter()
                .count()
        )
        assertEquals(
            1L,
            registry.get("ckc.record.process.duration")
                .tag("topic", "orders")
                .tag("app", "test")
                .tag("consumer_id", "orders")
                .timer()
                .count()
        )
        assertEquals(
            123.0,
            registry.get("ckc.record.age")
                .tag("topic", "orders")
                .tag("error", "none")
                .tag("app", "test")
                .tag("consumer_id", "orders")
                .summary()
                .totalAmount()
        )
    }

    @Test
    fun `when record fails then failure metrics include error tag`() {
        val registry = SimpleMeterRegistry()
        val metrics = MicrometerConsumerMetrics(registry).forConsumer<String, TestLifecycleEvent>()

        metrics.onRecordFailed(
            key = "key",
            value = TestLifecycleEvent("ORDER_CREATED"),
            record = testRecord(partition = 1),
            recordAgeMillis = 77,
            error = IOException("broken"),
            durationNanos = TimeUnit.MILLISECONDS.toNanos(10)
        )

        assertEquals(
            1.0,
            registry.get("ckc.record.failed")
                .tag("topic", "orders")
                .tag("error", "IOException")
                .counter()
                .count()
        )
        assertEquals(
            77.0,
            registry.get("ckc.record.age")
                .tag("topic", "orders")
                .tag("error", "IOException")
                .summary()
                .totalAmount()
        )
    }

    @Test
    fun `when retry commit poll and consumer failure happen then corresponding meters are recorded`() {
        val registry = SimpleMeterRegistry()
        val metrics = MicrometerConsumerMetrics(registry).forConsumer<String, TestLifecycleEvent>()

        metrics.onRetry("key", TestLifecycleEvent("ORDER_CREATED"), testRecord(partition = 0), 1, IOException("transient"))
        metrics.onPoll(recordsCount = 5, durationNanos = TimeUnit.MILLISECONDS.toNanos(15))
        metrics.onCommit(partitionsCount = 2, offsetsCount = 25, durationNanos = TimeUnit.MILLISECONDS.toNanos(5), success = false)
        metrics.onConsumerFailure(IllegalStateException("boom"))

        assertEquals(
            1.0,
            registry.get("ckc.record.retry")
                .tag("topic", "orders")
                .tag("attempt", "1")
                .tag("error", "IOException")
                .counter()
                .count()
        )
        assertEquals(
            5.0,
            registry.get("ckc.poll.records")
                .summary()
                .totalAmount()
        )
        assertEquals(
            2.0,
            registry.get("ckc.commit.partitions")
                .tag("success", "false")
                .summary()
                .totalAmount()
        )
        assertEquals(
            25.0,
            registry.get("ckc.commit.offsets")
                .tag("success", "false")
                .summary()
                .totalAmount()
        )
        assertEquals(
            1.0,
            registry.get("ckc.failure")
                .tag("error", "IllegalStateException")
                .counter()
                .count()
        )
    }

    @Test
    fun `when metrics are recorded then timers are registered`() {
        val registry = SimpleMeterRegistry()
        val metrics = MicrometerConsumerMetrics(registry).forConsumer<String, TestLifecycleEvent>()

        metrics.onPoll(recordsCount = 1, durationNanos = TimeUnit.MILLISECONDS.toNanos(1))
        metrics.onCommit(partitionsCount = 1, offsetsCount = 3, durationNanos = TimeUnit.MILLISECONDS.toNanos(2), success = true)

        assertNotNull(registry.find("ckc.poll.duration").timer())
        assertNotNull(registry.find("ckc.commit.duration").timer())
    }

    @Test
    fun `when runtime metrics are bound then gauges expose current runtime values with consumer id`() {
        val registry = SimpleMeterRegistry()
        val stats = MutableRuntimeStats(
            workerCount = 4,
            activeWorkerCount = 2,
            workQueueSize = 7,
            workQueueCapacity = 128,
            maxObservedWorkQueueSize = 11
        )
        val metrics = MicrometerConsumerMetrics(registry).forConsumer<String, TestLifecycleEvent>(consumerId = "telemetry")

        metrics.bindRuntimeMetrics(stats)

        assertEquals(4.0, registry.get("ckc.workers").tag("consumer_id", "telemetry").gauge().value())
        assertEquals(2.0, registry.get("ckc.workers.active").tag("consumer_id", "telemetry").gauge().value())
        assertEquals(7.0, registry.get("ckc.work.queue.size").tag("consumer_id", "telemetry").gauge().value())
        assertEquals(128.0, registry.get("ckc.work.queue.capacity").tag("consumer_id", "telemetry").gauge().value())
        assertEquals(11.0, registry.get("ckc.work.queue.max").tag("consumer_id", "telemetry").gauge().value())

        stats.activeWorkerCount = 3
        stats.workQueueSize = 9
        stats.maxObservedWorkQueueSize = 13

        assertEquals(3.0, registry.get("ckc.workers.active").tag("consumer_id", "telemetry").gauge().value())
        assertEquals(9.0, registry.get("ckc.work.queue.size").tag("consumer_id", "telemetry").gauge().value())
        assertEquals(13.0, registry.get("ckc.work.queue.max").tag("consumer_id", "telemetry").gauge().value())

        metrics.unbindRuntimeMetrics()

        assertTrue(registry.find("ckc.workers").tag("consumer_id", "telemetry").meters().isEmpty())
    }

    @Test
    fun `when partition metrics are bound then offset tracker capacity gauge exposes current value`() {
        val registry = SimpleMeterRegistry()
        val stats = MutablePartitionStats(
            topic = "orders",
            partition = 2,
            offsetTrackerBitCapacity = 128
        )
        val metrics = MicrometerConsumerMetrics(registry).forConsumer<String, TestLifecycleEvent>(consumerId = "telemetry")

        metrics.bindPartitionMetrics(stats)

        assertEquals(
            128.0,
            registry.get("ckc.offsettracker.capacity")
                .tag("consumer_id", "telemetry")
                .tag("topic", "orders")
                .tag("partition", "2")
                .gauge()
                .value()
        )

        stats.offsetTrackerBitCapacity = 512

        assertEquals(
            512.0,
            registry.get("ckc.offsettracker.capacity")
                .tag("consumer_id", "telemetry")
                .tag("topic", "orders")
                .tag("partition", "2")
                .gauge()
                .value()
        )

        metrics.unbindPartitionMetrics("orders", 2)

        assertTrue(
            registry.find("ckc.offsettracker.capacity")
                .tag("consumer_id", "telemetry")
                .tag("topic", "orders")
                .tag("partition", "2")
                .meters()
                .isEmpty()
        )
    }

    @Test
    fun `when record tag value provider is configured then custom tags are attached to record metrics`() {
        val eventTypeTag = recordMetricTag("event_type")
        val registry = SimpleMeterRegistry()
        val metrics = MicrometerConsumerMetrics(
            meterRegistry = registry,
            recordTagSchema = recordMetricTagSchema(eventTypeTag)
        ).forConsumer(
            recordTagValueProvider = consumerRecordTagValueProvider<String, TestLifecycleEvent> { _, event, _ ->
                set(eventTypeTag, event?.eventType)
            }
        )

        metrics.onRecordProcessed(
            key = "key",
            value = TestLifecycleEvent("BREWING_STARTED"),
            record = testRecord(partition = 3),
            recordAgeMillis = 42,
            durationNanos = TimeUnit.MILLISECONDS.toNanos(7)
        )

        assertEquals(
            1.0,
            registry.get("ckc.record.processed")
                .tag("topic", "orders")
                .tag("event_type", "BREWING_STARTED")
                .counter()
                .count()
        )
    }

    @Test
    fun `when tag value is omitted then schema missing value is used`() {
        val eventTypeTag = recordMetricTag("event_type")
        val registry = SimpleMeterRegistry()
        val metrics = MicrometerConsumerMetrics(
            meterRegistry = registry,
            recordTagSchema = recordMetricTagSchema(eventTypeTag)
        ).forConsumer<String, TestLifecycleEvent>()

        metrics.onRecordProcessed(
            key = "key",
            value = TestLifecycleEvent("IGNORED"),
            record = testRecord(partition = 4),
            recordAgeMillis = 11,
            durationNanos = TimeUnit.MILLISECONDS.toNanos(2)
        )

        assertEquals(
            1.0,
            registry.get("ckc.record.processed")
                .tag("topic", "orders")
                .tag("event_type", "NONE")
                .counter()
                .count()
        )
    }

    @Test
    fun `when provider uses undeclared tag then metrics reject it`() {
        val declaredTag = recordMetricTag("event_type")
        val undeclaredTag = recordMetricTag("tenant")
        val metrics = MicrometerConsumerMetrics(
            meterRegistry = SimpleMeterRegistry(),
            recordTagSchema = recordMetricTagSchema(declaredTag)
        ).forConsumer(
            recordTagValueProvider = consumerRecordTagValueProvider<String, TestLifecycleEvent> { _, _, _ ->
                set(undeclaredTag, "acme")
            }
        )

        assertThrows(IllegalArgumentException::class.java) {
            metrics.onRecordProcessed(
                key = "key",
                value = TestLifecycleEvent("ORDER_CREATED"),
                record = testRecord(partition = 1),
                recordAgeMillis = 5,
                durationNanos = TimeUnit.MILLISECONDS.toNanos(1)
            )
        }
    }

    @Test
    fun `when prometheus metrics use the same tag keys then all topic series are exposed`() {
        val eventTypeTag = recordMetricTag("event_type")
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        val sharedMetrics = MicrometerConsumerMetrics(
            meterRegistry = registry,
            recordTagSchema = recordMetricTagSchema(eventTypeTag)
        )
        val staticTopicMetrics = sharedMetrics.forConsumer<String, TestLifecycleEvent>(
            recordTagValueProvider = consumerRecordTagValueProvider<String, TestLifecycleEvent> { _, _, _ ->
                set(eventTypeTag, "CAULDRON_TELEMETRY")
            }
        )
        val lifecycleTopicMetrics = sharedMetrics.forConsumer<String, TestLifecycleEvent>(
            recordTagValueProvider = consumerRecordTagValueProvider<String, TestLifecycleEvent> { _, event, _ ->
                set(eventTypeTag, event?.eventType ?: "UNKNOWN")
            }
        )

        staticTopicMetrics.onRecordProcessed(
            key = "key",
            value = TestLifecycleEvent("IGNORED"),
            record = testRecord(topic = "cauldrons", partition = 1),
            recordAgeMillis = 10,
            durationNanos = TimeUnit.MILLISECONDS.toNanos(2)
        )
        lifecycleTopicMetrics.onRecordProcessed(
            key = "key",
            value = TestLifecycleEvent("BREWING_STARTED"),
            record = testRecord(topic = "orders", partition = 2),
            recordAgeMillis = 20,
            durationNanos = TimeUnit.MILLISECONDS.toNanos(3)
        )

        val scrape = registry.scrape()

        assertTrue(scrape.contains("ckc_record_processed_total{event_type=\"CAULDRON_TELEMETRY\",topic=\"cauldrons\"} 1.0"))
        assertTrue(scrape.contains("ckc_record_processed_total{event_type=\"BREWING_STARTED\",topic=\"orders\"} 1.0"))
    }

    @Test
    fun `when record age metrics are emitted for success and failure then prometheus exposes both series`() {
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        val metrics = MicrometerConsumerMetrics(registry).forConsumer<String, TestLifecycleEvent>()

        metrics.onRecordProcessed(
            key = "key",
            value = TestLifecycleEvent("ORDER_CREATED"),
            record = testRecord(partition = 1),
            recordAgeMillis = 5,
            durationNanos = TimeUnit.MILLISECONDS.toNanos(1)
        )
        metrics.onRecordFailed(
            key = "key",
            value = TestLifecycleEvent("ORDER_CREATED"),
            record = testRecord(partition = 1),
            recordAgeMillis = 7,
            error = IOException("broken"),
            durationNanos = TimeUnit.MILLISECONDS.toNanos(1)
        )

        val scrape = registry.scrape()

        assertTrue(scrape.contains("ckc_record_age_sum{error=\"none\",topic=\"orders\"} 5.0"))
        assertTrue(scrape.contains("ckc_record_age_sum{error=\"IOException\",topic=\"orders\"} 7.0"))
    }

    private fun testRecord(topic: String = "orders", partition: Int): ConsumerRecord<ByteArray, ByteArray> =
        ConsumerRecord(topic, partition, 0L, "key".toByteArray(), "value".toByteArray())

    private class MutableRuntimeStats(
        override val workerCount: Int,
        override var activeWorkerCount: Int,
        override var workQueueSize: Int,
        override val workQueueCapacity: Int,
        override var maxObservedWorkQueueSize: Int
    ) : ConsumerRuntimeStats

    private class MutablePartitionStats(
        override val topic: String,
        override val partition: Int,
        override var offsetTrackerBitCapacity: Int
    ) : ConsumerPartitionStats
}
