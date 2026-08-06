package avh.ckc.micrometer

import avh.ckc.core.metrics.BackpressureAction
import avh.ckc.core.metrics.ConsumerPartitionStats
import avh.ckc.core.metrics.ConsumerRuntimeStats
import avh.ckc.core.metrics.RecordDropReason
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.concurrent.TimeUnit

class MicrometerConsumerMetricsSchemaTest {

    private data class TestLifecycleEvent(
        val eventType: String
    )

    @Test
    fun `when record is processed then processing and end to end timers are recorded`() {
        val registry = SimpleMeterRegistry()
        val metrics = MicrometerConsumerMetricsSchema(
            meterRegistry = registry,
            metricPrefix = "test",
            staticTags = listOf(Tag.of("app", "test"))
        ).let { schema ->
            micrometerConsumerMetrics<String, TestLifecycleEvent>(schema) {
                consumerId = "orders"
            }
        }

        metrics.onRecordProcessed(
            key = "key",
            value = TestLifecycleEvent("ORDER_CREATED"),
            record = testRecord(partition = 2),
            endToEndLatencyMillis = 123,
            durationNanos = TimeUnit.MILLISECONDS.toNanos(25)
        )

        assertEquals(
            1L,
            registry.get("test.ckc.record.process.duration")
                .tag("topic", "orders")
                .tag("app", "test")
                .tag("consumer_id", "orders")
                .timer()
                .count()
        )
        assertEquals(
            123.0,
            registry.get("test.ckc.record.end.to.end.duration")
                .tag("topic", "orders")
                .tag("app", "test")
                .tag("consumer_id", "orders")
                .timer()
                .totalTime(TimeUnit.MILLISECONDS)
        )
        assertTrue(registry.find("ckc.record.processed").meters().isEmpty())
    }

    @Test
    fun `when record fails then failure metrics include error tag`() {
        val registry = SimpleMeterRegistry()
        val metrics = micrometerConsumerMetrics<String, TestLifecycleEvent>(
            MicrometerConsumerMetricsSchema(registry, metricPrefix = "test")
        )

        metrics.onRecordFailed(
            key = "key",
            value = TestLifecycleEvent("ORDER_CREATED"),
            record = testRecord(partition = 1),
            error = IOException("broken"),
            durationNanos = TimeUnit.MILLISECONDS.toNanos(10)
        )

        assertEquals(
            1L,
            registry.get("test.ckc.record.failed.duration")
                .tag("topic", "orders")
                .tag("error", "IOException")
                .timer()
                .count()
        )
        assertTrue(registry.find("test.ckc.record.end.to.end.duration").meters().isEmpty())
        assertTrue(registry.find("ckc.record.failed").meters().isEmpty())
    }

    @Test
    fun `when record is dropped then dropped counter is recorded`() {
        val registry = SimpleMeterRegistry()
        val metrics = MicrometerConsumerMetricsSchema(
            meterRegistry = registry,
            metricPrefix = "test",
            staticTags = listOf(Tag.of("app", "test"))
        ).let { schema ->
            micrometerConsumerMetrics<String, TestLifecycleEvent>(schema) {
                consumerId = "telemetry"
            }
        }

        metrics.onRecordDropped(
            testRecord(topic = "cauldrons", partition = 1),
            RecordDropReason.QUEUE_OVERFLOW
        )

        assertEquals(
            1.0,
            registry.get("test.ckc.record.dropped")
                .tag("topic", "cauldrons")
                .tag("reason", "queue_overflow")
                .tag("app", "test")
                .tag("consumer_id", "telemetry")
                .counter()
                .count()
        )
    }

    @Test
    fun `when record is dropped with reason then dropped counter includes reason tag`() {
        val registry = SimpleMeterRegistry()
        val metrics = micrometerConsumerMetrics<String, TestLifecycleEvent>(
            MicrometerConsumerMetricsSchema(registry, metricPrefix = "test")
        )

        metrics.onRecordDropped(
            testRecord(topic = "cauldrons", partition = 1),
            RecordDropReason.NEW_KEY_QUEUE_FULL
        )

        assertEquals(
            1.0,
            registry.get("test.ckc.record.dropped")
                .tag("topic", "cauldrons")
                .tag("reason", "new_key_queue_full")
                .counter()
                .count()
        )
    }

    @Test
    fun `when retry commit poll and consumer failure happen then corresponding meters are recorded`() {
        val registry = SimpleMeterRegistry()
        val metrics = micrometerConsumerMetrics<String, TestLifecycleEvent>(
            MicrometerConsumerMetricsSchema(registry, metricPrefix = "test")
        )

        metrics.onRetry("key", TestLifecycleEvent("ORDER_CREATED"), testRecord(partition = 0), 1, IOException("transient"))
        metrics.onPoll(recordsCount = 5, durationNanos = TimeUnit.MILLISECONDS.toNanos(15))
        metrics.onCommit(partitionsCount = 2, offsetsCount = 25, durationNanos = TimeUnit.MILLISECONDS.toNanos(5), success = false)
        metrics.onBackpressurePauseResume(BackpressureAction.PAUSE, partitionsCount = 3)
        metrics.onConsumerFailure(IllegalStateException("boom"))

        assertEquals(
            1.0,
            registry.get("test.ckc.record.retry")
                .tag("topic", "orders")
                .tag("attempt", "1")
                .tag("error", "IOException")
                .counter()
                .count()
        )
        assertEquals(
            5.0,
            registry.get("test.ckc.poll.records")
                .summary()
                .totalAmount()
        )
        assertEquals(
            1L,
            registry.get("test.ckc.commit.duration")
                .tag("success", "false")
                .timer()
                .count()
        )
        assertEquals(
            2.0,
            registry.get("test.ckc.commit.partitions")
                .tag("success", "false")
                .summary()
                .totalAmount()
        )
        assertEquals(
            25.0,
            registry.get("test.ckc.commit.offsets")
                .tag("success", "false")
                .summary()
                .totalAmount()
        )
        assertEquals(
            1.0,
            registry.get("test.ckc.pause.resume")
                .tag("action", "pause")
                .counter()
                .count()
        )
        assertEquals(
            3.0,
            registry.get("test.ckc.pause.resume.partitions")
                .tag("action", "pause")
                .summary()
                .totalAmount()
        )
        assertEquals(
            1.0,
            registry.get("test.ckc.failure")
                .tag("error", "IllegalStateException")
                .counter()
                .count()
        )
    }

    @Test
    fun `when metrics are recorded then timers are registered`() {
        val registry = SimpleMeterRegistry()
        val metrics = micrometerConsumerMetrics<String, TestLifecycleEvent>(
            MicrometerConsumerMetricsSchema(registry, metricPrefix = "test")
        )

        metrics.onPoll(recordsCount = 1, durationNanos = TimeUnit.MILLISECONDS.toNanos(1))
        metrics.onCommit(partitionsCount = 1, offsetsCount = 3, durationNanos = TimeUnit.MILLISECONDS.toNanos(2), success = true)

        assertNotNull(registry.find("test.ckc.poll.duration").timer())
        assertNotNull(registry.find("test.ckc.commit.duration").timer())
    }

    @Test
    fun `when metric prefix is configured then all meter names use it`() {
        val registry = SimpleMeterRegistry()
        val metrics = MicrometerConsumerMetricsSchema(
            meterRegistry = registry,
            metricPrefix = "myapp"
        ).let { schema ->
            micrometerConsumerMetrics<String, TestLifecycleEvent>(schema)
        }

        metrics.onRetry("key", TestLifecycleEvent("ORDER_CREATED"), testRecord(partition = 0), 1, IOException("transient"))
        metrics.onPoll(recordsCount = 1, durationNanos = TimeUnit.MILLISECONDS.toNanos(1))

        assertNotNull(registry.find("myapp.ckc.record.retry").counter())
        assertNotNull(registry.find("myapp.ckc.poll.duration").timer())
        assertTrue(registry.find("ckc.record.retry").meters().isEmpty())
    }

    @Test
    fun `when metric prefix includes ckc suffix then schema rejects it`() {
        assertThrows(IllegalArgumentException::class.java) {
            MicrometerConsumerMetricsSchema(
                meterRegistry = SimpleMeterRegistry(),
                metricPrefix = "myapp.ckc"
            )
        }
    }

    @Test
    fun `when consumer id is omitted then default consumer id tag is recorded`() {
        val registry = SimpleMeterRegistry()
        val metrics = micrometerConsumerMetrics<String, TestLifecycleEvent>(
            MicrometerConsumerMetricsSchema(registry, metricPrefix = "test")
        )

        metrics.onPoll(recordsCount = 1, durationNanos = TimeUnit.MILLISECONDS.toNanos(1))

        assertEquals(
            1L,
            registry.get("test.ckc.poll.duration")
                .tag("consumer_id", MicrometerConsumerMetricsSchema.DEFAULT_CONSUMER_ID)
                .timer()
                .count()
        )
    }

    @Test
    fun `when runtime metrics are bound then gauges expose current runtime values with consumer id`() {
        val registry = SimpleMeterRegistry()
        val stats = MutableRuntimeStats(
            workerCount = 4,
            activeWorkerCount = 2,
            workQueueSize = 7,
            workQueueCapacity = 128,
            maxObservedWorkQueueSize = 11,
            orderingQueueSize = 5,
            maxObservedOrderingQueueSize = 6
        )
        val metrics = micrometerConsumerMetrics<String, TestLifecycleEvent>(
            MicrometerConsumerMetricsSchema(registry, metricPrefix = "test")
        ) {
            consumerId = "telemetry"
        }

        metrics.bindRuntimeMetrics(stats)

        assertEquals(4.0, registry.get("test.ckc.workers").tag("consumer_id", "telemetry").gauge().value())
        assertEquals(2.0, registry.get("test.ckc.workers.active").tag("consumer_id", "telemetry").gauge().value())
        assertEquals(7.0, registry.get("test.ckc.work.queue.size").tag("consumer_id", "telemetry").gauge().value())
        assertEquals(128.0, registry.get("test.ckc.work.queue.capacity").tag("consumer_id", "telemetry").gauge().value())
        assertEquals(11.0, registry.get("test.ckc.work.queue.max").tag("consumer_id", "telemetry").gauge().value())
        assertEquals(5.0, registry.get("test.ckc.ordering.queue.size").tag("consumer_id", "telemetry").gauge().value())
        assertEquals(6.0, registry.get("test.ckc.ordering.queue.max").tag("consumer_id", "telemetry").gauge().value())

        stats.activeWorkerCount = 3
        stats.workQueueSize = 9
        stats.maxObservedWorkQueueSize = 13
        stats.orderingQueueSize = 1
        stats.maxObservedOrderingQueueSize = 8

        assertEquals(3.0, registry.get("test.ckc.workers.active").tag("consumer_id", "telemetry").gauge().value())
        assertEquals(9.0, registry.get("test.ckc.work.queue.size").tag("consumer_id", "telemetry").gauge().value())
        assertEquals(13.0, registry.get("test.ckc.work.queue.max").tag("consumer_id", "telemetry").gauge().value())
        assertEquals(1.0, registry.get("test.ckc.ordering.queue.size").tag("consumer_id", "telemetry").gauge().value())
        assertEquals(8.0, registry.get("test.ckc.ordering.queue.max").tag("consumer_id", "telemetry").gauge().value())

        metrics.unbindRuntimeMetrics()

        assertTrue(registry.find("test.ckc.workers").tag("consumer_id", "telemetry").meters().isEmpty())
    }

    @Test
    fun `when partition metrics are bound then offset tracker capacity gauge exposes current value`() {
        val registry = SimpleMeterRegistry()
        val stats = MutablePartitionStats(
            topic = "orders",
            partition = 2,
            offsetTrackerBitCapacity = 128
        )
        val metrics = micrometerConsumerMetrics<String, TestLifecycleEvent>(
            MicrometerConsumerMetricsSchema(registry, metricPrefix = "test")
        ) {
            consumerId = "telemetry"
        }

        metrics.bindPartitionMetrics(stats)

        assertEquals(
            128.0,
            registry.get("test.ckc.offsettracker.capacity")
                .tag("consumer_id", "telemetry")
                .tag("topic", "orders")
                .tag("partition", "2")
                .gauge()
                .value()
        )

        stats.offsetTrackerBitCapacity = 512

        assertEquals(
            512.0,
            registry.get("test.ckc.offsettracker.capacity")
                .tag("consumer_id", "telemetry")
                .tag("topic", "orders")
                .tag("partition", "2")
                .gauge()
                .value()
        )

        metrics.unbindPartitionMetrics("orders", 2)

        assertTrue(
            registry.find("test.ckc.offsettracker.capacity")
                .tag("consumer_id", "telemetry")
                .tag("topic", "orders")
                .tag("partition", "2")
                .meters()
                .isEmpty()
        )
    }

    @Test
    fun `when record tag value provider is configured then custom tags are attached to record metrics`() {
        val eventTypeTag = "event_type"
        val registry = SimpleMeterRegistry()
        val metrics = MicrometerConsumerMetricsSchema(
            meterRegistry = registry,
            metricPrefix = "test",
            recordDrivenTags = recordDrivenTags(eventTypeTag)
        ).let { schema ->
            micrometerConsumerMetrics<String, TestLifecycleEvent>(schema) {
                recordDrivenTagExtractors = recordDrivenTagExtractors {
                    tag(eventTypeTag) { record -> record.value()?.eventType }
                }
            }
        }

        metrics.onRecordProcessed(
            key = "key",
            value = TestLifecycleEvent("BREWING_STARTED"),
            record = testRecord(partition = 3, key = "key", value = TestLifecycleEvent("BREWING_STARTED")),
            endToEndLatencyMillis = 42,
            durationNanos = TimeUnit.MILLISECONDS.toNanos(7)
        )

        assertEquals(
            1L,
            registry.get("test.ckc.record.process.duration")
                .tag("topic", "orders")
                .tag("event_type", "BREWING_STARTED")
                .timer()
                .count()
        )
    }

    @Test
    fun `when tag value is omitted then schema missing value is used`() {
        val eventTypeTag = "event_type"
        val registry = SimpleMeterRegistry()
        val metrics = MicrometerConsumerMetricsSchema(
            meterRegistry = registry,
            metricPrefix = "test",
            recordDrivenTags = recordDrivenTags {
                tag(eventTypeTag, defaultValue = "UNKNOWN")
            }
        ).let { schema ->
            micrometerConsumerMetrics<String, TestLifecycleEvent>(schema)
        }

        metrics.onRecordProcessed(
            key = "key",
            value = TestLifecycleEvent("IGNORED"),
            record = testRecord(partition = 4, key = "key", value = TestLifecycleEvent("IGNORED")),
            endToEndLatencyMillis = 11,
            durationNanos = TimeUnit.MILLISECONDS.toNanos(2)
        )

        assertEquals(
            1L,
            registry.get("test.ckc.record.process.duration")
                .tag("topic", "orders")
                .tag("event_type", "UNKNOWN")
                .timer()
                .count()
        )
    }

    @Test
    fun `when provider uses undeclared tag then metrics ignore it`() {
        val declaredTag = "event_type"
        val undeclaredTag = "tenant"
        val registry = SimpleMeterRegistry()
        val metrics = MicrometerConsumerMetricsSchema(
            meterRegistry = registry,
            metricPrefix = "test",
            recordDrivenTags = recordDrivenTags(declaredTag)
        ).let { schema ->
            micrometerConsumerMetrics<String, TestLifecycleEvent>(schema) {
                recordDrivenTagExtractors = recordDrivenTagExtractors {
                    tag(undeclaredTag) { "acme" }
                }
            }
        }

        metrics.onRecordProcessed(
            key = "key",
            value = TestLifecycleEvent("ORDER_CREATED"),
            record = testRecord(partition = 1, key = "key", value = TestLifecycleEvent("ORDER_CREATED")),
            endToEndLatencyMillis = 5,
            durationNanos = TimeUnit.MILLISECONDS.toNanos(1)
        )

        assertEquals(
            1L,
            registry.get("test.ckc.record.process.duration")
                .tag("topic", "orders")
                .tag("event_type", "NONE")
                .timer()
                .count()
        )
    }

    @Test
    fun `when custom record tag uses consumer id then schema rejects it`() {
        assertThrows(IllegalArgumentException::class.java) {
            recordDrivenTags("consumer_id")
        }
    }

    @Test
    fun `when prometheus metrics use the same tag keys then all topic series are exposed`() {
        val eventTypeTag = "event_type"
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        val sharedSchema = MicrometerConsumerMetricsSchema(
            meterRegistry = registry,
            metricPrefix = "test",
            recordDrivenTags = recordDrivenTags(eventTypeTag)
        )
        val staticTopicMetrics = micrometerConsumerMetrics<String, TestLifecycleEvent>(sharedSchema) {
            recordDrivenTagExtractors = recordDrivenTagExtractors {
                tag(eventTypeTag) { "CAULDRON_TELEMETRY" }
            }
        }
        val lifecycleTopicMetrics = micrometerConsumerMetrics<String, TestLifecycleEvent>(sharedSchema) {
            recordDrivenTagExtractors = recordDrivenTagExtractors {
                tag(eventTypeTag) { record -> record.value()?.eventType ?: "UNKNOWN" }
            }
        }

        staticTopicMetrics.onRecordProcessed(
            key = "key",
            value = TestLifecycleEvent("IGNORED"),
            record = testRecord(topic = "cauldrons", partition = 1, key = "key", value = TestLifecycleEvent("IGNORED")),
            endToEndLatencyMillis = 10,
            durationNanos = TimeUnit.MILLISECONDS.toNanos(2)
        )
        lifecycleTopicMetrics.onRecordProcessed(
            key = "key",
            value = TestLifecycleEvent("BREWING_STARTED"),
            record = testRecord(topic = "orders", partition = 2, key = "key", value = TestLifecycleEvent("BREWING_STARTED")),
            endToEndLatencyMillis = 20,
            durationNanos = TimeUnit.MILLISECONDS.toNanos(3)
        )

        val scrape = registry.scrape()

        assertTrue(scrape.contains("test_ckc_record_process_duration_seconds_count{consumer_id=\"default\",event_type=\"CAULDRON_TELEMETRY\",topic=\"cauldrons\"} 1"))
        assertTrue(scrape.contains("test_ckc_record_process_duration_seconds_count{consumer_id=\"default\",event_type=\"BREWING_STARTED\",topic=\"orders\"} 1"))
    }

    @Test
    fun `when records succeed and fail then prometheus exposes end to end latency only for success`() {
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        val metrics = micrometerConsumerMetrics<String, TestLifecycleEvent>(
            MicrometerConsumerMetricsSchema(registry, metricPrefix = "test")
        )

        metrics.onRecordProcessed(
            key = "key",
            value = TestLifecycleEvent("ORDER_CREATED"),
            record = testRecord(partition = 1),
            endToEndLatencyMillis = 5,
            durationNanos = TimeUnit.MILLISECONDS.toNanos(1)
        )
        metrics.onRecordFailed(
            key = "key",
            value = TestLifecycleEvent("ORDER_CREATED"),
            record = testRecord(partition = 1),
            error = IOException("broken"),
            durationNanos = TimeUnit.MILLISECONDS.toNanos(1)
        )

        val scrape = registry.scrape()

        assertTrue(scrape.contains("test_ckc_record_end_to_end_duration_seconds_sum{consumer_id=\"default\",topic=\"orders\"} 0.005"))
        assertFalse(scrape.contains("test_ckc_record_age"))
    }

    private fun <K, V> testRecord(topic: String = "orders", partition: Int, key: K? = null, value: V? = null): ConsumerRecord<K, V> =
        ConsumerRecord(topic, partition, 0L, key, value)

    private class MutableRuntimeStats(
        override val workerCount: Int,
        override var activeWorkerCount: Int,
        override var workQueueSize: Int,
        override val workQueueCapacity: Int,
        override var maxObservedWorkQueueSize: Int,
        override var orderingQueueSize: Int,
        override var maxObservedOrderingQueueSize: Int
    ) : ConsumerRuntimeStats

    private class MutablePartitionStats(
        override val topic: String,
        override val partition: Int,
        override var offsetTrackerBitCapacity: Int
    ) : ConsumerPartitionStats
}
