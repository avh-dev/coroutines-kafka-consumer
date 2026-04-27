package avh.ckc.micrometer

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

class MicrometerConsumerTelemetryTest {

    private data class TestLifecycleEvent(
        val eventType: String
    )

    @Test
    fun `when record is processed then counters timer and age summary are recorded`() {
        val registry = SimpleMeterRegistry()
        val telemetry = MicrometerConsumerTelemetry(
            meterRegistry = registry,
            commonTags = listOf(Tag.of("app", "test"))
        ).forConsumer<String, TestLifecycleEvent>()

        telemetry.onRecordProcessed(
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
                .tag("partition", "2")
                .tag("app", "test")
                .counter()
                .count()
        )
        assertEquals(
            1L,
            registry.get("ckc.record.process.duration")
                .tag("topic", "orders")
                .tag("partition", "2")
                .tag("app", "test")
                .timer()
                .count()
        )
        assertEquals(
            123.0,
            registry.get("ckc.record.age")
                .tag("topic", "orders")
                .tag("partition", "2")
                .tag("error", "none")
                .tag("app", "test")
                .summary()
                .totalAmount()
        )
    }

    @Test
    fun `when record fails then failure metrics include error tag`() {
        val registry = SimpleMeterRegistry()
        val telemetry = MicrometerConsumerTelemetry(registry).forConsumer<String, TestLifecycleEvent>()

        telemetry.onRecordFailed(
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
                .tag("partition", "1")
                .tag("error", "IOException")
                .counter()
                .count()
        )
        assertEquals(
            77.0,
            registry.get("ckc.record.age")
                .tag("topic", "orders")
                .tag("partition", "1")
                .tag("error", "IOException")
                .summary()
                .totalAmount()
        )
    }

    @Test
    fun `when retry commit poll and consumer failure happen then corresponding meters are recorded`() {
        val registry = SimpleMeterRegistry()
        val telemetry = MicrometerConsumerTelemetry(registry).forConsumer<String, TestLifecycleEvent>()

        telemetry.onRetry("key", TestLifecycleEvent("ORDER_CREATED"), testRecord(partition = 0), 1, IOException("transient"))
        telemetry.onPoll(recordsCount = 5, durationNanos = TimeUnit.MILLISECONDS.toNanos(15))
        telemetry.onCommit(partitionsCount = 2, durationNanos = TimeUnit.MILLISECONDS.toNanos(5), success = false)
        telemetry.onConsumerFailure(IllegalStateException("boom"))

        assertEquals(
            1.0,
            registry.get("ckc.record.retry")
                .tag("topic", "orders")
                .tag("partition", "0")
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
        val telemetry = MicrometerConsumerTelemetry(registry).forConsumer<String, TestLifecycleEvent>()

        telemetry.onPoll(recordsCount = 1, durationNanos = TimeUnit.MILLISECONDS.toNanos(1))
        telemetry.onCommit(partitionsCount = 1, durationNanos = TimeUnit.MILLISECONDS.toNanos(2), success = true)

        assertNotNull(registry.find("ckc.poll.duration").timer())
        assertNotNull(registry.find("ckc.commit.duration").timer())
    }

    @Test
    fun `when record tag value provider is configured then custom tags are attached to record metrics`() {
        val eventTypeTag = recordMetricTag("event_type")
        val registry = SimpleMeterRegistry()
        val telemetry = MicrometerConsumerTelemetry(
            meterRegistry = registry,
            recordTagSchema = recordMetricTagSchema(eventTypeTag)
        ).forConsumer(
            consumerRecordTagValueProvider<String, TestLifecycleEvent> { _, event, _ ->
                set(eventTypeTag, event?.eventType)
            }
        )

        telemetry.onRecordProcessed(
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
                .tag("partition", "3")
                .tag("event_type", "BREWING_STARTED")
                .counter()
                .count()
        )
    }

    @Test
    fun `when tag value is omitted then schema missing value is used`() {
        val eventTypeTag = recordMetricTag("event_type")
        val registry = SimpleMeterRegistry()
        val telemetry = MicrometerConsumerTelemetry(
            meterRegistry = registry,
            recordTagSchema = recordMetricTagSchema(eventTypeTag)
        ).forConsumer<String, TestLifecycleEvent>()

        telemetry.onRecordProcessed(
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
                .tag("partition", "4")
                .tag("event_type", "NONE")
                .counter()
                .count()
        )
    }

    @Test
    fun `when provider uses undeclared tag then telemetry rejects it`() {
        val declaredTag = recordMetricTag("event_type")
        val undeclaredTag = recordMetricTag("tenant")
        val telemetry = MicrometerConsumerTelemetry(
            meterRegistry = SimpleMeterRegistry(),
            recordTagSchema = recordMetricTagSchema(declaredTag)
        ).forConsumer(
            consumerRecordTagValueProvider<String, TestLifecycleEvent> { _, _, _ ->
                set(undeclaredTag, "acme")
            }
        )

        assertThrows(IllegalArgumentException::class.java) {
            telemetry.onRecordProcessed(
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
        val sharedTelemetry = MicrometerConsumerTelemetry(
            meterRegistry = registry,
            recordTagSchema = recordMetricTagSchema(eventTypeTag)
        )
        val telemetryTopicMetrics = sharedTelemetry.forConsumer<String, TestLifecycleEvent>(
            consumerRecordTagValueProvider<String, TestLifecycleEvent> { _, _, _ ->
                set(eventTypeTag, "CAULDRON_TELEMETRY")
            }
        )
        val lifecycleTopicMetrics = sharedTelemetry.forConsumer<String, TestLifecycleEvent>(
            consumerRecordTagValueProvider<String, TestLifecycleEvent> { _, event, _ ->
                set(eventTypeTag, event?.eventType ?: "UNKNOWN")
            }
        )

        telemetryTopicMetrics.onRecordProcessed(
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

        assertTrue(scrape.contains("ckc_record_processed_total{event_type=\"CAULDRON_TELEMETRY\",partition=\"1\",topic=\"cauldrons\"} 1.0"))
        assertTrue(scrape.contains("ckc_record_processed_total{event_type=\"BREWING_STARTED\",partition=\"2\",topic=\"orders\"} 1.0"))
    }

    @Test
    fun `when record age metrics are emitted for success and failure then prometheus exposes both series`() {
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        val telemetry = MicrometerConsumerTelemetry(registry).forConsumer<String, TestLifecycleEvent>()

        telemetry.onRecordProcessed(
            key = "key",
            value = TestLifecycleEvent("ORDER_CREATED"),
            record = testRecord(partition = 1),
            recordAgeMillis = 5,
            durationNanos = TimeUnit.MILLISECONDS.toNanos(1)
        )
        telemetry.onRecordFailed(
            key = "key",
            value = TestLifecycleEvent("ORDER_CREATED"),
            record = testRecord(partition = 1),
            recordAgeMillis = 7,
            error = IOException("broken"),
            durationNanos = TimeUnit.MILLISECONDS.toNanos(1)
        )

        val scrape = registry.scrape()

        assertTrue(scrape.contains("ckc_record_age_sum{error=\"none\",partition=\"1\",topic=\"orders\"} 5.0"))
        assertTrue(scrape.contains("ckc_record_age_sum{error=\"IOException\",partition=\"1\",topic=\"orders\"} 7.0"))
    }

    private fun testRecord(topic: String = "orders", partition: Int): ConsumerRecord<ByteArray, ByteArray> =
        ConsumerRecord(topic, partition, 0L, "key".toByteArray(), "value".toByteArray())
}
