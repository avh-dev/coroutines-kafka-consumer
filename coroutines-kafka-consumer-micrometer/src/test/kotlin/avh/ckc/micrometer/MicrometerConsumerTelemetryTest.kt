package avh.ckc.micrometer

import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.concurrent.TimeUnit

class MicrometerConsumerTelemetryTest {

    @Test
    fun `when record is processed then counters timer and age summary are recorded`() {
        val registry = SimpleMeterRegistry()
        val telemetry = MicrometerConsumerTelemetry(
            meterRegistry = registry,
            commonTags = listOf(Tag.of("app", "test"))
        )

        telemetry.onRecordProcessed(
            topic = "orders",
            partition = 2,
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
                .tag("app", "test")
                .summary()
                .totalAmount()
        )
    }

    @Test
    fun `when record fails then failure metrics include error tag`() {
        val registry = SimpleMeterRegistry()
        val telemetry = MicrometerConsumerTelemetry(registry)

        telemetry.onRecordFailed(
            topic = "orders",
            partition = 1,
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
        val telemetry = MicrometerConsumerTelemetry(registry)

        telemetry.onRetry("orders", 0, 1, IOException("transient"))
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
        val telemetry = MicrometerConsumerTelemetry(registry)

        telemetry.onPoll(recordsCount = 1, durationNanos = TimeUnit.MILLISECONDS.toNanos(1))
        telemetry.onCommit(partitionsCount = 1, durationNanos = TimeUnit.MILLISECONDS.toNanos(2), success = true)

        assertNotNull(registry.find("ckc.poll.duration").timer())
        assertNotNull(registry.find("ckc.commit.duration").timer())
    }
}
