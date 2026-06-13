package avh.ckc.demo.consumer

import avh.ckc.core.ProcessingMode
import avh.ckc.demo.config.DemoApplicationProperties
import io.confluent.parallelconsumer.ParallelConsumerOptions
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ExternalConsumerProcessingModesTest {
    @Test
    fun `confluent processing order follows configured processing mode`() {
        assertEquals(
            ParallelConsumerOptions.ProcessingOrder.UNORDERED,
            ProcessingMode.AT_LEAST_ONCE_UNORDERED.toConfluentProcessingOrder()
        )
        assertEquals(
            ParallelConsumerOptions.ProcessingOrder.KEY,
            ProcessingMode.AT_LEAST_ONCE_ORDERED_BY_KEY.toConfluentProcessingOrder()
        )
        assertEquals(
            ParallelConsumerOptions.ProcessingOrder.PARTITION,
            ProcessingMode.AT_LEAST_ONCE_ORDERED_BY_PARTITION.toConfluentProcessingOrder()
        )
        assertEquals(
            ParallelConsumerOptions.ProcessingOrder.UNORDERED,
            ProcessingMode.FRESHNESS_FIRST.toConfluentProcessingOrder()
        )
        assertFailsWith<IllegalArgumentException> {
            ProcessingMode.FRESHNESS_FIRST_BY_KEY.toConfluentProcessingOrder()
        }
    }

    @Test
    fun `spring kafka accepts only freshness first and partition ordering`() {
        assertEquals(
            ProcessingMode.FRESHNESS_FIRST,
            ProcessingMode.FRESHNESS_FIRST.requireSupportedBySpringKafka()
        )
        assertEquals(
            ProcessingMode.AT_LEAST_ONCE_ORDERED_BY_PARTITION,
            ProcessingMode.AT_LEAST_ONCE_ORDERED_BY_PARTITION.requireSupportedBySpringKafka()
        )
        assertFailsWith<IllegalArgumentException> {
            ProcessingMode.AT_LEAST_ONCE_UNORDERED.requireSupportedBySpringKafka()
        }
        assertFailsWith<IllegalArgumentException> {
            ProcessingMode.AT_LEAST_ONCE_ORDERED_BY_KEY.requireSupportedBySpringKafka()
        }
        assertFailsWith<IllegalArgumentException> {
            ProcessingMode.FRESHNESS_FIRST_BY_KEY.requireSupportedBySpringKafka()
        }
    }

    @Test
    fun `freshness first filter discards only stale freshness first records`() {
        val properties = DemoApplicationProperties(
            consumers = DemoApplicationProperties.Consumers(freshnessFirstMaxRecordAgeSeconds = 10)
        )
        val filter = FreshnessFirstRecordFilter(properties)
        val freshnessFirst = DemoApplicationProperties.ConsumerRuntime(processingMode = ProcessingMode.FRESHNESS_FIRST)
        val freshnessFirstByKey = DemoApplicationProperties.ConsumerRuntime(
            processingMode = ProcessingMode.FRESHNESS_FIRST_BY_KEY
        )
        val ordered = DemoApplicationProperties.ConsumerRuntime(
            processingMode = ProcessingMode.AT_LEAST_ONCE_ORDERED_BY_PARTITION
        )

        assertFalse(filter.shouldDiscard(freshnessFirst, recordTimestamp = 90_000, nowMillis = 100_000))
        assertTrue(filter.shouldDiscard(freshnessFirst, recordTimestamp = 89_999, nowMillis = 100_000))
        assertTrue(filter.shouldDiscard(freshnessFirstByKey, recordTimestamp = 89_999, nowMillis = 100_000))
        assertFalse(filter.shouldDiscard(ordered, recordTimestamp = 1, nowMillis = 100_000))
        assertFalse(filter.shouldDiscard(freshnessFirst, recordTimestamp = 0, nowMillis = 100_000))
    }
}
