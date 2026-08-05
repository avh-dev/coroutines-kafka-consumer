package avh.ckc.core

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.polling.partition.offset.OffsetTracker
import avh.ckc.core.processing.RecordProcessor
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds

class RecordProcessorTest {

    @Test
    fun `when retry policy matches then worker retries and eventually succeeds`() = runBlocking {
        val attempts = AtomicInteger()
        val processed = CompletableDeferred<String>()
        val metrics = RecordingMetrics<String, String>()
        val processor = createRecordProcessor<String, String>(
            handler = KafkaRecordHandler { record ->
                if (attempts.getAndIncrement() < 2) {
                    throw IOException("transient")
                }
                processed.complete(record.value())
            },
            metrics = metrics,
            retryPolicy = retryPolicy {
                retry<IOException> {
                    maxRetries = 2
                    delay = 1.milliseconds
                }
            }
        )

        processor.process(typedTestRecord(offset = 10L, value = "payload"))

        assertEquals("payload", withTimeout(2_000) { processed.await() })
        assertEquals(3, attempts.get())
        assertEquals(listOf(1, 2), metrics.retries.map { it.attempt })
        assertEquals(1, metrics.processed.size)
        assertEquals("payload", metrics.processed.single().value)
        assertEquals("payload", metrics.retries.first().value)
        kotlin.test.assertTrue(metrics.processed.single().recordAgeMillis >= 0)
    }

    @Test
    fun `when handler fails with non retriable error then processing failure handler is invoked`() = runBlocking {
        val recovered = CompletableDeferred<Pair<Long?, String>>()
        val metrics = RecordingMetrics<Long, Long>()
        var processedRecordOffset: Long? = null
        val processor = createRecordProcessor<Long, Long>(
            handler = KafkaRecordHandler {
                throw IllegalStateException("boom")
            },
            metrics = metrics,
            processingFailureHandler = ProcessingFailureHandler { record, error ->
                recovered.complete(record.key() to "${record.offset()}:${error.message}")
            },
            onRecordProcessed = { processedRecordOffset = it.offset() }
        )

        processor.process(ConsumerRecord("topic-a", 0, 12L, 0L, 0L))

        assertEquals(0L to "12:boom", withTimeout(2_000) { recovered.await() })
        assertEquals(12L, processedRecordOffset)
        assertEquals(1, metrics.failed.size)
        assertEquals(0, metrics.processed.size)
        assertEquals("boom", metrics.failed.single().error.message)
        assertEquals(0L, metrics.failed.single().key)
        assertEquals(0L, metrics.failed.single().value)
        kotlin.test.assertTrue(metrics.failed.single().recordAgeMillis >= 0)
    }

    @Test
    fun `when retry rule contains several exception types then matching error is retried`() = runBlocking {
        val attempts = AtomicInteger()
        val processed = CompletableDeferred<Long>()
        val processor = createRecordProcessor<Long, Long>(
            handler = KafkaRecordHandler { record ->
                if (attempts.getAndIncrement() == 0) {
                    throw IllegalArgumentException("retry me")
                }
                processed.complete(record.value())
            },
            retryPolicy = retryPolicy {
                retry<IllegalArgumentException, IOException> {
                    maxRetries = 1
                }
            }
        )

        processor.process(ConsumerRecord("topic-a", 0, 13L, 0L, 13L))

        assertEquals(13L, withTimeout(2_000) { processed.await() })
        assertEquals(2, attempts.get())
    }

    @Test
    fun `when retry policy built from explicit exception list then matching error is retried`() = runBlocking {
        val attempts = AtomicInteger()
        val processed = CompletableDeferred<Long>()
        val processor = createRecordProcessor<Long, Long>(
            handler = KafkaRecordHandler { record ->
                if (attempts.getAndIncrement() == 0) {
                    throw IllegalStateException("retry me too")
                }
                processed.complete(record.value())
            },
            retryPolicy = retryPolicy {
                retry(listOf(IllegalStateException::class, IOException::class)) {
                    maxRetries = 1
                }
            }
        )

        processor.process(ConsumerRecord("topic-a", 0, 14L, 0L, 14L))

        assertEquals(14L, withTimeout(2_000) { processed.await() })
        assertEquals(2, attempts.get())
    }

    @Test
    fun `when processing failure handler throws then error is rethrown`() = runBlocking {
        var processedRecordOffset: Long? = null
        val processor = createRecordProcessor<Long, Long>(
            handler = KafkaRecordHandler {
                throw IllegalStateException("boom")
            },
            processingFailureHandler = ProcessingFailureHandler { _, _ ->
                throw UnsupportedOperationException("dlt failed")
            },
            onRecordProcessed = { processedRecordOffset = it.offset() }
        )

        val error = assertThrows(UnsupportedOperationException::class.java) {
            runBlocking {
                processor.process(ConsumerRecord("topic-a", 0, 22L, 0L, 0L))
            }
        }

        assertEquals("dlt failed", error.message)
        assertEquals(null, processedRecordOffset)
    }

    @Test
    fun `when processing succeeds in AT_LEAST_ONCE_NO_ORDERING mode then partition is marked processed`() = runBlocking {
        val record = ConsumerRecord("topic-a", 0, 25L, 0L, 0L)
        var processedRecordOffset: Long? = null
        val processor = RecordProcessor(
            handler = KafkaRecordHandler<Long, Long> { },
            retryPolicy = RetryPolicy.none(),
            metrics = noopConsumerMetrics(),
            processingFailureHandler = ProcessingFailureHandler.skip<Long, Long>(),
            onRecordProcessed = { processedRecordOffset = it.offset() }
        )

        processor.process(record)

        assertEquals(25L, processedRecordOffset)
    }

    @Test
    fun `when record context configured then handler runs inside it`() = runBlocking {
        val seen = CompletableDeferred<Boolean>()
        val processor = createRecordProcessor<Long, Long>(
            handler = KafkaRecordHandler {
                seen.complete(true)
            },
            recordProcessingContext = RecordProcessingContext { _, block ->
                seen.complete(false)
                block()
            }
        )

        processor.process(ConsumerRecord("topic-a", 0, 26L, 0L, 0L))

        assertEquals(false, withTimeout(2_000) { seen.await() })
    }

    @Test
    fun `when processing fails then failure handler runs inside record context`() = runBlocking {
        val events = mutableListOf<String>()
        val processor = createRecordProcessor<Long, Long>(
            handler = KafkaRecordHandler {
                events += "handler"
                throw IllegalStateException("boom")
            },
            processingFailureHandler = ProcessingFailureHandler { _, _ ->
                events += "failure-handler"
            },
            recordProcessingContext = RecordProcessingContext { _, block ->
                events += "context-before"
                block()
                events += "context-after"
            }
        )

        processor.process(ConsumerRecord("topic-a", 0, 27L, 0L, 0L))

        assertEquals(listOf("context-before", "handler", "failure-handler", "context-after"), events)
    }

    @Test
    fun `when failed record is skipped then later offsets can advance commit frontier`() = runBlocking {
        val tracker = OffsetTracker(initialProcessedOffset = -1)
        val processor = createRecordProcessor<Long, Long>(
            handler = KafkaRecordHandler { record ->
                if (record.offset() == 0L) {
                    throw IllegalStateException("skip first")
                }
            },
            onRecordProcessed = { tracker.markProcessed(it.offset()) }
        )

        processor.process(ConsumerRecord("topic-a", 0, 0L, 0L, 0L))
        processor.process(ConsumerRecord("topic-a", 0, 1L, 0L, 1L))

        assertEquals(1L, tracker.advanceProcessedOffset())
    }

    private fun <K, V> createRecordProcessor(
        handler: KafkaRecordHandler<K, V>,
        retryPolicy: RetryPolicy = RetryPolicy.none(),
        @Suppress("UNCHECKED_CAST")
        metrics: ConsumerMetrics<K, V> = ConsumerMetrics.NOOP as ConsumerMetrics<K, V>,
        processingFailureHandler: ProcessingFailureHandler<K, V> = ProcessingFailureHandler.skip(),
        onRecordProcessed: (ConsumerRecord<K, V>) -> Unit = {},
        recordProcessingContext: RecordProcessingContext<K, V>? = null
    ): RecordProcessor<K, V> = RecordProcessor(
        handler = handler,
        retryPolicy = retryPolicy,
        metrics = metrics,
        processingFailureHandler = processingFailureHandler,
        onRecordProcessed = onRecordProcessed,
        recordProcessingContext = recordProcessingContext
    )
}

@Suppress("UNCHECKED_CAST")
private fun <K, V> noopConsumerMetrics(): ConsumerMetrics<K, V> =
    ConsumerMetrics.NOOP as ConsumerMetrics<K, V>
