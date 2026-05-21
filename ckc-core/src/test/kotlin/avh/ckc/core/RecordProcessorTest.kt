package avh.ckc.core

import avh.ckc.core.deserialization.DeserializedRecord
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.processing.RecordProcessor
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds

class RecordProcessorTest {

    @BeforeEach
    fun resetTrackingDeserializers() {
        TrackingStringDeserializer.reset()
        TrackingLongDeserializer.reset()
    }

    @Test
    fun `when retry policy matches then worker retries and eventually succeeds`() = runBlocking {
        val attempts = AtomicInteger()
        val processed = CompletableDeferred<String>()
        val metrics = RecordingMetrics<String, String>()
        val processor = createRecordProcessor<String, String>(
            handler = KafkaRecordHandler { _, value, _ ->
                if (attempts.getAndIncrement() < 2) {
                    throw IOException("transient")
                }
                processed.complete(value!!)
            },
            metrics = metrics,
            retryPolicy = retryPolicy {
                retry<IOException> {
                    maxRetries = 2
                    delay = 1.milliseconds
                }
            }
        )

        processor.process(
            testRecord(offset = 10L, value = "payload"),
            DeserializedRecord(key = "key", value = "payload")
        )

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
        val processor = createRecordProcessor<Long, Long>(
            handler = KafkaRecordHandler { _, _, _ ->
                throw IllegalStateException("boom")
            },
            metrics = metrics,
            processingFailureHandler = ProcessingFailureHandler { key, _, rawRecord, error ->
                recovered.complete(key to "${rawRecord.offset()}:${error.message}")
            }
        )

        processor.process(
            testRecord(offset = 12L),
            DeserializedRecord(key = 0L, value = 0L)
        )

        assertEquals(0L to "12:boom", withTimeout(2_000) { recovered.await() })
        assertEquals(1, metrics.failed.size)
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
            handler = KafkaRecordHandler { _, value, _ ->
                if (attempts.getAndIncrement() == 0) {
                    throw IllegalArgumentException("retry me")
                }
                processed.complete(value!!)
            },
            retryPolicy = retryPolicy {
                retry<IllegalArgumentException, IOException> {
                    maxRetries = 1
                }
            }
        )

        processor.process(
            testRecord(offset = 13L, value = "13"),
            DeserializedRecord(key = 0L, value = 13L)
        )

        assertEquals(13L, withTimeout(2_000) { processed.await() })
        assertEquals(2, attempts.get())
    }

    @Test
    fun `when retry policy built from explicit exception list then matching error is retried`() = runBlocking {
        val attempts = AtomicInteger()
        val processed = CompletableDeferred<Long>()
        val processor = createRecordProcessor<Long, Long>(
            handler = KafkaRecordHandler { _, value, _ ->
                if (attempts.getAndIncrement() == 0) {
                    throw IllegalStateException("retry me too")
                }
                processed.complete(value!!)
            },
            retryPolicy = retryPolicy {
                retry(listOf(IllegalStateException::class, IOException::class)) {
                    maxRetries = 1
                }
            }
        )

        processor.process(
            testRecord(offset = 14L, value = "14"),
            DeserializedRecord(key = 0L, value = 14L)
        )

        assertEquals(14L, withTimeout(2_000) { processed.await() })
        assertEquals(2, attempts.get())
    }

    @Test
    fun `when processing failure handler throws then error is rethrown`() = runBlocking {
        val processor = createRecordProcessor<Long, Long>(
            handler = KafkaRecordHandler { _, _, _ ->
                throw IllegalStateException("boom")
            },
            processingFailureHandler = ProcessingFailureHandler { _, _, _, _ ->
                throw UnsupportedOperationException("dlt failed")
            }
        )

        val error = assertThrows(UnsupportedOperationException::class.java) {
            runBlocking {
                processor.process(
                    testRecord(offset = 22L),
                    DeserializedRecord(key = 0L, value = 0L)
                )
            }
        }

        assertEquals("dlt failed", error.message)
    }

    @Test
    fun `when processing succeeds in AT_LEAST_ONCE_UNORDERED mode then partition is marked processed`() = runBlocking {
        val record = testRecord(offset = 25L)
        var processedRecordOffset: Long? = null
        val processor = RecordProcessor(
            handler = KafkaRecordHandler<Long, Long> { _, _, _ -> },
            retryPolicy = RetryPolicy.none(),
            metrics = noopConsumerMetrics(),
            processingFailureHandler = ProcessingFailureHandler.skip<Long, Long>(),
            onRecordProcessed = { processedRecordOffset = it.offset() }
        )

        processor.process(record, DeserializedRecord(key = 0L, value = 0L))

        assertEquals(25L, processedRecordOffset)
    }

    private fun <K, V> createRecordProcessor(
        handler: KafkaRecordHandler<K, V>,
        retryPolicy: RetryPolicy = RetryPolicy.none(),
        @Suppress("UNCHECKED_CAST")
        metrics: ConsumerMetrics<K, V> = ConsumerMetrics.NOOP as ConsumerMetrics<K, V>,
        processingFailureHandler: ProcessingFailureHandler<K, V> = ProcessingFailureHandler.skip(),
        runtime: TestConsumerRuntime = testRuntime(processingMode = ProcessingMode.AT_LEAST_ONCE_UNORDERED),
        onRecordProcessed: (ConsumerRecord<ByteArray, ByteArray>) -> Unit = {}
    ): RecordProcessor<K, V> = RecordProcessor(
        handler = handler,
        retryPolicy = retryPolicy,
        metrics = metrics,
        processingFailureHandler = processingFailureHandler,
        onRecordProcessed = onRecordProcessed
    )
}

@Suppress("UNCHECKED_CAST")
private fun <K, V> noopConsumerMetrics(): ConsumerMetrics<K, V> =
    ConsumerMetrics.NOOP as ConsumerMetrics<K, V>
