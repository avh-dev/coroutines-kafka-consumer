package avh.ckc.core

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.common.TopicPartition
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
        val telemetry = RecordingTelemetry<String, String>()
        val processor = createRecordProcessor<String, String>(
            handler = KafkaRecordHandler { _, value, _ ->
                if (attempts.getAndIncrement() < 2) {
                    throw IOException("transient")
                }
                processed.complete(value!!)
            },
            telemetry = telemetry,
            retryPolicy = retryPolicy {
                retry<IOException> {
                    maxRetries = 2
                    delay = 1.milliseconds
                }
            }
        )

        processor.process(
            testRecord(offset = 10L, value = "payload"),
            defaultWorkerDeserializerFactoryForTests<String, String>(stringSerdeProperties())(0)
        )

        assertEquals("payload", withTimeout(2_000) { processed.await() })
        assertEquals(3, attempts.get())
        assertEquals(listOf(1, 2), telemetry.retries.map { it.attempt })
        assertEquals(1, telemetry.processed.size)
        assertEquals("payload", telemetry.processed.single().value)
        assertEquals("payload", telemetry.retries.first().value)
        kotlin.test.assertTrue(telemetry.processed.single().recordAgeMillis >= 0)
    }

    @Test
    fun `when handler fails with non retriable error then processing failure handler is invoked`() = runBlocking {
        val recovered = CompletableDeferred<Pair<Long?, String>>()
        val telemetry = RecordingTelemetry<Long, Long>()
        val processor = createRecordProcessor<Long, Long>(
            handler = KafkaRecordHandler { _, _, _ ->
                throw IllegalStateException("boom")
            },
            telemetry = telemetry,
            processingFailureHandler = ProcessingFailureHandler { key, _, rawRecord, error ->
                recovered.complete(key to "${rawRecord.offset()}:${error.message}")
            }
        )

        processor.process(
            testRecord(offset = 12L),
            defaultWorkerDeserializerFactoryForTests<Long, Long>(longSerdeProperties())(0)
        )

        assertEquals(0L to "12:boom", withTimeout(2_000) { recovered.await() })
        assertEquals(1, telemetry.failed.size)
        assertEquals("boom", telemetry.failed.single().error.message)
        assertEquals(0L, telemetry.failed.single().key)
        assertEquals(0L, telemetry.failed.single().value)
        kotlin.test.assertTrue(telemetry.failed.single().recordAgeMillis >= 0)
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
            defaultWorkerDeserializerFactoryForTests<Long, Long>(longSerdeProperties())(0)
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
            defaultWorkerDeserializerFactoryForTests<Long, Long>(longSerdeProperties())(0)
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
                    defaultWorkerDeserializerFactoryForTests<Long, Long>(longSerdeProperties())(0)
                )
            }
        }

        assertEquals("dlt failed", error.message)
    }

    @Test
    fun `when deserializer fails with transient error then it is retried internally`() = runBlocking {
        val processed = CompletableDeferred<Long?>()
        val processor = createRecordProcessor<Long, Long>(
            handler = KafkaRecordHandler { _, value, _ ->
                processed.complete(value)
            }
        )

        processor.process(
            testRecord(offset = 23L, value = "23"),
            WorkerDeserializers(
                keyDeserializer = TrackingLongDeserializer(),
                valueDeserializer = FlakyLongDeserializer(failuresBeforeSuccess = 2, failure = IOException("registry down"))
            )
        )

        assertEquals(23L, withTimeout(2_000) { processed.await() })
    }

    @Test
    fun `when deserializer fails with non transient error then processing fails`() = runBlocking {
        val telemetry = RecordingTelemetry<Long, Long>()
        val processor = createRecordProcessor<Long, Long>(
            handler = KafkaRecordHandler { _, _, _ -> },
            telemetry = telemetry
        )

        val error = assertThrows(IllegalStateException::class.java) {
            runBlocking {
                processor.process(
                    testRecord(offset = 24L, value = "24"),
                    WorkerDeserializers(
                        keyDeserializer = TrackingLongDeserializer(),
                        valueDeserializer = FlakyLongDeserializer(
                            failuresBeforeSuccess = Int.MAX_VALUE,
                            failure = IllegalStateException("broken payload")
                        )
                    )
                )
            }
        }

        assertEquals("broken payload", error.message)
        assertEquals(1, telemetry.failed.size)
        assertEquals("broken payload", telemetry.failed.single().error.message)
        assertEquals(null, telemetry.failed.single().value)
        kotlin.test.assertTrue(telemetry.failed.single().recordAgeMillis >= 0)
    }

    @Test
    fun `when processing succeeds in backpressure mode then partition is marked processed`() = runBlocking {
        val registry = PartitionRegistry()
        val record = testRecord(offset = 25L)
        val state = registry.onPartitionsAssigned(listOf(TopicPartition(record.topic(), record.partition()))).single()
        state.init(25L)
        val processor = RecordProcessor(
            deliveryStrategy = DeliveryStrategy.BACKPRESSURE,
            deserializationDispatcher = Dispatchers.IO,
            handler = KafkaRecordHandler<Long, Long> { _, _, _ -> },
            retryPolicy = RetryPolicy.none(),
            telemetry = ConsumerTelemetry.NOOP as ConsumerTelemetry<Long, Long>,
            processingFailureHandler = ProcessingFailureHandler.skip<Long, Long>(),
            partitionRegistry = registry
        )

        processor.process(record, defaultWorkerDeserializerFactoryForTests<Long, Long>(longSerdeProperties())(0))

        assertEquals(25L, state.advanceCommitOffset())
    }

    private fun <K, V> createRecordProcessor(
        handler: KafkaRecordHandler<K, V>,
        retryPolicy: RetryPolicy = RetryPolicy.none(),
        @Suppress("UNCHECKED_CAST")
        telemetry: ConsumerTelemetry<K, V> = ConsumerTelemetry.NOOP as ConsumerTelemetry<K, V>,
        processingFailureHandler: ProcessingFailureHandler<K, V> = ProcessingFailureHandler.skip(),
        runtime: TestConsumerRuntime = testRuntime(strategy = DeliveryStrategy.BACKPRESSURE),
        partitionRegistry: PartitionRegistry = PartitionRegistry()
    ): RecordProcessor<K, V> = RecordProcessor(
        deliveryStrategy = runtime.deliveryStrategy,
        deserializationDispatcher = runtime.deserializationDispatcher,
        handler = handler,
        retryPolicy = retryPolicy,
        telemetry = telemetry,
        processingFailureHandler = processingFailureHandler,
        partitionRegistry = partitionRegistry
    )
}
