package avh.ckc.core

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.partition.PartitionRegistry
import avh.ckc.core.processing.PolledRecordSink
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import kotlin.coroutines.EmptyCoroutineContext

class CoroutinesKafkaConsumerTest {

    @BeforeEach
    fun resetTrackingDeserializers() {
        TrackingStringDeserializer.reset()
        TrackingLongDeserializer.reset()
    }

    @Test
    fun `when record is received then key value and rawRecord are passed to handler`() = runBlocking {
        val processed = CompletableDeferred<Triple<String?, String?, Long>>()
        val consumer = createTestConsumer(
            records = listOf(testRecord(offset = 10L, key = "key-10", value = "payload")),
            consumerProperties = stringSerdeProperties(),
            handler = KafkaRecordHandler<String, String> { key, value, rawRecord ->
                processed.complete(Triple(key, value, rawRecord.offset()))
            }
        )

        consumer.start()

        assertEquals(Triple("key-10", "payload", 10L), withTimeout(2_000) { processed.await() })
        consumer.stop()
    }

    @Test
    fun `when stop called then it waits for inflight worker completion`() = runBlocking {
        val started = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val consumer = createTestConsumer(
            records = listOf(testRecord(offset = 11L)),
            consumerProperties = longSerdeProperties(),
            handler = KafkaRecordHandler<Long, Long> { _, _, _ ->
                started.complete(Unit)
                release.await()
            }
        )

        consumer.start()
        withTimeout(2_000) { started.await() }

        val stopJob = async { consumer.stop() }
        assertFalse(stopJob.isCompleted)

        release.complete(Unit)
        withTimeout(2_000) { stopJob.await() }
    }

    @Test
    fun `when consumer starts then each worker gets its own deserializer instances`() = runBlocking {
        val recordDeserializers = CopyOnWriteArrayList<Pair<TrackingLongDeserializer, TrackingLongDeserializer>>()
        val consumer = createTestConsumer(
            records = emptyList(),
            workerConcurrency = 2,
            consumerProperties = longSerdeProperties(),
            recordDeserializerFactory = { _ ->
                val key = TrackingLongDeserializer()
                val value = TrackingLongDeserializer()
                recordDeserializers += key to value
                TestRecordDeserializer(key, value)
            },
            handler = KafkaRecordHandler<Long, Long> { _, _, _ -> }
        )

        consumer.start()
        consumer.stop()

        assertEquals(2, recordDeserializers.size)
        assertNotSame(recordDeserializers[0].first, recordDeserializers[1].first)
        assertNotSame(recordDeserializers[0].second, recordDeserializers[1].second)
    }

    @Test
    fun `when deserializing then key and value run on deserialization dispatcher`() = runBlocking {
        val processed = CompletableDeferred<Pair<String, String>>()
        val dispatcher = Executors.newSingleThreadExecutor { runnable ->
            Thread(runnable, "deser-thread")
        }.asCoroutineDispatcher()

        try {
            val consumer = createTestConsumer(
                records = listOf(testRecord(offset = 21L)),
                consumerProperties = stringSerdeProperties(),
                runtime = testRuntime(
                    strategy = DeliveryStrategy.BACKPRESSURE,
                    deserializationDispatcher = dispatcher
                ),
                handler = KafkaRecordHandler<String, String> { _, _, _ ->
                    processed.complete(
                        TrackingStringDeserializer.lastKeyThreadName to TrackingStringDeserializer.lastValueThreadName
                    )
                }
            )

            consumer.start()
            val (keyThread, valueThread) = withTimeout(2_000) { processed.await() }
            assertTrue(keyThread.contains("deser-thread"))
            assertTrue(valueThread.contains("deser-thread"))
            consumer.stop()
        } finally {
            dispatcher.close()
        }
    }

    @Test
    fun `when processing then handler runs on processing dispatcher`() = runBlocking {
        val processedThread = CompletableDeferred<String>()
        val dispatcher = Executors.newSingleThreadExecutor { runnable ->
            Thread(runnable, "processing-thread")
        }.asCoroutineDispatcher()

        try {
            val consumer = createTestConsumer(
                records = listOf(testRecord(offset = 22L)),
                consumerProperties = stringSerdeProperties(),
                runtime = testRuntime(
                    strategy = DeliveryStrategy.BACKPRESSURE,
                    processingDispatcher = dispatcher
                ),
                handler = KafkaRecordHandler<String, String> { _, _, _ ->
                    processedThread.complete(Thread.currentThread().name)
                }
            )

            consumer.start()
            assertTrue(withTimeout(2_000) { processedThread.await() }.contains("processing-thread"))
            consumer.stop()
        } finally {
            dispatcher.close()
        }
    }

    @Test
    fun `when consumer starts then runtime metrics expose worker and queue stats`() = runBlocking {
        val metrics = RecordingMetrics<String, String>()
        val firstRecordStarted = CompletableDeferred<Unit>()
        val releaseFirstRecord = CompletableDeferred<Unit>()
        val consumer = createTestConsumer(
            records = listOf(
                testRecord(offset = 31L, key = "key-31", value = "value-31"),
                testRecord(offset = 32L, key = "key-32", value = "value-32")
            ),
            consumerProperties = stringSerdeProperties(),
            metrics = metrics,
            handler = KafkaRecordHandler<String, String> { _, _, rawRecord ->
                if (rawRecord.offset() == 31L) {
                    firstRecordStarted.complete(Unit)
                    releaseFirstRecord.await()
                }
            }
        )

        consumer.start()
        withTimeout(2_000) { firstRecordStarted.await() }

        val stats = metrics.boundRuntimeStats.single()
        assertEquals(1, stats.workerCount)
        assertEquals(1, stats.activeWorkerCount)
        assertEquals(1, stats.workQueueSize)
        assertEquals(1024, stats.workQueueCapacity)
        assertEquals(2, stats.maxObservedWorkQueueSize)

        releaseFirstRecord.complete(Unit)
        consumer.stop()

        assertSame(stats, metrics.boundRuntimeStats.single())
        assertEquals(1, metrics.unbindRuntimeMetricsCalls.size)
    }

    @Test
    fun `when poll loop fails then metrics receive consumer failure`() = runBlocking {
        val metrics = RecordingMetrics<String, String>()
        val expected = IllegalStateException("poll loop failed")
        val consumer: CoroutinesKafkaConsumer<String, String> = CoroutinesKafkaConsumer(
            deliveryStrategy = DeliveryStrategy.BACKPRESSURE,
            workerConcurrency = 1,
            consumerPollLoopConcurrency = 1,
            commitIntervalMs = 1_000L,
            workChannelCapacity = 16,
            deserializationDispatcher = kotlinx.coroutines.Dispatchers.IO,
            processingDispatcher = kotlinx.coroutines.Dispatchers.Default,
            consumerProperties = stringSerdeProperties(),
            retryPolicy = RetryPolicy.none(),
            metrics = metrics,
            handler = KafkaRecordHandler<String, String> { _, _, _ -> },
            processingFailureHandler = ProcessingFailureHandler.skip<String, String>(),
            parentContext = EmptyCoroutineContext,
            topics = listOf("topic-a"),
            topicsPattern = null,
            pollLoopFactory = { _: Int, context, _: DeliveryStrategy, _: Long, _: ConsumerMetrics<String, String>, _: Map<String, Any?>, _: ConsumerConfigAdapter, _: List<String>?, _: java.util.regex.Pattern?, _: PolledRecordSink, _: PartitionRegistry ->
                object : ConsumerPollLoopControl {
                    override fun start() = CoroutineScope(context).launch {
                        throw expected
                    }

                    override fun prepareForShutdown() = CompletableDeferred(Unit)
                }
            },
            processingRuntimeFactory = ::defaultProcessingRuntime,
            recordDeserializerFactory = defaultRecordDeserializerFactoryForTests(stringSerdeProperties())
        )

        consumer.start()
        withTimeout(2_000) {
            awaitFor(timeoutMillis = 2_000, pauseMillis = 10) {
                metrics.consumerFailures.firstOrNull()
            }
        }
        val thrown = assertThrows(IllegalStateException::class.java) {
            runBlocking {
                withTimeout(2_000) { consumer.stop() }
            }
        }

        assertEquals(expected.message, thrown.message)
        assertEquals(listOf(expected), metrics.consumerFailures)
    }
}
