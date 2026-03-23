package avh.ckc.core

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors

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
        val workerDeserializers = CopyOnWriteArrayList<Pair<TrackingLongDeserializer, TrackingLongDeserializer>>()
        val consumer = createTestConsumer(
            records = emptyList(),
            workerConcurrency = 2,
            consumerProperties = longSerdeProperties(),
            workerDeserializerFactory = { _ ->
                val key = TrackingLongDeserializer()
                val value = TrackingLongDeserializer()
                workerDeserializers += key to value
                WorkerDeserializers(key, value)
            },
            handler = KafkaRecordHandler<Long, Long> { _, _, _ -> }
        )

        consumer.start()
        consumer.stop()

        assertEquals(2, workerDeserializers.size)
        assertNotSame(workerDeserializers[0].first, workerDeserializers[1].first)
        assertNotSame(workerDeserializers[0].second, workerDeserializers[1].second)
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
                    strategy = OverflowStrategy.BACKPRESSURE,
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
                    strategy = OverflowStrategy.BACKPRESSURE,
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
}
