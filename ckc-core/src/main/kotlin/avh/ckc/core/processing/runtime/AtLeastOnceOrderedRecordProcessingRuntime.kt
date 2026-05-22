package avh.ckc.core.processing.runtime

import avh.ckc.core.KafkaRecordHandler
import avh.ckc.core.ProcessingFailureHandler
import avh.ckc.core.RetryPolicy
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.ConsumerRuntimeStatsTracker
import avh.ckc.core.processing.ProcessedRecordTracker
import avh.ckc.core.processing.RecordProcessingRuntime
import avh.ckc.core.processing.RecordProcessor
import avh.ckc.core.processing.deserialization.RecordDeserializer
import avh.ckc.core.processing.deserialization.RecordDeserializerFactory
import avh.ckc.core.processing.deserialization.closeAll
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

internal class AtLeastOnceOrderedRecordProcessingRuntime<K, V>(
    private val workerConcurrency: Int,
    workChannelCapacity: Int,
    private val ordering: Ordering,
    private val processingDispatcher: CoroutineDispatcher,
    private val scope: CoroutineScope,
    private val metrics: ConsumerMetrics<K, V>,
    private val recordDeserializerFactory: RecordDeserializerFactory<K, V>,
    handler: KafkaRecordHandler<K, V>,
    retryPolicy: RetryPolicy,
    processingFailureHandler: ProcessingFailureHandler<K, V>,
    private val processedRecordTracker: ProcessedRecordTracker
) : RecordProcessingRuntime<K, V> {
    enum class Ordering {
        BY_KEY,
        BY_PARTITION
    }

    private val recordProcessor = RecordProcessor(
        handler = handler,
        retryPolicy = retryPolicy,
        metrics = metrics,
        processingFailureHandler = processingFailureHandler,
        onRecordProcessed = processedRecordTracker::markProcessed
    )
    private val runtimeStats = ConsumerRuntimeStatsTracker(
        workerCount = workerConcurrency,
        workQueueCapacity = workChannelCapacity
    )
    private val admissionBudget = AdmissionBudget(workChannelCapacity, runtimeStats)
    private val states = ConcurrentHashMap<OrderingKey, KeyState>()
    private val acceptingRecords = AtomicBoolean(true)
    private val inputChannel by lazy {
        Channel<ConsumerRecord<ByteArray, ByteArray>>(
            capacity = workChannelCapacity,
            onBufferOverflow = BufferOverflow.SUSPEND,
            onUndeliveredElement = {
                admissionBudget.release()
            }
        )
    }
    private val workerChannel by lazy {
        Channel<WorkItem>(
            capacity = Channel.RENDEZVOUS,
            onUndeliveredElement = {
                admissionBudget.release()
            }
        )
    }

    private var schedulerJob: Job? = null
    private var workerJobs: List<Job> = emptyList()
    private var recordDeserializers: List<RecordDeserializer<K, V>> = emptyList()

    override fun start(onFailure: (Throwable) -> Unit) {
        recordDeserializers = try {
            List(workerConcurrency) { workerIndex ->
                recordDeserializerFactory(workerIndex)
            }
        } catch (error: Throwable) {
            recordDeserializers.closeAll()
            throw error
        }

        schedulerJob = scope.launch(processingDispatcher + CoroutineName("KafkaOrderedScheduler")) {
            runScheduler()
        }.also { job ->
            job.invokeOnCompletion { cause ->
                if (cause != null && cause !is CancellationException) {
                    close(cause)
                    onFailure(cause)
                }
            }
        }

        workerJobs = List(workerConcurrency) { workerIndex ->
            val recordDeserializer = recordDeserializers[workerIndex]
            scope.launch(processingDispatcher + CoroutineName("KafkaOrderedWorker-$workerIndex")) {
                runWorker(recordDeserializer)
            }.also { job ->
                job.invokeOnCompletion { cause ->
                    if (cause != null && cause !is CancellationException) {
                        close(cause)
                        onFailure(cause)
                    }
                }
            }
        }

        metrics.bindRuntimeMetrics(runtimeStats)
    }

    override fun tryEmit(record: ConsumerRecord<ByteArray, ByteArray>): Boolean {
        if (processedRecordTracker.isProcessed(record)) {
            return true
        }
        if (!acceptingRecords.get() || !admissionBudget.tryAcquire()) {
            return false
        }

        val result = inputChannel.trySend(record)
        if (!result.isSuccess) {
            admissionBudget.release()
        }
        return result.isSuccess
    }

    override fun close(cause: Throwable?) {
        acceptingRecords.set(false)
        inputChannel.close(cause)
        workerChannel.close(cause)
        releaseBufferedKeyQueues()
    }

    override suspend fun stop() {
        acceptingRecords.set(false)
        try {
            inputChannel.close()
            schedulerJob?.join()
            workerJobs.joinAll()
        } finally {
            releaseBufferedKeyQueues()
            metrics.unbindRuntimeMetrics()
            recordDeserializers.closeAll()
        }
    }

    private suspend fun runScheduler() {
        try {
            while (currentCoroutineContext().isActive) {
                val record = inputChannel.receiveCatching().getOrNull() ?: break
                val key = ordering.keyFor(record)
                val workItem = WorkItem(key, record)
                var dispatch: WorkItem? = null

                states.compute(key) { _, current ->
                    val state = current ?: KeyState()
                    if (state.inFlight) {
                        state.queue.addLast(record)
                    } else {
                        state.inFlight = true
                        dispatch = workItem
                    }
                    state
                }

                dispatch?.let { dispatchToWorker(it) }
            }
        } finally {
            workerChannel.close()
        }
    }

    private suspend fun dispatchToWorker(workItem: WorkItem) {
        try {
            workerChannel.send(workItem)
        } catch (error: Throwable) {
            releaseDispatchedRecord(workItem.key)
            admissionBudget.release()
            throw error
        }
    }

    private suspend fun runWorker(recordDeserializer: RecordDeserializer<K, V>) {
        while (currentCoroutineContext().isActive) {
            var workItem = workerChannel.receiveCatching().getOrNull() ?: break
            while (true) {
                runtimeStats.onWorkerStarted()
                try {
                    processRecord(workItem.record, recordDeserializer)
                } finally {
                    runtimeStats.onWorkerFinished()
                    admissionBudget.release()
                }

                val nextRecord = nextRecordFor(workItem.key) ?: break
                workItem = WorkItem(workItem.key, nextRecord)
            }
        }
    }

    private fun nextRecordFor(key: OrderingKey): ConsumerRecord<ByteArray, ByteArray>? {
        var next: ConsumerRecord<ByteArray, ByteArray>? = null
        states.compute(key) { _, state ->
            if (state == null) {
                return@compute null
            }
            val queued = state.queue.removeFirstOrNull()
            if (queued == null) {
                state.inFlight = false
                null
            } else {
                next = queued
                state
            }
        }
        return next
    }

    private fun releaseDispatchedRecord(key: OrderingKey) {
        states.compute(key) { _, state ->
            if (state == null) {
                null
            } else if (state.queue.isEmpty()) {
                state.inFlight = false
                null
            } else {
                state
            }
        }
    }

    private fun releaseBufferedKeyQueues() {
        states.forEach { (key, _) ->
            states.compute(key) { _, state ->
                if (state != null) {
                    repeat(state.queue.size) {
                        admissionBudget.release()
                    }
                    state.queue.clear()
                }
                null
            }
        }
    }

    private suspend fun processRecord(
        record: ConsumerRecord<ByteArray, ByteArray>,
        recordDeserializer: RecordDeserializer<K, V>
    ) {
        val startedAt = System.nanoTime()
        val recordAgeMillis = (System.currentTimeMillis() - record.timestamp()).coerceAtLeast(0L)
        val deserializedRecord = try {
            recordDeserializer.deserialize(record)
        } catch (error: Throwable) {
            if (error is CancellationException) {
                throw error
            }
            metrics.onRecordFailed(
                key = null,
                value = null,
                record = record,
                recordAgeMillis = recordAgeMillis,
                error = error,
                durationNanos = System.nanoTime() - startedAt
            )
            throw error
        }
        recordProcessor.process(record, deserializedRecord)
    }

    private fun Ordering.keyFor(record: ConsumerRecord<ByteArray, ByteArray>): OrderingKey =
        when (this) {
            Ordering.BY_KEY -> {
                val key = record.key()
                if (key == null) {
                    OrderingKey.NullKey
                } else {
                    OrderingKey.RawKey(ByteArrayKey(key))
                }
            }
            Ordering.BY_PARTITION -> OrderingKey.Partition(record.topic(), record.partition())
        }

    private class KeyState(
        val queue: ArrayDeque<ConsumerRecord<ByteArray, ByteArray>> = ArrayDeque(),
        var inFlight: Boolean = false
    )

    private data class WorkItem(
        val key: OrderingKey,
        val record: ConsumerRecord<ByteArray, ByteArray>
    )

    private sealed interface OrderingKey {
        data object NullKey : OrderingKey

        data class RawKey(val key: ByteArrayKey) : OrderingKey

        data class Partition(val topic: String, val partition: Int) : OrderingKey
    }

    private class ByteArrayKey(private val bytes: ByteArray) {
        private val hash = bytes.contentHashCode()

        override fun equals(other: Any?): Boolean =
            other is ByteArrayKey && bytes.contentEquals(other.bytes)

        override fun hashCode(): Int = hash
    }

    private class AdmissionBudget(
        private val capacity: Int,
        private val runtimeStats: ConsumerRuntimeStatsTracker
    ) {
        private val used = AtomicInteger(0)

        fun tryAcquire(): Boolean {
            while (true) {
                val current = used.get()
                if (current >= capacity) {
                    return false
                }
                if (used.compareAndSet(current, current + 1)) {
                    runtimeStats.onWorkEnqueued()
                    return true
                }
            }
        }

        fun release() {
            val released = used.getAndUpdate { current ->
                if (current > 0) current - 1 else 0
            }
            if (released > 0) {
                runtimeStats.onWorkDequeued()
            }
        }
    }
}
