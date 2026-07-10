package avh.ckc.core.processing.runtime

import avh.ckc.core.KafkaRecordHandler
import avh.ckc.core.ProcessingFailureHandler
import avh.ckc.core.RetryPolicy
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.ConsumerRuntimeStatsTracker
import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.core.processing.ProcessedRecordTracker
import avh.ckc.core.processing.RecordProcessingRuntime
import avh.ckc.core.processing.RecordProcessor
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
    private val states = ConcurrentHashMap<OrderingKey, KeyState<K, V>>()
    private val acceptingRecords = AtomicBoolean(true)
    private val inputChannel by lazy {
        Channel<ConsumerRecord<K, V>>(
            capacity = workChannelCapacity,
            onBufferOverflow = BufferOverflow.SUSPEND,
            onUndeliveredElement = {
                admissionBudget.release()
            }
        )
    }
    private val workerChannel by lazy {
        Channel<WorkItem<K, V>>(capacity = Channel.RENDEZVOUS)
    }

    private var schedulerJob: Job? = null
    private var workerJobs: List<Job> = emptyList()
    override fun start(onFailure: (Throwable) -> Unit) {
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
            scope.launch(processingDispatcher + CoroutineName("KafkaOrderedWorker-$workerIndex")) {
                runWorker()
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

    override fun tryEmit(record: ConsumerRecord<K, V>): Boolean {
        if (processedRecordTracker.isProcessed(record)) {
            metrics.onRecordDropped(record, RecordDropReason.ALREADY_PROCESSED)
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

    override fun stateSnapshot() = runtimeStats.snapshot()

    override suspend fun stop() {
        acceptingRecords.set(false)
        try {
            inputChannel.close()
            schedulerJob?.join()
            workerJobs.joinAll()
        } finally {
            releaseBufferedKeyQueues()
            metrics.unbindRuntimeMetrics()
        }
    }

    private suspend fun runScheduler() {
        try {
            while (currentCoroutineContext().isActive) {
                val record = inputChannel.receiveCatching().getOrNull() ?: break
                val key = ordering.keyFor(record)
                val workItem = WorkItem(key, record)
                var dispatch: WorkItem<K, V>? = null

                states.compute(key) { _, current ->
                    val state = current ?: KeyState()
                    if (state.inFlight) {
                        state.queue.addLast(record)
                        runtimeStats.onOrderingWorkQueued()
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

    private suspend fun dispatchToWorker(workItem: WorkItem<K, V>) {
        try {
            workerChannel.send(workItem)
        } catch (error: Throwable) {
            releaseDispatchedRecord(workItem.key)
            admissionBudget.release()
            throw error
        }
    }

    private suspend fun runWorker() {
        while (currentCoroutineContext().isActive) {
            var workItem = workerChannel.receiveCatching().getOrNull() ?: break
            while (true) {
                runtimeStats.onWorkerStarted()
                try {
                    recordProcessor.process(workItem.record)
                } finally {
                    runtimeStats.onWorkerFinished()
                    admissionBudget.release()
                }

                val nextRecord = nextRecordFor(workItem.key) ?: break
                workItem = WorkItem(workItem.key, nextRecord)
            }
        }
    }

    private fun nextRecordFor(key: OrderingKey): ConsumerRecord<K, V>? {
        var next: ConsumerRecord<K, V>? = null
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
                runtimeStats.onOrderingWorkDequeued()
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
                        runtimeStats.onOrderingWorkDequeued()
                    }
                    state.queue.clear()
                }
                null
            }
        }
    }

    private fun Ordering.keyFor(record: ConsumerRecord<K, V>): OrderingKey =
        when (this) {
            Ordering.BY_KEY -> {
                val key = record.key()
                if (key == null) {
                    OrderingKey.NullKey
                } else {
                    OrderingKey.DeserializedKey(key)
                }
            }
            Ordering.BY_PARTITION -> OrderingKey.Partition(record.topic(), record.partition())
        }

    private class KeyState<K, V>(
        val queue: ArrayDeque<ConsumerRecord<K, V>> = ArrayDeque(),
        var inFlight: Boolean = false
    )

    private data class WorkItem<K, V>(
        val key: OrderingKey,
        val record: ConsumerRecord<K, V>
    )

    private sealed interface OrderingKey {
        data object NullKey : OrderingKey

        data class DeserializedKey(val key: Any) : OrderingKey

        data class Partition(val topic: String, val partition: Int) : OrderingKey
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
