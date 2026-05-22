package avh.ckc.core.processing.runtime

import avh.ckc.core.KafkaRecordHandler
import avh.ckc.core.ProcessingFailureHandler
import avh.ckc.core.RetryPolicy
import avh.ckc.core.processing.deserialization.RecordDeserializer
import avh.ckc.core.processing.deserialization.RecordDeserializerFactory
import avh.ckc.core.processing.deserialization.closeAll
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.ConsumerRuntimeStatsTracker
import avh.ckc.core.processing.ProcessedRecordTracker
import avh.ckc.core.processing.RecordProcessingRuntime
import avh.ckc.core.processing.RecordProcessor
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerRecord

internal abstract class BaseUnorderedRecordProcessingRuntime<K, V>(
    private val workerConcurrency: Int,
    private val workChannelCapacity: Int,
    private val processingDispatcher: CoroutineDispatcher,
    private val scope: CoroutineScope,
    protected val metrics: ConsumerMetrics<K, V>,
    private val recordDeserializerFactory: RecordDeserializerFactory<K, V>,
    private val handler: KafkaRecordHandler<K, V>,
    private val retryPolicy: RetryPolicy,
    private val processingFailureHandler: ProcessingFailureHandler<K, V>,
    private val processedRecordTracker: ProcessedRecordTracker
) : RecordProcessingRuntime<K, V> {
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
    private val workChannel by lazy {
        createChannel(workChannelCapacity, runtimeStats)
    }

    private var workerJobs: List<Job> = emptyList()
    private var recordDeserializers: List<RecordDeserializer<K, V>> = emptyList()

    protected abstract fun createChannel(
        capacity: Int,
        runtimeStats: ConsumerRuntimeStatsTracker
    ): Channel<ConsumerRecord<ByteArray, ByteArray>>

    override fun start(onFailure: (Throwable) -> Unit) {
        recordDeserializers = try {
            List(workerConcurrency) { workerIndex ->
                recordDeserializerFactory(workerIndex)
            }
        } catch (error: Throwable) {
            recordDeserializers.closeAll()
            throw error
        }

        workerJobs = List(workerConcurrency) { workerIndex ->
            val recordDeserializer = recordDeserializers[workerIndex]
            scope.launch(processingDispatcher + CoroutineName("KafkaWorker-$workerIndex")) {
                while (true) {
                    val record = workChannel.receiveCatching().getOrNull() ?: break
                    runtimeStats.onWorkDequeued()
                    runtimeStats.onWorkerStarted()
                    try {
                        processRecord(record, recordDeserializer)
                    } finally {
                        runtimeStats.onWorkerFinished()
                    }
                }
            }.also { job ->
                job.invokeOnCompletion { cause ->
                    if (cause != null) {
                        workChannel.close(cause)
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

        val result = workChannel.trySend(record)
        if (result.isSuccess) {
            runtimeStats.onWorkEnqueued()
        }
        return result.isSuccess
    }

    override fun close(cause: Throwable?) {
        workChannel.close(cause)
    }

    override suspend fun stop() {
        try {
            workChannel.close()
            workerJobs.joinAll()
        } finally {
            metrics.unbindRuntimeMetrics()
            recordDeserializers.closeAll()
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
            if (error is kotlinx.coroutines.CancellationException) {
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
}
