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
    private val handler: KafkaRecordHandler<K, V>,
    private val retryPolicy: RetryPolicy,
    private val processingFailureHandler: ProcessingFailureHandler<K, V>,
    private val processedRecordTracker: ProcessedRecordTracker,
    private val recordDropPolicy: FreshnessRecordAgeDropPolicy<K, V>? = null
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
    protected abstract fun createChannel(
        capacity: Int,
        runtimeStats: ConsumerRuntimeStatsTracker
    ): Channel<ConsumerRecord<K, V>>

    override fun start(onFailure: (Throwable) -> Unit) {
        workerJobs = List(workerConcurrency) { workerIndex ->
            scope.launch(processingDispatcher + CoroutineName("KafkaWorker-$workerIndex")) {
                while (true) {
                    val record = workChannel.receiveCatching().getOrNull() ?: break
                    runtimeStats.onWorkDequeued()
                    runtimeStats.onWorkerStarted()
                    try {
                        if (recordDropPolicy?.shouldDrop(record) != true) {
                            recordProcessor.process(record)
                        }
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

    override fun tryEmit(record: ConsumerRecord<K, V>): Boolean {
        if (processedRecordTracker.isProcessed(record)) {
            metrics.onRecordDropped(record, RecordDropReason.ALREADY_PROCESSED)
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

    override fun stateSnapshot() = runtimeStats.snapshot()

    override suspend fun stop() {
        try {
            workChannel.close()
            workerJobs.joinAll()
        } finally {
            metrics.unbindRuntimeMetrics()
        }
    }
}
