package avh.ckc.core.processing.runtime

import avh.ckc.core.KafkaRecordHandler
import avh.ckc.core.ProcessingFailureHandler
import avh.ckc.core.RecordProcessingContext
import avh.ckc.core.RetryPolicy
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.ConsumerRuntimeStatsTracker
import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.core.metrics.RecordHandoffReason
import avh.ckc.core.processing.ProcessedRecordTracker
import avh.ckc.core.processing.RecordProcessingRuntime
import avh.ckc.core.processing.RecordProcessor
import avh.ckc.core.processing.RecordProcessingOutcome
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.supervisorScope
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
    protected val processedRecordTracker: ProcessedRecordTracker,
    private val recordProcessingContext: RecordProcessingContext<K, V>? = null,
    private val recordDropPolicy: FreshnessRecordAgeDropPolicy<K, V>? = null,
    private val handoffOnAssignmentEnd: Boolean = false
) : RecordProcessingRuntime<K, V> {
    private val recordProcessor = RecordProcessor(
        handler = handler,
        retryPolicy = retryPolicy,
        metrics = metrics,
        processingFailureHandler = processingFailureHandler,
        onRecordProcessed = processedRecordTracker::markProcessed,
        awaitRetryPermission = processedRecordTracker::awaitRetryPermission,
        recordProcessingContext = recordProcessingContext
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
                    if (!processedRecordTracker.tryStart(record)) {
                        reportAssignmentEnd(record)
                        processedRecordTracker.discard(record)
                        continue
                    }
                    runtimeStats.onWorkerStarted()
                    try {
                        if (recordDropPolicy?.shouldDrop(record) != true) {
                            supervisorScope {
                                val pipeline = async(start = CoroutineStart.LAZY) { recordProcessor.process(record) }
                                processedRecordTracker.attach(record, pipeline)
                                pipeline.start()
                                try {
                                    if (pipeline.await() == RecordProcessingOutcome.HANDED_OFF) {
                                        reportAssignmentEnd(record)
                                    }
                                } catch (error: kotlinx.coroutines.CancellationException) {
                                    if (!processedRecordTracker.isCancellationRequested(record)) throw error
                                    reportAssignmentEnd(record)
                                    // A lost assignment cancels only this child pipeline; the worker stays
                                    // available for valid assignments after recovery.
                                }
                            }
                        }
                    } finally {
                        runtimeStats.onWorkerFinished()
                        processedRecordTracker.finish(record)
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
            processedRecordTracker.discard(record)
            return true
        }

        // Account before publication: a worker may receive the element before trySend returns.
        // Failed non-suspending sends never invoke the channel's undelivered-element callback.
        runtimeStats.onWorkEnqueued()
        val result = workChannel.trySend(record)
        if (!result.isSuccess) {
            runtimeStats.onWorkDequeued()
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

    protected fun reportAssignmentEnd(record: ConsumerRecord<K, V>) {
        if (handoffOnAssignmentEnd) {
            metrics.onRecordHandedOff(record, handoffReason())
        } else {
            metrics.onRecordDropped(record, RecordDropReason.LIFECYCLE_DISCARD)
        }
    }

    protected fun handoffReason(): RecordHandoffReason =
        if (processedRecordTracker.isTerminallyClosed()) {
            RecordHandoffReason.CONSUMER_FAILED
        } else {
            RecordHandoffReason.ASSIGNMENT_ENDED
        }
}
