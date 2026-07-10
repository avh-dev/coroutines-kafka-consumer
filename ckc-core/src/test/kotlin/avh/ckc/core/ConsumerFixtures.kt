package avh.ckc.core

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.polling.ConsumerPollLoopControl
import avh.ckc.core.processing.PolledRecordSink
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerRecord

internal fun <K, V> createTestConsumer(
    records: List<ConsumerRecord<K, V>>,
    consumerProperties: Map<String, Any?>,
    handler: KafkaRecordHandler<K, V>,
    retryPolicy: RetryPolicy = RetryPolicy.none(),
    @Suppress("UNCHECKED_CAST")
    metrics: ConsumerMetrics<K, V> = ConsumerMetrics.NOOP as ConsumerMetrics<K, V>,
    processingFailureHandler: ProcessingFailureHandler<K, V> = ProcessingFailureHandler.skip(),
    workerConcurrency: Int = 1,
    runtime: TestConsumerRuntime = testRuntime(processingMode = ProcessingMode.AT_LEAST_ONCE_UNORDERED)
): CoroutinesKafkaConsumer<K, V> =
    CoroutinesKafkaConsumer(
        consumerProperties = consumerProperties,
        processingMode = runtime.processingMode,
        workerConcurrency = workerConcurrency,
        consumerPollLoopConcurrency = runtime.consumerPollLoopConcurrency,
        commitIntervalMs = runtime.commitIntervalMs,
        workChannelCapacity = runtime.workChannelCapacity,
        processingDispatcher = runtime.processingDispatcher,
        retryPolicy = retryPolicy,
        metrics = metrics,
        processingFailureHandler = processingFailureHandler,
        parentContext = kotlinx.coroutines.Dispatchers.Default,
        topics = listOf("topic-a"),
        topicsPattern = null,
        handler = handler,
        pollLoopFactory = { _, context, _, _, _, _, _, _, _, recordSink, _ ->
            FakePollLoopControl(context, recordSink, records)
        },
        processingRuntimeFactory = ::defaultProcessingRuntime
    )

internal class FakePollLoopControl<K, V>(
    coroutineContext: kotlin.coroutines.CoroutineContext,
    private val recordSink: PolledRecordSink<K, V>,
    private val records: List<ConsumerRecord<K, V>>
) : ConsumerPollLoopControl {
    private val scope = CoroutineScope(coroutineContext)
    private val stopSignal = CompletableDeferred<Unit>()
    @Volatile
    private var started = false
    @Volatile
    private var running = false

    override fun start(): Job = scope.launch {
        started = true
        running = true
        try {
            for (record in records) {
                recordSink.tryEmit(record)
            }
            stopSignal.await()
        } finally {
            running = false
        }
    }

    override fun prepareForShutdown(): Deferred<Unit> {
        stopSignal.complete(Unit)
        return CompletableDeferred(Unit)
    }

    override fun stateSnapshot(): PollLoopStateSnapshot =
        PollLoopStateSnapshot(
            id = 0,
            started = started,
            running = running,
            shutdownRequested = stopSignal.isCompleted,
            assignedPartitions = records
                .map { AssignedPartitionSnapshot(it.topic(), it.partition()) }
                .distinct()
                .sortedWith(compareBy(AssignedPartitionSnapshot::topic, AssignedPartitionSnapshot::partition)),
            lastPollEpochMillis = null,
            lastPollRecordCount = records.size.takeIf { started },
            lastCommitAttemptEpochMillis = null,
            lastCommitSucceeded = null
        )
}
