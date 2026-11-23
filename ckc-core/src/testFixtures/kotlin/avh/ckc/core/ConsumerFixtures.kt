package avh.ckc.core

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.polling.ConsumerPollLoopControl
import avh.ckc.core.polling.partition.PartitionRegistry
import avh.ckc.core.processing.PolledRecordSink
import avh.ckc.core.extension.RecordAdmission
import avh.ckc.core.processing.AdmittedRecordInterceptor
import avh.ckc.core.processing.localRecordInterceptor
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import kotlin.coroutines.CoroutineContext

fun <K, V> createTestConsumer(
    records: List<ConsumerRecord<K, V>>,
    consumerProperties: Map<String, Any?>,
    handler: KafkaRecordHandler<K, V>,
    retryPolicy: RetryPolicy = RetryPolicy.none(),
    @Suppress("UNCHECKED_CAST")
    metrics: ConsumerMetrics<K, V> = ConsumerMetrics.NOOP as ConsumerMetrics<K, V>,
    processingFailureHandler: ProcessingFailureHandler<K, V> = ProcessingFailureHandler.skip(),
    recordProcessingContext: RecordProcessingContext<K, V>? = null,
    recordAdmission: RecordAdmission<K, V>? = null,
    interceptor: AdmittedRecordInterceptor<K, V> = localRecordInterceptor(),
    beforeAdmission: suspend (ConsumerRecord<K, V>) -> Unit = {},
    workerConcurrency: Int = 1,
    parentContext: CoroutineContext = kotlinx.coroutines.Dispatchers.Default,
    runtime: TestConsumerRuntime = testRuntime(processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING)
): CoroutinesKafkaConsumer<K, V> =
    CoroutinesKafkaConsumer(
        consumerProperties = consumerProperties,
        processingMode = runtime.processingMode,
        workerConcurrency = workerConcurrency,
        consumerPollLoopConcurrency = runtime.consumerPollLoopConcurrency,
        commitIntervalMs = runtime.commitIntervalMs,
        commitRecordsThreshold = runtime.commitRecordsThreshold,
        workChannelCapacity = runtime.workChannelCapacity,
        freshnessMaxRecordAge = runtime.freshnessMaxRecordAge,
        processingDispatcher = runtime.processingDispatcher,
        retryPolicy = retryPolicy,
        metrics = metrics,
        recordProcessingContext = recordProcessingContext,
        processingFailureHandler = processingFailureHandler,
        parentContext = parentContext,
        topics = listOf("topic-a"),
        topicsPattern = null,
        recordAdmission = recordAdmission,
        admittedRecordInterceptor = interceptor,
        handler = handler,
        pollLoopFactory = { _, context, processingMode, _, _, pollMetrics, _, _, _, _, recordSink, registry, _ ->
            FakePollLoopControl(context, processingMode, pollMetrics, recordSink, records, registry, beforeAdmission)
        },
        processingRuntimeFactory = ::defaultProcessingRuntime
    )

private class FakePollLoopControl<K, V>(
    coroutineContext: kotlin.coroutines.CoroutineContext,
    private val processingMode: ProcessingMode,
    private val metrics: ConsumerMetrics<K, V>,
    private val recordSink: PolledRecordSink<K, V>,
    private val records: List<ConsumerRecord<K, V>>,
    private val partitionRegistry: PartitionRegistry,
    private val beforeAdmission: suspend (ConsumerRecord<K, V>) -> Unit
) : ConsumerPollLoopControl {
    private val scope = CoroutineScope(coroutineContext)
    private val stopSignal = CompletableDeferred<Unit>()
    private val readinessSignal = CompletableDeferred<Unit>()
    @Volatile
    private var started = false
    @Volatile
    private var running = false

    override fun start(): Job = scope.launch {
        started = true
        running = true
        try {
            // Mirror the production admission path so ownership-gated runtimes receive valid leases.
            val recordsByPartition = records.groupBy { TopicPartition(it.topic(), it.partition()) }
            partitionRegistry.onPartitionsAssigned(recordsByPartition.keys).forEach { state ->
                state.init(recordsByPartition.getValue(state.topicPartition).minOf { it.offset() })
            }
            readinessSignal.complete(Unit)
            metrics.onPoll(records.size, 0L)
            for (record in records) {
                beforeAdmission(record)
                partitionRegistry.observe(
                    record,
                    observeOffset = processingMode.tracksProcessedOffsets(),
                    acceptOffset = processingMode.isFreshnessFirst()
                )
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

    override fun readiness(): Deferred<Unit> = readinessSignal

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
