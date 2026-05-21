package avh.ckc.core

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.deserialization.RecordDeserializerFactory
import avh.ckc.core.deserialization.defaultRecordDeserializerFactory
import avh.ckc.core.polling.ConsumerPollLoopControl
import avh.ckc.core.processing.PolledRecordSink
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerRecord

internal fun <K, V> createTestConsumer(
    records: List<ConsumerRecord<ByteArray, ByteArray>>,
    consumerProperties: Map<String, Any?>,
    handler: KafkaRecordHandler<K, V>,
    retryPolicy: RetryPolicy = RetryPolicy.none(),
    @Suppress("UNCHECKED_CAST")
    metrics: ConsumerMetrics<K, V> = ConsumerMetrics.NOOP as ConsumerMetrics<K, V>,
    processingFailureHandler: ProcessingFailureHandler<K, V> = ProcessingFailureHandler.skip(),
    workerConcurrency: Int = 1,
    runtime: TestConsumerRuntime = testRuntime(processingMode = ProcessingMode.AT_LEAST_ONCE_UNORDERED),
    recordDeserializerFactory: RecordDeserializerFactory<K, V> =
        defaultRecordDeserializerFactory(consumerProperties, runtime.deserializationDispatcher)
): CoroutinesKafkaConsumer<K, V> =
    CoroutinesKafkaConsumer(
        consumerProperties = consumerProperties,
        processingMode = runtime.processingMode,
        workerConcurrency = workerConcurrency,
        consumerPollLoopConcurrency = runtime.consumerPollLoopConcurrency,
        commitIntervalMs = runtime.commitIntervalMs,
        workChannelCapacity = runtime.workChannelCapacity,
        deserializationDispatcher = runtime.deserializationDispatcher,
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
        processingRuntimeFactory = ::defaultProcessingRuntime,
        recordDeserializerFactory = recordDeserializerFactory
    )

internal class FakePollLoopControl(
    coroutineContext: kotlin.coroutines.CoroutineContext,
    private val recordSink: PolledRecordSink,
    private val records: List<ConsumerRecord<ByteArray, ByteArray>>
) : ConsumerPollLoopControl {
    private val scope = CoroutineScope(coroutineContext)
    private val stopSignal = CompletableDeferred<Unit>()

    override fun start(): Job = scope.launch {
        for (record in records) {
            recordSink.tryEmit(record)
        }
        stopSignal.await()
    }

    override fun prepareForShutdown(): Deferred<Unit> {
        stopSignal.complete(Unit)
        return CompletableDeferred(Unit)
    }
}
