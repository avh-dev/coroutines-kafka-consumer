package avh.ckc.core

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerRecord

internal fun <K, V> createTestConsumer(
    records: List<ConsumerRecord<ByteArray, ByteArray>>,
    consumerProperties: Map<String, Any?>,
    handler: KafkaRecordHandler<K, V>,
    retryPolicy: RetryPolicy = RetryPolicy.none(),
    processingFailureHandler: ProcessingFailureHandler<K, V> = ProcessingFailureHandler.skip(),
    workerConcurrency: Int = 1,
    runtime: TestConsumerRuntime = testRuntime(strategy = OverflowStrategy.BACKPRESSURE),
    workerDeserializerFactory: WorkerDeserializerFactory<K, V> = defaultWorkerDeserializerFactoryForTests(consumerProperties)
): CoroutinesKafkaConsumer<K, V> =
    CoroutinesKafkaConsumer(
        consumerProperties = consumerProperties,
        overflowStrategy = runtime.overflowStrategy,
        workerConcurrency = workerConcurrency,
        consumerPollLoopConcurrency = runtime.consumerPollLoopConcurrency,
        commitIntervalMs = runtime.commitIntervalMs,
        workChannelCapacity = runtime.workChannelCapacity,
        deserializationDispatcher = runtime.deserializationDispatcher,
        processingDispatcher = runtime.processingDispatcher,
        retryPolicy = retryPolicy,
        processingFailureHandler = processingFailureHandler,
        parentContext = kotlinx.coroutines.Dispatchers.Default,
        topics = listOf("topic-a"),
        topicsPattern = null,
        handler = handler,
        pollLoopFactory = { _, context, _, _, _, _, _, _, channel, _ ->
            FakePollLoopControl(context, channel, records)
        },
        workerDeserializerFactory = workerDeserializerFactory
    )

internal class FakePollLoopControl(
    coroutineContext: kotlin.coroutines.CoroutineContext,
    private val workChannel: SendChannel<ConsumerRecord<ByteArray, ByteArray>>,
    private val records: List<ConsumerRecord<ByteArray, ByteArray>>
) : ConsumerPollLoopControl {
    private val scope = CoroutineScope(coroutineContext)
    private val stopSignal = CompletableDeferred<Unit>()

    override fun start(): Job = scope.launch {
        for (record in records) {
            workChannel.send(record)
        }
        stopSignal.await()
    }

    override fun prepareForShutdown(): Deferred<Unit> {
        stopSignal.complete(Unit)
        return CompletableDeferred(Unit)
    }
}
