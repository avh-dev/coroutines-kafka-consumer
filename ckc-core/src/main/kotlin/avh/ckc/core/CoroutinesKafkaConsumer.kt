package avh.ckc.core

import avh.ckc.core.deserialization.RecordDeserializerFactory
import avh.ckc.core.deserialization.defaultRecordDeserializerFactory
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.partition.PartitionRegistry
import avh.ckc.core.polling.ConsumerPollLoop
import avh.ckc.core.polling.ConsumerPollLoopControl
import avh.ckc.core.processing.DefaultRecordProcessingRuntime
import avh.ckc.core.processing.NoopProcessedRecordTracker
import avh.ckc.core.processing.PartitionProcessedRecordTracker
import avh.ckc.core.processing.PolledRecordSink
import avh.ckc.core.processing.ProcessedRecordTracker
import avh.ckc.core.processing.RecordProcessingLifecycle
import avh.ckc.core.processing.RecordProcessingRuntime
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.concurrent.atomic.AtomicReference
import java.util.regex.Pattern
import kotlin.concurrent.Volatile
import kotlin.coroutines.CoroutineContext

fun interface KafkaRecordHandler<K, V> {
    suspend fun process(key: K?, value: V?, rawRecord: ConsumerRecord<ByteArray, ByteArray>)
}

/**
 * Callback invoked when the user record handler fails after all configured retries are exhausted.
 *
 * Typical implementations publish the failed record to a DLT, log and skip it,
 * or trigger application-specific alerting/recovery.
 */
fun interface ProcessingFailureHandler<K, V> {
    suspend fun handle(key: K?, value: V?, rawRecord: ConsumerRecord<ByteArray, ByteArray>, error: Throwable)

    companion object {
        fun <K, V> skip(): ProcessingFailureHandler<K, V> = ProcessingFailureHandler { _, _, _, _ -> }
    }
}

private typealias PollLoopFactory<K, V> = (
    id: Int,
    parentContext: CoroutineContext,
    deliveryStrategy: DeliveryStrategy,
    commitIntervalMs: Long,
    metrics: ConsumerMetrics<K, V>,
    consumerProperties: Map<String, Any?>,
    consumerConfigAdapter: ConsumerConfigAdapter,
    topics: List<String>?,
    topicsPattern: Pattern?,
    recordSink: PolledRecordSink,
    partitionRegistry: PartitionRegistry
) -> ConsumerPollLoopControl

private typealias ProcessingRuntimeFactory<K, V> = (
    parentScope: CoroutineScope,
    deliveryStrategy: DeliveryStrategy,
    workerConcurrency: Int,
    workChannelCapacity: Int,
    processingDispatcher: CoroutineDispatcher,
    metrics: ConsumerMetrics<K, V>,
    recordDeserializerFactory: RecordDeserializerFactory<K, V>,
    handler: KafkaRecordHandler<K, V>,
    retryPolicy: RetryPolicy,
    processingFailureHandler: ProcessingFailureHandler<K, V>,
    processedRecordTracker: ProcessedRecordTracker
) -> RecordProcessingRuntime<K, V>

/**
 * Coroutine-based Kafka consumer orchestration layer.
 *
 * Coordinates:
 * - poll loop lifecycle and graceful shutdown;
 * - worker coroutine startup and failure propagation;
 * - per-worker deserializer instances;
 * - dispatch of raw Kafka records into the processing pipeline.
 *
 * Public users are expected to construct instances via [coroutinesKafkaConsumer].
 * This class keeps the low-level constructor for internal wiring and tests.
 */
class CoroutinesKafkaConsumer<K, V> internal constructor(
    private val deliveryStrategy: DeliveryStrategy,
    private val workerConcurrency: Int,
    private val consumerPollLoopConcurrency: Int,
    private val commitIntervalMs: Long,
    private val workChannelCapacity: Int,
    private val deserializationDispatcher: CoroutineDispatcher,
    private val processingDispatcher: CoroutineDispatcher,
    private val consumerProperties: Map<String, Any?>,
    private val handler: KafkaRecordHandler<K, V>,
    private val retryPolicy: RetryPolicy,
    private val metrics: ConsumerMetrics<K, V>,
    private val processingFailureHandler: ProcessingFailureHandler<K, V>,
    parentContext: CoroutineContext,
    private val topics: List<String>? = null,
    private val topicsPattern: Pattern? = null,
    private val pollLoopFactory: PollLoopFactory<K, V>,
    private val processingRuntimeFactory: ProcessingRuntimeFactory<K, V>,
    private val recordDeserializerFactory: RecordDeserializerFactory<K, V>
) {
    private val lifecycleMutex = Mutex()
    private val failure = AtomicReference<Throwable?>(null)
    private val consumerConfigAdapter = ConsumerConfigAdapter(consumerProperties)
    private val partitionRegistry = PartitionRegistry()
    private val processedRecordTracker: ProcessedRecordTracker = when (deliveryStrategy) {
        DeliveryStrategy.BACKPRESSURE -> PartitionProcessedRecordTracker(partitionRegistry)
        DeliveryStrategy.LOSSY -> NoopProcessedRecordTracker
    }
    private val scope = CoroutineScope(
        SupervisorJob(parentContext[Job]) +
                parentContext.minusKey(Job) +
                CoroutineName("CoroutinesKafkaConsumer")
    )
    private val processingRuntime: RecordProcessingRuntime<K, V> = processingRuntimeFactory(
        scope,
        deliveryStrategy,
        workerConcurrency,
        workChannelCapacity,
        processingDispatcher,
        metrics,
        recordDeserializerFactory,
        handler,
        retryPolicy,
        processingFailureHandler,
        processedRecordTracker
    )
    private val processingLifecycle: RecordProcessingLifecycle = processingRuntime
    private val pollLoops = List(consumerPollLoopConcurrency) { index ->
        pollLoopFactory(
            index,
            scope.coroutineContext,
            deliveryStrategy,
            commitIntervalMs,
            metrics,
            consumerProperties,
            consumerConfigAdapter,
            topics,
            topicsPattern,
            processingRuntime,
            partitionRegistry
        )
    }

    @Volatile
    private var started = false

    @Volatile
    private var stopped = false

    private var pollLoopJobs: List<Job> = emptyList()
    private var stopDeferred: Deferred<Unit>? = null

    init {
        require(workerConcurrency > 0) { "workerConcurrency must be > 0" }
        require(consumerPollLoopConcurrency > 0) { "consumerPollLoopConcurrency must be > 0" }
        require(commitIntervalMs > 0) { "commitIntervalMs must be > 0" }
        require(workChannelCapacity > 0) { "workChannelCapacity must be > 0" }
        require((topics == null) != (topicsPattern == null)) {
            "Exactly one of topics or topicsPattern must be specified"
        }
        require(consumerProperties.containsKey(KEY_DESERIALIZER_CLASS_CONFIG)) {
            "Kafka property '$KEY_DESERIALIZER_CLASS_CONFIG' must be specified"
        }
        require(consumerProperties.containsKey(VALUE_DESERIALIZER_CLASS_CONFIG)) {
            "Kafka property '$VALUE_DESERIALIZER_CLASS_CONFIG' must be specified"
        }
        if (deliveryStrategy == DeliveryStrategy.LOSSY) {
            require(consumerConfigAdapter.getBoolean(ENABLE_AUTO_COMMIT_CONFIG) == true) {
                "Kafka property '$ENABLE_AUTO_COMMIT_CONFIG' must be true when deliveryStrategy=LOSSY"
            }
        }
    }

    internal constructor(
        consumerProperties: Map<String, Any?>,
        deliveryStrategy: DeliveryStrategy,
        workerConcurrency: Int,
        consumerPollLoopConcurrency: Int,
        commitIntervalMs: Long,
        workChannelCapacity: Int = 1024,
        deserializationDispatcher: CoroutineDispatcher = Dispatchers.IO,
        processingDispatcher: CoroutineDispatcher = Dispatchers.Default,
        retryPolicy: RetryPolicy = RetryPolicy.none(),
        @Suppress("UNCHECKED_CAST")
        metrics: ConsumerMetrics<K, V> = ConsumerMetrics.NOOP as ConsumerMetrics<K, V>,
        processingFailureHandler: ProcessingFailureHandler<K, V> = ProcessingFailureHandler.skip(),
        parentContext: CoroutineContext = Dispatchers.Default,
        topics: List<String>? = null,
        topicsPattern: Pattern? = null,
        handler: KafkaRecordHandler<K, V>
    ) : this(
        deliveryStrategy = deliveryStrategy,
        workerConcurrency = workerConcurrency,
        consumerPollLoopConcurrency = consumerPollLoopConcurrency,
        commitIntervalMs = commitIntervalMs,
        workChannelCapacity = workChannelCapacity,
        deserializationDispatcher = deserializationDispatcher,
        processingDispatcher = processingDispatcher,
        consumerProperties = consumerProperties,
        handler = handler,
        retryPolicy = retryPolicy,
        metrics = metrics,
        processingFailureHandler = processingFailureHandler,
        parentContext = parentContext,
        topics = topics,
        topicsPattern = topicsPattern,
        pollLoopFactory = defaultPollLoopFactory(),
        processingRuntimeFactory = ::defaultProcessingRuntime,
        recordDeserializerFactory = defaultRecordDeserializerFactory<K, V>(
            consumerProperties = consumerProperties,
            dispatcher = deserializationDispatcher
        )
    )

    fun start() {
        check(!started) { "Consumer already started" }
        started = true

        processingLifecycle.start { handleFailure(it) }

        pollLoopJobs = pollLoops.map { loop ->
            loop.start().also { observeFailure(it) }
        }
    }

    private fun prepareForStop(): Deferred<Unit> = synchronized(this) {
        stopDeferred ?: scope.launchStopSequence().also { stopDeferred = it }
    }

    suspend fun stop() {
        val deferred = prepareForStop()
        deferred.await()
        failure.get()?.let { throw it }
    }

    private fun observeFailure(job: Job) {
        job.invokeOnCompletion { cause ->
            if (cause != null && !cause.isCancellation()) {
                handleFailure(cause)
            }
        }
    }

    private fun handleFailure(cause: Throwable) {
        if (failure.compareAndSet(null, cause)) {
            metrics.onConsumerFailure(cause)
        }
        processingLifecycle.close(cause)
    }

    private fun CoroutineScope.launchStopSequence(): Deferred<Unit> = async {
        lifecycleMutex.withLock {
            if (stopped) {
                return@withLock
            }
            stopped = true

            try {
                val readySignals = pollLoops.map { it.prepareForShutdown() }
                readySignals.forEach { it.await() }

                pollLoopJobs.forEach { job ->
                    if (job.isActive) {
                        job.cancelAndJoin()
                    } else {
                        job.join()
                    }
                }

                processingLifecycle.stop()
            } finally {
            }
        }
    }
}

private fun Throwable.isCancellation(): Boolean = this is CancellationException

private fun <K, V> defaultPollLoopFactory(): PollLoopFactory<K, V> =
    { id, context, deliveryStrategy, commitIntervalMs, metrics, consumerProperties, consumerConfigAdapter, loopTopics, loopTopicsPattern, recordSink, registry ->
        ConsumerPollLoop<K, V>(
            id = id,
            parentContext = context,
            deliveryStrategy = deliveryStrategy,
            commitIntervalMs = commitIntervalMs,
            metrics = metrics,
            consumerProperties = consumerProperties,
            consumerConfigAdapter = consumerConfigAdapter,
            topics = loopTopics,
            topicsPattern = loopTopicsPattern,
            recordSink = recordSink,
            partitionStateRegistry = registry
        )
    }

internal fun <K, V> defaultProcessingRuntime(
    parentScope: CoroutineScope,
    deliveryStrategy: DeliveryStrategy,
    workerConcurrency: Int,
    workChannelCapacity: Int,
    processingDispatcher: CoroutineDispatcher,
    metrics: ConsumerMetrics<K, V>,
    recordDeserializerFactory: RecordDeserializerFactory<K, V>,
    handler: KafkaRecordHandler<K, V>,
    retryPolicy: RetryPolicy,
    processingFailureHandler: ProcessingFailureHandler<K, V>,
    processedRecordTracker: ProcessedRecordTracker
): RecordProcessingRuntime<K, V> =
    DefaultRecordProcessingRuntime(
        deliveryStrategy = deliveryStrategy,
        workerConcurrency = workerConcurrency,
        workChannelCapacity = workChannelCapacity,
        processingDispatcher = processingDispatcher,
        scope = parentScope,
        metrics = metrics,
        recordDeserializerFactory = recordDeserializerFactory,
        handler = handler,
        retryPolicy = retryPolicy,
        processingFailureHandler = processingFailureHandler,
        processedRecordTracker = processedRecordTracker
    )
