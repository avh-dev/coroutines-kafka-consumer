package avh.ckc.core

import avh.ckc.core.kafka.KafkaConsumerConfigAdapter
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.polling.partition.PartitionRegistry
import avh.ckc.core.polling.ConsumerPollLoop
import avh.ckc.core.polling.ConsumerPollLoopControl
import avh.ckc.core.processing.NoopProcessedRecordTracker
import avh.ckc.core.processing.PartitionProcessedRecordTracker
import avh.ckc.core.processing.PolledRecordSink
import avh.ckc.core.processing.ProcessedRecordTracker
import avh.ckc.core.processing.RecordProcessingLifecycle
import avh.ckc.core.processing.RecordProcessingRuntime
import avh.ckc.core.processing.runtime.AtLeastOnceOrderedRecordProcessingRuntime
import avh.ckc.core.processing.runtime.AtLeastOnceUnorderedRecordProcessingRuntime
import avh.ckc.core.processing.runtime.FreshnessFirstByKeyRecordProcessingRuntime
import avh.ckc.core.processing.runtime.FreshnessFirstUnorderedRecordProcessingRuntime
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
    suspend fun process(record: ConsumerRecord<K, V>)
}

/**
 * Callback invoked when the user record handler fails after all configured retries are exhausted.
 *
 * Typical implementations publish the failed record to a DLT, log and skip it,
 * or trigger application-specific alerting/recovery.
 */
fun interface ProcessingFailureHandler<K, V> {
    suspend fun handle(record: ConsumerRecord<K, V>, error: Throwable)

    companion object {
        fun <K, V> skip(): ProcessingFailureHandler<K, V> = ProcessingFailureHandler { _, _ -> }
    }
}

private typealias PollLoopFactory<K, V> = (
    id: Int,
    parentContext: CoroutineContext,
    processingMode: ProcessingMode,
    commitIntervalMs: Long,
    metrics: ConsumerMetrics<K, V>,
    consumerProperties: Map<String, Any?>,
    consumerConfigAdapter: KafkaConsumerConfigAdapter,
    topics: List<String>?,
    topicsPattern: Pattern?,
    recordSink: PolledRecordSink<K, V>,
    partitionRegistry: PartitionRegistry
) -> ConsumerPollLoopControl

private typealias ProcessingRuntimeFactory<K, V> = (
    parentScope: CoroutineScope,
    processingMode: ProcessingMode,
    workerConcurrency: Int,
    workChannelCapacity: Int,
    processingDispatcher: CoroutineDispatcher,
    metrics: ConsumerMetrics<K, V>,
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
 * - dispatch of typed Kafka records into the processing pipeline.
 *
 * Public users are expected to construct instances via [coroutinesKafkaConsumer].
 * This class keeps the low-level constructor for internal wiring and tests.
 */
class CoroutinesKafkaConsumer<K, V> internal constructor(
    private val processingMode: ProcessingMode,
    private val workerConcurrency: Int,
    private val consumerPollLoopConcurrency: Int,
    private val commitIntervalMs: Long,
    private val workChannelCapacity: Int,
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
    private val processingRuntimeFactory: ProcessingRuntimeFactory<K, V>
) {
    private val lifecycleMutex = Mutex()
    private val failure = AtomicReference<Throwable?>(null)
    private val consumerConfigAdapter = KafkaConsumerConfigAdapter(consumerProperties)
    private val partitionRegistry = PartitionRegistry()
    private val processedRecordTracker: ProcessedRecordTracker = when (processingMode) {
        ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
        ProcessingMode.AT_LEAST_ONCE_KEY_ORDERING,
        ProcessingMode.AT_LEAST_ONCE_PARTITION_ORDERING -> PartitionProcessedRecordTracker(partitionRegistry)
        ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST,
        ProcessingMode.FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY -> NoopProcessedRecordTracker
    }
    private val scope = CoroutineScope(
        SupervisorJob(parentContext[Job]) +
                parentContext.minusKey(Job) +
                CoroutineName("CoroutinesKafkaConsumer")
    )
    private val processingRuntime: RecordProcessingRuntime<K, V> = processingRuntimeFactory(
        scope,
        processingMode,
        workerConcurrency,
        workChannelCapacity,
        processingDispatcher,
        metrics,
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
            processingMode,
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
        if (processingMode == ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST || processingMode == ProcessingMode.FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY) {
            require(consumerConfigAdapter.getBoolean(ENABLE_AUTO_COMMIT_CONFIG) == true) {
                "Kafka property '$ENABLE_AUTO_COMMIT_CONFIG' must be true when processingMode=$processingMode"
            }
        }
    }

    internal constructor(
        consumerProperties: Map<String, Any?>,
        processingMode: ProcessingMode,
        workerConcurrency: Int,
        consumerPollLoopConcurrency: Int,
        commitIntervalMs: Long,
        workChannelCapacity: Int = 1024,
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
        processingMode = processingMode,
        workerConcurrency = workerConcurrency,
        consumerPollLoopConcurrency = consumerPollLoopConcurrency,
        commitIntervalMs = commitIntervalMs,
        workChannelCapacity = workChannelCapacity,
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
        processingRuntimeFactory = ::defaultProcessingRuntime
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

    fun stateSnapshot(): ConsumerStateSnapshot {
        val currentFailure = failure.get()
        return ConsumerStateSnapshot(
            started = started,
            stopped = stopped,
            failed = currentFailure != null,
            failureClass = currentFailure?.javaClass?.name,
            failureMessage = currentFailure?.message,
            processingMode = processingMode,
            workerConcurrency = workerConcurrency,
            consumerPollLoopConcurrency = consumerPollLoopConcurrency,
            workChannelCapacity = workChannelCapacity,
            processing = processingRuntime.stateSnapshot(),
            pollLoops = pollLoops.map { it.stateSnapshot() }
        )
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
    { id, context, processingMode, commitIntervalMs, metrics, consumerProperties, consumerConfigAdapter, loopTopics, loopTopicsPattern, recordSink, registry ->
        ConsumerPollLoop<K, V>(
            id = id,
            parentContext = context,
            processingMode = processingMode,
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
    processingMode: ProcessingMode,
    workerConcurrency: Int,
    workChannelCapacity: Int,
    processingDispatcher: CoroutineDispatcher,
    metrics: ConsumerMetrics<K, V>,
    handler: KafkaRecordHandler<K, V>,
    retryPolicy: RetryPolicy,
    processingFailureHandler: ProcessingFailureHandler<K, V>,
    processedRecordTracker: ProcessedRecordTracker
): RecordProcessingRuntime<K, V> =
    when (processingMode) {
        ProcessingMode.AT_LEAST_ONCE_NO_ORDERING -> AtLeastOnceUnorderedRecordProcessingRuntime(
            workerConcurrency = workerConcurrency,
            workChannelCapacity = workChannelCapacity,
            processingDispatcher = processingDispatcher,
            scope = parentScope,
            metrics = metrics,
            handler = handler,
            retryPolicy = retryPolicy,
            processingFailureHandler = processingFailureHandler,
            processedRecordTracker = processedRecordTracker
        )

        ProcessingMode.AT_LEAST_ONCE_KEY_ORDERING -> AtLeastOnceOrderedRecordProcessingRuntime(
            workerConcurrency = workerConcurrency,
            workChannelCapacity = workChannelCapacity,
            ordering = AtLeastOnceOrderedRecordProcessingRuntime.Ordering.BY_KEY,
            processingDispatcher = processingDispatcher,
            scope = parentScope,
            metrics = metrics,
            handler = handler,
            retryPolicy = retryPolicy,
            processingFailureHandler = processingFailureHandler,
            processedRecordTracker = processedRecordTracker
        )

        ProcessingMode.AT_LEAST_ONCE_PARTITION_ORDERING -> AtLeastOnceOrderedRecordProcessingRuntime(
            workerConcurrency = workerConcurrency,
            workChannelCapacity = workChannelCapacity,
            ordering = AtLeastOnceOrderedRecordProcessingRuntime.Ordering.BY_PARTITION,
            processingDispatcher = processingDispatcher,
            scope = parentScope,
            metrics = metrics,
            handler = handler,
            retryPolicy = retryPolicy,
            processingFailureHandler = processingFailureHandler,
            processedRecordTracker = processedRecordTracker
        )

        ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST -> FreshnessFirstUnorderedRecordProcessingRuntime(
            workerConcurrency = workerConcurrency,
            workChannelCapacity = workChannelCapacity,
            processingDispatcher = processingDispatcher,
            scope = parentScope,
            metrics = metrics,
            handler = handler,
            retryPolicy = retryPolicy,
            processingFailureHandler = processingFailureHandler,
            processedRecordTracker = processedRecordTracker
        )

        ProcessingMode.FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY -> FreshnessFirstByKeyRecordProcessingRuntime(
            workerConcurrency = workerConcurrency,
            workChannelCapacity = workChannelCapacity,
            processingDispatcher = processingDispatcher,
            scope = parentScope,
            metrics = metrics,
            handler = handler,
            retryPolicy = retryPolicy,
            processingFailureHandler = processingFailureHandler,
            processedRecordTracker = processedRecordTracker
        )
    }
