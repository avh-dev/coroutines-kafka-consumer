package avh.ckc.core

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
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Deserializer
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

internal interface ConsumerPollLoopControl {
    fun start(): Job
    fun prepareForShutdown(): Deferred<Unit>
}

private class ConsumerPollLoopControlAdapter(
    private val delegate: ConsumerPollLoop
) : ConsumerPollLoopControl {
    override fun start(): Job = delegate.start()

    override fun prepareForShutdown(): Deferred<Unit> = delegate.prepareForShutdown()
}

internal data class WorkerDeserializers<K, V>(
    val keyDeserializer: Deserializer<K>,
    val valueDeserializer: Deserializer<V>
)

private typealias PollLoopFactory = (
    id: Int,
    parentContext: CoroutineContext,
    overflowStrategy: OverflowStrategy,
    commitIntervalMs: Long,
    consumerProperties: Map<String, Any?>,
    consumerConfigAdapter: ConsumerConfigAdapter,
    topics: List<String>?,
    topicsPattern: Pattern?,
    workChannel: SendChannel<ConsumerRecord<ByteArray, ByteArray>>,
    partitionRegistry: PartitionRegistry
) -> ConsumerPollLoopControl

internal typealias WorkerDeserializerFactory<K, V> = (workerIndex: Int) -> WorkerDeserializers<K, V>

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
    private val overflowStrategy: OverflowStrategy,
    private val workerConcurrency: Int,
    private val consumerPollLoopConcurrency: Int,
    private val commitIntervalMs: Long,
    private val workChannelCapacity: Int,
    private val deserializationDispatcher: CoroutineDispatcher,
    private val processingDispatcher: CoroutineDispatcher,
    private val consumerProperties: Map<String, Any?>,
    private val handler: KafkaRecordHandler<K, V>,
    private val retryPolicy: RetryPolicy,
    private val processingFailureHandler: ProcessingFailureHandler<K, V>,
    parentContext: CoroutineContext,
    private val topics: List<String>? = null,
    private val topicsPattern: Pattern? = null,
    private val pollLoopFactory: PollLoopFactory,
    private val workerDeserializerFactory: WorkerDeserializerFactory<K, V>
) {
    private val lifecycleMutex = Mutex()
    private val failure = AtomicReference<Throwable?>(null)
    private val consumerConfigAdapter = ConsumerConfigAdapter(consumerProperties)
    private val partitionRegistry = PartitionRegistry()
    private val recordProcessor = RecordProcessor(
        overflowStrategy = overflowStrategy,
        deserializationDispatcher = deserializationDispatcher,
        handler = handler,
        retryPolicy = retryPolicy,
        processingFailureHandler = processingFailureHandler,
        partitionRegistry = partitionRegistry
    )
    private val workChannel = Channel<ConsumerRecord<ByteArray, ByteArray>>(
        capacity = workChannelCapacity,
        onBufferOverflow = when (overflowStrategy) {
            OverflowStrategy.BACKPRESSURE -> BufferOverflow.SUSPEND
            OverflowStrategy.THROTTLING -> BufferOverflow.DROP_OLDEST
        }
    )
    private val scope = CoroutineScope(
        SupervisorJob(parentContext[Job]) +
                parentContext.minusKey(Job) +
                CoroutineName("CoroutinesKafkaConsumer")
    )
    private val pollLoops = List(consumerPollLoopConcurrency) { index ->
        pollLoopFactory(
            index,
            scope.coroutineContext,
            overflowStrategy,
            commitIntervalMs,
            consumerProperties,
            consumerConfigAdapter,
            topics,
            topicsPattern,
            workChannel,
            partitionRegistry
        )
    }

    @Volatile
    private var started = false

    @Volatile
    private var stopped = false

    private var pollLoopJobs: List<Job> = emptyList()
    private var workerJobs: List<Job> = emptyList()
    private var workerDeserializers: List<WorkerDeserializers<K, V>> = emptyList()
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
    }

    internal constructor(
        consumerProperties: Map<String, Any?>,
        overflowStrategy: OverflowStrategy,
        workerConcurrency: Int,
        consumerPollLoopConcurrency: Int,
        commitIntervalMs: Long,
        workChannelCapacity: Int = 1024,
        deserializationDispatcher: CoroutineDispatcher = Dispatchers.IO,
        processingDispatcher: CoroutineDispatcher = Dispatchers.Default,
        retryPolicy: RetryPolicy = RetryPolicy.none(),
        processingFailureHandler: ProcessingFailureHandler<K, V> = ProcessingFailureHandler.skip(),
        parentContext: CoroutineContext = Dispatchers.Default,
        topics: List<String>? = null,
        topicsPattern: Pattern? = null,
        handler: KafkaRecordHandler<K, V>
    ) : this(
        overflowStrategy = overflowStrategy,
        workerConcurrency = workerConcurrency,
        consumerPollLoopConcurrency = consumerPollLoopConcurrency,
        commitIntervalMs = commitIntervalMs,
        workChannelCapacity = workChannelCapacity,
        deserializationDispatcher = deserializationDispatcher,
        processingDispatcher = processingDispatcher,
        consumerProperties = consumerProperties,
        handler = handler,
        retryPolicy = retryPolicy,
        processingFailureHandler = processingFailureHandler,
        parentContext = parentContext,
        topics = topics,
        topicsPattern = topicsPattern,
        pollLoopFactory = defaultPollLoopFactory(),
        workerDeserializerFactory = defaultWorkerDeserializerFactory(consumerProperties)
    )

    fun start() {
        check(!started) { "Consumer already started" }
        started = true

        workerDeserializers = try {
            List(workerConcurrency) { workerIndex ->
                workerDeserializerFactory(workerIndex)
            }
        } catch (error: Throwable) {
            workerDeserializers.closeAll()
            throw error
        }

        workerJobs = List(workerConcurrency) { workerIndex ->
            val deserializers = workerDeserializers[workerIndex]
            scope.launch(processingDispatcher + CoroutineName("KafkaWorker-$workerIndex")) {
                for (record in workChannel) {
                    recordProcessor.process(record, deserializers)
                }
            }.also { observeFailure(it) }
        }

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
                failure.compareAndSet(null, cause)
                workChannel.close(cause)
            }
        }
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

                workChannel.close()
                workerJobs.joinAll()
            } finally {
                workerDeserializers.closeAll()
            }
        }
    }
}

private fun Throwable.isCancellation(): Boolean = this is CancellationException

private fun defaultPollLoopFactory(): PollLoopFactory =
    { id, context, overflowStrategy, commitIntervalMs, consumerProperties, consumerConfigAdapter, loopTopics, loopTopicsPattern, channel, registry ->
        ConsumerPollLoopControlAdapter(
            ConsumerPollLoop(
                id = id,
                parentContext = context,
                overflowStrategy = overflowStrategy,
                commitIntervalMs = commitIntervalMs,
                consumerProperties = consumerProperties,
                consumerConfigAdapter = consumerConfigAdapter,
                topics = loopTopics,
                topicsPattern = loopTopicsPattern,
                workChannel = channel,
                partitionStateRegistry = registry
            )
        )
    }
