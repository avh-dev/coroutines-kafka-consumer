package avh.ckc.core

import avh.ckc.core.kafka.KafkaConsumerConfigAdapter
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.LifecycleOutcome
import avh.ckc.core.polling.partition.PartitionRegistry
import avh.ckc.core.polling.ConsumerPollLoop
import avh.ckc.core.polling.ConsumerPollLoopControl
import avh.ckc.core.processing.NoopProcessedRecordTracker
import avh.ckc.core.processing.AdmittedRecordInterceptor
import avh.ckc.core.processing.PartitionProcessedRecordTracker
import avh.ckc.core.processing.PolledRecordSink
import avh.ckc.core.processing.ProcessedRecordTracker
import avh.ckc.core.processing.RecordProcessingLifecycle
import avh.ckc.core.processing.RecordProcessingRuntime
import avh.ckc.core.processing.localRecordInterceptor
import avh.ckc.core.processing.runtime.AtLeastOnceOrderingRecordProcessingRuntime
import avh.ckc.core.processing.runtime.AtLeastOnceNoOrderingRecordProcessingRuntime
import avh.ckc.core.processing.runtime.FreshnessFirstReplacePendingByKeyRecordProcessingRuntime
import avh.ckc.core.processing.runtime.FreshnessFirstDropOldestRecordProcessingRuntime
import avh.ckc.core.warmup.WarmupRecipientSessionFilteringSink
import avh.ckc.core.warmup.WarmupSessionId
import avh.ckc.core.warmup.WarmupSourcePartition
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import java.util.concurrent.atomic.AtomicReference
import java.util.regex.Pattern
import kotlin.concurrent.Volatile
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

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
    commitRecordsThreshold: Int,
    metrics: ConsumerMetrics<K, V>,
    consumerProperties: Map<String, Any?>,
    consumerConfigAdapter: KafkaConsumerConfigAdapter,
    topics: List<String>?,
    topicsPattern: Pattern?,
    recordSink: PolledRecordSink<K, V>,
    partitionRegistry: PartitionRegistry,
    fixedAssignment: Map<TopicPartition, Long>?
) -> ConsumerPollLoopControl

private typealias ProcessingRuntimeFactory<K, V> = (
    parentScope: CoroutineScope,
    processingMode: ProcessingMode,
    workerConcurrency: Int,
    workChannelCapacity: Int,
    freshnessMaxRecordAge: Duration?,
    processingDispatcher: CoroutineDispatcher,
    metrics: ConsumerMetrics<K, V>,
    handler: KafkaRecordHandler<K, V>,
    retryPolicy: RetryPolicy,
    recordProcessingContext: RecordProcessingContext<K, V>?,
    processingFailureHandler: ProcessingFailureHandler<K, V>,
    processedRecordTracker: ProcessedRecordTracker,
    interceptor: AdmittedRecordInterceptor<K, V>
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
    private val commitRecordsThreshold: Int,
    private val maxTrackedOffsetsPerPartition: Int = 1_048_576,
    private val workChannelCapacity: Int,
    private val freshnessMaxRecordAge: Duration?,
    private val partitionHandoffTimeout: Duration = 30.seconds,
    private val shutdownDrainTimeout: Duration = 30.seconds,
    private val processingCancellationTimeout: Duration = 30.seconds,
    private val recoveryTimeout: Duration = 120.seconds,
    private val recoveryRetryBackoff: Duration = 1.seconds,
    private val processingDispatcher: CoroutineDispatcher,
    consumerProperties: Map<String, Any?>,
    private val handler: KafkaRecordHandler<K, V>,
    private val retryPolicy: RetryPolicy,
    private val metrics: ConsumerMetrics<K, V>,
    private val recordProcessingContext: RecordProcessingContext<K, V>?,
    private val processingFailureHandler: ProcessingFailureHandler<K, V>,
    parentContext: CoroutineContext,
    private val topics: List<String>? = null,
    private val topicsPattern: Pattern? = null,
    private val admittedRecordInterceptor: AdmittedRecordInterceptor<K, V> = localRecordInterceptor(),
    private val warmupRecipientSessionId: WarmupSessionId? = null,
    private val fixedAssignment: Map<TopicPartition, Long>? = null,
    private val pollLoopFactory: PollLoopFactory<K, V>,
    private val processingRuntimeFactory: ProcessingRuntimeFactory<K, V>
) {
    private val effectiveConsumerProperties = consumerProperties.withExplicitCommitManagement()
    private val lifecycleMutex = Mutex()
    private val failure = AtomicReference<Throwable?>(null)
    private val terminalFailureSignal = CompletableDeferred<Unit>()
    private val termination = CompletableDeferred<Throwable?>()
    private val shutdownCompletion = CompletableDeferred<Unit>()
    private val consumerConfigAdapter = KafkaConsumerConfigAdapter(effectiveConsumerProperties)
    private val partitionRegistry = PartitionRegistry(maxTrackedOffsetsPerPartition)
    // Freshness-first needs ownership generation checks, but must not retain/advance an offset tracker.
    private val processedRecordTracker: ProcessedRecordTracker = PartitionProcessedRecordTracker(
        partitionRegistry = partitionRegistry,
        trackOffsets = processingMode.tracksProcessedOffsets()
    )
    private val lifecycleJob = SupervisorJob(parentContext[Job])
    private val scope = CoroutineScope(
        lifecycleJob +
                parentContext.minusKey(Job) +
                CoroutineName("CoroutinesKafkaConsumer")
    )
    private val shutdownLifecycleJob = SupervisorJob()
    private val shutdownScope = CoroutineScope(
        shutdownLifecycleJob +
                parentContext.minusKey(Job) +
                CoroutineName("CoroutinesKafkaConsumerShutdown")
    )
    private val processingRuntime: RecordProcessingRuntime<K, V> = processingRuntimeFactory(
        scope,
        processingMode,
        workerConcurrency,
        workChannelCapacity,
        freshnessMaxRecordAge,
        processingDispatcher,
        metrics,
        handler,
        retryPolicy,
        recordProcessingContext,
        processingFailureHandler,
        processedRecordTracker,
        admittedRecordInterceptor
    )
    private val processingLifecycle: RecordProcessingLifecycle = processingRuntime
    private val recordSink: PolledRecordSink<K, V> = warmupRecipientSessionId?.let { sessionId ->
        WarmupRecipientSessionFilteringSink(sessionId, processingRuntime, processedRecordTracker)
    } ?: processingRuntime
    private val pollLoops = List(consumerPollLoopConcurrency) { index ->
        pollLoopFactory(
            index,
            scope.coroutineContext,
            processingMode,
            commitIntervalMs,
            commitRecordsThreshold,
            metrics,
            effectiveConsumerProperties,
            consumerConfigAdapter,
            topics,
            topicsPattern,
            recordSink,
            partitionRegistry,
            fixedAssignment
        )
    }

    @Volatile
    private var started = false

    @Volatile
    private var stopped = false

    @Volatile
    private var startCompleted = false

    @Volatile
    private var pollLoopJobs: List<Job> = emptyList()
    private var stopDeferred: Deferred<Unit>? = null

    init {
        require(workerConcurrency > 0) { "workerConcurrency must be > 0" }
        require(consumerPollLoopConcurrency > 0) { "consumerPollLoopConcurrency must be > 0" }
        require(commitIntervalMs > 0) { "commitIntervalMs must be > 0" }
        require(commitRecordsThreshold > 0) { "commitRecordsThreshold must be > 0" }
        require(
            maxTrackedOffsetsPerPartition >= 128 &&
                maxTrackedOffsetsPerPartition and (maxTrackedOffsetsPerPartition - 1) == 0
        ) {
            "maxTrackedOffsetsPerPartition must be a power of two and >= 128"
        }
        require(workChannelCapacity > 0) { "workChannelCapacity must be > 0" }
        freshnessMaxRecordAge?.let {
            require(it.isPositive()) { "freshnessMaxRecordAge must be > 0" }
            require(processingMode.isFreshnessFirst()) {
                "freshnessMaxRecordAge is supported only for freshness-first processing modes"
            }
        }
        listOf(
            "partitionHandoffTimeout" to partitionHandoffTimeout,
            "shutdownDrainTimeout" to shutdownDrainTimeout,
            "processingCancellationTimeout" to processingCancellationTimeout,
            "recoveryTimeout" to recoveryTimeout,
            "recoveryRetryBackoff" to recoveryRetryBackoff
        ).forEach { (name, value) -> require(value.isPositive()) { "$name must be > 0" } }
        val effectiveMaxPollIntervalMs = consumerProperties["max.poll.interval.ms"]?.toString()?.toLongOrNull() ?: 300_000L
        require(partitionHandoffTimeout.inWholeMilliseconds < effectiveMaxPollIntervalMs) {
            "partitionHandoffTimeout must be less than effective max.poll.interval.ms"
        }
        pollLoops.forEach {
            it.configurePartitionHandoffTimeout(partitionHandoffTimeout)
            it.configureShutdownDrainTimeout(shutdownDrainTimeout)
            it.configureProcessingCancellationTimeout(processingCancellationTimeout)
            it.configureRecovery(recoveryTimeout, recoveryRetryBackoff)
        }
        require((topics == null) != (topicsPattern == null)) {
            "Exactly one of topics or topicsPattern must be specified"
        }
        fixedAssignment?.let { assignment ->
            require(consumerPollLoopConcurrency == 1) { "Fixed assignment requires exactly one poll loop" }
            require(assignment.isNotEmpty() && assignment.values.all { it >= 0 }) {
                "Fixed assignment requires non-negative start offsets"
            }
            require(topicsPattern == null && topics?.toSet() == assignment.keys.map { it.topic() }.toSet()) {
                "Fixed assignment must match the configured topics"
            }
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
        processingMode: ProcessingMode,
        workerConcurrency: Int,
        consumerPollLoopConcurrency: Int,
        commitIntervalMs: Long,
        commitRecordsThreshold: Int = 1_000,
        maxTrackedOffsetsPerPartition: Int = 1_048_576,
        workChannelCapacity: Int = 1024,
        freshnessMaxRecordAge: Duration? = null,
        partitionHandoffTimeout: Duration = 30.seconds,
        shutdownDrainTimeout: Duration = 30.seconds,
        processingCancellationTimeout: Duration = 30.seconds,
        recoveryTimeout: Duration = 120.seconds,
        recoveryRetryBackoff: Duration = 1.seconds,
        processingDispatcher: CoroutineDispatcher = Dispatchers.Default,
        retryPolicy: RetryPolicy = RetryPolicy.none(),
        @Suppress("UNCHECKED_CAST")
        metrics: ConsumerMetrics<K, V> = ConsumerMetrics.NOOP as ConsumerMetrics<K, V>,
        recordProcessingContext: RecordProcessingContext<K, V>? = null,
        processingFailureHandler: ProcessingFailureHandler<K, V> = ProcessingFailureHandler.skip(),
        parentContext: CoroutineContext = Dispatchers.Default,
        topics: List<String>? = null,
        topicsPattern: Pattern? = null,
        admittedRecordInterceptor: AdmittedRecordInterceptor<K, V> = localRecordInterceptor(),
        warmupRecipientSessionId: WarmupSessionId? = null,
        fixedAssignment: Map<TopicPartition, Long>? = null,
        handler: KafkaRecordHandler<K, V>
    ) : this(
        processingMode = processingMode,
        workerConcurrency = workerConcurrency,
        consumerPollLoopConcurrency = consumerPollLoopConcurrency,
        commitIntervalMs = commitIntervalMs,
        commitRecordsThreshold = commitRecordsThreshold,
        maxTrackedOffsetsPerPartition = maxTrackedOffsetsPerPartition,
        workChannelCapacity = workChannelCapacity,
        freshnessMaxRecordAge = freshnessMaxRecordAge,
        partitionHandoffTimeout = partitionHandoffTimeout,
        shutdownDrainTimeout = shutdownDrainTimeout,
        processingCancellationTimeout = processingCancellationTimeout,
        recoveryTimeout = recoveryTimeout,
        recoveryRetryBackoff = recoveryRetryBackoff,
        processingDispatcher = processingDispatcher,
        consumerProperties = consumerProperties,
        handler = handler,
        retryPolicy = retryPolicy,
        metrics = metrics,
        recordProcessingContext = recordProcessingContext,
        processingFailureHandler = processingFailureHandler,
        parentContext = parentContext,
        topics = topics,
        topicsPattern = topicsPattern,
        admittedRecordInterceptor = admittedRecordInterceptor,
        warmupRecipientSessionId = warmupRecipientSessionId,
        fixedAssignment = fixedAssignment,
        pollLoopFactory = defaultPollLoopFactory(),
        processingRuntimeFactory = ::defaultProcessingRuntime
    )

    fun start() {
        check(!started) { "Consumer already started" }
        started = true

        try {
            processingLifecycle.start { handleFailure(it) }

            pollLoops.forEach { loop ->
                val job = loop.start().also { observeFailure(it) }
                pollLoopJobs = pollLoopJobs + job
            }
        } catch (error: Throwable) {
            handleFailure(error)
            throw error
        } finally {
            startCompleted = true
            // A worker or already-started poll loop may fail while the remaining loops start.
            if (failure.get() != null) {
                pollLoopJobs.forEach { it.cancel() }
                prepareForStop()
            }
        }
    }

    /** Waits until every poll loop has established its first Kafka assignment and initial positions. */
    suspend fun awaitReady() {
        check(started) { "Consumer must be started before awaiting readiness" }
        try {
            pollLoops.map { it.readiness() }.awaitAll()
        } catch (error: Throwable) {
            failure.get()?.let { throw it }
            throw error
        }
        failure.get()?.let { throw it }
    }

    internal fun currentWarmupSourceOwnership(topic: String): Set<WarmupSourcePartition>? {
        val pollLoopReadiness = pollLoops.map { it.readiness() }
        if (!started || stopped || failure.get() != null ||
            pollLoopReadiness.any { !it.isCompleted || it.isCancelled }
        ) return null
        return partitionRegistry.currentLeases(topic)
            .mapTo(linkedSetOf()) { lease ->
                WarmupSourcePartition(
                    partition = lease.topicPartition.partition(),
                    ownershipGeneration = lease.generation
                )
            }
            .takeIf(Set<WarmupSourcePartition>::isNotEmpty)
    }

    private fun prepareForStop(): Deferred<Unit> = synchronized(this) {
        stopDeferred ?: shutdownScope.launchStopSequence().also { stopDeferred = it }
    }

    suspend fun stop() {
        val deferred = prepareForStop()
        deferred.await()
        failure.get()?.let { throw it }
    }

    /** Waits for normal shutdown or rethrows the one terminal failure to every caller. */
    suspend fun awaitTermination() {
        termination.await()?.let { throw it }
    }

    /** Waits until the shutdown sequence has finished waiting for all consumer-owned work. */
    suspend fun awaitShutdownCompletion() {
        shutdownCompletion.await()
    }

    internal fun isShutdownComplete(): Boolean = shutdownCompletion.isCompleted

    fun stateSnapshot(): ConsumerStateSnapshot {
        val currentFailure = failure.get()
        val loopSnapshots = pollLoops.map { it.stateSnapshot() }
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
            pollLoops = loopSnapshots,
            lifecycleState = when {
                currentFailure != null -> ConsumerLifecycleState.FAILED
                stopped && stopDeferred?.isCompleted == false -> ConsumerLifecycleState.STOPPING
                stopped -> ConsumerLifecycleState.STOPPED
                loopSnapshots.any { it.recovering } -> ConsumerLifecycleState.RECOVERING
                else -> ConsumerLifecycleState.RUNNING
            },
            lastRecoveryOrFailureReason = currentFailure?.message
                ?: loopSnapshots.firstNotNullOfOrNull { it.lastRecoveryReason }
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
            partitionRegistry.cancelAllActive()
            processingLifecycle.close(cause)
            terminalFailureSignal.complete(Unit)
            termination.complete(cause)
            pollLoopJobs.forEach { it.cancel() }
            if (startCompleted) {
                prepareForStop()
            }
        }
    }

    private fun CoroutineScope.launchStopSequence(): Deferred<Unit> = async {
        lifecycleMutex.withLock {
            if (stopped) {
                return@withLock
            }
            stopped = true
            val shutdownStartedAt = System.nanoTime()
            var shutdownOutcome = LifecycleOutcome.SUCCESS
            var shutdownError: Throwable? = null
            var processingShutdownCompleted = false
            fun recordShutdownError(error: Throwable) {
                shutdownOutcome = LifecycleOutcome.FAILURE
                val existingError = shutdownError
                if (existingError == null) {
                    shutdownError = error
                } else {
                    existingError.addSuppressed(error)
                }
            }

            try {
                if (failure.get() == null) {
                    try {
                        val shutdownDeadlineNanos = System.nanoTime() + shutdownDrainTimeout.inWholeNanoseconds
                        withTimeout(shutdownDrainTimeout) {
                            val readySignals = pollLoops.map { it.prepareForShutdown(shutdownDeadlineNanos) }
                            for (readySignal in readySignals) {
                                select {
                                    readySignal.onAwait { }
                                    terminalFailureSignal.onAwait { }
                                }
                                if (terminalFailureSignal.isCompleted) {
                                    break
                                }
                            }
                            if (!terminalFailureSignal.isCompleted) {
                                // Stash is now in the runtime. Closing it drains accepted work and waits for
                                // handlers, retries, and failure handlers before Kafka final commit.
                                processingLifecycle.stop()
                                processingShutdownCompleted = true
                                pollLoops.map { it.finalizeShutdown() }.forEach { it.await() }
                            }
                        }
                    } catch (timeout: TimeoutCancellationException) {
                        handleFailure(timeout)
                        throw timeout
                    }
                }
            } catch (error: Throwable) {
                shutdownError = error
                shutdownOutcome = if (error is TimeoutCancellationException) {
                    LifecycleOutcome.TIMEOUT
                } else {
                    LifecycleOutcome.FAILURE
                }
            }

            withContext(NonCancellable) {
                try {
                    pollLoopJobs.forEach { job ->
                        if (job.isActive) {
                            job.cancelAndJoin()
                        } else {
                            job.join()
                        }
                    }
                } catch (error: Throwable) {
                    recordShutdownError(error)
                }

                try {
                    if (!processingShutdownCompleted) {
                        processingLifecycle.stop()
                        processingShutdownCompleted = true
                    }
                } catch (error: Throwable) {
                    recordShutdownError(error)
                }

                pollLoops.forEach { loop ->
                    try {
                        loop.releaseResources()
                    } catch (error: Throwable) {
                        recordShutdownError(error)
                    }
                }

                try {
                    if (failure.get() != null && shutdownOutcome == LifecycleOutcome.SUCCESS) {
                        shutdownOutcome = LifecycleOutcome.FAILURE
                    }
                    metrics.onShutdown(System.nanoTime() - shutdownStartedAt, shutdownOutcome)
                    termination.complete(failure.get())
                } finally {
                    lifecycleJob.complete()
                    shutdownLifecycleJob.complete()
                    shutdownCompletion.complete(Unit)
                }
            }

            shutdownError?.let { throw it }
        }
    }
}

private fun Map<String, Any?>.withExplicitCommitManagement(): Map<String, Any?> {
    val configured = this[ENABLE_AUTO_COMMIT_CONFIG]
    val explicitlyEnabled = when (configured) {
        is Boolean -> configured
        is String -> configured.equals("true", ignoreCase = true)
        else -> false
    }
    require(!explicitlyEnabled) {
        "Kafka property '$ENABLE_AUTO_COMMIT_CONFIG' must not be true; CKC manages commits explicitly"
    }
    return this + (ENABLE_AUTO_COMMIT_CONFIG to false)
}

private fun Throwable.isCancellation(): Boolean = this is CancellationException

private fun <K, V> defaultPollLoopFactory(): PollLoopFactory<K, V> =
    { id, context, processingMode, commitIntervalMs, commitRecordsThreshold, metrics, consumerProperties, consumerConfigAdapter, loopTopics, loopTopicsPattern, recordSink, registry, fixedAssignment ->
        ConsumerPollLoop<K, V>(
            id = id,
            parentContext = context,
            processingMode = processingMode,
            commitIntervalMs = commitIntervalMs,
            commitRecordsThreshold = commitRecordsThreshold,
            metrics = metrics,
            consumerProperties = consumerProperties,
            consumerConfigAdapter = consumerConfigAdapter,
            topics = loopTopics,
            topicsPattern = loopTopicsPattern,
            fixedAssignment = fixedAssignment,
            recordSink = recordSink,
            partitionStateRegistry = registry
        )
    }

internal fun <K, V> defaultProcessingRuntime(
    parentScope: CoroutineScope,
    processingMode: ProcessingMode,
    workerConcurrency: Int,
    workChannelCapacity: Int,
    freshnessMaxRecordAge: Duration?,
    processingDispatcher: CoroutineDispatcher,
    metrics: ConsumerMetrics<K, V>,
    handler: KafkaRecordHandler<K, V>,
    retryPolicy: RetryPolicy,
    recordProcessingContext: RecordProcessingContext<K, V>?,
    processingFailureHandler: ProcessingFailureHandler<K, V>,
    processedRecordTracker: ProcessedRecordTracker,
    interceptor: AdmittedRecordInterceptor<K, V> = localRecordInterceptor()
): RecordProcessingRuntime<K, V> =
    when (processingMode) {
        ProcessingMode.AT_LEAST_ONCE_NO_ORDERING -> AtLeastOnceNoOrderingRecordProcessingRuntime(
            workerConcurrency = workerConcurrency,
            workChannelCapacity = workChannelCapacity,
            processingDispatcher = processingDispatcher,
            scope = parentScope,
            metrics = metrics,
            handler = handler,
            retryPolicy = retryPolicy,
            recordProcessingContext = recordProcessingContext,
            processingFailureHandler = processingFailureHandler,
            processedRecordTracker = processedRecordTracker,
            interceptor = interceptor
        )

        ProcessingMode.AT_LEAST_ONCE_KEY_ORDERING -> AtLeastOnceOrderingRecordProcessingRuntime(
            workerConcurrency = workerConcurrency,
            workChannelCapacity = workChannelCapacity,
            ordering = AtLeastOnceOrderingRecordProcessingRuntime.Ordering.BY_KEY,
            processingDispatcher = processingDispatcher,
            scope = parentScope,
            metrics = metrics,
            handler = handler,
            retryPolicy = retryPolicy,
            recordProcessingContext = recordProcessingContext,
            processingFailureHandler = processingFailureHandler,
            processedRecordTracker = processedRecordTracker,
            interceptor = interceptor
        )

        ProcessingMode.AT_LEAST_ONCE_PARTITION_ORDERING -> AtLeastOnceOrderingRecordProcessingRuntime(
            workerConcurrency = workerConcurrency,
            workChannelCapacity = workChannelCapacity,
            ordering = AtLeastOnceOrderingRecordProcessingRuntime.Ordering.BY_PARTITION,
            processingDispatcher = processingDispatcher,
            scope = parentScope,
            metrics = metrics,
            handler = handler,
            retryPolicy = retryPolicy,
            recordProcessingContext = recordProcessingContext,
            processingFailureHandler = processingFailureHandler,
            processedRecordTracker = processedRecordTracker,
            interceptor = interceptor
        )

        ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST -> FreshnessFirstDropOldestRecordProcessingRuntime(
            workerConcurrency = workerConcurrency,
            workChannelCapacity = workChannelCapacity,
            freshnessMaxRecordAge = freshnessMaxRecordAge,
            processingDispatcher = processingDispatcher,
            scope = parentScope,
            metrics = metrics,
            handler = handler,
            retryPolicy = retryPolicy,
            recordProcessingContext = recordProcessingContext,
            processingFailureHandler = processingFailureHandler,
            processedRecordTracker = processedRecordTracker,
            interceptor = interceptor
        )

        ProcessingMode.FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY -> FreshnessFirstReplacePendingByKeyRecordProcessingRuntime(
            workerConcurrency = workerConcurrency,
            workChannelCapacity = workChannelCapacity,
            freshnessMaxRecordAge = freshnessMaxRecordAge,
            processingDispatcher = processingDispatcher,
            scope = parentScope,
            metrics = metrics,
            handler = handler,
            retryPolicy = retryPolicy,
            recordProcessingContext = recordProcessingContext,
            processingFailureHandler = processingFailureHandler,
            processedRecordTracker = processedRecordTracker,
            interceptor = interceptor
        )
    }
