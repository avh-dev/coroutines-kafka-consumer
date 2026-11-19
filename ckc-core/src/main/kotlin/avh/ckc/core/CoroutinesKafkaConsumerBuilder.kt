package avh.ckc.core

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.processing.AdmittedRecordInterceptor
import avh.ckc.core.processing.localRecordInterceptor
import avh.ckc.core.warmup.WarmupCompletionPublisher
import avh.ckc.core.warmup.TrafficWarmupConsumerConfiguration
import avh.ckc.core.warmup.TrafficWarmupContext
import avh.ckc.core.warmup.TrafficWarmupParticipant
import avh.ckc.core.warmup.WarmupParticipantDataPlane
import avh.ckc.core.warmup.WarmupParticipantId
import avh.ckc.core.warmup.WarmupRecipientOutcomeMetrics
import avh.ckc.core.warmup.WarmupResultAdmission
import avh.ckc.core.warmup.WarmupSessionId
import avh.ckc.core.warmup.WarmupTopicMapping
import avh.ckc.core.warmup.mappingFingerprint
import avh.ckc.core.warmup.validateWarmupParticipantConnections
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import java.util.regex.Pattern
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Builder for the public Kotlin DSL used to create [CoroutinesKafkaConsumer] instances.
 *
 * Aggregates consumer runtime settings, topic subscription mode, processing semantics,
 * failure handling, and the main record handler into a single user-facing definition.
 */
class CoroutinesKafkaConsumerBuilder<K, V> {
    /**
     * Processing semantics used by the consumer runtime.
     */
    var processingMode: ProcessingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING

    /**
     * Number of worker coroutines processing records from the internal work channel.
     */
    var workerConcurrency: Int = 1

    /**
     * Number of independent Kafka poll loops.
     */
    var consumerPollLoopConcurrency: Int = 1

    /**
     * Interval for periodic best-effort explicit offset commits in every processing mode.
     *
     * The default matches Kafka's `auto.commit.interval.ms` default.
     */
    var commitIntervalMs: Long = 5_000L

    /**
     * Number of commit-ready records that triggers an offset commit.
     *
     * At-least-once modes count the processed contiguous frontier; freshness-first modes count accepted records.
     * The time-based [commitIntervalMs] remains a fallback when traffic does not reach this threshold.
     */
    var commitRecordsThreshold: Int = 1_000

    /** Maximum uncompressed offset-tracker ring size for one partition, in bits. */
    var maxTrackedOffsetsPerPartition: Int = 1_048_576

    /**
     * Capacity of the internal work channel between poll loops and workers.
     *
     * In [ProcessingMode.FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY], this limits queued distinct keys rather than total buffered
     * Kafka records. Existing queued keys can still be replaced by newer records without consuming additional
     * capacity.
     */
    var workChannelCapacity: Int = 1024

    /**
     * Maximum Kafka record age accepted by freshness-first modes before user handling.
     *
     * When set, workers drop records older than this duration instead of invoking the handler. This is supported
     * only by freshness-first processing modes because dropping by age would violate at-least-once semantics.
     */
    var freshnessMaxRecordAge: Duration? = null

    /** Maximum time a normal rebalance callback may wait for active partition work. */
    var partitionHandoffTimeout: Duration = 30.seconds
    /** Maximum time stop may drain accepted local work. */
    var shutdownDrainTimeout: Duration = 30.seconds
    /** Maximum time to wait after cancelling work owned by a lost assignment. */
    var processingCancellationTimeout: Duration = 30.seconds
    /** Overall deadline for automatic Kafka-client recovery after membership loss. */
    var recoveryTimeout: Duration = 120.seconds
    /** Delay between recoverable Kafka-client recreation attempts. */
    var recoveryRetryBackoff: Duration = 1.seconds

    /**
     * Dispatcher used for the main user-defined record handler.
     *
     * The application retains ownership of this dispatcher and must close it when appropriate.
     */
    var processingDispatcher: CoroutineDispatcher = Dispatchers.Default

    /**
     * Retry policy applied to the main business handler.
     */
    var retryPolicy: RetryPolicy = RetryPolicy.none()

    /**
     * Metrics sink used to observe runtime activity and failures.
     */
    @Suppress("UNCHECKED_CAST")
    var metrics: ConsumerMetrics<K, V> = ConsumerMetrics.NOOP as ConsumerMetrics<K, V>

    /**
     * Optional context wrapper applied around user record processing callbacks.
     */
    var recordProcessingContext: RecordProcessingContext<K, V>? = null

    /**
     * Fallback callback invoked after handler retries are exhausted.
     */
    var processingFailureHandler: ProcessingFailureHandler<K, V> = ProcessingFailureHandler.skip()

    /**
     * Parent coroutine context used for lifecycle and cancellation propagation.
     */
    var parentContext: CoroutineContext = Dispatchers.Default

    private var topics: List<String>? = null
    private var topicsPattern: Pattern? = null
    private var handler: KafkaRecordHandler<K, V>? = null
    private var warmupDefinition: WarmupDefinition? = null

    private data class WarmupDefinition(
        val context: TrafficWarmupContext,
        val participantId: WarmupParticipantId,
        val configuration: TrafficWarmupConsumerConfiguration
    )

    /**
     * Subscribes the consumer to the provided topic names.
     */
    fun topics(vararg names: String) {
        topics = names.toList()
        topicsPattern = null
    }

    /**
     * Subscribes the consumer to the provided topic names.
     */
    fun topics(names: List<String>) {
        topics = names.toList()
        topicsPattern = null
    }

    /**
     * Subscribes the consumer using a topic name pattern.
     */
    fun topicsPattern(pattern: Pattern) {
        topicsPattern = pattern
        topics = null
    }

    /**
     * Configures a callback invoked after the processing retry policy is exhausted.
     */
    fun onProcessingFailure(handler: suspend (ConsumerRecord<K, V>, Throwable) -> Unit) {
        processingFailureHandler = ProcessingFailureHandler(handler)
    }

    /**
     * Configures the main business handler for deserialized Kafka records.
     */
    fun handle(handler: suspend (ConsumerRecord<K, V>) -> Unit) {
        this.handler = KafkaRecordHandler(handler)
    }

    /** Registers this consumer as one typed participant of a shared protocol-v6 context. */
    fun trafficWarmup(
        context: TrafficWarmupContext,
        participantId: String,
        block: TrafficWarmupConsumerConfiguration.() -> Unit
    ) {
        check(warmupDefinition == null) { "Traffic warm-up is already configured for this consumer" }
        warmupDefinition = WarmupDefinition(
            context,
            WarmupParticipantId(participantId),
            TrafficWarmupConsumerConfiguration().apply(block)
        )
    }

    /**
     * Builds a low-level [CoroutinesKafkaConsumer] from the accumulated builder state.
     */
    internal fun build(consumerProperties: Map<String, Any?>): CoroutinesKafkaConsumer<K, V> {
        val warmup = warmupDefinition ?: return buildSubscribed(consumerProperties, localRecordInterceptor())
        val configuredTopics = requireNotNull(topics) {
            "Traffic warm-up requires an explicit topic subscription"
        }
        require(warmup.configuration.mappings.isNotEmpty()) {
            "At least one warm-up topic mapping is required"
        }
        require(warmup.configuration.mappings.keys.all(configuredTopics::contains)) {
            "Every warm-up source topic must belong to the main consumer subscription"
        }
        validateWarmupParticipantConnections(
            warmup.configuration.warmupConsumerProperties,
            warmup.configuration.forwardingProducerProperties
        )
        val protocolMappings = warmup.configuration.mappings.values.map { mapping ->
            WarmupTopicMapping(
                participantId = warmup.participantId,
                sourceTopic = mapping.sourceTopic,
                warmupTopic = mapping.warmupTopic,
                mappingFingerprint = mappingFingerprint(
                    warmup.participantId,
                    mapping,
                    warmup.configuration.warmupConsumerProperties
                ),
                quota = mapping.quota
            )
        }
        val dataPlane = WarmupParticipantDataPlane<K, V>()
        val main = buildSubscribed(consumerProperties, dataPlane)
        val participant = TrafficWarmupParticipant(
            participantId = warmup.participantId,
            mappings = protocolMappings,
            warmupConsumerProperties = warmup.configuration.warmupConsumerProperties,
            forwardingProducerProperties = warmup.configuration.forwardingProducerProperties,
            mainProperties = consumerProperties,
            recipientBuilder = snapshotForRecipient(),
            mainConsumer = main,
            dataPlane = dataPlane,
            metrics = metrics,
            delegationTimeoutMillis = warmup.context.delegationTimeoutMillis(),
            recipientShutdownTimeoutMillis = warmup.context.recipientShutdownTimeoutMillis(),
            serviceDispatcher = warmup.context.serviceDispatcher(),
            forwardingObserver = warmup.configuration.forwardingObserver
        )
        main.attachManagedLifecycle(warmup.context.register(participant))
        return main
    }

    private fun snapshotForRecipient(): CoroutinesKafkaConsumerBuilder<K, V> {
        val configuredHandler = requireNotNull(handler) { "Kafka record handler must be specified" }
        return CoroutinesKafkaConsumerBuilder<K, V>().also { copy ->
            copy.processingMode = processingMode
            copy.workerConcurrency = workerConcurrency
            copy.consumerPollLoopConcurrency = consumerPollLoopConcurrency
            copy.commitIntervalMs = commitIntervalMs
            copy.commitRecordsThreshold = commitRecordsThreshold
            copy.maxTrackedOffsetsPerPartition = maxTrackedOffsetsPerPartition
            copy.workChannelCapacity = workChannelCapacity
            copy.freshnessMaxRecordAge = freshnessMaxRecordAge
            copy.partitionHandoffTimeout = partitionHandoffTimeout
            copy.shutdownDrainTimeout = shutdownDrainTimeout
            copy.processingCancellationTimeout = processingCancellationTimeout
            copy.recoveryTimeout = recoveryTimeout
            copy.recoveryRetryBackoff = recoveryRetryBackoff
            copy.processingDispatcher = processingDispatcher
            copy.retryPolicy = retryPolicy
            copy.metrics = metrics
            copy.recordProcessingContext = recordProcessingContext
            copy.processingFailureHandler = processingFailureHandler
            copy.parentContext = parentContext
            copy.topics(requireNotNull(topics))
            copy.handle(configuredHandler::process)
        }
    }

    /** Builds a subscribed runtime whose admitted records may be delegated by a warm-up donor. */
    internal fun buildWarmupDonor(
        consumerProperties: Map<String, Any?>,
        interceptor: AdmittedRecordInterceptor<K, V>
    ): CoroutinesKafkaConsumer<K, V> = buildSubscribed(consumerProperties, interceptor)

    private fun buildSubscribed(
        consumerProperties: Map<String, Any?>,
        interceptor: AdmittedRecordInterceptor<K, V>
    ): CoroutinesKafkaConsumer<K, V> {
        val configuredHandler = requireNotNull(handler) { "Kafka record handler must be specified" }
        require((topics == null) != (topicsPattern == null)) {
            "Exactly one of topics or topicsPattern must be specified"
        }

        return CoroutinesKafkaConsumer(
            consumerProperties = consumerProperties,
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
            retryPolicy = retryPolicy,
            metrics = metrics,
            recordProcessingContext = recordProcessingContext,
            processingFailureHandler = processingFailureHandler,
            parentContext = parentContext,
            topics = topics,
            topicsPattern = topicsPattern,
            admittedRecordInterceptor = interceptor,
            handler = configuredHandler
        )
    }

    /** Builds one session-filtered runtime with an explicit warm-up partition and captured start. */
    internal fun buildWarmupRecipient(
        consumerProperties: Map<String, Any?>,
        sessionId: WarmupSessionId,
        topicPartition: TopicPartition,
        startOffset: Long,
        completionPublisher: WarmupCompletionPublisher? = null,
        resultAdmission: WarmupResultAdmission? = null,
        runtimeMetrics: ConsumerMetrics<K, V> = metrics,
        runtimeShutdownTimeout: Duration = shutdownDrainTimeout
    ): CoroutinesKafkaConsumer<K, V> {
        val configuredHandler = requireNotNull(handler) { "Kafka record handler must be specified" }
        require(startOffset >= 0) { "Warm-up start offset must not be negative" }
        return CoroutinesKafkaConsumer(
            consumerProperties = consumerProperties,
            processingMode = processingMode,
            workerConcurrency = workerConcurrency,
            consumerPollLoopConcurrency = 1,
            commitIntervalMs = commitIntervalMs,
            commitRecordsThreshold = commitRecordsThreshold,
            maxTrackedOffsetsPerPartition = maxTrackedOffsetsPerPartition,
            workChannelCapacity = workChannelCapacity,
            freshnessMaxRecordAge = freshnessMaxRecordAge,
            partitionHandoffTimeout = partitionHandoffTimeout,
            shutdownDrainTimeout = minOf(shutdownDrainTimeout, runtimeShutdownTimeout),
            processingCancellationTimeout = processingCancellationTimeout,
            recoveryTimeout = recoveryTimeout,
            recoveryRetryBackoff = recoveryRetryBackoff,
            processingDispatcher = processingDispatcher,
            retryPolicy = retryPolicy,
            metrics = completionPublisher?.let {
                WarmupRecipientOutcomeMetrics(sessionId, it, runtimeMetrics)
            } ?: runtimeMetrics,
            recordProcessingContext = recordProcessingContext,
            processingFailureHandler = processingFailureHandler,
            parentContext = parentContext,
            topics = listOf(topicPartition.topic()),
            warmupRecipientSessionId = sessionId,
            warmupResultAdmission = resultAdmission,
            fixedAssignment = mapOf(topicPartition to startOffset),
            handler = configuredHandler
        )
    }
}

/**
 * Creates a [CoroutinesKafkaConsumer] using a Kotlin DSL.
 *
 * This is the main public entry point for library users. Raw Kafka client properties
 * are supplied separately, while consumer behaviour is configured inside [block].
 */
fun <K, V> coroutinesKafkaConsumer(
    consumerProperties: Map<String, Any?>,
    block: CoroutinesKafkaConsumerBuilder<K, V>.() -> Unit
): CoroutinesKafkaConsumer<K, V> =
    CoroutinesKafkaConsumerBuilder<K, V>()
        .apply(block)
        .build(consumerProperties)
