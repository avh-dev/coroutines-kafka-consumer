package avh.ckc.core

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.warmup.WarmupForwardingConfiguration
import avh.ckc.core.warmup.WarmupForwardingObserver
import avh.ckc.core.warmup.WarmupRecordForwarding
import avh.ckc.core.warmup.SwitchableWarmupRecordForwarding
import avh.ckc.core.warmup.TrafficWarmupContext
import avh.ckc.core.warmup.WarmupConsumerDefinition
import avh.ckc.core.warmup.WarmupConsumerId
import avh.ckc.core.warmup.WarmupForwardingProfile
import avh.ckc.core.warmup.WarmupParticipantConfiguration
import avh.ckc.core.warmup.WarmupSessionId
import avh.ckc.core.warmup.WarmupTopicMapping
import avh.ckc.core.warmup.kafkaWarmupRecordForwarding
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
    private var assignedPartitionsAtEnd: Set<TopicPartition>? = null
    private var warmupForwardingConfiguration: WarmupForwardingConfiguration? = null
    private var warmupForwardingObserver: WarmupForwardingObserver = WarmupForwardingObserver.NOOP
    private var warmupRecordForwarding: WarmupRecordForwarding<K, V>? = null
    private var trafficWarmup: TrafficWarmupConsumerConfiguration? = null
    private var handler: KafkaRecordHandler<K, V>? = null

    /**
     * Subscribes the consumer to the provided topic names.
     */
    fun topics(vararg names: String) {
        topics = names.toList()
        topicsPattern = null
        assignedPartitionsAtEnd = null
    }

    /**
     * Subscribes the consumer to the provided topic names.
     */
    fun topics(names: List<String>) {
        topics = names.toList()
        topicsPattern = null
        assignedPartitionsAtEnd = null
    }

    /**
     * Subscribes the consumer using a topic name pattern.
     */
    fun topicsPattern(pattern: Pattern) {
        topicsPattern = pattern
        topics = null
        assignedPartitionsAtEnd = null
    }

    /**
     * Assigns prepared warm-up partitions directly and starts at their current end positions.
     *
     * This narrow mode supports only [ProcessingMode.AT_LEAST_ONCE_NO_ORDERING] and one poll loop.
     * Call [CoroutinesKafkaConsumer.awaitReady] before allowing a donor to publish records.
     */
    fun assignedPartitionsAtEnd(partitions: Collection<TopicPartition>) {
        assignedPartitionsAtEnd = LinkedHashSet(partitions)
        topics = null
        topicsPattern = null
    }

    /** See [assignedPartitionsAtEnd]. */
    fun assignedPartitionsAtEnd(vararg partitions: TopicPartition) {
        assignedPartitionsAtEnd(partitions.asList())
    }

    /**
     * Selects and forwards donor records to assigned warm-up partitions before business handling.
     */
    fun warmupForwarding(
        configuration: WarmupForwardingConfiguration,
        observer: WarmupForwardingObserver = WarmupForwardingObserver.NOOP
    ) {
        warmupForwardingConfiguration = configuration
        warmupForwardingObserver = observer
    }

    internal fun warmupForwarding(forwarding: WarmupRecordForwarding<K, V>) {
        warmupRecordForwarding = forwarding
    }

    /** Enables library-managed real-traffic warm-up without changing this consumer's lifecycle API. */
    fun trafficWarmup(context: TrafficWarmupContext, block: TrafficWarmupConsumerBuilder.() -> Unit) {
        check(trafficWarmup == null) { "Traffic warm-up can be configured only once" }
        trafficWarmup = TrafficWarmupConsumerBuilder(context).apply(block).build()
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

    /**
     * Builds a low-level [CoroutinesKafkaConsumer] from the accumulated builder state.
     */
    internal fun build(consumerProperties: Map<String, Any?>): CoroutinesKafkaConsumer<K, V> {
        val configuredHandler = requireNotNull(handler) { "Kafka record handler must be specified" }
        val configuredSubscriptionModes = listOf(topics, topicsPattern, assignedPartitionsAtEnd).count { it != null }
        require(configuredSubscriptionModes == 1) {
            "Exactly one of topics, topicsPattern, or assignedPartitionsAtEnd must be specified"
        }
        require((warmupForwardingConfiguration == null && warmupRecordForwarding == null) ||
            processingMode == ProcessingMode.AT_LEAST_ONCE_NO_ORDERING) {
            "Warm-up forwarding supports only AT_LEAST_ONCE_NO_ORDERING"
        }
        require((warmupForwardingConfiguration == null && warmupRecordForwarding == null) ||
            assignedPartitionsAtEnd == null) {
            "Explicitly assigned warm-up recipients cannot forward records"
        }
        require(warmupForwardingConfiguration == null || warmupRecordForwarding == null) {
            "Warm-up forwarding can be configured only once"
        }
        val managedWarmup = trafficWarmup
        require(managedWarmup == null || warmupForwardingConfiguration == null && warmupRecordForwarding == null) {
            "Managed traffic warm-up cannot be combined with explicit warm-up forwarding"
        }
        if (managedWarmup != null) {
            require(processingMode == ProcessingMode.AT_LEAST_ONCE_NO_ORDERING) {
                "Real-traffic warm-up supports only AT_LEAST_ONCE_NO_ORDERING"
            }
            require(topics != null && topicsPattern == null && assignedPartitionsAtEnd == null) {
                "Real-traffic warm-up requires an explicit topics subscription"
            }
            val configuredTopics = requireNotNull(topics).toSet()
            val mappedTopics = managedWarmup.topicMappings.map(WarmupTopicMapping::sourceTopic).toSet()
            require(configuredTopics == mappedTopics) {
                "Every subscribed topic must have exactly one warm-up topic mapping"
            }
            val forwarding = SwitchableWarmupRecordForwarding<K, V>()
            val main = buildConsumer(
                consumerProperties,
                configuredHandler,
                topics,
                topicsPattern,
                assignedPartitionsAtEnd,
                forwarding
            )
            val warmupProperties = managedWarmup.warmupConsumerProperties ?: consumerProperties
            val definition = WarmupConsumerDefinition<K, V>(
                configuration = WarmupParticipantConfiguration(
                    WarmupConsumerId(managedWarmup.consumerId),
                    managedWarmup.topicMappings,
                    managedWarmup.forwardingProfile,
                    processingMode
                ),
                mainConsumerProperties = consumerProperties,
                warmupConsumerProperties = warmupProperties,
                forwardingProducerProperties = managedWarmup.forwardingProducerProperties,
                forwardingObserver = managedWarmup.forwardingObserver,
                configure = { error("Managed traffic warm-up uses captured consumer configuration") }
            )
            managedWarmup.context.attach(definition, main, forwarding) { partitions ->
                buildConsumer(
                    warmupProperties,
                    configuredHandler,
                    null,
                    null,
                    partitions,
                    null
                )
            }
            return main
        }
        val configuredWarmupRecordForwarding: WarmupRecordForwarding<K, V>? = warmupRecordForwarding
            ?: warmupForwardingConfiguration?.let {
            kafkaWarmupRecordForwarding(it, warmupForwardingObserver)
        }

        return buildConsumer(
            consumerProperties,
            configuredHandler,
            topics,
            topicsPattern,
            assignedPartitionsAtEnd,
            configuredWarmupRecordForwarding
        )
    }

    private fun buildConsumer(
        consumerProperties: Map<String, Any?>,
        configuredHandler: KafkaRecordHandler<K, V>,
        topics: List<String>?,
        topicsPattern: Pattern?,
        assignedPartitionsAtEnd: Set<TopicPartition>?,
        forwarding: WarmupRecordForwarding<K, V>?
    ): CoroutinesKafkaConsumer<K, V> = CoroutinesKafkaConsumer(
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
            assignedPartitionsAtEnd = assignedPartitionsAtEnd,
            warmupRecordForwarding = forwarding,
            handler = configuredHandler
        )
}

class TrafficWarmupConsumerBuilder internal constructor(
    internal val context: TrafficWarmupContext
) {
    var consumerId: String? = null
    var warmupConsumerProperties: Map<String, Any?>? = null
    var forwardingProducerProperties: Map<String, Any?> = emptyMap()
    var forwardingProfile: WarmupForwardingProfile = WarmupForwardingProfile()
    var forwardingObserver: (WarmupSessionId) -> WarmupForwardingObserver = { WarmupForwardingObserver.NOOP }
    private val mappings = mutableListOf<WarmupTopicMapping>()

    fun warmupTopic(source: String, destination: String) {
        mappings += WarmupTopicMapping(source, destination)
    }

    internal fun build(): TrafficWarmupConsumerConfiguration {
        val id = requireNotNull(consumerId) { "consumerId must be specified" }
        require(forwardingProducerProperties.isNotEmpty()) {
            "forwardingProducerProperties must be specified"
        }
        return TrafficWarmupConsumerConfiguration(
            context,
            id,
            mappings.toList(),
            warmupConsumerProperties?.toMap(),
            forwardingProducerProperties.toMap(),
            forwardingProfile,
            forwardingObserver
        )
    }
}

internal data class TrafficWarmupConsumerConfiguration(
    val context: TrafficWarmupContext,
    val consumerId: String,
    val topicMappings: List<WarmupTopicMapping>,
    val warmupConsumerProperties: Map<String, Any?>?,
    val forwardingProducerProperties: Map<String, Any?>,
    val forwardingProfile: WarmupForwardingProfile,
    val forwardingObserver: (WarmupSessionId) -> WarmupForwardingObserver
)

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
