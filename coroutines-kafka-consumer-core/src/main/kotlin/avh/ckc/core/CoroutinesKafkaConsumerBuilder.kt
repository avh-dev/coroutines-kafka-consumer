package avh.ckc.core

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.regex.Pattern
import kotlin.coroutines.CoroutineContext

/**
 * Builder for the public Kotlin DSL used to create [CoroutinesKafkaConsumer] instances.
 *
 * Aggregates consumer runtime settings, topic subscription mode, failure handling
 * strategy and the main record handler into a single user-facing definition.
 */
class CoroutinesKafkaConsumerBuilder<K, V> {
    /**
     * Strategy used when workers cannot keep up with incoming records.
     */
    var overflowStrategy: OverflowStrategy = OverflowStrategy.BACKPRESSURE

    /**
     * Number of worker coroutines processing records from the internal work channel.
     */
    var workerConcurrency: Int = 1

    /**
     * Number of independent Kafka poll loops.
     */
    var consumerPollLoopConcurrency: Int = 1

    /**
     * Interval for periodic best-effort offset commits in backpressure mode.
     */
    var commitIntervalMs: Long = 60_000L

    /**
     * Capacity of the internal work channel between poll loops and workers.
     */
    var workChannelCapacity: Int = 1024

    /**
     * Dispatcher used for key/value deserialization.
     */
    var deserializationDispatcher: CoroutineDispatcher = Dispatchers.IO

    /**
     * Dispatcher used for the main user-defined record handler.
     */
    var processingDispatcher: CoroutineDispatcher = Dispatchers.Default

    /**
     * Retry policy applied to the main business handler.
     */
    var retryPolicy: RetryPolicy = RetryPolicy.none()

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
    fun onProcessingFailure(handler: suspend (K?, V?, ConsumerRecord<ByteArray, ByteArray>, Throwable) -> Unit) {
        processingFailureHandler = ProcessingFailureHandler(handler)
    }

    /**
     * Configures the main business handler for deserialized Kafka records.
     */
    fun handle(handler: suspend (K?, V?, ConsumerRecord<ByteArray, ByteArray>) -> Unit) {
        this.handler = KafkaRecordHandler(handler)
    }

    /**
     * Builds a low-level [CoroutinesKafkaConsumer] from the accumulated builder state.
     */
    internal fun build(consumerProperties: Map<String, Any?>): CoroutinesKafkaConsumer<K, V> {
        val configuredHandler = requireNotNull(handler) { "Kafka record handler must be specified" }
        require((topics == null) != (topicsPattern == null)) {
            "Exactly one of topics or topicsPattern must be specified"
        }

        return CoroutinesKafkaConsumer(
            consumerProperties = consumerProperties,
            overflowStrategy = overflowStrategy,
            workerConcurrency = workerConcurrency,
            consumerPollLoopConcurrency = consumerPollLoopConcurrency,
            commitIntervalMs = commitIntervalMs,
            workChannelCapacity = workChannelCapacity,
            deserializationDispatcher = deserializationDispatcher,
            processingDispatcher = processingDispatcher,
            retryPolicy = retryPolicy,
            processingFailureHandler = processingFailureHandler,
            parentContext = parentContext,
            topics = topics,
            topicsPattern = topicsPattern,
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
