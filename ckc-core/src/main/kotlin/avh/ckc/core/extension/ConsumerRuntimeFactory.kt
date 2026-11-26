package avh.ckc.core.extension

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.processing.AdmittedRecordInterceptor
import kotlin.time.Duration
import org.apache.kafka.common.TopicPartition

/**
 * Builds runtimes from a snapshot of one consumer definition. The extension owns any runtime
 * returned by this factory and supplies the Kafka properties for its connection.
 */
interface ConsumerRuntimeFactory<K, V> {
    /** Explicit source topics. A pattern subscription has no explicit topic list. */
    val subscribedTopics: List<String>?
    /** Metrics configured for the main runtime. Stateful adapters need independent auxiliary bindings. */
    val metrics: ConsumerMetrics<K, V>

    /** Builds the regular subscribed runtime with an interceptor for admitted records. */
    fun subscribed(
        consumerProperties: Map<String, Any?>,
        interceptor: AdmittedRecordInterceptor<K, V>
    ): CoroutinesKafkaConsumer<K, V>

    /**
     * Builds a runtime with explicit partition assignment and initial seek offsets. Uses the
     * captured handler and policies, with optional pre-queue [admission]. Supply a distinct
     * [metrics] binding when it owns native Kafka or runtime meters. [shutdownTimeout] caps the
     * captured graceful drain budget and defaults to no additional cap.
     */
    fun assigned(
        consumerProperties: Map<String, Any?>,
        startOffsets: Map<TopicPartition, Long>,
        admission: RecordAdmission<K, V>? = null,
        metrics: ConsumerMetrics<K, V> = this.metrics,
        shutdownTimeout: Duration = Duration.INFINITE
    ): CoroutinesKafkaConsumer<K, V>
}
