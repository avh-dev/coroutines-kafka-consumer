package avh.ckc.core.extension

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.processing.AdmittedRecordInterceptor
import kotlin.time.Duration
import org.apache.kafka.common.TopicPartition

/** Builds runtimes with the handler and processing policies captured from a consumer definition. */
interface ConsumerRuntimeFactory<K, V> {
    val subscribedTopics: List<String>?
    val metrics: ConsumerMetrics<K, V>

    /** Builds the regular subscribed runtime with an interceptor for admitted records. */
    fun subscribed(
        consumerProperties: Map<String, Any?>,
        interceptor: AdmittedRecordInterceptor<K, V>
    ): CoroutinesKafkaConsumer<K, V>

    /** Builds a runtime that owns the specified partitions and seeks to the supplied start offsets. */
    fun assigned(
        consumerProperties: Map<String, Any?>,
        startOffsets: Map<TopicPartition, Long>,
        admission: RecordAdmission<K, V>? = null,
        metrics: ConsumerMetrics<K, V> = this.metrics,
        shutdownTimeout: Duration = Duration.INFINITE
    ): CoroutinesKafkaConsumer<K, V>
}
