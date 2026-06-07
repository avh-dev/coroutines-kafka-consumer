package avh.ckc.demo.service

import avh.ckc.core.metrics.ConsumerMetrics
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service

@Service
@Profile("spring-kafka", "spring-kafka-coroutines-naive")
class DemoRecordMetrics {
    fun <V> onProcessed(
        metrics: ConsumerMetrics<String, V>,
        context: DemoConsumerRecordContext,
        value: V,
        startedAtNanos: Long
    ) {
        metrics.onRecordProcessed(
            key = context.key,
            value = value,
            record = context.record(value),
            recordAgeMillis = context.recordAgeMillis(),
            durationNanos = System.nanoTime() - startedAtNanos
        )
    }

    fun <V> onFailed(
        metrics: ConsumerMetrics<String, V>,
        context: DemoConsumerRecordContext,
        value: V,
        startedAtNanos: Long,
        error: Throwable
    ) {
        metrics.onRecordFailed(
            key = context.key,
            value = value,
            record = context.record(value),
            recordAgeMillis = context.recordAgeMillis(),
            error = error,
            durationNanos = System.nanoTime() - startedAtNanos
        )
    }
}

data class DemoConsumerRecordContext(
    val key: String?,
    val topic: String,
    val partition: Int,
    val offset: Long,
    val timestamp: Long
) {
    fun recordAgeMillis(): Long =
        if (timestamp > 0L) {
            (System.currentTimeMillis() - timestamp).coerceAtLeast(0L)
        } else {
            0L
        }

    fun <V> record(value: V): ConsumerRecord<String, V> =
        ConsumerRecord(
            topic,
            partition,
            offset,
            key,
            value
        )
}
