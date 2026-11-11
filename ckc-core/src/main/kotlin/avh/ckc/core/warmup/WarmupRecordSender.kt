package avh.ckc.core.warmup

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runInterruptible
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeaders
import java.time.Duration as JavaDuration
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

data class WarmupSendAcknowledgement(
    val topicPartition: TopicPartition,
    val offset: Long
)

fun interface WarmupRecordSender<K, V> {
    suspend fun send(record: ConsumerRecord<K, V>, destination: WarmupDestination): WarmupSendAcknowledgement
}

internal class KafkaWarmupRecordSender<K, V>(
    producerProperties: Map<String, Any?>,
    private val blockingDispatcher: CoroutineDispatcher = Dispatchers.IO,
    private val closeTimeout: Duration = 5.seconds,
    producerFactory: (Map<String, Any?>) -> Producer<K, V> = { KafkaProducer(it) }
) : WarmupRecordSender<K, V>, AutoCloseable {
    private val producerProperties = producerProperties.toMap()
    private val producerDelegate = lazy { producerFactory(this.producerProperties) }
    private val producer: Producer<K, V> by producerDelegate

    init {
        require(this.producerProperties.containsKey(KEY_SERIALIZER_CLASS_CONFIG)) {
            "Kafka property '$KEY_SERIALIZER_CLASS_CONFIG' must be specified for warm-up forwarding"
        }
        require(this.producerProperties.containsKey(VALUE_SERIALIZER_CLASS_CONFIG)) {
            "Kafka property '$VALUE_SERIALIZER_CLASS_CONFIG' must be specified for warm-up forwarding"
        }
        require(closeTimeout.isPositive()) { "Warm-up producer close timeout must be > 0" }
    }

    override suspend fun send(
        record: ConsumerRecord<K, V>,
        destination: WarmupDestination
    ): WarmupSendAcknowledgement = runInterruptible(blockingDispatcher) {
        val metadata = producer.send(record.copyTo(destination)).get()
        val actual = TopicPartition(metadata.topic(), metadata.partition())
        check(actual == TopicPartition(destination.topic, destination.partition) && metadata.offset() >= 0) {
            "Unexpected warm-up producer acknowledgement: $metadata"
        }
        WarmupSendAcknowledgement(actual, metadata.offset())
    }

    override fun close() {
        if (!producerDelegate.isInitialized()) return
        producer.close(JavaDuration.ofMillis(closeTimeout.inWholeMilliseconds.coerceAtLeast(1L)))
    }

    private fun ConsumerRecord<K, V>.copyTo(destination: WarmupDestination): ProducerRecord<K, V> =
        ProducerRecord(
            destination.topic,
            destination.partition,
            timestamp().takeIf { it >= 0L },
            key(),
            value(),
            RecordHeaders(headers().toArray())
        )
}
