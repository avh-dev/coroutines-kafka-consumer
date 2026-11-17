package avh.ckc.core.warmup

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CompletableDeferred
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

internal data class WarmupSendAcknowledgement(
    val topicPartition: TopicPartition,
    val offset: Long
)

internal fun interface WarmupRecordSender<K, V> {
    suspend fun send(
        record: ConsumerRecord<K, V>,
        destination: WarmupDestination,
        sessionId: WarmupSessionId
    ): WarmupSendAcknowledgement
}

/** Copies an admitted source record and adds exactly one current warm-up session header. */
internal class KafkaWarmupRecordSender<K, V>(
    producerProperties: Map<String, Any?>,
    private val submissionDispatcher: CoroutineDispatcher = warmupProducerSubmissionDispatcher,
    private val closeTimeout: Duration = 5.seconds,
    producerFactory: (Map<String, Any?>) -> Producer<K, V> = { KafkaProducer(it) }
) : WarmupRecordSender<K, V>, AutoCloseable {
    private val producerProperties = buildWarmupForwardingProducerProperties(producerProperties)
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
        destination: WarmupDestination,
        sessionId: WarmupSessionId
    ): WarmupSendAcknowledgement {
        val acknowledgement = CompletableDeferred<WarmupSendAcknowledgement>()
        val forwarded = record.copyTo(destination, sessionId)
        runInterruptible(submissionDispatcher) {
            producer.send(forwarded) { metadata, error ->
                when {
                    error != null -> acknowledgement.completeExceptionally(error)
                    metadata == null -> acknowledgement.completeExceptionally(
                        IllegalStateException("Warm-up producer acknowledgement did not contain metadata")
                    )
                    else -> runCatching {
                        metadata.toAcknowledgement(destination)
                    }.fold(
                        onSuccess = acknowledgement::complete,
                        onFailure = acknowledgement::completeExceptionally
                    )
                }
            }
        }
        return acknowledgement.await()
    }

    override fun close() {
        if (!producerDelegate.isInitialized()) return
        producer.close(JavaDuration.ofMillis(closeTimeout.inWholeMilliseconds.coerceAtLeast(1L)))
    }

    private fun ConsumerRecord<K, V>.copyTo(
        destination: WarmupDestination,
        sessionId: WarmupSessionId
    ): ProducerRecord<K, V> {
        val headers = RecordHeaders(headers().toArray())
        headers.remove(WARMUP_SESSION_HEADER)
        headers.add(WARMUP_SESSION_HEADER, sessionId.value.toByteArray(Charsets.UTF_8))
        return ProducerRecord(
            destination.topic,
            destination.partition,
            timestamp().takeIf { it >= 0L },
            key(),
            value(),
            headers
        )
    }

    private fun org.apache.kafka.clients.producer.RecordMetadata.toAcknowledgement(
        destination: WarmupDestination
    ): WarmupSendAcknowledgement {
        val actual = TopicPartition(topic(), partition())
        check(actual == TopicPartition(destination.topic, destination.partition) && offset() >= 0) {
            "Unexpected warm-up producer acknowledgement: $this"
        }
        return WarmupSendAcknowledgement(actual, offset())
    }
}

/** Bounds only KafkaProducer.send submission; acknowledgement waiting never occupies an IO worker. */
private val warmupProducerSubmissionDispatcher = Dispatchers.IO.limitedParallelism(
    parallelism = 4,
    name = "WarmupProducerSubmission"
)

internal const val WARMUP_SESSION_HEADER = "ckc-warmup-session"
