package avh.ckc.warmup

import avh.ckc.warmup.protocol.WARMUP_SESSION_HEADER
import avh.ckc.warmup.protocol.WarmupSessionId
import avh.ckc.warmup.protocol.setWarmupDelegationId
import java.time.Duration as JavaDuration
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
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

/** Coordinates of an append to the warm-up destination. */
internal data class WarmupSendAcknowledgement(
    val topicPartition: TopicPartition,
    val offset: Long
)

/** Forwards one record with the recipient session and optional delegation correlation information. */
internal fun interface WarmupRecordSender<K, V> {
    suspend fun send(
        record: ConsumerRecord<K, V>,
        destination: WarmupDestination,
        sessionId: WarmupSessionId
    ): WarmupSendAcknowledgement

    suspend fun send(
        record: ConsumerRecord<K, V>,
        destination: WarmupDestination,
        sessionId: WarmupSessionId,
        delegationId: UUID?
    ): WarmupSendAcknowledgement = send(record, destination, sessionId)
}

/** Copies an admitted source record and adds exactly one current warm-up session header. */
internal class KafkaWarmupRecordSender<K, V>(
    producerProperties: Map<String, Any?>,
    private val submissionDispatcher: CoroutineDispatcher = Dispatchers.IO,
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
    ): WarmupSendAcknowledgement = send(record, destination, sessionId, null)

    override suspend fun send(
        record: ConsumerRecord<K, V>,
        destination: WarmupDestination,
        sessionId: WarmupSessionId,
        delegationId: UUID?
    ): WarmupSendAcknowledgement {
        val acknowledgement = CompletableDeferred<WarmupSendAcknowledgement>()
        val forwarded = record.copyTo(destination, sessionId, delegationId)
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
        sessionId: WarmupSessionId,
        delegationId: UUID?
    ): ProducerRecord<K, V> {
        val headers = RecordHeaders(headers().toArray())
        headers.remove(WARMUP_SESSION_HEADER)
        headers.add(WARMUP_SESSION_HEADER, sessionId.value.toByteArray(Charsets.UTF_8))
        headers.setWarmupDelegationId(delegationId)
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
