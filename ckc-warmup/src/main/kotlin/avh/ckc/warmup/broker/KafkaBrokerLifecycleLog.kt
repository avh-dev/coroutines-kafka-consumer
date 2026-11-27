package avh.ckc.warmup.broker

import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer

/** Reader cursors are local to each observer. The dedicated topic contains only lifecycle records. */
internal class KafkaBrokerLifecycleLog(
    private val topic: String,
    connection: Map<String, Any?>,
    operationTimeoutMillis: Long
) : AutoCloseable {
    private val timeoutMillis = operationTimeoutMillis.coerceIn(1, Int.MAX_VALUE.toLong()).toInt()
    private val properties = connection + mapOf(
        "request.timeout.ms" to timeoutMillis,
        "default.api.timeout.ms" to timeoutMillis
    )
    private val partition = TopicPartition(topic, 0)
    private val admin = lazy { Admin.create(properties) }
    private val consumer = lazy {
        KafkaConsumer(
            properties + mapOf(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
                ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG to false,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "none"
            ),
            ByteArrayDeserializer(),
            ByteArrayDeserializer()
        ).apply { assign(listOf(partition)) }
    }
    private val producer = lazy {
        KafkaProducer(
            properties + mapOf(
                ProducerConfig.ACKS_CONFIG to "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
                ProducerConfig.LINGER_MS_CONFIG to 0,
                ProducerConfig.MAX_BLOCK_MS_CONFIG to timeoutMillis.toLong(),
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG to timeoutMillis
            ),
            ByteArraySerializer(),
            ByteArraySerializer()
        )
    }

    private fun description(timeout: Duration): TopicDescription =
        admin.value.describeTopics(listOf(topic)).allTopicNames()
            .get(timeout.toMillis(), TimeUnit.MILLISECONDS).getValue(topic)

    data class Metadata(val topicId: Uuid, val advertisedBroker: String)

    fun metadata(timeout: Duration): Metadata {
        val description = description(timeout)
        val leader = description.partitions().singleOrNull()?.takeIf { it.partition() == 0 }?.leader()
        check(leader != null && !leader.isEmpty) { "Lifecycle topic must have one available partition 0" }
        val id = description.topicId()
        check(id != Uuid.ZERO_UUID) { "Lifecycle topic identity unavailable" }
        val host = leader.host().let { if (':' in it) "[$it]" else it }
        return Metadata(id, "$host:${leader.port()}")
    }

    fun topicId(timeout: Duration): Uuid = metadata(timeout).topicId

    fun seek(offset: Long) = consumer.value.seek(partition, offset)
    fun beginning(timeout: Duration): Long = consumer.value.beginningOffsets(listOf(partition), timeout).getValue(partition)
    fun end(timeout: Duration): Long = consumer.value.endOffsets(listOf(partition), timeout).getValue(partition)
    fun position(timeout: Duration): Long = consumer.value.position(partition, timeout)

    fun poll(timeout: Duration): List<BrokerLifecycleRecord> =
        consumer.value.poll(timeout).map {
            BrokerLifecycleRecord(it.offset(), BrokerLifecycleCodec.decode(it.value()), it.timestamp())
        }

    fun submit(event: BrokerLifecycleEvent): CompletableFuture<Long> {
        val result = CompletableFuture<Long>()
        producer.value.send(ProducerRecord(topic, 0, null, BrokerLifecycleCodec.encode(event))) { metadata, error ->
            if (error == null) result.complete(metadata.offset()) else result.completeExceptionally(error)
        }
        return result
    }

    override fun close() {
        try {
            if (producer.isInitialized()) producer.value.close(Duration.ZERO)
        } finally {
            try {
                if (consumer.isInitialized()) consumer.value.close(Duration.ZERO)
            } finally {
                if (admin.isInitialized()) admin.value.close(Duration.ZERO)
            }
        }
    }
}
