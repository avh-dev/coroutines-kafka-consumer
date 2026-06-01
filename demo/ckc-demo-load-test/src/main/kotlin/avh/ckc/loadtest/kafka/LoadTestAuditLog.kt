package avh.ckc.loadtest.kafka

import io.lettuce.core.RedisClient
import io.lettuce.core.RedisFuture
import io.lettuce.core.api.StatefulRedisConnection
import org.apache.kafka.clients.producer.RecordMetadata
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

class LoadTestAuditLog private constructor(
    private val client: RedisClient,
    private val connection: StatefulRedisConnection<String, String>
) : AutoCloseable {
    private val outstanding = ConcurrentHashMap.newKeySet<RedisFuture<Long>>()
    private val failure = AtomicReference<Throwable>()

    fun published(metadata: RecordMetadata, key: String, eventType: String) {
        append(encodeAuditRecord("P", metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp(), key))
    }

    fun generated(topic: String, key: String, eventType: String) {
        append(encodeAuditRecord("P", topic, -1, -1, System.currentTimeMillis(), key))
    }

    override fun close() {
        try {
            outstanding.toList().forEach { it.get() }
            failure.get()?.let { throw IllegalStateException("Redis audit write failed", it) }
        } finally {
            connection.close()
            client.shutdown()
        }
    }

    private fun append(record: String) {
        failure.get()?.let { throw IllegalStateException("Redis audit write failed", it) }
        val future = connection.async().rpush(AUDIT_KEY, record)
        outstanding += future
        future.whenComplete { _, error ->
            outstanding -= future
            if (error != null) {
                failure.compareAndSet(null, error)
            }
        }
    }

    companion object {
        fun fromEnvironment(environment: Map<String, String> = System.getenv()): LoadTestAuditLog {
            val host = environment["REDIS_HOST"] ?: "localhost"
            val port = environment["REDIS_PORT"]?.toIntOrNull() ?: 6379
            val client = RedisClient.create("redis://$host:$port")
            return LoadTestAuditLog(client, client.connect())
        }
    }
}

internal fun encodeAuditRecord(
    type: String,
    topic: String,
    partition: Int,
    offset: Long,
    kafkaTimestampMs: Long,
    messageKey: String?
): String =
    "$type\t${auditTopicId(topic)}\t$partition\t$offset\t$kafkaTimestampMs\t${System.currentTimeMillis()}\t${sanitizeAuditKey(messageKey)}"

private fun auditTopicId(topic: String): Int =
    when (topic) {
        "order.events.v1" -> 1
        "batch.events.v1" -> 2
        "cauldron.events.v1" -> 3
        else -> error("No audit topic id configured for topic '$topic'")
    }

private fun sanitizeAuditKey(key: String?): String =
    key.orEmpty().replace('\t', ' ').replace('\r', ' ').replace('\n', ' ')

private const val AUDIT_KEY = "audit"
