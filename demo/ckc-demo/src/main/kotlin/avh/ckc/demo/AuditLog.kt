package avh.ckc.demo

import avh.ckc.demo.config.DemoApplicationProperties
import kotlinx.coroutines.future.await
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Component
import java.util.concurrent.CompletionStage

@Component
class AuditLog(
    properties: DemoApplicationProperties,
    private val redisTemplate: ReactiveRedisTemplate<String, ByteArray>
) {
    private val enabled = properties.audit.enabled

    suspend fun processedSuspending(record: ConsumerRecord<*, *>) {
        append(record).await()
    }

    fun processed(record: ConsumerRecord<*, *>) {
        append(record).toCompletableFuture().join()
    }

    fun processed(topic: String, key: String?, partition: Int, offset: Long, kafkaTimestampMs: Long) {
        append(topic, key, partition, offset, kafkaTimestampMs).toCompletableFuture().join()
    }

    private fun append(record: ConsumerRecord<*, *>): CompletionStage<Long?> =
        append(
            topic = record.topic(),
            key = record.key()?.toString(),
            partition = record.partition(),
            offset = record.offset(),
            kafkaTimestampMs = record.timestamp()
        )

    private fun append(topic: String, key: String?, partition: Int, offset: Long, kafkaTimestampMs: Long): CompletionStage<Long?> {
        if (!enabled) {
            return java.util.concurrent.CompletableFuture.completedFuture(null)
        }
        return redisTemplate.opsForList()
            .rightPush(AUDIT_KEY, encodeAuditRecord("C", topic, partition, offset, kafkaTimestampMs, key))
            .toFuture()
    }
}

internal fun encodeAuditRecord(
    type: String,
    topic: String,
    partition: Int,
    offset: Long,
    kafkaTimestampMs: Long,
    messageKey: String?
): ByteArray =
    "$type\t${auditTopicId(topic)}\t$partition\t$offset\t$kafkaTimestampMs\t${System.currentTimeMillis()}\t${sanitizeAuditKey(messageKey)}"
        .encodeToByteArray()

private fun auditTopicId(topic: String): Int =
    when (topic) {
        DemoTopics.ORDER_EVENTS -> 1
        DemoTopics.BATCH_EVENTS -> 2
        DemoTopics.CAULDRON_EVENTS -> 3
        else -> error("No audit topic id configured for topic '$topic'")
    }

private fun sanitizeAuditKey(key: String?): String =
    key.orEmpty().replace('\t', ' ').replace('\r', ' ').replace('\n', ' ')

private const val AUDIT_KEY = "audit"
