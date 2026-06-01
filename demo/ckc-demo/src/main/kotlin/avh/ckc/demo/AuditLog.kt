package avh.ckc.demo

import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.config.DemoRedisCommands
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.stereotype.Component

@OptIn(ExperimentalLettuceCoroutinesApi::class)
@Component
class AuditLog(
    properties: DemoApplicationProperties,
    private val redisCommands: DemoRedisCommands
) {
    private val enabled = properties.audit.enabled

    suspend fun processedSuspending(record: ConsumerRecord<*, *>) {
        if (enabled) {
            redisCommands.coroutines().rpush(AUDIT_KEY, encodeProcessedRecord(record))
        }
    }

    fun processed(record: ConsumerRecord<*, *>) {
        if (enabled) {
            redisCommands.sync().rpush(AUDIT_KEY, encodeProcessedRecord(record))
        }
    }

    fun processed(topic: String, key: String?, partition: Int, offset: Long, kafkaTimestampMs: Long) {
        if (enabled) {
            redisCommands.sync().rpush(AUDIT_KEY, encodeAuditRecord("C", topic, partition, offset, kafkaTimestampMs, key))
        }
    }

    private fun encodeProcessedRecord(record: ConsumerRecord<*, *>): ByteArray =
        encodeAuditRecord(
            type = "C",
            topic = record.topic(),
            messageKey = record.key()?.toString(),
            partition = record.partition(),
            offset = record.offset(),
            kafkaTimestampMs = record.timestamp()
        )
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
