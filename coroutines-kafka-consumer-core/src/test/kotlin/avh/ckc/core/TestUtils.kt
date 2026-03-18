package avh.ckc.core

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition
import java.util.Properties

suspend fun <T : Any> awaitFor(
    timeoutMillis: Long,
    pauseMillis: Long = 50,
    block: suspend CoroutineScope.() -> T?
): T = withTimeout(timeoutMillis) {
    while (true) {
        val value = block()
        if (value != null) return@withTimeout value
        delay(pauseMillis)
    }
    error("unreachable")
}

fun testConfig(
    strategy: OverflowStrategy,
    commitIntervalMs: Long = 60_000L
): CoroutineKafkaConsumerConfig {
    val props = Properties().apply {
        put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092")
        put(ConsumerConfig.GROUP_ID_CONFIG, "test-group")
        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10")
        put(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.ByteArrayDeserializer"
        )
        put(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.ByteArrayDeserializer"
        )
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    }

    return CoroutineKafkaConsumerConfig(
        overflowStrategy = strategy,
        workerConcurrency = 1,
        consumerPollLoopConcurrency = 1,
        commitIntervalMs = commitIntervalMs,
        kafkaProperties = props
    )
}

fun emptyRecords(): ConsumerRecords<ByteArray, ByteArray> =
    ConsumerRecords(emptyMap())

fun recordsOf(
    topicPartition: TopicPartition,
    vararg records: ConsumerRecord<ByteArray, ByteArray>
): ConsumerRecords<ByteArray, ByteArray> =
    ConsumerRecords(mapOf(topicPartition to records.toList()))
