package avh.ckc.core

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition

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

fun testRuntime(
    strategy: OverflowStrategy,
    commitIntervalMs: Long = 60_000L,
    deserializationDispatcher: CoroutineDispatcher = Dispatchers.IO,
    processingDispatcher: CoroutineDispatcher = Dispatchers.Default
): TestConsumerRuntime =
    TestConsumerRuntime(
        overflowStrategy = strategy,
        workerConcurrency = 1,
        consumerPollLoopConcurrency = 1,
        commitIntervalMs = commitIntervalMs,
        workChannelCapacity = 1024,
        deserializationDispatcher = deserializationDispatcher,
        processingDispatcher = processingDispatcher
    )

fun testConsumerProperties(vararg extraProperties: Pair<String, String>): Map<String, Any?> =
    buildMap {
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
        extraProperties.forEach { (key, value) -> put(key, value) }
    }

fun testRecord(
    topic: String = "topic-a",
    partition: Int = 0,
    offset: Long,
    key: String = "key",
    value: String = "value"
) = ConsumerRecord(
    topic,
    partition,
    offset,
    key.toByteArray(),
    value.toByteArray()
)

fun emptyRecords(): ConsumerRecords<ByteArray, ByteArray> =
    ConsumerRecords(emptyMap())

fun recordsOf(
    topicPartition: TopicPartition,
    vararg records: ConsumerRecord<ByteArray, ByteArray>
): ConsumerRecords<ByteArray, ByteArray> =
    ConsumerRecords(mapOf(topicPartition to records.toList()))

data class TestConsumerRuntime(
    val overflowStrategy: OverflowStrategy,
    val workerConcurrency: Int,
    val consumerPollLoopConcurrency: Int,
    val commitIntervalMs: Long,
    val workChannelCapacity: Int,
    val deserializationDispatcher: CoroutineDispatcher,
    val processingDispatcher: CoroutineDispatcher
)
