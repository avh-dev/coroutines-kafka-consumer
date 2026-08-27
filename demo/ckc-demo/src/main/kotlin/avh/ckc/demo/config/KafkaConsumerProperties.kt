package avh.ckc.demo.config

import org.apache.kafka.clients.consumer.ConsumerConfig

internal fun DemoApplicationProperties.kafkaConsumerProperties(
    runtime: DemoApplicationProperties.ConsumerRuntime
): Map<String, Any> {
    val shared = kafka.consumer
    val topic = runtime.kafka
    return mapOf(
        ConsumerConfig.FETCH_MIN_BYTES_CONFIG to (topic.fetchMinBytes ?: shared.fetchMinBytes),
        ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG to (topic.fetchMaxWaitMs ?: shared.fetchMaxWaitMs),
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG to (topic.maxPollRecords ?: shared.maxPollRecords),
        ConsumerConfig.FETCH_MAX_BYTES_CONFIG to (topic.fetchMaxBytes ?: shared.fetchMaxBytes),
        ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG to
            (topic.maxPartitionFetchBytes ?: shared.maxPartitionFetchBytes)
    )
}
