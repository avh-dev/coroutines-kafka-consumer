package avh.ckc.warmup

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

private val controlProducerDefaults = mapOf<String, Any?>(
    ProducerConfig.LINGER_MS_CONFIG to 0
)

private val forwardingProducerDefaults = mapOf<String, Any?>(
    ProducerConfig.LINGER_MS_CONFIG to 5
)

private val controlConsumerDefaults = mapOf<String, Any?>(
    ConsumerConfig.FETCH_MIN_BYTES_CONFIG to 1,
    ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG to 3_000
)

private val recipientConsumerDefaults = mapOf<String, Any?>(
    ConsumerConfig.FETCH_MIN_BYTES_CONFIG to 1,
    ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG to 3_000
)

internal fun buildControlProducerProperties(
    userProperties: Map<String, Any?>,
    requiredProperties: Map<String, Any?>
): Map<String, Any?> = controlProducerDefaults + userProperties + requiredProperties

internal fun buildControlConsumerProperties(
    userProperties: Map<String, Any?>,
    requiredProperties: Map<String, Any?>
): Map<String, Any?> = controlConsumerDefaults + userProperties + requiredProperties

internal fun buildWarmupForwardingProducerProperties(
    userProperties: Map<String, Any?>
): Map<String, Any?> = forwardingProducerDefaults + userProperties

internal fun buildWarmupRecipientConsumerProperties(
    userProperties: Map<String, Any?>,
    requiredProperties: Map<String, Any?>
): Map<String, Any?> =
    (recipientConsumerDefaults + userProperties + requiredProperties) - ConsumerConfig.GROUP_INSTANCE_ID_CONFIG
