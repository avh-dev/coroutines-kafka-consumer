package avh.ckc.core.deserialization

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.serialization.Deserializer
import java.io.IOException

internal fun <K, V> defaultRecordDeserializerFactory(
    consumerProperties: Map<String, Any?>,
    dispatcher: CoroutineDispatcher
): RecordDeserializerFactory<K, V> = {
    KafkaRecordDeserializer(
        keyDeserializer = instantiateAndConfigureDeserializer(
            consumerProperties,
            KEY_DESERIALIZER_CLASS_CONFIG,
            isKey = true
        ),
        valueDeserializer = instantiateAndConfigureDeserializer(
            consumerProperties,
            VALUE_DESERIALIZER_CLASS_CONFIG,
            isKey = false
        ),
        dispatcher = dispatcher
    )
}

private class KafkaRecordDeserializer<K, V>(
    private val keyDeserializer: Deserializer<K>,
    private val valueDeserializer: Deserializer<V>,
    private val dispatcher: CoroutineDispatcher
) : RecordDeserializer<K, V> {
    override suspend fun deserialize(record: ConsumerRecord<ByteArray, ByteArray>): DeserializedRecord<K, V> {
        var retries = 0

        while (true) {
            try {
                return withContext(dispatcher) {
                    DeserializedRecord(
                        key = keyDeserializer.deserialize(record.topic(), record.headers(), record.key()),
                        value = valueDeserializer.deserialize(record.topic(), record.headers(), record.value())
                    )
                }
            } catch (error: Throwable) {
                if (error.isCancellation()) {
                    throw error
                }

                if (!error.isTransientDeserializationFailure() || retries >= DESERIALIZATION_MAX_RETRIES) {
                    throw error
                }

                retries++
                delay(DESERIALIZATION_RETRY_DELAY_MS)
            }
        }
    }

    override fun close() {
        try {
            keyDeserializer.close()
        } catch (_: Exception) {
        }
        try {
            valueDeserializer.close()
        } catch (_: Exception) {
        }
    }
}

private fun <T> instantiateAndConfigureDeserializer(
    consumerProperties: Map<String, Any?>,
    configKey: String,
    isKey: Boolean
): Deserializer<T> {
    val configured = consumerProperties[configKey]
        ?: error("Kafka property '$configKey' must be specified")
    val deserializerClass = when (configured) {
        is Class<*> -> configured
        is String -> Class.forName(configured)
        else -> error("Kafka property '$configKey' must be a Class or class name, but was ${configured::class.java.name}")
    }

    @Suppress("UNCHECKED_CAST")
    val deserializer = deserializerClass.getDeclaredConstructor().newInstance() as Deserializer<T>
    deserializer.configure(consumerProperties, isKey)
    return deserializer
}

internal fun <K, V> List<RecordDeserializer<K, V>>.closeAll() {
    forEach { recordDeserializer ->
        try {
            recordDeserializer.close()
        } catch (_: Exception) {
        }
    }
}

private fun Throwable.isTransientDeserializationFailure(): Boolean {
    var current: Throwable? = this
    while (current != null) {
        if (current is RetriableException || current is IOException) {
            return true
        }
        current = current.cause
    }
    return false
}

private fun Throwable.isCancellation(): Boolean = this is CancellationException

private const val DESERIALIZATION_MAX_RETRIES = 3
private const val DESERIALIZATION_RETRY_DELAY_MS = 250L
