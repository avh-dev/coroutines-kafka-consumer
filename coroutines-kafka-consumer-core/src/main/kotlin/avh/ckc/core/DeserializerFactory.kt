package avh.ckc.core

import org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.common.serialization.Deserializer

internal fun <K, V> defaultWorkerDeserializerFactory(
    consumerProperties: Map<String, Any?>
): WorkerDeserializerFactory<K, V> = {
    WorkerDeserializers(
        keyDeserializer = instantiateAndConfigureDeserializer(
            consumerProperties,
            KEY_DESERIALIZER_CLASS_CONFIG,
            isKey = true
        ),
        valueDeserializer = instantiateAndConfigureDeserializer(
            consumerProperties,
            VALUE_DESERIALIZER_CLASS_CONFIG,
            isKey = false
        )
    )
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

internal fun <K, V> List<WorkerDeserializers<K, V>>.closeAll() {
    forEach { workerDeserializers ->
        try {
            workerDeserializers.keyDeserializer.close()
        } catch (_: Exception) {
        }
        try {
            workerDeserializers.valueDeserializer.close()
        } catch (_: Exception) {
        }
    }
}
