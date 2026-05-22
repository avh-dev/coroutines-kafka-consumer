package avh.ckc.core

import avh.ckc.core.processing.deserialization.DeserializedRecord
import avh.ckc.core.processing.deserialization.RecordDeserializer
import avh.ckc.core.processing.deserialization.RecordDeserializerFactory
import avh.ckc.core.processing.deserialization.defaultRecordDeserializerFactory
import kotlinx.coroutines.Dispatchers
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.Deserializer
import java.nio.charset.StandardCharsets
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.Volatile

internal fun stringSerdeProperties(): Map<String, Any?> = mapOf(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "dummy:9092",
    ConsumerConfig.GROUP_ID_CONFIG to "test-group",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to TrackingStringDeserializer::class.java,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to TrackingStringDeserializer::class.java
)

internal fun longSerdeProperties(): Map<String, Any?> = mapOf(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "dummy:9092",
    ConsumerConfig.GROUP_ID_CONFIG to "test-group",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to TrackingLongDeserializer::class.java,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to TrackingLongDeserializer::class.java
)

internal fun <K, V> defaultRecordDeserializerFactoryForTests(
    consumerProperties: Map<String, Any?>
): RecordDeserializerFactory<K, V> =
    defaultRecordDeserializerFactory(consumerProperties, Dispatchers.Unconfined)

internal class TestRecordDeserializer<K, V>(
    private val keyDeserializer: Deserializer<K>,
    private val valueDeserializer: Deserializer<V>
) : RecordDeserializer<K, V> {
    override suspend fun deserialize(record: ConsumerRecord<ByteArray, ByteArray>): DeserializedRecord<K, V> =
        DeserializedRecord(
            key = keyDeserializer.deserialize(record.topic(), record.headers(), record.key()),
            value = valueDeserializer.deserialize(record.topic(), record.headers(), record.value())
        )

    override fun close() {
        keyDeserializer.close()
        valueDeserializer.close()
    }
}

internal fun <T> instantiateAndConfigureDeserializerForTests(
    consumerProperties: Map<String, Any?>,
    configKey: String,
    isKey: Boolean
): Deserializer<T> {
    val clazz = consumerProperties[configKey] as Class<*>
    @Suppress("UNCHECKED_CAST")
    return clazz.getDeclaredConstructor().newInstance().let { instance ->
        (instance as Deserializer<T>).apply { configure(consumerProperties, isKey) }
    }
}

internal class TrackingStringDeserializer : Deserializer<String> {
    private var isKeyDeserializer: Boolean = false

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
        isKeyDeserializer = isKey
        configuredIsKey += isKey
    }

    override fun deserialize(topic: String?, data: ByteArray?): String? =
        deserialize(topic, null, data)

    override fun deserialize(topic: String?, headers: Headers?, data: ByteArray?): String? {
        val threadName = Thread.currentThread().name
        if (isKeyDeserializer) {
            lastKeyThreadName = threadName
        } else {
            lastValueThreadName = threadName
        }
        return data?.toString(StandardCharsets.UTF_8)
    }

    override fun close() {
        closedCount.incrementAndGet()
    }

    companion object {
        val configuredIsKey = CopyOnWriteArrayList<Boolean>()
        val createdCount = AtomicInteger()
        val closedCount = AtomicInteger()

        @Volatile
        var lastKeyThreadName: String = ""

        @Volatile
        var lastValueThreadName: String = ""

        fun reset() {
            configuredIsKey.clear()
            createdCount.set(0)
            closedCount.set(0)
            lastKeyThreadName = ""
            lastValueThreadName = ""
        }
    }

    init {
        createdCount.incrementAndGet()
    }
}

internal class TrackingLongDeserializer : Deserializer<Long> {
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) = Unit

    override fun deserialize(topic: String?, data: ByteArray?): Long? =
        deserialize(topic, null, data)

    override fun deserialize(topic: String?, headers: Headers?, data: ByteArray?): Long? =
        data?.toString(StandardCharsets.UTF_8)?.toLongOrNull()
            ?: 0L

    override fun close() = Unit

    companion object {
        fun reset() = Unit
    }
}

internal class FlakyLongDeserializer(
    private val failuresBeforeSuccess: Int,
    private val failure: Throwable
) : Deserializer<Long> {
    constructor() : this(nextFailuresBeforeSuccess, nextFailure)

    private val attempts = AtomicInteger()

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) = Unit

    override fun deserialize(topic: String?, data: ByteArray?): Long? =
        deserialize(topic, null, data)

    override fun deserialize(topic: String?, headers: Headers?, data: ByteArray?): Long? {
        if (attempts.getAndIncrement() < failuresBeforeSuccess) {
            throw failure
        }
        return data?.toString(StandardCharsets.UTF_8)?.toLong()
    }

    override fun close() = Unit

    companion object {
        @Volatile
        private var nextFailuresBeforeSuccess: Int = 0

        @Volatile
        private var nextFailure: Throwable = RuntimeException("configured failure")

        fun configureNext(failuresBeforeSuccess: Int, failure: Throwable) {
            nextFailuresBeforeSuccess = failuresBeforeSuccess
            nextFailure = failure
        }
    }
}
