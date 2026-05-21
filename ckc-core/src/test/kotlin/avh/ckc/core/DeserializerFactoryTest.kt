package avh.ckc.core

import avh.ckc.core.deserialization.closeAll
import avh.ckc.core.deserialization.defaultRecordDeserializerFactory
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class DeserializerFactoryTest {

    @BeforeEach
    fun resetTrackingDeserializers() {
        TrackingStringDeserializer.reset()
        TrackingLongDeserializer.reset()
    }

    @Test
    fun `when creating worker deserializers from properties then configure and close are invoked`() {
        val factory = defaultRecordDeserializerFactory<String, String>(stringSerdeProperties(), Dispatchers.Unconfined)

        val deserializer = factory(0)
        listOf(deserializer).closeAll()

        assertEquals(listOf(true, false), TrackingStringDeserializer.configuredIsKey.sortedDescending())
        assertEquals(2, TrackingStringDeserializer.createdCount.get())
        assertEquals(2, TrackingStringDeserializer.closedCount.get())
    }

    @Test
    fun `when creating worker deserializers then each invocation gets separate instances`() {
        val factory = defaultRecordDeserializerFactory<String, String>(stringSerdeProperties(), Dispatchers.Unconfined)

        val first = factory(0)
        val second = factory(1)

        listOf(first, second).closeAll()
        assertEquals(4, TrackingStringDeserializer.createdCount.get())
        assertEquals(4, TrackingStringDeserializer.closedCount.get())
    }

    @Test
    fun `when deserializer fails with transient error then it is retried internally`() = runBlocking {
        FlakyLongDeserializer.configureNext(failuresBeforeSuccess = 2, failure = java.io.IOException("registry down"))
        val deserializer = defaultRecordDeserializerFactory<Long, Long>(
            longSerdeProperties() + mapOf(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to FlakyLongDeserializer::class.java),
            Dispatchers.Unconfined
        )

        val record = deserializer(0).deserialize(testRecord(offset = 23L, value = "23"))

        assertEquals(23L, record.value)
    }

    @Test
    fun `when deserializer property has invalid type then factory throws`() {
        val properties = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "dummy:9092",
            ConsumerConfig.GROUP_ID_CONFIG to "test-group",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to 123,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to TrackingStringDeserializer::class.java
        )

        val error = assertThrows(IllegalStateException::class.java) {
            defaultRecordDeserializerFactory<String, String>(properties, Dispatchers.Unconfined).invoke(0)
        }

        assertEquals(
            "Kafka property '${ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG}' must be a Class or class name, but was java.lang.Integer",
            error.message
        )
    }
}
