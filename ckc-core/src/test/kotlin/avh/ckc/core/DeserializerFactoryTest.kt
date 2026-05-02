package avh.ckc.core

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotSame
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
        val factory = defaultWorkerDeserializerFactory<String, String>(stringSerdeProperties())

        val deserializers = factory(0)
        listOf(deserializers).closeAll()

        assertEquals(listOf(true, false), TrackingStringDeserializer.configuredIsKey.sortedDescending())
        assertEquals(2, TrackingStringDeserializer.createdCount.get())
        assertEquals(2, TrackingStringDeserializer.closedCount.get())
    }

    @Test
    fun `when creating worker deserializers then each invocation gets separate instances`() {
        val factory = defaultWorkerDeserializerFactory<Long, Long>(longSerdeProperties())

        val first = factory(0)
        val second = factory(1)

        assertNotSame(first.keyDeserializer, second.keyDeserializer)
        assertNotSame(first.valueDeserializer, second.valueDeserializer)
        listOf(first, second).closeAll()
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
            defaultWorkerDeserializerFactory<String, String>(properties).invoke(0)
        }

        assertEquals(
            "Kafka property '${ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG}' must be a Class or class name, but was java.lang.Integer",
            error.message
        )
    }
}
