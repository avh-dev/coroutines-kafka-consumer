package avh.ckc.loadtest.kafka

import avh.ckc.loadtest.config.KafkaProducerSettings
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.ProducerRecord
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SwitchableProducerPoolTest {
    @Test
    fun `reconfiguration is lazy before first send`() {
        val created = mutableListOf<FakeProducerPool>()
        val pool = SwitchableProducerPool<String, String>(KafkaProducerSettings()) { settings, generation ->
            FakeProducerPool(settings, generation).also(created::add)
        }

        pool.reconfigure(KafkaProducerSettings(lingerMs = 50))

        assertTrue(created.isEmpty())
        assertEquals(50, pool.currentSettings().lingerMs)
    }

    @Test
    fun `active pool is replaced before old pool is closed`() {
        val created = mutableListOf<FakeProducerPool>()
        val pool = SwitchableProducerPool<String, String>(KafkaProducerSettings()) { settings, generation ->
            FakeProducerPool(settings, generation).also(created::add)
        }
        val callback = Callback { _, _ -> }

        pool.send(ProducerRecord("topic", "first", "value"), callback)
        pool.reconfigure(KafkaProducerSettings(lingerMs = 100))
        pool.send(ProducerRecord("topic", "second", "value"), callback)

        assertEquals(2, created.size)
        assertEquals(listOf<String?>("first"), created[0].keys)
        assertTrue(created[0].closed)
        assertEquals(0, created[0].generation)
        assertEquals(listOf<String?>("second"), created[1].keys)
        assertFalse(created[1].closed)
        assertEquals(100, created[1].settings.lingerMs)
        assertEquals(1, created[1].generation)
    }
}

private class FakeProducerPool(
    val settings: KafkaProducerSettings,
    val generation: Int
) : ProducerPool<String, String> {
    val keys = mutableListOf<String?>()
    var closed = false

    override fun send(record: ProducerRecord<String, String>, callback: Callback) {
        check(!closed)
        keys += record.key()
    }

    override fun flush() = Unit

    override fun close() {
        closed = true
    }
}
