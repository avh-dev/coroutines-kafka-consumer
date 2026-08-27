package avh.ckc.loadtest.kafka

import avh.ckc.loadtest.config.KafkaProducerSettings
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

internal interface ProducerPool<K, V> : AutoCloseable {
    fun send(record: ProducerRecord<K, V>, callback: Callback)

    fun flush()
}

internal class SwitchableProducerPool<K, V>(
    initialSettings: KafkaProducerSettings,
    private val factory: (KafkaProducerSettings, Int) -> ProducerPool<K, V>
) : AutoCloseable {
    private val lock = ReentrantReadWriteLock()
    private var settings = initialSettings
    private var generation = 0
    private var current: ProducerPool<K, V>? = null
    private var closed = false

    fun send(record: ProducerRecord<K, V>, callback: Callback) {
        ensureInitialized()
        lock.read { requireNotNull(current).send(record, callback) }
    }

    fun flush() {
        lock.read { current?.flush() }
    }

    fun reconfigure(newSettings: KafkaProducerSettings) {
        val oldPool = lock.write {
            check(!closed) { "producer pool is closed" }
            settings = newSettings
            val old = current ?: return@write null
            generation += 1
            current = factory(settings, generation)
            old
        }
        oldPool?.close()
    }

    fun currentSettings(): KafkaProducerSettings = lock.read { settings }

    override fun close() {
        val oldPool = lock.write {
            if (closed) return@write null
            closed = true
            val old = current
            current = null
            old
        }
        oldPool?.close()
    }

    private fun ensureInitialized() {
        if (lock.read { current != null }) return
        lock.write {
            check(!closed) { "producer pool is closed" }
            if (current == null) current = factory(settings, generation)
        }
    }
}
