package avh.ckc.core.warmup

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * A dormant forwarding hook installed in a long-lived main consumer. The coordinator activates
 * exactly one session after its recipient is ready and returns the hook to local-only routing
 * after the session boundary has drained.
 */
internal class SwitchableWarmupRecordForwarding<K, V>(
    private val forwardingFactory: (
        WarmupForwardingConfiguration,
        WarmupForwardingObserver
    ) -> WarmupRecordForwarding<K, V> = { configuration, observer ->
        kafkaWarmupRecordForwarding(configuration, observer)
    }
) : WarmupRecordForwarding<K, V> {
    private val accessLock = Any()
    private val lifecycleMutex = Mutex()
    private var active: ActiveForwarding<K, V>? = null
    private var closed = false

    override suspend fun forwardIfSelected(record: ConsumerRecord<K, V>): Boolean {
        val current = synchronized(accessLock) {
            active?.also { it.readers++ }
        } ?: return false
        return try {
            current.forwarding.forwardIfSelected(record)
        } finally {
            synchronized(accessLock) {
                current.readers--
                if (current.retired && current.readers == 0) current.noReaders.complete(Unit)
            }
        }
    }

    suspend fun activate(
        configuration: WarmupForwardingConfiguration,
        observer: WarmupForwardingObserver
    ) = lifecycleMutex.withLock {
        synchronized(accessLock) {
            check(!closed) { "Warm-up forwarding gateway is closed" }
            check(active == null) { "A warm-up forwarding session is already active" }
        }
        val forwarding = forwardingFactory(configuration, observer).also { it.start() }
        synchronized(accessLock) {
            check(!closed && active == null) { "Warm-up forwarding gateway changed during activation" }
            active = ActiveForwarding(configuration.sessionId, forwarding)
        }
    }

    override suspend fun complete(): WarmupForwardingBoundary =
        synchronized(accessLock) {
            requireNotNull(active) { "No warm-up forwarding session is active" }.forwarding
        }.complete()

    suspend fun finish(sessionId: WarmupSessionId) = lifecycleMutex.withLock {
        val current = retireActive(sessionId)
        current.noReaders.await()
        current.forwarding.close()
    }

    override suspend fun close() {
        lifecycleMutex.withLock {
            val current = synchronized(accessLock) {
                if (closed) return@withLock
                closed = true
                active?.also {
                    active = null
                    it.retired = true
                    if (it.readers == 0) it.noReaders.complete(Unit)
                }
            }
            current?.let {
                it.noReaders.await()
                it.forwarding.close()
            }
        }
    }

    private fun retireActive(sessionId: WarmupSessionId): ActiveForwarding<K, V> = synchronized(accessLock) {
        val current = requireNotNull(active) { "No warm-up forwarding session is active" }
        check(current.sessionId == sessionId) { "A different warm-up forwarding session is active" }
        active = null
        current.retired = true
        if (current.readers == 0) current.noReaders.complete(Unit)
        current
    }

    private class ActiveForwarding<K, V>(
        val sessionId: WarmupSessionId,
        val forwarding: WarmupRecordForwarding<K, V>
    ) {
        var readers: Int = 0
        var retired: Boolean = false
        val noReaders = CompletableDeferred<Unit>()
    }
}
