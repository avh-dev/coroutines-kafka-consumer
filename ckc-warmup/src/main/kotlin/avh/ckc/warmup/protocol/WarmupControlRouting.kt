package avh.ckc.warmup.protocol

/** A decoded control message paired with its Kafka log offset. */
internal data class WarmupControlRecord(val offset: Long, val message: WarmupControlMessage) {
    init {
        require(offset >= 0) { "Control offset must not be negative" }
    }
}

/** Consumes a routed control record in dispatch order. */
internal fun interface WarmupControlMessageHandler {
    suspend fun onMessage(record: WarmupControlRecord)
}

/** Dispatches only locally targeted or broadcast messages from the configured v6 namespace. */
internal class WarmupControlRouter(
    private val localInstanceId: WarmupInstanceId,
    private val namespace: WarmupControlNamespace
) : WarmupControlMessageHandler {
    private val handlers = linkedSetOf<WarmupControlMessageHandler>()

    fun register(handler: WarmupControlMessageHandler): AutoCloseable {
        synchronized(handlers) { check(handlers.add(handler)) { "Control handler is already registered" } }
        return AutoCloseable { synchronized(handlers) { handlers.remove(handler) } }
    }

    override suspend fun onMessage(record: WarmupControlRecord) {
        if (record.message.namespace != namespace) return
        val target = record.message.targetInstanceId
        if (target != null && target != localInstanceId) return
        synchronized(handlers) { handlers.toList() }.forEach { it.onMessage(record) }
    }
}
