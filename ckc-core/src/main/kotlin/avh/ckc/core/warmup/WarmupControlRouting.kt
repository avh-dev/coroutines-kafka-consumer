package avh.ckc.core.warmup

internal data class WarmupControlRecord(val offset: Long, val message: WarmupControlMessage) {
    init { require(offset >= 0) { "Control offset must not be negative" } }
}

internal fun interface WarmupControlMessageHandler {
    suspend fun onMessage(record: WarmupControlRecord)
}

/** Routes one physical transport to the complete state table and to locally targeted lifecycle handlers. */
internal class WarmupControlRouter(private val localInstanceId: WarmupInstanceId) : WarmupControlMessageHandler {
    private val handlers = linkedSetOf<WarmupControlMessageHandler>()
    private val globalHandlers = linkedSetOf<WarmupControlMessageHandler>()

    fun register(handler: WarmupControlMessageHandler): AutoCloseable {
        synchronized(handlers) { check(handlers.add(handler)) { "Control handler is already registered" } }
        return AutoCloseable { synchronized(handlers) { handlers.remove(handler) } }
    }

    fun registerGlobal(handler: WarmupControlMessageHandler): AutoCloseable {
        synchronized(globalHandlers) { check(globalHandlers.add(handler)) { "Global control handler is already registered" } }
        return AutoCloseable { synchronized(globalHandlers) { globalHandlers.remove(handler) } }
    }

    override suspend fun onMessage(record: WarmupControlRecord) {
        synchronized(globalHandlers) { globalHandlers.toList() }.forEach { it.onMessage(record) }
        val target = record.message.targetInstanceId
        if (target != null && target != localInstanceId) return
        synchronized(handlers) { handlers.toList() }.forEach { it.onMessage(record) }
    }
}
