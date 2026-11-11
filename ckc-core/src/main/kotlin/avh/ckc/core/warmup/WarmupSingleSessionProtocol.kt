package avh.ckc.core.warmup

internal data class WarmupControlRecord(val offset: Long, val message: WarmupControlMessage) {
    init { require(offset >= 0) { "Control offset must not be negative" } }
}

internal enum class WarmupControlDecisionKind {
    ACTIVATED,
    ACCEPTED,
    DUPLICATE,
    CONFLICT,
    ENDED,
    IGNORED_EXPIRED,
    IGNORED_FOREIGN_TARGET,
    IGNORED_STALE
}

internal data class WarmupControlDecision(
    val kind: WarmupControlDecisionKind,
    val activeSessionId: WarmupSessionId?
)

/**
 * A deliberately small local gate for the one-session MVP protocol.
 * Ordered control offsets choose the first unexpired request; conflicts fail closed in callers.
 */
internal class WarmupSingleSessionProtocol(
    private val localInstanceId: WarmupInstanceId,
    private val currentTimeMillis: () -> Long = System::currentTimeMillis
) {
    private data class ActiveSession(val requestOffset: Long, val request: WarmupRequested)

    private var active: ActiveSession? = null
    private var lastSeenOffset = -1L

    @Synchronized
    fun evaluate(record: WarmupControlRecord): WarmupControlDecision {
        if (record.offset <= lastSeenOffset) {
            return decision(WarmupControlDecisionKind.IGNORED_STALE)
        }
        lastSeenOffset = record.offset
        val message = record.message
        if (message.targetInstanceId != null && message.targetInstanceId != localInstanceId) {
            return decision(WarmupControlDecisionKind.IGNORED_FOREIGN_TARGET)
        }

        val now = currentTimeMillis()
        if (active?.request?.expiresAtEpochMillis?.let { it <= now } == true) active = null
        if (message.expiresAtEpochMillis <= now) {
            return decision(WarmupControlDecisionKind.IGNORED_EXPIRED)
        }

        if (message is WarmupRequested) {
            val current = active
            if (current == null) {
                active = ActiveSession(record.offset, message)
                return decision(WarmupControlDecisionKind.ACTIVATED)
            }
            return if (current.request == message) {
                decision(WarmupControlDecisionKind.DUPLICATE)
            } else {
                decision(WarmupControlDecisionKind.CONFLICT)
            }
        }

        val current = active ?: return decision(WarmupControlDecisionKind.IGNORED_STALE)
        if (
            message.sessionId != current.request.sessionId ||
            message.configurationFingerprint != current.request.configurationFingerprint ||
            message.expiresAtEpochMillis != current.request.expiresAtEpochMillis ||
            !hasValidRole(message, current.request.senderInstanceId)
        ) {
            return decision(WarmupControlDecisionKind.IGNORED_STALE)
        }

        if (message is SessionEnded) {
            active = null
            return WarmupControlDecision(WarmupControlDecisionKind.ENDED, null)
        }
        return decision(WarmupControlDecisionKind.ACCEPTED)
    }

    @Synchronized
    fun activeSessionId(): WarmupSessionId? = active?.request?.sessionId

    @Synchronized
    fun winningRequestOffset(): Long? = active?.requestOffset

    private fun decision(kind: WarmupControlDecisionKind) =
        WarmupControlDecision(kind, active?.request?.sessionId)

    private fun hasValidRole(message: WarmupControlMessage, recipient: WarmupInstanceId): Boolean =
        when (message) {
            is WarmupRequested -> true
            is SessionAccepted,
            is RecipientReady,
            is ControlRecordCompleted,
            is RecipientDrained ->
                message.senderInstanceId == recipient && message.targetInstanceId != recipient
            is SessionEnded -> message.senderInstanceId == recipient
            is DonorOffered,
            is DonorAccepted,
            is SessionRejected,
            is ForwardingStopped ->
                message.senderInstanceId != recipient && message.targetInstanceId == recipient
        }
}

internal fun interface WarmupControlMessageHandler {
    suspend fun onMessage(record: WarmupControlRecord)
}

/** Routes one physical context transport to local lifecycle participants without blocking its poll loop. */
internal class WarmupControlRouter(private val localInstanceId: WarmupInstanceId) : WarmupControlMessageHandler {
    private val handlers = linkedSetOf<WarmupControlMessageHandler>()

    fun register(handler: WarmupControlMessageHandler): AutoCloseable {
        synchronized(handlers) { check(handlers.add(handler)) { "Control handler is already registered" } }
        return AutoCloseable { synchronized(handlers) { handlers.remove(handler) } }
    }

    override suspend fun onMessage(record: WarmupControlRecord) {
        val target = record.message.targetInstanceId
        if (target != null && target != localInstanceId) return
        val snapshot = synchronized(handlers) { handlers.toList() }
        snapshot.forEach { it.onMessage(record) }
    }
}
