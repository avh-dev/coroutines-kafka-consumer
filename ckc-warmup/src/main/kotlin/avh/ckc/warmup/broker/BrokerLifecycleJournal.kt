package avh.ckc.warmup.broker

/** A broker lifecycle event with its journal offset and Kafka timestamp. */
internal data class BrokerLifecycleRecord(val offset: Long, val event: BrokerLifecycleEvent, val timestamp: Long = 0)

/** Replays a contiguous lifecycle log. A fresh Started establishes a new membership history. */
internal class BrokerLifecycleJournal {
    data class Drain(
        val offset: Long,
        val request: BrokerLifecycleEvent.DrainRequested,
        val expected: Set<String>,
        val accepted: MutableSet<String> = linkedSetOf(),
        val drained: MutableSet<String> = linkedSetOf()
    ) {
        val missing: Set<String> get() = expected - drained
    }

    private val joined = linkedSetOf<String>()
    var epoch: String? = null
        private set
    var drain: Drain? = null
        private set
    var nextOffset: Long? = null
        private set

    fun accept(record: BrokerLifecycleRecord) {
        val event = record.event
        val next = nextOffset
        if (next != null && record.offset != next) {
            epoch = null
            joined.clear()
            drain = null
        }
        nextOffset = record.offset + 1
        if (event is BrokerLifecycleEvent.Started) {
            check(event.epoch != epoch) { "Duplicate broker startup epoch" }
            epoch = event.epoch
            joined.clear()
            drain = null
            return
        }
        if (event.epoch != epoch) return
        val boundary = drain
        if (boundary == null) {
            when (event) {
                is BrokerLifecycleEvent.Joined -> joined += event.context
                is BrokerLifecycleEvent.Left -> joined -= event.context
                is BrokerLifecycleEvent.DrainRequested -> drain = Drain(record.offset, event, joined.toSet())
                else -> Unit
            }
            return
        }
        if (record.timestamp > boundary.request.deadlineMillis) return
        when (event) {
            is BrokerLifecycleEvent.DrainAccepted ->
                if (event.drainId == boundary.request.drainId && event.context in boundary.expected) {
                    boundary.accepted += event.context
                }
            is BrokerLifecycleEvent.ContextDrained ->
                if (event.drainId == boundary.request.drainId && event.context in boundary.expected) {
                    boundary.drained += event.context
                }
            else -> Unit
        }
    }
}
