package avh.ckc.demo.audit

const val AUDIT_ORDER_EVENTS_TOPIC = "order.events.v1"
const val AUDIT_BATCH_EVENTS_TOPIC = "batch.events.v1"
const val AUDIT_CAULDRON_EVENTS_TOPIC = "cauldron.events.v1"

fun auditTopicId(topic: String): Int =
    when (topic) {
        AUDIT_ORDER_EVENTS_TOPIC -> 1
        AUDIT_BATCH_EVENTS_TOPIC -> 2
        AUDIT_CAULDRON_EVENTS_TOPIC -> 3
        else -> error("No audit topic id configured for topic '$topic'")
    }
