package avh.ckc.demo.audit

const val AUDIT_ORDER_EVENTS_TOPIC = "order.events.v1"
const val AUDIT_BATCH_EVENTS_TOPIC = "batch.events.v1"
const val AUDIT_CAULDRON_EVENTS_TOPIC = "cauldron.events.v1"
const val AUDIT_WARMUP_ORDER_EVENTS_TOPIC = "ckc.traffic-warmup.v6.order.events"
const val AUDIT_WARMUP_BATCH_EVENTS_TOPIC = "ckc.traffic-warmup.v6.batch.events"
const val AUDIT_WARMUP_CAULDRON_EVENTS_TOPIC = "ckc.traffic-warmup.v6.cauldron.events"

fun auditTopicId(topic: String): Int =
    when (topic) {
        AUDIT_ORDER_EVENTS_TOPIC -> 1
        AUDIT_BATCH_EVENTS_TOPIC -> 2
        AUDIT_CAULDRON_EVENTS_TOPIC -> 3
        AUDIT_WARMUP_ORDER_EVENTS_TOPIC -> 101
        AUDIT_WARMUP_BATCH_EVENTS_TOPIC -> 102
        AUDIT_WARMUP_CAULDRON_EVENTS_TOPIC -> 103
        else -> error("No audit topic id configured for topic '$topic'")
    }
