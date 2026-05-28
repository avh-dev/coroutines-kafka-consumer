package avh.ckc.demo.audit

class AuditTopicIds(private val idsByTopic: Map<String, Int>) {
    fun idOf(topic: String): Int =
        idsByTopic[topic] ?: error("No audit topic id configured for topic '$topic'")

    companion object {
        val DemoDefaults = AuditTopicIds(
            mapOf(
                "order.events.v1" to 1,
                "batch.events.v1" to 2,
                "cauldron.events.v1" to 3
            )
        )
    }
}
