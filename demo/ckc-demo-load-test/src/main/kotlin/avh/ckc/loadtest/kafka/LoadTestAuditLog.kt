package avh.ckc.loadtest.kafka

import org.apache.kafka.clients.producer.RecordMetadata

object LoadTestAuditLog {
    fun published(metadata: RecordMetadata, key: String, eventType: String) {
        println("PUBL ${metadata.topic()} $key $eventType ${metadata.partition()} ${metadata.offset()} ${System.currentTimeMillis()}")
    }

    fun generated(topic: String, key: String, eventType: String) {
        println("PUBL $topic $key $eventType - - ${System.currentTimeMillis()} dry-run")
    }
}
