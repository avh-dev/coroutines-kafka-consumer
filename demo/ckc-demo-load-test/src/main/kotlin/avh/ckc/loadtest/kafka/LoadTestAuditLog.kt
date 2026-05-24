package avh.ckc.loadtest.kafka

import org.apache.kafka.clients.producer.RecordMetadata

object LoadTestAuditLog {
    fun published(metadata: RecordMetadata, key: String) {
        println("PUBL ${metadata.topic()} $key ${metadata.partition()} ${metadata.offset()} ${System.currentTimeMillis()}")
    }
}
