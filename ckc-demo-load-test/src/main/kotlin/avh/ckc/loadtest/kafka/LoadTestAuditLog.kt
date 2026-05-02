package avh.ckc.loadtest.kafka

import org.apache.kafka.clients.producer.RecordMetadata

object LoadTestAuditLog {
    fun published(metadata: RecordMetadata) {
        println("PUBL ${metadata.topic()} ${metadata.partition()} ${metadata.offset()} ${System.currentTimeMillis()}")
    }
}
