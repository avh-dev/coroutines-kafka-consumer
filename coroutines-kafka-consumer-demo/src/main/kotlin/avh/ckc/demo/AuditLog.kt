package avh.ckc.demo

import org.apache.kafka.clients.consumer.ConsumerRecord

object AuditLog {
    fun processed(record: ConsumerRecord<*, *>) {
        processed(record.topic(), record.partition(), record.offset())
    }

    fun processed(topic: String, partition: Int, offset: Long) {
        println("PROC $topic $partition $offset ${System.currentTimeMillis()}")
    }
}
