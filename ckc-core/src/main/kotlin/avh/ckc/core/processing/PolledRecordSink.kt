package avh.ckc.core.processing

import org.apache.kafka.clients.consumer.ConsumerRecord

internal interface PolledRecordSink<K, V> {
    fun tryEmit(record: ConsumerRecord<K, V>): Boolean
}
