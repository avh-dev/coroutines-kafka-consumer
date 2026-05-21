package avh.ckc.core.processing

import org.apache.kafka.clients.consumer.ConsumerRecord

internal interface PolledRecordSink {
    fun tryEmit(record: ConsumerRecord<ByteArray, ByteArray>): Boolean
}
