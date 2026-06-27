package avh.ckc.demo.service

import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service

@Service
@Profile("confluent-parallel", "confluent-parallel-reactor")
class DemoRecordAgeMetrics(
    private val meterRegistry: MeterRegistry
) {
    fun <V> onProcessed(
        consumerId: String,
        record: ConsumerRecord<String, V>
    ) {
        record(consumerId, record, error = "none")
    }

    fun <V> onFailed(
        consumerId: String,
        record: ConsumerRecord<String, V>,
        error: Throwable
    ) {
        record(consumerId, record, error = error::class.java.simpleName)
    }

    private fun <V> record(
        consumerId: String,
        record: ConsumerRecord<String, V>,
        error: String
    ) {
        DistributionSummary.builder("ckc.record.age")
            .tags(
                Tags.of(
                    "consumer_id",
                    consumerId,
                    "topic",
                    record.topic(),
                    "event_type",
                    eventType(record.value()),
                    "error",
                    error
                )
            )
            .register(meterRegistry)
            .record(recordAgeMillis(record).toDouble())
    }

    private fun recordAgeMillis(record: ConsumerRecord<*, *>): Long =
        if (record.timestamp() > 0L) {
            (System.currentTimeMillis() - record.timestamp()).coerceAtLeast(0L)
        } else {
            0L
        }

    private fun eventType(value: Any?): String =
        when (value) {
            is OrderLifecycleEvent -> value.eventType?.name ?: "UNKNOWN"
            is BatchLifecycleEvent -> value.eventType?.name ?: "UNKNOWN"
            is CauldronTelemetryEvent -> "CAULDRON_TELEMETRY"
            else -> "UNKNOWN"
        }
}
