package avh.ckc.loadtest.domain

import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.EventMetadata
import com.google.protobuf.ByteString
import java.time.Instant
import kotlin.math.sin
import kotlin.random.Random

class CauldronTelemetryFactory(
    private val diagnosticsBlobSize: Int,
    private val random: Random = Random.Default
) {
    fun create(activeBatch: ActiveBatch, occurredAt: Instant): CauldronTelemetryEvent {
        val sample = activeBatch.telemetrySequence++
        val wobble = sin(sample.toDouble() / 4.0)

        return CauldronTelemetryEvent.newBuilder()
            .setMetadata(
                EventMetadata.newBuilder()
                    .setEventId("evt-${activeBatch.batchId}-telemetry-$sample")
                    .setOccurredAt(occurredAt.toString())
                    .setEventVersion(1)
                    .setRegulatoryTraceId("mrb-${activeBatch.batchId}-$sample")
                    .build()
            )
            .setCauldronId(activeBatch.cauldronId)
            .setBatchId(activeBatch.batchId)
            .setTemperatureC(88.0 + (wobble * 6.0))
            .setDensitySg(1.12 + (wobble * 0.04))
            .setBubbleRateHz(7.5 + (wobble * 1.2))
            .setFuelLevelPct((65.0 - sample * 0.4).coerceAtLeast(5.0))
            .setSootIndex((0.2 + sample * 0.01).coerceAtMost(0.95))
            .setDiagnosticsBlob(ByteString.copyFrom(random.nextBytes(diagnosticsBlobSize)))
            .build()
    }
}
