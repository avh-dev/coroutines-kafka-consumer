package avh.ckc.demo

import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.EventMetadata
import com.google.protobuf.ByteString

fun sampleTelemetryEvent(): CauldronTelemetryEvent =
    CauldronTelemetryEvent.newBuilder()
        .setMetadata(
            EventMetadata.newBuilder()
                .setEventId("evt-cauldron-3-telemetry-001")
                .setOccurredAt("2026-03-25T10:15:31.245Z")
                .setEventVersion(1)
                .build()
        )
        .setCauldronId("cauldron-3")
        .setBatchId("batch-healing-001")
        .setTemperatureC(91.4)
        .setDensitySg(1.18)
        .setBubbleRateHz(8.7)
        .setFuelLevelPct(42.0)
        .setSootIndex(0.31)
        .setDiagnosticsBlob(ByteString.copyFrom(ByteArray(512) { (it % 32).toByte() }))
        .build()
