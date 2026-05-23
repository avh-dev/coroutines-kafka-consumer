package avh.ckc.demo.ml.eta

import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.EventMetadata
import avh.ckc.demo.model.Batch
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ArcaneEtaNormalizerTest {
    private val normalizer = ArcaneEtaNormalizer("test-profile")

    @Test
    fun `normalizer produces normalized eta estimate with propagated model metadata`() {
        val estimate = normalizer.normalize(
            batch = Batch(
                batchId = "batch-healing-001",
                recipeId = "healing-elixir-v2",
                potionId = "healing-elixir",
                cauldronId = "cauldron-3",
                status = "BREWING",
                orderIds = listOf("ord-7421", "ord-7422"),
                updatedAt = "2026-03-25T10:15:28Z"
            ),
            telemetryEvent = CauldronTelemetryEvent.newBuilder()
                .setMetadata(
                    EventMetadata.newBuilder()
                        .setEventId("telemetry-event")
                        .setOccurredAt("2026-03-25T10:15:31.245Z")
                        .setEventVersion(1)
                        .build()
                )
                .setBatchId("batch-healing-001")
                .setCauldronId("cauldron-3")
                .setTemperatureC(91.4)
                .setDensitySg(1.18)
                .build(),
            modelResponse = ArcaneEtaResponse(
                requestId = "req-1",
                regulatoryTraceId = "mrb-1",
                magicalEtaUnits = 480.0,
                moonPhase = "waxing_gibbous",
                planetaryAlignment = "favorable"
            )
        )

        assertEquals("batch-healing-001", estimate.batchId)
        assertEquals("cauldron-3", estimate.cauldronId)
        assertEquals("mrb-1", estimate.regulatoryTraceId)
        assertEquals("test-profile", estimate.normalizationProfile)
        assertTrue(estimate.etaSeconds > 0)
    }
}
