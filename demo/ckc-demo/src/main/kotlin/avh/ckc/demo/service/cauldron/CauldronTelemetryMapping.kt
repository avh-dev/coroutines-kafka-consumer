package avh.ckc.demo.service.cauldron

import avh.ckc.demo.ml.eta.ArcaneEtaRequest
import avh.ckc.demo.ml.eta.ArcaneEtaResponse
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.model.Batch
import avh.ckc.demo.model.EtaContext

internal fun modelRequest(
    batch: Batch,
    telemetryEvent: CauldronTelemetryEvent,
    previous: EtaContext?
): ArcaneEtaRequest =
    ArcaneEtaRequest(
        batchId = batch.batchId,
        recipeId = batch.recipeId,
        cauldronId = telemetryEvent.cauldronId,
        temperatureC = telemetryEvent.temperatureC,
        densitySg = telemetryEvent.densitySg,
        previousTemperatureC = previous?.previousTemperatureC,
        previousDensitySg = previous?.previousDensitySg,
        previousBubbleRateHz = previous?.previousBubbleRateHz,
        previousMagicalEtaUnits = previous?.previousMagicalEtaUnits
    )

internal fun etaContext(
    batch: Batch,
    telemetryEvent: CauldronTelemetryEvent,
    modelResponse: ArcaneEtaResponse
): EtaContext =
    EtaContext(
        batchId = batch.batchId,
        previousTemperatureC = telemetryEvent.temperatureC,
        previousDensitySg = telemetryEvent.densitySg,
        previousBubbleRateHz = telemetryEvent.bubbleRateHz,
        previousMagicalEtaUnits = modelResponse.magicalEtaUnits,
        previousModelRequestId = modelResponse.requestId,
        updatedAt = telemetryEvent.metadata.occurredAt
    )
