package avh.ckc.demo.service.cauldron

import avh.ckc.demo.ml.eta.ArcaneEtaRequest
import avh.ckc.demo.ml.eta.ArcaneEtaResponse
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.model.BatchState
import avh.ckc.demo.model.ModelContextState

internal fun modelRequest(
    batchState: BatchState,
    telemetryEvent: CauldronTelemetryEvent,
    previous: ModelContextState?
): ArcaneEtaRequest =
    ArcaneEtaRequest(
        batchId = batchState.batchId,
        recipeId = batchState.recipeId,
        cauldronId = telemetryEvent.cauldronId,
        temperatureC = telemetryEvent.temperatureC,
        densitySg = telemetryEvent.densitySg,
        previousTemperatureC = previous?.previousTemperatureC,
        previousDensitySg = previous?.previousDensitySg,
        previousBubbleRateHz = previous?.previousBubbleRateHz,
        previousMagicalEtaUnits = previous?.previousMagicalEtaUnits
    )

internal fun modelContext(
    batchState: BatchState,
    telemetryEvent: CauldronTelemetryEvent,
    modelResponse: ArcaneEtaResponse
): ModelContextState =
    ModelContextState(
        batchId = batchState.batchId,
        previousTemperatureC = telemetryEvent.temperatureC,
        previousDensitySg = telemetryEvent.densitySg,
        previousBubbleRateHz = telemetryEvent.bubbleRateHz,
        previousMagicalEtaUnits = modelResponse.magicalEtaUnits,
        previousModelRequestId = modelResponse.requestId,
        updatedAt = telemetryEvent.metadata.occurredAt
    )
