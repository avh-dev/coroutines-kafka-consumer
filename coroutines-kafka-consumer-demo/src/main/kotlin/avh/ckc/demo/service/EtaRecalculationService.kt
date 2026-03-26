package avh.ckc.demo.service

import avh.ckc.demo.model.ArcaneEtaModelClient
import avh.ckc.demo.model.ArcaneEtaNormalizer
import avh.ckc.demo.model.ArcaneEtaRequest
import avh.ckc.demo.model.NormalizedEtaEstimate
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.repository.BatchState
import avh.ckc.demo.repository.BrewingStateRepository
import avh.ckc.demo.repository.ModelContextState
import org.springframework.stereotype.Service
import java.util.concurrent.CompletionStage

@Service
class EtaRecalculationService(
    private val modelClient: ArcaneEtaModelClient,
    private val normalizer: ArcaneEtaNormalizer,
    private val brewingStateRepository: BrewingStateRepository
) {
    fun recalculate(
        batchState: BatchState,
        telemetryEvent: CauldronTelemetryEvent
    ): CompletionStage<NormalizedEtaEstimate> {
        return brewingStateRepository.findModelContext(batchState.batchId).thenCompose { previous ->
            modelClient.estimate(
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
            )
                .thenCompose { modelResponse ->
                    brewingStateRepository.saveModelContext(
                        ModelContextState(
                            batchId = batchState.batchId,
                            previousTemperatureC = telemetryEvent.temperatureC,
                            previousDensitySg = telemetryEvent.densitySg,
                            previousBubbleRateHz = telemetryEvent.bubbleRateHz,
                            previousMagicalEtaUnits = modelResponse.magicalEtaUnits,
                            previousModelRequestId = modelResponse.requestId,
                            updatedAt = telemetryEvent.metadata.occurredAt
                        )
                    ).thenApply {
                        normalizer.normalize(batchState, telemetryEvent, modelResponse)
                    }
                }
        }
    }
}
