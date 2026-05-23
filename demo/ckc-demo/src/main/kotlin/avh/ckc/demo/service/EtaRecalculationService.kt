package avh.ckc.demo.service

import avh.ckc.demo.modelclient.eta.ArcaneEtaNormalizer
import avh.ckc.demo.modelclient.eta.ArcaneEtaRequest
import avh.ckc.demo.modelclient.eta.NormalizedEtaEstimate
import avh.ckc.demo.modelclient.eta.SuspendArcaneEtaModelClient
import avh.ckc.demo.modelclient.eta.SyncArcaneEtaModelClient
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.repository.BatchState
import avh.ckc.demo.repository.ModelContextState
import avh.ckc.demo.repository.SuspendBrewingStateRepository
import avh.ckc.demo.repository.SyncBrewingStateRepository
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class SyncEtaRecalculationService(
    private val modelClient: SyncArcaneEtaModelClient,
    private val normalizer: ArcaneEtaNormalizer,
    private val brewingStateRepository: SyncBrewingStateRepository
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun recalculate(telemetryEvent: CauldronTelemetryEvent): NormalizedEtaEstimate? {
        try {
            val batchState = findBatchState(telemetryEvent) ?: return null
            val previous = brewingStateRepository.findModelContext(batchState.batchId)
            val modelResponse = modelClient.estimate(modelRequest(batchState, telemetryEvent, previous))

            brewingStateRepository.saveModelContext(modelContext(batchState, telemetryEvent, modelResponse))

            val estimate = normalizer.normalize(batchState, telemetryEvent, modelResponse)
            logger.info(
                "Spring Kafka ETA recalculated for batch={}, cauldron={}, etaSeconds={}",
                estimate.batchId,
                estimate.cauldronId,
                estimate.etaSeconds
            )
            return estimate
        } catch (error: Throwable) {
            logger.error(
                "Spring Kafka telemetry processing failed for cauldronId={}, batchId={}, occurredAt={}",
                telemetryEvent.cauldronId,
                telemetryEvent.batchId,
                telemetryEvent.metadata.occurredAt,
                error
            )
            throw error
        }
    }

    private fun findBatchState(telemetryEvent: CauldronTelemetryEvent): BatchState? {
        val batchId = telemetryEvent.batchId.ifBlank {
            brewingStateRepository.findActiveBatchId(telemetryEvent.cauldronId) ?: ""
        }
        if (batchId.isBlank()) {
            return null
        }

        return brewingStateRepository.findBatch(batchId)
    }
}

@Service
class SuspendEtaRecalculationService(
    private val modelClient: SuspendArcaneEtaModelClient,
    private val normalizer: ArcaneEtaNormalizer,
    private val brewingStateRepository: SuspendBrewingStateRepository
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    suspend fun recalculate(telemetryEvent: CauldronTelemetryEvent): NormalizedEtaEstimate? {
        try {
            val batchState = findBatchState(telemetryEvent) ?: return null
            val previous = brewingStateRepository.findModelContext(batchState.batchId)
            val modelResponse = modelClient.estimate(modelRequest(batchState, telemetryEvent, previous))

            brewingStateRepository.saveModelContext(modelContext(batchState, telemetryEvent, modelResponse))

            val estimate = normalizer.normalize(batchState, telemetryEvent, modelResponse)
            logger.info(
                "CKC ETA recalculated for batch={}, cauldron={}, etaSeconds={}",
                estimate.batchId,
                estimate.cauldronId,
                estimate.etaSeconds
            )
            return estimate
        } catch (error: Throwable) {
            logger.error(
                "CKC telemetry processing failed for cauldronId={}, batchId={}, occurredAt={}",
                telemetryEvent.cauldronId,
                telemetryEvent.batchId,
                telemetryEvent.metadata.occurredAt,
                error
            )
            throw error
        }
    }

    private suspend fun findBatchState(telemetryEvent: CauldronTelemetryEvent): BatchState? {
        val batchId = telemetryEvent.batchId.ifBlank {
            brewingStateRepository.findActiveBatchId(telemetryEvent.cauldronId) ?: ""
        }
        if (batchId.isBlank()) {
            return null
        }

        return brewingStateRepository.findBatch(batchId)
    }
}

private fun modelRequest(
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

private fun modelContext(
    batchState: BatchState,
    telemetryEvent: CauldronTelemetryEvent,
    modelResponse: avh.ckc.demo.modelclient.eta.ArcaneEtaResponse
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
