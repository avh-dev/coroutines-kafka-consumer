package avh.ckc.demo.service.cauldron

import avh.ckc.demo.modelclient.eta.ArcaneEtaNormalizer
import avh.ckc.demo.modelclient.eta.NormalizedEtaEstimate
import avh.ckc.demo.modelclient.eta.SyncArcaneEtaModelClient
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.repository.BatchState
import avh.ckc.demo.repository.SyncBrewingStateRepository
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class SyncCauldronTelemetryService(
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
