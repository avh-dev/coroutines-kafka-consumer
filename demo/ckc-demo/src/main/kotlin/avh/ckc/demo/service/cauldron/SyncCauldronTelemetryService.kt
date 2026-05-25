package avh.ckc.demo.service.cauldron

import avh.ckc.demo.ml.eta.ArcaneEtaNormalizer
import avh.ckc.demo.ml.eta.NormalizedEtaEstimate
import avh.ckc.demo.ml.eta.SyncArcaneEtaModelClient
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.model.Batch
import avh.ckc.demo.repository.SyncBrewingStateRepository
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service

@Service
@Profile("spring-kafka", "confluent-parallel")
class SyncCauldronTelemetryService(
    private val modelClient: SyncArcaneEtaModelClient,
    private val normalizer: ArcaneEtaNormalizer,
    private val brewingStateRepository: SyncBrewingStateRepository
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun recalculate(telemetryEvent: CauldronTelemetryEvent): NormalizedEtaEstimate? {
        try {
            val batch = findBatch(telemetryEvent) ?: return null
            val previous = brewingStateRepository.findEtaContext(batch.batchId)
            val modelResponse = modelClient.estimate(modelRequest(batch, telemetryEvent, previous))

            brewingStateRepository.saveEtaContext(etaContext(batch, telemetryEvent, modelResponse))

            val estimate = normalizer.normalize(batch, telemetryEvent, modelResponse)
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

    private fun findBatch(telemetryEvent: CauldronTelemetryEvent): Batch? {
        val batchId = telemetryEvent.batchId.ifBlank {
            brewingStateRepository.findActiveBatchId(telemetryEvent.cauldronId) ?: ""
        }
        if (batchId.isBlank()) {
            return null
        }

        return brewingStateRepository.findBatch(batchId)
    }
}
