package avh.ckc.demo.service.batch

import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.BatchLifecycleEventType
import avh.ckc.demo.registry.SuspendBrewingStepRegistryClient
import avh.ckc.demo.repository.SuspendBrewingStateRepository
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service

@Service
@Profile("ckc", "confluent-parallel-reactor", "spring-kafka-coroutines-naive")
class SuspendBatchLifecycleService(
    private val brewingStateRepository: SuspendBrewingStateRepository,
    private val registryClient: SuspendBrewingStepRegistryClient
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    suspend fun apply(event: BatchLifecycleEvent) {
        try {
            val existingBatch = brewingStateRepository.findBatch(event.batchId)
            brewingStateRepository.saveBatch(mergeBatch(event, existingBatch))
            updateActiveBatch(event, brewingStateRepository)
            reportBrewingStepIfNeeded(event)
        } catch (error: Throwable) {
            logger.error(
                "CKC batch processing failed for batchId={}, eventType={}",
                event.batchId,
                event.eventType.name,
                error
            )
            throw error
        }
    }

    private suspend fun reportBrewingStepIfNeeded(event: BatchLifecycleEvent) {
        if (event.eventType != BatchLifecycleEventType.BATCH_BREWING_STEP_COMPLETED) {
            return
        }
        val response = registryClient.reportStep(registryRequest(event))
        brewingStateRepository.saveBrewingStepReceipt(brewingStepReceipt(event, response))
    }
}
