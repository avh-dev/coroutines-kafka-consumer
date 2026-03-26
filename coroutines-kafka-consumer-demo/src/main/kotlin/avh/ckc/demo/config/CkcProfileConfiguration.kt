package avh.ckc.demo.config

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.ConsumerTelemetry
import avh.ckc.demo.DemoConsumers
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.repository.BrewingStateRepository
import avh.ckc.demo.service.EtaRecalculationService
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.SmartLifecycle
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile

@Configuration(proxyBeanMethods = false)
@Profile("ckc")
class CkcProfileConfiguration {
    @Bean
    @ConditionalOnProperty(prefix = "demo.kafka", name = ["enabled"], havingValue = "true")
    fun ckcConsumerRuntime(
        properties: DemoApplicationProperties,
        brewingStateRepository: BrewingStateRepository,
        etaRecalculationService: EtaRecalculationService,
        consumerTelemetry: ConsumerTelemetry
    ): SmartLifecycle =
        CkcConsumerRuntime(
            properties,
            brewingStateRepository,
            etaRecalculationService,
            consumerTelemetry
        )
}

private class CkcConsumerRuntime(
    private val properties: DemoApplicationProperties,
    private val brewingStateRepository: BrewingStateRepository,
    private val etaRecalculationService: EtaRecalculationService,
    private val consumerTelemetry: ConsumerTelemetry
) : SmartLifecycle {
    private val logger = LoggerFactory.getLogger(javaClass)
    private lateinit var lifecycleConsumer: CoroutinesKafkaConsumer<String, OrderLifecycleEvent>
    private lateinit var telemetryConsumer: CoroutinesKafkaConsumer<String, CauldronTelemetryEvent>
    @Volatile
    private var running = false

    override fun start() {
        val commonProperties = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to properties.kafka.bootstrapServers,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
        )

        lifecycleConsumer = DemoConsumers.lifecycleConsumer(
            commonProperties + mapOf(ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.lifecycleGroupId),
            consumerTelemetry
        ) { _, event ->
            brewingStateRepository.applyLifecycleEvent(event).await()
        }

        telemetryConsumer = DemoConsumers.telemetryConsumer(
            commonProperties + mapOf(ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.telemetryGroupId),
            consumerTelemetry
        ) { _, telemetry ->
            val batchId = telemetry.batchId.ifBlank { brewingStateRepository.findActiveBatchId(telemetry.cauldronId).await() ?: "" }
            if (batchId.isBlank()) {
                return@telemetryConsumer
            }
            val batchState = brewingStateRepository.findBatch(batchId).await() ?: return@telemetryConsumer
            val estimate = etaRecalculationService.recalculate(batchState, telemetry).await()
            logger.info(
                "CKC ETA recalculated for batch={}, cauldron={}, etaSeconds={}",
                estimate.batchId,
                estimate.cauldronId,
                estimate.etaSeconds
            )
        }

        lifecycleConsumer.start()
        telemetryConsumer.start()
        running = true
    }

    override fun stop() {
        if (!running) {
            return
        }
        runBlocking {
            telemetryConsumer.stop()
            lifecycleConsumer.stop()
        }
        running = false
    }

    override fun isRunning(): Boolean = running

    override fun isAutoStartup(): Boolean = true
}
