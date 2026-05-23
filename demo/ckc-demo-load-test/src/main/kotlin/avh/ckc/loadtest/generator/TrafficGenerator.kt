package avh.ckc.loadtest.generator

import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.loadtest.config.LoadTestConfig
import avh.ckc.loadtest.domain.ActiveBatch
import avh.ckc.loadtest.domain.CauldronTelemetryFactory
import avh.ckc.loadtest.domain.GeneratedBatch
import avh.ckc.loadtest.domain.OrderLifecycleStateMachine
import avh.ckc.loadtest.domain.PendingOrder
import avh.ckc.loadtest.domain.PotionRecipe
import avh.ckc.loadtest.kafka.LoadTestPublisher
import avh.ckc.loadtest.runtime.ShardContext
import avh.ckc.loadtest.scenario.LoadScenario
import avh.ckc.loadtest.scenario.ScenarioEvaluationContext
import kotlinx.coroutines.delay
import java.time.Duration
import java.time.Instant
import java.util.ArrayDeque
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.floor

class TrafficGenerator(
    private val shardContext: ShardContext,
    private val config: LoadTestConfig,
    private val scenario: LoadScenario,
    private val producers: LoadTestPublisher
) {
    private val recipes = listOf(
        PotionRecipe("healing-elixir", "healing-elixir-v2"),
        PotionRecipe("mana-tonic", "mana-tonic-v1"),
        PotionRecipe("night-vision-draught", "night-vision-v3")
    )
    private val stateMachine = OrderLifecycleStateMachine(shardContext)
    private val telemetryFactory = CauldronTelemetryFactory(config.diagnosticsBlobSize)
    private val orderBacklog = ArrayDeque<OrderLifecycleEvent>()
    private val batchBacklog = ArrayDeque<BatchLifecycleEvent>()
    private val telemetryCauldrons = ArrayDeque<TelemetryCauldron>()
    private val orderSequence = AtomicLong(0)
    private val batchSequence = AtomicLong(0)
    private val cauldronSequence = AtomicLong(0)
    private var orderAccumulator = 0.0
    private var telemetryAccumulator = 0.0
    private var lastHeartbeatAt = Instant.EPOCH
    private var emitBatchNext = false

    suspend fun run() {
        val startedAt = shardContext.testRunStartedAt ?: Instant.now()
        val lifecycleContext = ScenarioEvaluationContext(config.lifecycleBaseRate)
        val telemetryContext = ScenarioEvaluationContext(config.telemetryBaseRate)
        val tickSeconds = config.tickInterval.toMillis().toDouble() / 1000.0

        while (true) {
            val now = Instant.now()
            val lifecyclePhase = scenario.phaseAt(now, startedAt, lifecycleContext)
            val telemetryPhase = scenario.phaseAt(now, startedAt, telemetryContext)
            maybeLogHeartbeat(now, lifecyclePhase?.name ?: "completed")

            if (lifecyclePhase == null && telemetryPhase == null) {
                producers.flush()
                return
            }

            orderAccumulator += (lifecyclePhase?.currentRate() ?: 0.0) * tickSeconds
            telemetryAccumulator += (telemetryPhase?.currentRate() ?: 0.0) * tickSeconds

            repeat(floor(orderAccumulator).toInt()) {
                emitLifecycle(now)
                orderAccumulator -= 1.0
            }

            repeat(floor(telemetryAccumulator).toInt()) {
                emitTelemetry(now)
                telemetryAccumulator -= 1.0
            }

            delay(config.tickInterval.toMillis())
        }
    }

    private fun maybeLogHeartbeat(now: Instant, phaseName: String) {
        if (Duration.between(lastHeartbeatAt, now) < Duration.ofSeconds(15)) {
            return
        }
        lastHeartbeatAt = now
        producers.logSnapshot(
            "heartbeat phase=$phaseName orderBacklog=${orderBacklog.size} batchBacklog=${batchBacklog.size} activeCauldrons=${telemetryCauldrons.size} " +
                "generatedOrders=${orderSequence.get()} generatedBatches=${batchSequence.get()}"
        )
    }

    private fun emitLifecycle(now: Instant) {
        if (orderBacklog.isEmpty() && batchBacklog.isEmpty()) {
            val batch = createLifecycleBatch(now)
            orderBacklog.addAll(batch.orderEvents)
            batchBacklog.addAll(batch.batchEvents)
        }

        if (batchBacklog.isNotEmpty() && (emitBatchNext || orderBacklog.isEmpty())) {
            emitBatchNext = false
            val event = batchBacklog.removeFirst()
            producers.sendBatch(event.batchId, event)
        } else if (orderBacklog.isNotEmpty()) {
            emitBatchNext = batchBacklog.isNotEmpty()
            val event = orderBacklog.removeFirst()
            producers.sendOrder(event.orderId, event)
        }
    }

    private fun createLifecycleBatch(now: Instant): GeneratedBatch {
        val firstOrderIndex = orderSequence.getAndAdd(config.lifecycleOrdersPerBatch.toLong()).toInt() + 1
        val batchSlot = batchSequence.incrementAndGet().toInt()
        val recipe = recipes[(batchSlot + shardContext.shardIndex) % recipes.size]

        val generated = stateMachine.createOrderBatch(
            orderIndex = firstOrderIndex,
            batchSlot = batchSlot,
            ordersPerBatch = config.lifecycleOrdersPerBatch,
            potionId = recipe.potionId,
            recipeId = recipe.recipeId
        )

        return generated.copy(
            orderEvents = generated.orderEvents.map { it.toBuilder().setMetadata(it.metadata.toBuilder().setOccurredAt(now.toString())).build() },
            batchEvents = generated.batchEvents.map { it.toBuilder().setMetadata(it.metadata.toBuilder().setOccurredAt(now.toString())).build() }
        )
    }

    private fun emitTelemetry(now: Instant) {
        val cauldron = when {
            telemetryCauldrons.isEmpty() -> createTelemetryCauldron(now)
            Duration.between(telemetryCauldrons.first().lastTelemetryAt, now) < config.telemetryInterval ->
                createTelemetryCauldron(now)
            else -> telemetryCauldrons.removeFirst()
        }

        producers.sendTelemetry(cauldron.activeBatch.cauldronId, telemetryFactory.create(cauldron.activeBatch, now))
        telemetryCauldrons.addLast(cauldron.copy(lastTelemetryAt = now))
    }

    private fun createTelemetryCauldron(now: Instant): TelemetryCauldron {
        val slot = cauldronSequence.incrementAndGet().toInt()
        val recipe = recipes[(slot + shardContext.shardIndex) % recipes.size]
        val order = PendingOrder(
            orderId = "tel-ord-${shardContext.shardToken()}-${slot.toString().padStart(8, '0')}",
            customerId = "telemetry-customer-${shardContext.shardToken()}-${slot.toString().padStart(6, '0')}",
            potion = recipe,
            createdAt = now
        )
        val batchId = "tel-batch-${shardContext.shardToken()}-${slot.toString().padStart(8, '0')}"

        return TelemetryCauldron(
            activeBatch = ActiveBatch(
                batchId = batchId,
                cauldronId = "cauldron-${shardContext.shardToken()}-${slot.toString().padStart(8, '0')}",
                potion = recipe,
                orders = listOf(order),
                startedAt = now,
                completesAt = Instant.MAX
            ),
            lastTelemetryAt = now
        )
    }

    private data class TelemetryCauldron(
        val activeBatch: ActiveBatch,
        val lastTelemetryAt: Instant
    )
}
