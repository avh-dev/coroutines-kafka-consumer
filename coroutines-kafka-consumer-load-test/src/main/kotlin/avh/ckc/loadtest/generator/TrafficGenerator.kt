package avh.ckc.loadtest.generator

import avh.ckc.loadtest.config.LoadTestConfig
import avh.ckc.loadtest.domain.ActiveBatch
import avh.ckc.loadtest.domain.CauldronTelemetryFactory
import avh.ckc.loadtest.domain.PendingOrder
import avh.ckc.loadtest.domain.PotionRecipe
import avh.ckc.loadtest.domain.SimulatedCauldron
import avh.ckc.loadtest.domain.OrderLifecycleStateMachine
import avh.ckc.loadtest.kafka.LoadTestProducers
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
    private val producers: LoadTestProducers
) {
    private val recipes = listOf(
        PotionRecipe("healing-elixir", "healing-elixir-v2"),
        PotionRecipe("mana-tonic", "mana-tonic-v1"),
        PotionRecipe("night-vision-draught", "night-vision-v3")
    )
    private val stateMachine = OrderLifecycleStateMachine(shardContext)
    private val telemetryFactory = CauldronTelemetryFactory(config.diagnosticsBlobSize)
    private val cauldrons = ArrayDeque((1..config.cauldronCount).map { SimulatedCauldron("cauldron-$it") })
    private val pendingOrders = linkedMapOf<String, ArrayDeque<PendingOrder>>()
    private val activeBatches = linkedMapOf<String, ActiveBatch>()
    private val orderSequence = AtomicLong(0)
    private val batchSequence = AtomicLong(0)
    private var orderAccumulator = 0.0
    private var telemetryAccumulator = 0.0

    suspend fun run() {
        val startedAt = shardContext.testRunStartedAt ?: Instant.now()
        val context = ScenarioEvaluationContext(config.baseRate)
        val tickSeconds = config.tickInterval.toMillis().toDouble() / 1000.0

        while (true) {
            val now = Instant.now()
            val activePhase = scenario.phaseAt(now, startedAt, context)
            completeFinishedBatches(now)

            if (activePhase == null) {
                if (activeBatches.isEmpty() && pendingOrders.values.all { it.isEmpty() }) {
                    producers.flush()
                    return
                }
                emitTelemetry(now, 0.0, tickSeconds)
                dispatchReadyBatches(now)
                delay(config.tickInterval.toMillis())
                continue
            }

            orderAccumulator += activePhase.currentRate() * tickSeconds
            telemetryAccumulator += (activePhase.currentRate() * config.telemetryRateMultiplier) * tickSeconds

            repeat(floor(orderAccumulator).toInt()) {
                createOrder(now)
                orderAccumulator -= 1.0
            }

            dispatchReadyBatches(now)
            emitTelemetry(now, telemetryAccumulator, tickSeconds)
            delay(config.tickInterval.toMillis())
        }
    }

    private fun createOrder(now: Instant) {
        val absoluteIndex = orderSequence.incrementAndGet().toInt()
        val recipe = recipes[(absoluteIndex + shardContext.shardIndex) % recipes.size]
        val orderId = "ord-${shardContext.shardToken()}-${absoluteIndex.toString().padStart(8, '0')}"
        val order = PendingOrder(
            orderId = orderId,
            customerId = "customer-${shardContext.shardToken()}-${absoluteIndex.toString().padStart(6, '0')}",
            potion = recipe,
            createdAt = now
        )

        pendingOrders.computeIfAbsent(recipe.recipeId) { ArrayDeque() }.addLast(order)
        val event = stateMachine.createOrderCreated(order)
        producers.sendLifecycle(order.orderId, event)
    }

    private fun dispatchReadyBatches(now: Instant) {
        if (cauldrons.isEmpty()) {
            return
        }

        val availableRecipes = pendingOrders.values.filter { it.isNotEmpty() }
        availableRecipes.forEach { queue ->
            if (cauldrons.isEmpty()) {
                return
            }

            val shouldStart = queue.size >= config.ordersPerBatch ||
                Duration.between(queue.first().createdAt, now) >= config.maxBatchWait
            if (!shouldStart) {
                return@forEach
            }

            val cauldron = cauldrons.removeFirst()
            val orders = buildList {
                repeat(minOf(config.ordersPerBatch, queue.size)) {
                    add(queue.removeFirst())
                }
            }
            if (queue.isEmpty()) {
                pendingOrders.remove(orders.first().potion.recipeId)
            }

            val batchSlot = batchSequence.incrementAndGet().toInt()
            val generated = stateMachine.createAssignedBatch(
                batchSlot = batchSlot,
                cauldronId = cauldron.cauldronId,
                orders = orders,
                brewDuration = config.brewDuration,
                startedAt = now
            )
            generated.lifecycleEvents.forEach { producers.sendLifecycle(it.orderId, it) }
            activeBatches[generated.batchId] = ActiveBatch(
                batchId = generated.batchId,
                cauldronId = generated.cauldronId,
                potion = orders.first().potion,
                orders = orders,
                startedAt = now,
                completesAt = now.plus(config.brewDuration)
            )
        }
    }

    private fun completeFinishedBatches(now: Instant) {
        val completedIds = activeBatches.values
            .filter { !it.completesAt.isAfter(now) }
            .map { it.batchId }

        completedIds.forEach { batchId ->
            val batch = activeBatches.remove(batchId) ?: return@forEach
            stateMachine.createCompletedEvents(batch, now)
                .forEach { producers.sendLifecycle(it.orderId, it) }
            cauldrons.addLast(SimulatedCauldron(batch.cauldronId))
        }
    }

    private fun emitTelemetry(now: Instant, requestedSamples: Double, tickSeconds: Double) {
        if (activeBatches.isEmpty()) {
            telemetryAccumulator = requestedSamples
            return
        }

        repeat(floor(requestedSamples).toInt()) { iteration ->
            val activeBatch = activeBatches.values.elementAt(iteration % activeBatches.size)
            val telemetry = telemetryFactory.create(activeBatch, now)
            producers.sendTelemetry(activeBatch.cauldronId, telemetry)
            telemetryAccumulator -= 1.0
        }
    }
}
