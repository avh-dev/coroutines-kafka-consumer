package avh.ckc.loadtest.generator

import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.loadtest.config.LoadTestConfig
import avh.ckc.loadtest.config.TelemetrySourceMode
import avh.ckc.loadtest.domain.ActiveBatch
import avh.ckc.loadtest.domain.CauldronTelemetryFactory
import avh.ckc.loadtest.domain.LoadTestEventFactory
import avh.ckc.loadtest.domain.SimulatedBatch
import avh.ckc.loadtest.domain.SimulationState
import avh.ckc.loadtest.kafka.LoadTestPublisher
import java.time.Instant

interface EventGenerator {
    val name: String
    val topic: TrafficTopic
    val weight: Double
    fun emit(now: Instant): EmitResult
}

enum class TrafficTopic {
    ORDER,
    BATCH,
    CAULDRON
}

data class EmitResult(
    val emitted: Boolean,
    val emittedCount: Int = if (emitted) 1 else 0,
    val delegated: Int = 0,
    val blocked: Boolean = false
) {
    init {
        require(emittedCount >= 0) { "emittedCount must be non-negative" }
        require(!emitted || emittedCount > 0) { "emitted results must have a positive emittedCount" }
        require(emitted || emittedCount == 0) { "non-emitted results must have a zero emittedCount" }
    }
}

fun eventGenerators(
    config: LoadTestConfig,
    state: SimulationState,
    factory: LoadTestEventFactory,
    publisher: LoadTestPublisher
): List<EventGenerator> {
    val context = DelegatingGenerationContext(
        config = config,
        state = state,
        factory = factory,
        publisher = publisher
    )
    return context.generators()
}

private class DelegatingGenerationContext(
    private val config: LoadTestConfig,
    private val state: SimulationState,
    private val factory: LoadTestEventFactory,
    private val publisher: LoadTestPublisher
) {
    private val averageBrewingSteps = (config.minBrewingSteps + config.maxBrewingSteps) / 2.0
    private val telemetryFactory = CauldronTelemetryFactory(config.diagnosticsBlobSize)
    private val maxDelegationDepth = config.maxBrewingSteps + config.maxOrdersPerBatch + 16
    private var brewingStepEmitAttempts = 0L

    fun generators(): List<EventGenerator> = listOf(
        simple("order_created", TrafficTopic.ORDER) { now -> emitOrderCreated(now) },
        simple("order_batch_assigned", TrafficTopic.ORDER) { now -> emitOrderBatchAssigned(now, depth = 0) },
        simple("order_waiting_for_bottling", TrafficTopic.ORDER) { now -> emitOrderWaitingForBottling(now, depth = 0) },
        simple("order_completed", TrafficTopic.ORDER) { now -> emitOrderCompleted(now, depth = 0) },
        simple("batch_created", TrafficTopic.BATCH) { now -> emitBatchCreated(now, depth = 0) },
        simple("batch_reagents_preparation_started", TrafficTopic.BATCH) { now ->
            emitBatchTransition(now, depth = 0, prerequisite = ::emitBatchCreated, take = state::takeCreatedBatch, move = state::addPreparingBatch) {
                batch, eventTime -> factory.batchReagentsPreparationStarted(batch, eventTime)
            }
        },
        simple("batch_reagents_prepared", TrafficTopic.BATCH) { now ->
            emitBatchTransition(now, depth = 0, prerequisite = ::emitBatchReagentsPreparationStarted, take = state::takePreparingBatch, move = state::addPreparedBatch) {
                batch, eventTime -> factory.batchReagentsPrepared(batch, eventTime)
            }
        },
        simple("batch_cauldron_requested", TrafficTopic.BATCH) { now -> emitBatchCauldronRequested(now, depth = 0) },
        simple("batch_cauldron_assigned", TrafficTopic.BATCH) { now -> emitBatchCauldronAssigned(now, depth = 0) },
        simple("batch_brewing_started", TrafficTopic.BATCH) { now -> emitBatchBrewingStarted(now, depth = 0) },
        simple("batch_brewing_step_completed", TrafficTopic.BATCH, averageBrewingSteps) { now ->
            emitBatchBrewingStepCompleted(now, depth = 0)
        },
        simple("batch_brewing_completed", TrafficTopic.BATCH) { now -> emitBatchBrewingCompleted(now, depth = 0) },
        simple("batch_bottling_started", TrafficTopic.BATCH) { now -> emitBatchBottlingStarted(now, depth = 0) },
        simple("batch_bottling_completed", TrafficTopic.BATCH) { now -> emitBatchBottlingCompleted(now, depth = 0) },
        simple("cauldron_telemetry", TrafficTopic.CAULDRON) { now -> emitCauldronTelemetry(now, depth = 0) }
    )

    private fun simple(
        name: String,
        topic: TrafficTopic,
        weight: Double = 1.0,
        emit: (Instant) -> EmitResult
    ): EventGenerator = object : EventGenerator {
        override val name = name
        override val topic = topic
        override val weight = weight
        override fun emit(now: Instant): EmitResult = emit(now)
    }

    private fun emitOrderCreated(now: Instant): EmitResult {
        val order = state.createOrder(now)
        publisher.sendOrder(order.orderId, factory.orderCreated(order, now))
        return EmitResult(emitted = true)
    }

    private fun emitOrderBatchAssigned(now: Instant, depth: Int): EmitResult {
        checkDepth(depth) ?: return blocked()
        var delegated = 0
        var order = state.takeOrderForBatchAssigned()
        if (order == null) {
            val prerequisite = emitBatchCreated(now, depth + 1)
            delegated += prerequisite.totalEmitted()
            if (!prerequisite.emitted) return blocked(delegated)
            order = state.takeOrderForBatchAssigned()
        }
        order ?: return blocked(delegated)
        publisher.sendOrder(order.orderId, factory.orderBatchAssigned(order, now))
        return EmitResult(emitted = true, delegated = delegated)
    }

    private fun emitOrderWaitingForBottling(now: Instant, depth: Int): EmitResult {
        checkDepth(depth) ?: return blocked()
        var delegated = 0
        var order = state.takeOrderForWaitingForBottling()
        if (order == null) {
            val prerequisite = emitBatchBrewingCompleted(now, depth + 1)
            delegated += prerequisite.totalEmitted()
            if (!prerequisite.emitted) return blocked(delegated)
            order = state.takeOrderForWaitingForBottling()
        }
        order ?: return blocked(delegated)
        state.addOrderReadyForCompletion(order)
        publisher.sendOrder(order.orderId, factory.orderWaitingForBottling(order, now))
        return EmitResult(emitted = true, delegated = delegated)
    }

    private fun emitOrderCompleted(now: Instant, depth: Int): EmitResult {
        checkDepth(depth) ?: return blocked()
        var delegated = 0
        var order = state.takeOrderForCompleted()
        if (order == null) {
            val prerequisite = emitOrderWaitingForBottling(now, depth + 1)
            delegated += prerequisite.totalEmitted()
            if (!prerequisite.emitted) return blocked(delegated)
            order = state.takeOrderForCompleted()
        }
        order ?: return blocked(delegated)
        state.markOrderCompleted(order)
        publisher.sendOrder(order.orderId, factory.orderCompleted(order, now))
        return EmitResult(emitted = true, delegated = delegated)
    }

    private fun emitBatchCreated(now: Instant, depth: Int): EmitResult {
        checkDepth(depth) ?: return blocked()
        val orderCount = ordersPerBatch(config, now)
        var delegated = 0
        var batch = state.createBatch(now, orderCount, brewingSteps(config, now))
        while (batch == null && delegated < orderCount) {
            val prerequisite = emitOrderCreated(now)
            delegated += prerequisite.totalEmitted()
            batch = state.createBatch(now, orderCount, brewingSteps(config, now))
        }
        batch ?: return blocked(delegated)
        publisher.sendBatch(batch.batchId, factory.batchCreated(batch, now))
        return EmitResult(emitted = true, delegated = delegated)
    }

    private fun emitBatchReagentsPreparationStarted(now: Instant, depth: Int): EmitResult =
        emitBatchTransition(now, depth, ::emitBatchCreated, state::takeCreatedBatch, state::addPreparingBatch) { batch, eventTime ->
            factory.batchReagentsPreparationStarted(batch, eventTime)
        }

    private fun emitBatchReagentsPrepared(now: Instant, depth: Int): EmitResult =
        emitBatchTransition(now, depth, ::emitBatchReagentsPreparationStarted, state::takePreparingBatch, state::addPreparedBatch) { batch, eventTime ->
            factory.batchReagentsPrepared(batch, eventTime)
        }

    private fun emitBatchCauldronRequested(now: Instant, depth: Int): EmitResult =
        emitBatchTransition(now, depth, ::emitBatchReagentsPrepared, state::takePreparedBatch, state::addWaitingForCauldronBatch) { batch, eventTime ->
            factory.batchCauldronRequested(batch, eventTime)
        }

    private fun emitBatchCauldronAssigned(now: Instant, depth: Int): EmitResult {
        checkDepth(depth) ?: return blocked()
        var delegated = 0
        var waiting = state.takeWaitingForCauldronBatch()
        if (waiting == null) {
            val prerequisite = emitBatchCauldronRequested(now, depth + 1)
            delegated += prerequisite.totalEmitted()
            if (!prerequisite.emitted) return blocked(delegated)
            waiting = state.takeWaitingForCauldronBatch()
        }
        waiting ?: return blocked(delegated)
        var assigned = state.assignCauldron(waiting)
        if (assigned == null) {
            state.addWaitingForCauldronBatch(waiting)
            val prerequisite = emitBatchBrewingCompleted(now, depth + 1)
            delegated += prerequisite.totalEmitted()
            waiting = state.takeWaitingForCauldronBatch()
            assigned = waiting?.let(state::assignCauldron)
        }
        if (waiting != null && assigned == null) {
            state.addWaitingForCauldronBatch(waiting)
        }
        assigned ?: return blocked(delegated)
        publisher.sendBatch(assigned.batchId, factory.batchCauldronAssigned(assigned, now))
        return EmitResult(emitted = true, delegated = delegated)
    }

    private fun emitBatchBrewingStarted(now: Instant, depth: Int): EmitResult =
        emitBatchTransition(now, depth, ::emitBatchCauldronAssigned, state::takeCauldronAssignedBatch, state::addBrewingBatch) { batch, eventTime ->
            factory.batchBrewingStarted(batch, eventTime)
        }

    private fun emitBatchBrewingStepCompleted(now: Instant, depth: Int): EmitResult {
        checkDepth(depth) ?: return blocked()
        var delegated = 0
        var batch = state.takeBrewingBatchForStep()
        if (batch == null) {
            val prerequisite = emitBatchBrewingStarted(now, depth + 1)
            delegated += prerequisite.totalEmitted()
            if (!prerequisite.emitted) return blocked(delegated)
            batch = state.takeBrewingBatchForStep()
        }
        var activeBatch = batch ?: return blocked(delegated)
        val burstSize = brewingStepBurstSize(now)
            .coerceAtMost(activeBatch.brewingStepsTotal - activeBatch.brewingStepsCompleted)
        repeat(burstSize) {
            publisher.sendBatch(activeBatch.batchId, factory.batchBrewingStepCompleted(activeBatch, now))
            activeBatch = state.markBrewingStepCompleted(activeBatch)
        }
        return EmitResult(emitted = true, emittedCount = burstSize, delegated = delegated)
    }

    private fun emitBatchBrewingCompleted(now: Instant, depth: Int): EmitResult {
        checkDepth(depth) ?: return blocked()
        var delegated = 0
        var batch = state.takeBrewingCompletedBatch()
        var attempts = 0
        while (batch == null && attempts < config.maxBrewingSteps) {
            val prerequisite = emitBatchBrewingStepCompleted(now, depth + 1)
            delegated += prerequisite.totalEmitted()
            if (!prerequisite.emitted) return blocked(delegated)
            batch = state.takeBrewingCompletedBatch()
            attempts++
        }
        batch ?: return blocked(delegated)
        state.markOrderWaitingForBottling(batch)
        state.addWaitingForBottlingBatch(batch)
        publisher.sendBatch(batch.batchId, factory.batchBrewingCompleted(batch, now))
        return EmitResult(emitted = true, delegated = delegated)
    }

    private fun emitBatchBottlingStarted(now: Instant, depth: Int): EmitResult =
        emitBatchTransition(now, depth, ::emitBatchBrewingCompleted, state::takeWaitingForBottlingBatch, state::addBottlingBatch) { batch, eventTime ->
            factory.batchBottlingStarted(batch, eventTime)
        }

    private fun emitBatchBottlingCompleted(now: Instant, depth: Int): EmitResult {
        checkDepth(depth) ?: return blocked()
        var delegated = 0
        var batch = state.takeBottlingBatchForCompleted()
        if (batch == null) {
            val prerequisite = emitBatchBottlingStarted(now, depth + 1)
            delegated += prerequisite.totalEmitted()
            if (!prerequisite.emitted) return blocked(delegated)
            repeat(config.maxOrdersPerBatch) {
                if (batch == null) {
                    val orderCompleted = emitOrderCompleted(now, depth + 1)
                    delegated += orderCompleted.totalEmitted()
                    batch = state.takeBottlingBatchForCompleted()
                }
            }
        }
        batch ?: return blocked(delegated)
        publisher.sendBatch(batch.batchId, factory.batchBottlingCompleted(batch, now))
        return EmitResult(emitted = true, delegated = delegated)
    }

    private fun emitBatchTransition(
        now: Instant,
        depth: Int,
        prerequisite: (Instant, Int) -> EmitResult,
        take: () -> SimulatedBatch?,
        move: (SimulatedBatch) -> Unit,
        event: (SimulatedBatch, Instant) -> BatchLifecycleEvent
    ): EmitResult {
        checkDepth(depth) ?: return blocked()
        var delegated = 0
        var batch = take()
        if (batch == null) {
            val prerequisiteResult = prerequisite(now, depth + 1)
            delegated += prerequisiteResult.totalEmitted()
            if (!prerequisiteResult.emitted) return blocked(delegated)
            batch = take()
        }
        batch ?: return blocked(delegated)
        move(batch)
        publisher.sendBatch(batch.batchId, event(batch, now))
        return EmitResult(emitted = true, delegated = delegated)
    }

    private fun emitCauldronTelemetry(now: Instant, depth: Int): EmitResult {
        val value = when (config.telemetrySourceMode) {
            TelemetrySourceMode.ACTIVE_BATCHES -> {
                var batch = state.takeActiveBatchForTelemetry()
                var delegated = 0
                if (batch == null) {
                    val prerequisite = emitBatchBrewingStarted(now, depth + 1)
                    delegated += prerequisite.totalEmitted()
                    if (!prerequisite.emitted) return blocked(delegated)
                    batch = state.takeActiveBatchForTelemetry()
                }
                batch ?: return blocked(delegated)
                publishTelemetry(batch, now)
                return EmitResult(emitted = true, delegated = delegated)
            }
            TelemetrySourceMode.FIXED_FLEET -> state.takeFixedFleetBatchForTelemetry(now)
        }
        publishTelemetry(value, now)
        return EmitResult(emitted = true)
    }

    private fun publishTelemetry(batch: SimulatedBatch, now: Instant) {
        val active = ActiveBatch(
            batchId = batch.batchId,
            cauldronId = batch.cauldronId.orEmpty(),
            potion = batch.potion,
            orders = batch.orders,
            startedAt = batch.createdAt,
            completesAt = now,
            telemetrySequence = batch.telemetrySequence
        )
        publisher.sendTelemetry(active.cauldronId, telemetryFactory.create(active, now))
    }

    private fun checkDepth(depth: Int): Unit? = Unit.takeIf { depth <= maxDelegationDepth }

    private fun blocked(delegated: Int = 0): EmitResult = EmitResult(emitted = false, delegated = delegated, blocked = true)

    private fun brewingStepBurstSize(now: Instant): Int {
        brewingStepEmitAttempts++
        if (config.brewingStepBurstEvery == 0 || brewingStepEmitAttempts % config.brewingStepBurstEvery != 0L) {
            return 1
        }
        val range = config.maxBrewingStepBurst - config.minBrewingStepBurst + 1
        return config.minBrewingStepBurst + ((now.toEpochMilli() / 31).floorMod(range))
    }

    private fun EmitResult.totalEmitted(): Int = delegated + emittedCount
}

private fun ordersPerBatch(config: LoadTestConfig, now: Instant): Int {
    val range = config.maxOrdersPerBatch - config.minOrdersPerBatch + 1
    return config.minOrdersPerBatch + (now.toEpochMilli().floorMod(range))
}

private fun brewingSteps(config: LoadTestConfig, now: Instant): Int {
    val range = config.maxBrewingSteps - config.minBrewingSteps + 1
    return config.minBrewingSteps + ((now.toEpochMilli() / 17).floorMod(range))
}

private fun Long.floorMod(mod: Int): Int = Math.floorMod(this, mod.toLong()).toInt()
