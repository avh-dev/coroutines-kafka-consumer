 package avh.ckc.loadtest.generator

import avh.ckc.loadtest.config.LoadTestConfig
import avh.ckc.loadtest.config.TelemetrySourceMode
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
    val real: Boolean
)

fun eventGenerators(
    config: LoadTestConfig,
    state: SimulationState,
    factory: LoadTestEventFactory,
    publisher: LoadTestPublisher
): List<EventGenerator> {
    val averageBrewingSteps = (config.minBrewingSteps + config.maxBrewingSteps) / 2.0
    return listOf(
        object : EventGenerator {
            override val name = "order_created"
            override val topic = TrafficTopic.ORDER
            override val weight = 1.0
            override fun emit(now: Instant): EmitResult {
                val order = state.createOrder(now)
                publisher.sendOrder(order.orderId, factory.orderCreated(order, now))
                return EmitResult(real = true)
            }
        },
        object : EventGenerator {
            override val name = "order_batch_assigned"
            override val topic = TrafficTopic.ORDER
            override val weight = 1.0
            override fun emit(now: Instant): EmitResult {
                val order = state.takeOrderForBatchAssigned()
                val real = order != null
                val value = order ?: state.fakeOrder(now)
                publisher.sendOrder(value.orderId, factory.orderBatchAssigned(value, now))
                return EmitResult(real)
            }
        },
        object : EventGenerator {
            override val name = "order_waiting_for_bottling"
            override val topic = TrafficTopic.ORDER
            override val weight = 1.0
            override fun emit(now: Instant): EmitResult {
                val order = state.takeOrderForWaitingForBottling()
                val real = order != null
                val value = order ?: state.fakeOrder(now)
                if (real) {
                    state.addOrderReadyForCompletion(value)
                }
                publisher.sendOrder(value.orderId, factory.orderWaitingForBottling(value, now))
                return EmitResult(real)
            }
        },
        object : EventGenerator {
            override val name = "order_completed"
            override val topic = TrafficTopic.ORDER
            override val weight = 1.0
            override fun emit(now: Instant): EmitResult {
                val order = state.takeOrderForCompleted()
                val real = order != null
                val value = order ?: state.fakeOrder(now)
                if (real) {
                    state.markOrderCompleted(value)
                }
                publisher.sendOrder(value.orderId, factory.orderCompleted(value, now))
                return EmitResult(real)
            }
        },
        object : EventGenerator {
            override val name = "batch_created"
            override val topic = TrafficTopic.BATCH
            override val weight = 1.0
            override fun emit(now: Instant): EmitResult {
                val batch = state.createBatch(
                    now = now,
                    orderCount = ordersPerBatch(config, now),
                    brewingSteps = brewingSteps(config, now)
                )
                val real = batch != null
                val value = batch ?: state.fakeBatch(now)
                publisher.sendBatch(value.batchId, factory.batchCreated(value, now))
                return EmitResult(real)
            }
        },
        object : BatchTransitionGenerator("batch_reagents_preparation_started", state, factory, publisher) {
            override fun take(): SimulatedBatch? = state.takeCreatedBatch()
            override fun move(batch: SimulatedBatch) = state.addPreparingBatch(batch)
            override fun event(batch: SimulatedBatch, now: Instant) = factory.batchReagentsPreparationStarted(batch, now)
        },
        object : BatchTransitionGenerator("batch_reagents_prepared", state, factory, publisher) {
            override fun take(): SimulatedBatch? = state.takePreparingBatch()
            override fun move(batch: SimulatedBatch) = state.addPreparedBatch(batch)
            override fun event(batch: SimulatedBatch, now: Instant) = factory.batchReagentsPrepared(batch, now)
        },
        object : BatchTransitionGenerator("batch_cauldron_requested", state, factory, publisher) {
            override fun take(): SimulatedBatch? = state.takePreparedBatch()
            override fun move(batch: SimulatedBatch) = state.addWaitingForCauldronBatch(batch)
            override fun event(batch: SimulatedBatch, now: Instant) = factory.batchCauldronRequested(batch, now)
        },
        object : EventGenerator {
            override val name = "batch_cauldron_assigned"
            override val topic = TrafficTopic.BATCH
            override val weight = 1.0
            override fun emit(now: Instant): EmitResult {
                val waiting = state.takeWaitingForCauldronBatch()
                val assigned = waiting?.let(state::assignCauldron)
                if (waiting != null && assigned == null) {
                    state.addWaitingForCauldronBatch(waiting)
                }
                val real = assigned != null
                val value = assigned ?: state.fakeBatch(now, cauldron = true)
                publisher.sendBatch(value.batchId, factory.batchCauldronAssigned(value, now))
                return EmitResult(real)
            }
        },
        object : BatchTransitionGenerator("batch_brewing_started", state, factory, publisher) {
            override fun take(): SimulatedBatch? = state.takeCauldronAssignedBatch()
            override fun move(batch: SimulatedBatch) = state.addBrewingBatch(batch)
            override fun event(batch: SimulatedBatch, now: Instant) = factory.batchBrewingStarted(batch, now)
        },
        object : EventGenerator {
            override val name = "batch_brewing_step_completed"
            override val topic = TrafficTopic.BATCH
            override val weight = averageBrewingSteps
            override fun emit(now: Instant): EmitResult {
                val batch = state.takeBrewingBatchForStep()
                val real = batch != null
                val value = batch ?: state.fakeBatch(now, cauldron = true)
                publisher.sendBatch(value.batchId, factory.batchBrewingStepCompleted(value, now))
                if (real) {
                    state.markBrewingStepCompleted(value)
                }
                return EmitResult(real)
            }
        },
        object : EventGenerator {
            override val name = "batch_brewing_completed"
            override val topic = TrafficTopic.BATCH
            override val weight = 1.0
            override fun emit(now: Instant): EmitResult {
                val batch = state.takeBrewingCompletedBatch()
                val real = batch != null
                val value = batch ?: state.fakeBatch(now, cauldron = true)
                if (real) {
                    state.markOrderWaitingForBottling(value)
                    state.addWaitingForBottlingBatch(value)
                }
                publisher.sendBatch(value.batchId, factory.batchBrewingCompleted(value, now))
                return EmitResult(real)
            }
        },
        object : BatchTransitionGenerator("batch_bottling_started", state, factory, publisher) {
            override fun take(): SimulatedBatch? = state.takeWaitingForBottlingBatch()
            override fun move(batch: SimulatedBatch) = state.addBottlingBatch(batch)
            override fun event(batch: SimulatedBatch, now: Instant) = factory.batchBottlingStarted(batch, now)
        },
        object : EventGenerator {
            override val name = "batch_bottling_completed"
            override val topic = TrafficTopic.BATCH
            override val weight = 1.0
            override fun emit(now: Instant): EmitResult {
                val batch = state.takeBottlingBatchForCompleted()
                val real = batch != null
                val value = batch ?: state.fakeBatch(now)
                publisher.sendBatch(value.batchId, factory.batchBottlingCompleted(value, now))
                return EmitResult(real)
            }
        },
        object : EventGenerator {
            override val name = "cauldron_telemetry"
            override val topic = TrafficTopic.CAULDRON
            override val weight = 1.0
            private val telemetryFactory = avh.ckc.loadtest.domain.CauldronTelemetryFactory(config.diagnosticsBlobSize)
            override fun emit(now: Instant): EmitResult {
                val value = when (config.telemetrySourceMode) {
                    TelemetrySourceMode.ACTIVE_BATCHES -> state.takeActiveBatchForTelemetry()
                        ?: state.fakeBatch(now, cauldron = true)
                    TelemetrySourceMode.FIXED_FLEET -> state.takeFixedFleetBatchForTelemetry(now)
                }
                val real = config.telemetrySourceMode == TelemetrySourceMode.FIXED_FLEET ||
                    !value.batchId.startsWith("${config.fakeEntityPrefix}-")
                val active = avh.ckc.loadtest.domain.ActiveBatch(
                    batchId = value.batchId,
                    cauldronId = value.cauldronId.orEmpty(),
                    potion = value.potion,
                    orders = value.orders,
                    startedAt = value.createdAt,
                    completesAt = now,
                    telemetrySequence = value.telemetrySequence
                )
                publisher.sendTelemetry(active.cauldronId, telemetryFactory.create(active, now))
                return EmitResult(real)
            }
        }
    )
}

abstract class BatchTransitionGenerator(
    override val name: String,
    private val state: SimulationState,
    private val factory: LoadTestEventFactory,
    private val publisher: LoadTestPublisher
) : EventGenerator {
    override val topic = TrafficTopic.BATCH
    override val weight = 1.0

    abstract fun take(): SimulatedBatch?
    abstract fun move(batch: SimulatedBatch)
    abstract fun event(batch: SimulatedBatch, now: Instant): avh.ckc.demo.proto.BatchLifecycleEvent

    override fun emit(now: Instant): EmitResult {
        val batch = take()
        val real = batch != null
        val value = batch ?: state.fakeBatch(now)
        if (real) {
            move(value)
        }
        publisher.sendBatch(value.batchId, event(value, now))
        return EmitResult(real)
    }
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
