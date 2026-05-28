package avh.ckc.loadtest.domain

import avh.ckc.loadtest.runtime.GeneratorIdentity
import java.time.Instant
import java.util.ArrayDeque
import java.util.BitSet

data class PotionRecipe(
    val potionId: String,
    val recipeId: String
)

data class PendingOrder(
    val orderId: String,
    val customerId: String,
    val potion: PotionRecipe,
    val createdAt: Instant,
    val batchId: String? = null
)

data class SimulatedBatch(
    val batchId: String,
    val cauldronId: String?,
    val potion: PotionRecipe,
    val orders: List<PendingOrder>,
    val createdAt: Instant,
    val brewingStepsTotal: Int,
    val brewingStepsCompleted: Int = 0,
    var telemetrySequence: Long = 0
)

data class ActiveBatch(
    val batchId: String,
    val cauldronId: String,
    val potion: PotionRecipe,
    val orders: List<PendingOrder>,
    val startedAt: Instant,
    val completesAt: Instant,
    var telemetrySequence: Long = 0
)

class SimulationState(
    private val cauldronCount: Int,
    private val fakePrefix: String,
    private val identity: GeneratorIdentity
) {
    private val recipes = listOf(
        PotionRecipe("healing-elixir", "healing-elixir-v2"),
        PotionRecipe("mana-tonic", "mana-tonic-v1"),
        PotionRecipe("night-vision-draught", "night-vision-v3")
    )
    private val createdOrders = ArrayDeque<PendingOrder>()
    private val assignedOrders = ArrayDeque<PendingOrder>()
    private val waitingForBottlingOrders = ArrayDeque<PendingOrder>()
    private val readyForCompletionOrders = ArrayDeque<PendingOrder>()
    private val completedOrdersByBatch = mutableMapOf<String, Int>()

    private val createdBatches = ArrayDeque<SimulatedBatch>()
    private val preparingBatches = ArrayDeque<SimulatedBatch>()
    private val preparedBatches = ArrayDeque<SimulatedBatch>()
    private val waitingForCauldronBatches = ArrayDeque<SimulatedBatch>()
    private val cauldronAssignedBatches = ArrayDeque<SimulatedBatch>()
    private val brewingBatches = ArrayDeque<SimulatedBatch>()
    private val brewingCompletedBatches = ArrayDeque<SimulatedBatch>()
    private val waitingForBottlingBatches = ArrayDeque<SimulatedBatch>()
    private val bottlingBatches = ArrayDeque<SimulatedBatch>()

    private val activeCauldrons = BitSet(cauldronCount)
    private val cauldronBatches = mutableMapOf<Int, SimulatedBatch>()
    private val fixedFleetBatches = mutableListOf<SimulatedBatch>()
    private var cauldronCursor = 0
    private var fixedFleetCursor = 0
    private var realOrderSequence = 0L
    private var realBatchSequence = 0L
    private var fakeSequence = 0L

    @Synchronized
    fun createOrder(now: Instant): PendingOrder {
        val order = newOrder(real = true, now = now)
        createdOrders.addLast(order)
        return order
    }

    @Synchronized
    fun createBatch(now: Instant, orderCount: Int, brewingSteps: Int): SimulatedBatch? {
        if (createdOrders.size < orderCount) {
            return null
        }
        val orders = (1..orderCount).map { createdOrders.removeFirst() }
        val batchId = identity.entityId("batch", ++realBatchSequence, width = 8)
        val batch = SimulatedBatch(
            batchId = batchId,
            cauldronId = null,
            potion = orders.first().potion,
            orders = orders.map { it.copy(batchId = batchId) },
            createdAt = now,
            brewingStepsTotal = brewingSteps
        )
        createdBatches.addLast(batch)
        batch.orders.forEach(assignedOrders::addLast)
        return batch
    }

    @Synchronized fun takeOrderForBatchAssigned(): PendingOrder? = assignedOrders.poll()

    @Synchronized fun takeOrderForWaitingForBottling(): PendingOrder? = waitingForBottlingOrders.poll()

    @Synchronized fun addOrderReadyForCompletion(order: PendingOrder) = readyForCompletionOrders.addLast(order)

    @Synchronized fun takeOrderForCompleted(): PendingOrder? = readyForCompletionOrders.poll()

    @Synchronized
    fun markOrderWaitingForBottling(batch: SimulatedBatch) {
        batch.orders.forEach(waitingForBottlingOrders::addLast)
    }

    @Synchronized
    fun markOrderCompleted(order: PendingOrder) {
        val batchId = order.batchId ?: return
        completedOrdersByBatch[batchId] = completedOrdersByBatch.getOrDefault(batchId, 0) + 1
    }

    @Synchronized fun takeCreatedBatch(): SimulatedBatch? = createdBatches.poll()

    @Synchronized fun addPreparingBatch(batch: SimulatedBatch) = preparingBatches.addLast(batch)

    @Synchronized fun takePreparingBatch(): SimulatedBatch? = preparingBatches.poll()

    @Synchronized fun addPreparedBatch(batch: SimulatedBatch) = preparedBatches.addLast(batch)

    @Synchronized fun takePreparedBatch(): SimulatedBatch? = preparedBatches.poll()

    @Synchronized fun addWaitingForCauldronBatch(batch: SimulatedBatch) = waitingForCauldronBatches.addLast(batch)

    @Synchronized fun takeWaitingForCauldronBatch(): SimulatedBatch? = waitingForCauldronBatches.poll()

    @Synchronized
    fun assignCauldron(batch: SimulatedBatch): SimulatedBatch? {
        val cauldronIndex = nextFreeCauldronIndex() ?: return null
        activeCauldrons.set(cauldronIndex)
        val assigned = batch.copy(cauldronId = cauldronId(cauldronIndex))
        cauldronBatches[cauldronIndex] = assigned
        cauldronAssignedBatches.addLast(assigned)
        return assigned
    }

    @Synchronized fun takeCauldronAssignedBatch(): SimulatedBatch? = cauldronAssignedBatches.poll()

    @Synchronized fun addBrewingBatch(batch: SimulatedBatch) = brewingBatches.addLast(batch)

    @Synchronized
    fun takeBrewingBatchForStep(): SimulatedBatch? {
        repeat(brewingBatches.size) {
            val batch = brewingBatches.removeFirst()
            if (batch.brewingStepsCompleted < batch.brewingStepsTotal) {
                return batch
            }
            brewingCompletedBatches.addLast(batch)
        }
        return null
    }

    @Synchronized
    fun markBrewingStepCompleted(batch: SimulatedBatch): SimulatedBatch {
        val updated = batch.copy(brewingStepsCompleted = batch.brewingStepsCompleted + 1)
        if (updated.brewingStepsCompleted >= updated.brewingStepsTotal) {
            brewingCompletedBatches.addLast(updated)
        } else {
            brewingBatches.addLast(updated)
        }
        updateActiveBatch(updated)
        return updated
    }

    @Synchronized
    fun takeBrewingCompletedBatch(): SimulatedBatch? {
        val batch = brewingCompletedBatches.poll() ?: return null
        releaseCauldron(batch)
        return batch
    }

    @Synchronized fun addWaitingForBottlingBatch(batch: SimulatedBatch) = waitingForBottlingBatches.addLast(batch)

    @Synchronized fun takeWaitingForBottlingBatch(): SimulatedBatch? = waitingForBottlingBatches.poll()

    @Synchronized fun addBottlingBatch(batch: SimulatedBatch) = bottlingBatches.addLast(batch)

    @Synchronized
    fun takeBottlingBatchForCompleted(): SimulatedBatch? {
        repeat(bottlingBatches.size) {
            val batch = bottlingBatches.removeFirst()
            if (completedOrdersByBatch.getOrDefault(batch.batchId, 0) >= batch.orders.size) {
                completedOrdersByBatch.remove(batch.batchId)
                return batch
            }
            bottlingBatches.addLast(batch)
        }
        return null
    }

    @Synchronized
    fun takeActiveBatchForTelemetry(): SimulatedBatch? {
        if (activeCauldrons.isEmpty) {
            return null
        }
        var index = activeCauldrons.nextSetBit(cauldronCursor)
        if (index < 0) {
            index = activeCauldrons.nextSetBit(0)
        }
        if (index < 0) {
            return null
        }
        cauldronCursor = (index + 1) % cauldronCount
        return cauldronBatches[index]
    }

    @Synchronized
    fun fixedFleetBatches(now: Instant): List<SimulatedBatch> {
        if (fixedFleetBatches.isEmpty()) {
            repeat(cauldronCount) { index ->
                val sequence = (index + 1).toLong()
                val batchId = identity.entityId("fleet-batch", sequence, width = 8)
                val order = PendingOrder(
                    orderId = identity.entityId("fleet-order", sequence, width = 8),
                    customerId = identity.entityId("fleet-customer", sequence, width = 8),
                    potion = recipe(sequence),
                    createdAt = now,
                    batchId = batchId
                )
                fixedFleetBatches += SimulatedBatch(
                    batchId = batchId,
                    cauldronId = cauldronId(index),
                    potion = order.potion,
                    orders = listOf(order),
                    createdAt = now,
                    brewingStepsTotal = 1
                )
            }
        }
        return fixedFleetBatches.toList()
    }

    @Synchronized
    fun takeFixedFleetBatchForTelemetry(now: Instant): SimulatedBatch {
        val fleet = fixedFleetBatches(now)
        val batch = fleet[fixedFleetCursor]
        fixedFleetCursor = (fixedFleetCursor + 1) % fleet.size
        return batch
    }

    @Synchronized
    fun fakeOrder(now: Instant, batchId: String? = null): PendingOrder {
        val order = newOrder(real = false, now = now.minusSeconds(300))
        return order.copy(batchId = batchId ?: fakeBatchId())
    }

    @Synchronized
    fun fakeBatch(now: Instant, cauldron: Boolean = false): SimulatedBatch {
        val recipe = recipe(fakeSequence)
        val batchId = fakeBatchId()
        val cauldronId = if (cauldron) fakeCauldronId() else null
        return SimulatedBatch(
            batchId = batchId,
            cauldronId = cauldronId,
            potion = recipe,
            orders = listOf(fakeOrder(now.minusSeconds(240), batchId)),
            createdAt = now.minusSeconds(240),
            brewingStepsTotal = 1,
            brewingStepsCompleted = 1
        )
    }

    @Synchronized
    fun snapshot(): SimulationSnapshot =
        SimulationSnapshot(
            createdOrders = createdOrders.size,
            assignedOrders = assignedOrders.size,
            waitingForBottlingOrders = waitingForBottlingOrders.size,
            readyForCompletionOrders = readyForCompletionOrders.size,
            createdBatches = createdBatches.size,
            preparingBatches = preparingBatches.size,
            preparedBatches = preparedBatches.size,
            waitingForCauldronBatches = waitingForCauldronBatches.size,
            cauldronAssignedBatches = cauldronAssignedBatches.size,
            brewingBatches = brewingBatches.size,
            brewingCompletedBatches = brewingCompletedBatches.size,
            waitingForBottlingBatches = waitingForBottlingBatches.size,
            bottlingBatches = bottlingBatches.size,
            activeCauldrons = activeCauldrons.cardinality()
        )

    private fun newOrder(real: Boolean, now: Instant): PendingOrder {
        val sequence = if (real) ++realOrderSequence else ++fakeSequence
        val prefix = if (real) "order" else "$fakePrefix-order"
        val recipe = recipe(sequence)
        return PendingOrder(
            orderId = identity.entityId(prefix, sequence, width = 10),
            customerId = identity.entityId("customer", sequence, width = 8),
            potion = recipe,
            createdAt = now
        )
    }

    private fun nextFreeCauldronIndex(): Int? {
        val index = activeCauldrons.nextClearBit(0)
        return index.takeIf { it < cauldronCount }
    }

    private fun updateActiveBatch(batch: SimulatedBatch) {
        val index = cauldronIndex(batch.cauldronId) ?: return
        if (index in 0 until cauldronCount && activeCauldrons[index]) {
            cauldronBatches[index] = batch
        }
    }

    private fun releaseCauldron(batch: SimulatedBatch) {
        val index = cauldronIndex(batch.cauldronId) ?: return
        if (index in 0 until cauldronCount) {
            activeCauldrons.clear(index)
            cauldronBatches.remove(index)
        }
    }

    private fun cauldronId(index: Int): String = identity.entityId("cauldron", (index + 1).toLong(), width = 4)

    private fun fakeBatchId(): String = identity.entityId("$fakePrefix-batch", ++fakeSequence, width = 10)

    private fun fakeCauldronId(): String = identity.entityId("$fakePrefix-cauldron", ++fakeSequence, width = 10)

    private fun cauldronIndex(cauldronId: String?): Int? =
        cauldronId?.substringAfterLast('-')?.toIntOrNull()?.minus(1)

    private fun recipe(sequence: Long): PotionRecipe = recipes[(sequence % recipes.size).toInt()]
}

data class SimulationSnapshot(
    val createdOrders: Int,
    val assignedOrders: Int,
    val waitingForBottlingOrders: Int,
    val readyForCompletionOrders: Int,
    val createdBatches: Int,
    val preparingBatches: Int,
    val preparedBatches: Int,
    val waitingForCauldronBatches: Int,
    val cauldronAssignedBatches: Int,
    val brewingBatches: Int,
    val brewingCompletedBatches: Int,
    val waitingForBottlingBatches: Int,
    val bottlingBatches: Int,
    val activeCauldrons: Int
)
