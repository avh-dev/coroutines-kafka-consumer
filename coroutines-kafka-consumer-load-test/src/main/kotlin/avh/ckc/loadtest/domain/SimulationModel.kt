package avh.ckc.loadtest.domain

import java.time.Instant

data class PotionRecipe(
    val potionId: String,
    val recipeId: String
)

data class PendingOrder(
    val orderId: String,
    val customerId: String,
    val potion: PotionRecipe,
    val createdAt: Instant
)

data class SimulatedCauldron(
    val cauldronId: String
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
