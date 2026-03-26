package avh.ckc.demo.repository

import kotlinx.serialization.Serializable

@Serializable
data class OrderState(
    val orderId: String,
    val batchId: String?,
    val potionId: String,
    val recipeId: String?,
    val customerId: String,
    val cauldronId: String?,
    val status: String,
    val updatedAt: String
)

@Serializable
data class BatchState(
    val batchId: String,
    val recipeId: String?,
    val potionId: String?,
    val cauldronId: String?,
    val status: String,
    val orderIds: List<String>,
    val updatedAt: String
)

@Serializable
data class ModelContextState(
    val batchId: String,
    val previousTemperatureC: Double? = null,
    val previousDensitySg: Double? = null,
    val previousBubbleRateHz: Double? = null,
    val previousMagicalEtaUnits: Double? = null,
    val previousModelRequestId: String? = null,
    val updatedAt: String
)
