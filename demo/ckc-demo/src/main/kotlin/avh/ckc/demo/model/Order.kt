package avh.ckc.demo.model

import kotlinx.serialization.Serializable

@Serializable
data class Order(
    val orderId: String,
    val batchId: String?,
    val potionId: String,
    val recipeId: String?,
    val customerId: String,
    val status: String,
    val updatedAt: String
)
