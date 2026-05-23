package avh.ckc.demo.model

import kotlinx.serialization.Serializable

@Serializable
data class Batch(
    val batchId: String,
    val recipeId: String?,
    val potionId: String?,
    val cauldronId: String?,
    val status: String,
    val orderIds: List<String>,
    val updatedAt: String
)
