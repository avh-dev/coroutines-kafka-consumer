package avh.ckc.demo.model

import kotlinx.serialization.Serializable

@Serializable
data class BrewingStepReceipt(
    val batchId: String,
    val cauldronId: String,
    val stepNumber: Int,
    val stepCode: String,
    val receiptId: String,
    val acceptedAt: String,
    val registryShard: String,
    val regulatoryTraceId: String,
    val updatedAt: String
)
