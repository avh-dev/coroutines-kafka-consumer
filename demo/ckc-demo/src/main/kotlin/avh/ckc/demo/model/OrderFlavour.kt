package avh.ckc.demo.model

import kotlinx.serialization.Serializable

@Serializable
data class OrderFlavour(
    val orderId: String,
    val flavourProfileId: String,
    val palette: String,
    val etaCorrectionFactor: Double,
    val moonPhase: String,
    val modelRequestId: String,
    val updatedAt: String
)
