package avh.ckc.demo.model

import kotlinx.serialization.Serializable

@Serializable
data class OrderState(
    val orderId: String,
    val batchId: String?,
    val potionId: String,
    val recipeId: String?,
    val customerId: String,
    val status: String,
    val updatedAt: String
)

typealias Order = OrderState

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

typealias Batch = BatchState

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

typealias EtaContext = ModelContextState

@Serializable
data class OrderFlavourState(
    val orderId: String,
    val flavourProfileId: String,
    val palette: String,
    val etaCorrectionFactor: Double,
    val moonPhase: String,
    val modelRequestId: String,
    val updatedAt: String
)

typealias OrderFlavour = OrderFlavourState
