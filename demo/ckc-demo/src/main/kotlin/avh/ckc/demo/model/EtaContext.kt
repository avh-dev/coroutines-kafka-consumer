package avh.ckc.demo.model

import kotlinx.serialization.Serializable

@Serializable
data class EtaContext(
    val batchId: String,
    val previousTemperatureC: Double? = null,
    val previousDensitySg: Double? = null,
    val previousBubbleRateHz: Double? = null,
    val previousMagicalEtaUnits: Double? = null,
    val previousModelRequestId: String? = null,
    val updatedAt: String
)
