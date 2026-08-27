package avh.ckc.loadtest.config

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class ProducerConfigStep(
    val atSeconds: Long,
    val topic: ProducerTopic = ProducerTopic.ALL,
    val lingerMs: Int? = null,
    val batchSize: Int? = null,
    val compressionType: String? = null,
    val bufferMemory: Long? = null
) {
    internal fun validate(context: String) {
        require(lingerMs == null || lingerMs >= 0) { "$context.lingerMs must be non-negative" }
        require(batchSize == null || batchSize > 0) { "$context.batchSize must be positive" }
        require(compressionType == null || compressionType.isNotBlank()) {
            "$context.compressionType must not be blank"
        }
        require(bufferMemory == null || bufferMemory > 0) { "$context.bufferMemory must be positive" }
        require(lingerMs != null || batchSize != null || compressionType != null || bufferMemory != null) {
            "$context must override at least one producer setting"
        }
    }
}

@Serializable
enum class ProducerTopic {
    @SerialName("all")
    ALL,

    @SerialName("order")
    ORDER,

    @SerialName("batch")
    BATCH,

    @SerialName("telemetry")
    TELEMETRY
}
