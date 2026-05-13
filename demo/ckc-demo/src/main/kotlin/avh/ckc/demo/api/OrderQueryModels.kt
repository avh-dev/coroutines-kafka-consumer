package avh.ckc.demo.api

data class OrderTrackingResponse(
    val order: OrderView,
    val batch: BatchView?
)

data class OrderView(
    val orderId: String,
    val batchId: String?,
    val potionId: String,
    val recipeId: String?,
    val customerId: String,
    val cauldronId: String?,
    val status: String,
    val updatedAt: String
)

data class BatchView(
    val batchId: String,
    val recipeId: String?,
    val potionId: String?,
    val cauldronId: String?,
    val status: String,
    val orderIds: List<String>,
    val updatedAt: String
)
