package avh.ckc.loadtest.domain

import avh.ckc.demo.proto.OrderLifecycleEvent

data class GeneratedBatch(
    val batchId: String,
    val cauldronId: String,
    val orderIds: List<String>,
    val lifecycleEvents: List<OrderLifecycleEvent>
)
