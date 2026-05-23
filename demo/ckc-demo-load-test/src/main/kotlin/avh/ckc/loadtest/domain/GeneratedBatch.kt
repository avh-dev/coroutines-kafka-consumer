package avh.ckc.loadtest.domain

import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.OrderLifecycleEvent

data class GeneratedBatch(
    val batchId: String,
    val cauldronId: String,
    val orderIds: List<String>,
    val orderEvents: List<OrderLifecycleEvent>,
    val batchEvents: List<BatchLifecycleEvent>
)
