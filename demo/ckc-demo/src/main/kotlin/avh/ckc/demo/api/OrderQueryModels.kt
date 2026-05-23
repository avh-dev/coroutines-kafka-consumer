package avh.ckc.demo.api

import avh.ckc.demo.model.Batch
import avh.ckc.demo.model.Order
import avh.ckc.demo.model.OrderFlavour

data class OrderTrackingResponse(
    val order: Order,
    val batch: Batch?,
    val flavour: OrderFlavour?
)
