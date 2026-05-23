package avh.ckc.demo.handler.order

import avh.ckc.demo.proto.OrderLifecycleEvent

interface SuspendOrderEventHandler {
    suspend fun handle(event: OrderLifecycleEvent)
}
