package avh.ckc.demo.handler.order

import avh.ckc.demo.proto.OrderLifecycleEvent

interface OrderEventHandler {
    fun handle(event: OrderLifecycleEvent)
}
