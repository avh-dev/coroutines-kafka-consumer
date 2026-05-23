package avh.ckc.demo.handler.order

import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.service.SyncOrderLifecycleService
import org.springframework.stereotype.Component

@Component
class DefaultOrderEventHandler(
    private val service: SyncOrderLifecycleService
) : OrderEventHandler {
    override fun handle(event: OrderLifecycleEvent) {
        service.apply(event)
    }
}
