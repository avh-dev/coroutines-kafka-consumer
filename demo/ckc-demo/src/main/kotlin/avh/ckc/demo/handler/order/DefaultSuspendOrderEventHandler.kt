package avh.ckc.demo.handler.order

import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.service.SuspendOrderLifecycleService
import org.springframework.stereotype.Component

@Component
class DefaultSuspendOrderEventHandler(
    private val service: SuspendOrderLifecycleService
) : SuspendOrderEventHandler {
    override suspend fun handle(event: OrderLifecycleEvent) {
        service.apply(event)
    }
}
