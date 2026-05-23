package avh.ckc.demo.handler.batch

import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.service.SuspendBatchLifecycleService
import org.springframework.stereotype.Component

@Component
class DefaultSuspendBatchEventHandler(
    private val service: SuspendBatchLifecycleService
) : SuspendBatchEventHandler {
    override suspend fun handle(event: BatchLifecycleEvent) {
        service.apply(event)
    }
}
