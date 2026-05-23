package avh.ckc.demo.handler.batch

import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.service.SyncBatchLifecycleService
import org.springframework.stereotype.Component

@Component
class DefaultBatchEventHandler(
    private val service: SyncBatchLifecycleService
) : BatchEventHandler {
    override fun handle(event: BatchLifecycleEvent) {
        service.apply(event)
    }
}
