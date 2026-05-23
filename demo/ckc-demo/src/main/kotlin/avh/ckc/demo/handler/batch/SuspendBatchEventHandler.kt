package avh.ckc.demo.handler.batch

import avh.ckc.demo.proto.BatchLifecycleEvent

interface SuspendBatchEventHandler {
    suspend fun handle(event: BatchLifecycleEvent)
}
