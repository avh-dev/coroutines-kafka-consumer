package avh.ckc.demo.handler.batch

import avh.ckc.demo.proto.BatchLifecycleEvent

interface BatchEventHandler {
    fun handle(event: BatchLifecycleEvent)
}
