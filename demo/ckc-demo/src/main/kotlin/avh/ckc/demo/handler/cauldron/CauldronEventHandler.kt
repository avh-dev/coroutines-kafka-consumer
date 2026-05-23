package avh.ckc.demo.handler.cauldron

import avh.ckc.demo.proto.CauldronTelemetryEvent

interface CauldronEventHandler {
    fun handle(event: CauldronTelemetryEvent)
}
