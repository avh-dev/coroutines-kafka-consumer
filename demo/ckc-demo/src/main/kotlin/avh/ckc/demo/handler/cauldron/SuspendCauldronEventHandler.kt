package avh.ckc.demo.handler.cauldron

import avh.ckc.demo.proto.CauldronTelemetryEvent

interface SuspendCauldronEventHandler {
    suspend fun handle(event: CauldronTelemetryEvent)
}
