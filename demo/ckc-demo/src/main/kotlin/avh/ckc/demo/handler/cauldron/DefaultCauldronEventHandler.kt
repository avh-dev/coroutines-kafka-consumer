package avh.ckc.demo.handler.cauldron

import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.service.SyncEtaRecalculationService
import org.springframework.stereotype.Component

@Component
class DefaultCauldronEventHandler(
    private val service: SyncEtaRecalculationService
) : CauldronEventHandler {
    override fun handle(event: CauldronTelemetryEvent) {
        service.recalculate(event)
    }
}
