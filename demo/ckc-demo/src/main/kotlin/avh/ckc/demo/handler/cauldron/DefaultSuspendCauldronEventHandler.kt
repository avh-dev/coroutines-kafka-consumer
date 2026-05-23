package avh.ckc.demo.handler.cauldron

import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.service.SuspendEtaRecalculationService
import org.springframework.stereotype.Component

@Component
class DefaultSuspendCauldronEventHandler(
    private val service: SuspendEtaRecalculationService
) : SuspendCauldronEventHandler {
    override suspend fun handle(event: CauldronTelemetryEvent) {
        service.recalculate(event)
    }
}
