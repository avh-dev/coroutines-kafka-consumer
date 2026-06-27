package avh.ckc.demo.service.cauldron

import avh.ckc.demo.model.EtaContext
import avh.ckc.demo.proto.CauldronTelemetryEvent
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit

@Service
class CauldronTelemetryMetrics(
    private val meterRegistry: MeterRegistry
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun recordEventGap(telemetryEvent: CauldronTelemetryEvent, previous: EtaContext?) {
        previous ?: return
        val previousAt = parseInstant(previous.updatedAt) ?: return
        val currentAt = parseInstant(telemetryEvent.metadata.occurredAt) ?: return
        val gapMillis = Duration.between(previousAt, currentAt).toMillis()

        if (gapMillis < 0) {
            outOfOrderCounter().increment()
            return
        }

        gapTimer().record(gapMillis, TimeUnit.MILLISECONDS)
    }

    private fun parseInstant(value: String): Instant? =
        runCatching { Instant.parse(value) }
            .onFailure {
                logger.debug("Cannot parse cauldron telemetry timestamp value={}", value, it)
            }
            .getOrNull()

    private fun gapTimer(): Timer =
        Timer.builder("ckc.demo.cauldron.telemetry.event.gap")
            .description("Event-time gap between consecutive processed cauldron telemetry records.")
            .register(meterRegistry)

    private fun outOfOrderCounter(): Counter =
        Counter.builder("ckc.demo.cauldron.telemetry.event.out.of.order")
            .description("Processed cauldron telemetry records older than the previous persisted ETA context timestamp.")
            .register(meterRegistry)
}
