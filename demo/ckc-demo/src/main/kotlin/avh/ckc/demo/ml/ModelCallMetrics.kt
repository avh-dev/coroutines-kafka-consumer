package avh.ckc.demo.ml

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import java.util.concurrent.TimeUnit

class ModelCallMetrics(
    private val meterRegistry: MeterRegistry
) {
    fun <T> record(
        model: String,
        operation: String,
        clientMode: String,
        transport: String,
        block: () -> T
    ): T {
        val startNanos = meterRegistry.config().clock().monotonicTime()
        var outcome = "success"
        try {
            return block()
        } catch (ex: Throwable) {
            outcome = "error"
            throw ex
        } finally {
            recordDuration(model, operation, clientMode, transport, outcome, startNanos)
        }
    }

    suspend fun <T> recordSuspend(
        model: String,
        operation: String,
        clientMode: String,
        transport: String,
        block: suspend () -> T
    ): T {
        val startNanos = meterRegistry.config().clock().monotonicTime()
        var outcome = "success"
        try {
            return block()
        } catch (ex: Throwable) {
            outcome = "error"
            throw ex
        } finally {
            recordDuration(model, operation, clientMode, transport, outcome, startNanos)
        }
    }

    private fun recordDuration(
        model: String,
        operation: String,
        clientMode: String,
        transport: String,
        outcome: String,
        startNanos: Long
    ) {
        val durationNanos = meterRegistry.config().clock().monotonicTime() - startNanos
        Timer.builder("ckc.demo.model.call.duration")
            .description("Duration of demo model-client calls. Timer count can be used as model-call throughput.")
            .tag("model", model)
            .tag("operation", operation)
            .tag("client_mode", clientMode)
            .tag("transport", transport)
            .tag("outcome", outcome)
            .register(meterRegistry)
            .record(durationNanos, TimeUnit.NANOSECONDS)
    }
}
