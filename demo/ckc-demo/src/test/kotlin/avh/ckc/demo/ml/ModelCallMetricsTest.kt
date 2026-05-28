package avh.ckc.demo.ml

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull

class ModelCallMetricsTest {
    private val meterRegistry = SimpleMeterRegistry()
    private val metrics = ModelCallMetrics(meterRegistry)

    @Test
    fun `records successful sync model call duration`() {
        val result = metrics.record(
            model = "arcane_eta",
            operation = "estimate",
            clientMode = "sync",
            transport = "jdk"
        ) {
            "ok"
        }

        assertEquals("ok", result)
        val timer = meterRegistry.find("ckc.demo.model.call.duration")
            .tag("model", "arcane_eta")
            .tag("operation", "estimate")
            .tag("client_mode", "sync")
            .tag("transport", "jdk")
            .tag("outcome", "success")
            .timer()

        assertNotNull(timer)
        assertEquals(1, timer.count())
    }

    @Test
    fun `records failed suspend model call duration`() = runBlocking {
        assertFailsWith<IllegalStateException> {
            metrics.recordSuspend(
                model = "order_flavour",
                operation = "analyse",
                clientMode = "suspend",
                transport = "ktor_cio"
            ) {
                error("model failed")
            }
        }

        val timer = meterRegistry.find("ckc.demo.model.call.duration")
            .tag("model", "order_flavour")
            .tag("operation", "analyse")
            .tag("client_mode", "suspend")
            .tag("transport", "ktor_cio")
            .tag("outcome", "error")
            .timer()

        assertNotNull(timer)
        assertEquals(1, timer.count())
    }
}
