package avh.ckc.demostubs

import kotlin.test.Test
import kotlin.test.assertEquals

class DemoStubsConfigTest {
    @Test
    fun `defaults keep the stub server small`() {
        val config = DemoStubsConfig.fromEnvironment(emptyMap())

        assertEquals(8080, config.port)
        assertEquals(4, config.workers)
        assertEquals(
            DemoStubsSettings(
                eta = ModelLatencySettings(40, 80, 160, 300),
                flavour = ModelLatencySettings(40, 80, 160, 300),
                errorRatePercent = 0
            ),
            DemoStubsSettings.baseline()
        )
    }

    @Test
    fun `workers can be overridden`() {
        val config = DemoStubsConfig.fromEnvironment(mapOf("STUB_WORKERS" to "16"))

        assertEquals(16, config.workers)
    }
}
