package avh.ckc.demostubs

import kotlin.test.Test
import kotlin.test.assertEquals

class DemoStubsConfigTest {
    @Test
    fun `defaults keep the stub server small`() {
        val config = DemoStubsConfig.fromEnvironment(emptyMap())

        assertEquals(8080, config.port)
        assertEquals(4, config.workers)
        assertEquals(0, config.errorRatePercent)
    }

    @Test
    fun `workers can be overridden`() {
        val config = DemoStubsConfig.fromEnvironment(mapOf("STUB_WORKERS" to "16"))

        assertEquals(16, config.workers)
    }
}
