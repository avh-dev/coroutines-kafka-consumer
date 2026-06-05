package avh.ckc.demostubs

import kotlin.test.Test
import kotlin.test.assertEquals

class DemoStubsConfigTest {
    @Test
    fun `defaults keep the stub server small`() {
        val config = DemoStubsConfig.fromEnvironment(emptyMap())

        assertEquals(8080, config.port)
        assertEquals(4, config.workers)
        assertEquals("localhost", config.redisHost)
        assertEquals(6379, config.redisPort)
        assertEquals(
            DemoStubsSettings(
                eta = ModelLatencySettings(40, 80, 160, 300),
                flavour = ModelLatencySettings(40, 80, 160, 300),
                registry = ModelLatencySettings(2, 3, 4, 5),
                errorRatePercent = 0
            ),
            DemoStubsSettings.baseline()
        )
    }

    @Test
    fun `environment settings can be overridden`() {
        val config = DemoStubsConfig.fromEnvironment(
            mapOf(
                "STUB_WORKERS" to "16",
                "REDIS_HOST" to "redis.example",
                "REDIS_PORT" to "6380"
            )
        )

        assertEquals(16, config.workers)
        assertEquals("redis.example", config.redisHost)
        assertEquals(6380, config.redisPort)
    }
}
