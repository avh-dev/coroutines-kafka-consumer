package avh.ckc.demostubs

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class DemoStubsConfigTest {
    @Test
    fun `defaults keep the stub server small`() {
        val config = DemoStubsConfig.fromEnvironment(emptyMap())

        assertEquals(8080, config.port)
        assertEquals(4, config.workers)
        assertEquals(0, config.requestTimeoutMillis)
        assertEquals("localhost", config.redisHost)
        assertEquals(6379, config.redisPort)
        assertEquals(
            DemoStubsSettings(
                eta = ModelLatencySettings(mapOf("p90" to 40, "p95" to 80, "p99" to 160, "p100" to 300)),
                flavour = ModelLatencySettings(mapOf("p90" to 40, "p95" to 80, "p99" to 160, "p100" to 300)),
                registry = ModelLatencySettings(mapOf("p90" to 2, "p95" to 3, "p99" to 4, "p100" to 5)),
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
                "STUB_REQUEST_TIMEOUT_MS" to "75000",
                "REDIS_HOST" to "redis.example",
                "REDIS_PORT" to "6380"
            )
        )

        assertEquals(16, config.workers)
        assertEquals(75000, config.requestTimeoutMillis)
        assertEquals("redis.example", config.redisHost)
        assertEquals(6380, config.redisPort)
    }

    @Test
    fun `request timeout cannot be negative`() {
        assertFailsWith<IllegalArgumentException> {
            DemoStubsConfig.fromEnvironment(mapOf("STUB_REQUEST_TIMEOUT_MS" to "-1"))
        }
    }

    @Test
    fun `arbitrary percentile keys are converted to quantiles`() {
        val settings = ModelLatencySettings(
            linkedMapOf("p50" to 10, "p75" to 20, "p999" to 80, "p100" to 60_000)
        )

        assertEquals(listOf(0.5, 0.75, 0.999, 1.0), settings.buckets.map { it.quantile })
    }

    @Test
    fun `terminal percentile is required`() {
        assertFailsWith<IllegalArgumentException> {
            ModelLatencySettings(mapOf("p99" to 10))
        }
    }

    @Test
    fun `percentile delays must be monotonic`() {
        assertFailsWith<IllegalArgumentException> {
            ModelLatencySettings(mapOf("p99" to 20, "p100" to 10))
        }
    }

    @Test
    fun `different names cannot identify the same quantile`() {
        assertFailsWith<IllegalArgumentException> {
            ModelLatencySettings(mapOf("p5" to 10, "p50" to 20, "p100" to 30))
        }
    }
}
