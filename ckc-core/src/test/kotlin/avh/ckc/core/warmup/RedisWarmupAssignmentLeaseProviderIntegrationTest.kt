package avh.ckc.core.warmup

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class RedisWarmupAssignmentLeaseProviderIntegrationTest {
    @Test
    fun `independent providers share one global reservation and completed session releases it`() = runBlocking {
        val configuration = configuration("completed")
        RedisWarmupAssignmentLeaseProvider(configuration).use { first ->
            RedisWarmupAssignmentLeaseProvider(configuration).use { second ->
                val lease = assertNotNull(first.tryAcquire(session("one"), instance("donor-1"), instance("r1"), participants))
                assertNull(second.tryAcquire(session("two"), instance("donor-2"), instance("r2"), participants))
                assertEquals(listOf(0), lease.assignments.map { it.warmupPartition }.distinct())

                lease.releaseCompleted()

                assertNotNull(second.tryAcquire(session("two"), instance("donor-2"), instance("r2"), participants))
                    .quarantine()
            }
        }
    }

    @Test
    fun `quarantined session is not automatically returned to another provider`() = runBlocking {
        val configuration = configuration("quarantined")
        RedisWarmupAssignmentLeaseProvider(configuration).use { first ->
            RedisWarmupAssignmentLeaseProvider(configuration).use { second ->
                assertNotNull(first.tryAcquire(session("one"), instance("donor-1"), instance("r1"), participants))
                    .quarantine()

                assertNull(second.tryAcquire(session("two"), instance("donor-2"), instance("r2"), participants))
            }
        }
    }

    private fun configuration(suffix: String) = WarmupAssignmentConfiguration(
        redisUri = "redis://${redis.host}:${redis.getMappedPort(REDIS_PORT)}",
        lockKey = "ckc:test:$suffix:${UUID.randomUUID()}"
    )

    private fun session(value: String) = WarmupSessionId("$value-${UUID.randomUUID()}")
    private fun instance(value: String) = WarmupInstanceId(value)

    private companion object {
        const val REDIS_PORT = 6379
        val participants = listOf(
            WarmupParticipantConfiguration(
                WarmupConsumerId("orders"),
                listOf(WarmupTopicMapping("orders", "warmup-orders")),
                WarmupForwardingProfile()
            )
        )

        @Container
        @JvmStatic
        val redis = RedisContainer(DockerImageName.parse("redis:7.4-alpine"))
    }
}

private class RedisContainer(image: DockerImageName) : GenericContainer<RedisContainer>(image) {
    init {
        withExposedPorts(6379)
    }
}
