package avh.ckc.loadtest.runtime

import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals

class GeneratorIdentityTest {
    @Test
    fun `builds compact ids from external shard and internal worker`() {
        val identity = GeneratorIdentity(
            externalShardIndex = 1,
            totalExternalShards = 3,
            workerIndex = 5,
            totalWorkers = 8
        )

        assertEquals("order-1-5-00021212", identity.entityId("order", 21_212, width = 8))
        assertEquals("mrb-1-5-0", identity.regulatoryTraceId(Instant.EPOCH))
        assertEquals("externalShard=1/3 worker=5/8", identity.label())
    }
}
