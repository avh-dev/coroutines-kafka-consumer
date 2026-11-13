package avh.ckc.core.warmup

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class WarmupControlRouterTest {
    private val local = WarmupInstanceId("recipient")
    private val donor = WarmupInstanceId("donor")
    private val mapping = WarmupTopicMapping(WarmupConsumerId("orders"), "orders", "warmup-orders")

    @Test
    fun `router exposes every record to state handlers and only local messages to lifecycle handlers`() = runBlocking {
        val router = WarmupControlRouter(local)
        val state = mutableListOf<Long>()
        val lifecycle = mutableListOf<Long>()
        router.registerGlobal { state += it.offset }
        router.register { lifecycle += it.offset }

        router.onMessage(record(1, request("broadcast")))
        router.onMessage(record(2, accepted(request("local"), local)))
        router.onMessage(record(3, accepted(request("foreign"), donor)))

        assertEquals(listOf(1L, 2L, 3L), state)
        assertEquals(listOf(1L, 2L), lifecycle)
    }

    private fun request(id: String) = WarmupRequested(
        WarmupSessionId(id), local, listOf(mapping), 2_000L, "fingerprint", 10_000L
    )

    private fun accepted(request: WarmupRequested, target: WarmupInstanceId) = DonorAccepted(
        request.sessionId, donor, target, request.configurationFingerprint, request.expiresAtEpochMillis
    )

    private fun record(offset: Long, message: WarmupControlMessage) = WarmupControlRecord(offset, message)
}
