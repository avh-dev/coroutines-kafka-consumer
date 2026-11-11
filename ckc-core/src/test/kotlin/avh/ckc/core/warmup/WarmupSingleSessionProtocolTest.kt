package avh.ckc.core.warmup

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicLong

class WarmupSingleSessionProtocolTest {
    private val local = WarmupInstanceId("recipient")
    private val recipient = local
    private val donor = WarmupInstanceId("donor")
    private val mapping = WarmupTopicMapping(WarmupConsumerId("orders"), "orders", "warmup-orders")

    @Test
    fun `earliest valid request wins and simultaneous request conflicts`() {
        val protocol = WarmupSingleSessionProtocol(local) { 1_000L }
        val first = request("first", deadline = 10_000L)
        val second = request("second", deadline = 10_000L)

        assertEquals(WarmupControlDecisionKind.ACTIVATED, protocol.evaluate(record(12, first)).kind)
        val conflict = protocol.evaluate(record(13, second))

        assertEquals(WarmupControlDecisionKind.CONFLICT, conflict.kind)
        assertEquals(first.sessionId, conflict.activeSessionId)
        assertEquals(12L, protocol.winningRequestOffset())
        assertEquals(
            WarmupControlDecisionKind.IGNORED_STALE,
            protocol.evaluate(record(11, request("older", deadline = 10_000L))).kind
        )
    }

    @Test
    fun `expired request cannot block next valid request`() {
        val now = AtomicLong(5_000L)
        val protocol = WarmupSingleSessionProtocol(local, now::get)

        assertEquals(
            WarmupControlDecisionKind.IGNORED_EXPIRED,
            protocol.evaluate(record(1, request("expired", deadline = 4_999L))).kind
        )
        assertEquals(
            WarmupControlDecisionKind.ACTIVATED,
            protocol.evaluate(record(2, request("active", deadline = 10_000L))).kind
        )
    }

    @Test
    fun `stale foreign and repeated messages cannot mutate active session`() {
        val protocol = WarmupSingleSessionProtocol(local) { 1_000L }
        val request = request("active", deadline = 10_000L)
        protocol.evaluate(record(1, request))

        assertEquals(WarmupControlDecisionKind.DUPLICATE, protocol.evaluate(record(2, request)).kind)
        assertEquals(
            WarmupControlDecisionKind.IGNORED_FOREIGN_TARGET,
            protocol.evaluate(record(3, accepted(request, WarmupInstanceId("other")))).kind
        )
        assertEquals(
            WarmupControlDecisionKind.IGNORED_STALE,
            protocol.evaluate(record(4, accepted(request.copy(configurationFingerprint = "stale"), local))).kind
        )
        assertEquals(
            WarmupControlDecisionKind.ACCEPTED,
            protocol.evaluate(record(5, accepted(request, local))).kind
        )
        assertEquals(
            WarmupControlDecisionKind.IGNORED_STALE,
            protocol.evaluate(record(6, ended(request).copy(senderInstanceId = donor))).kind
        )
        assertEquals(request.sessionId, protocol.activeSessionId())
        assertEquals(
            WarmupControlDecisionKind.IGNORED_STALE,
            protocol.evaluate(
                record(
                    7,
                    DonorAccepted(
                        request.sessionId,
                        recipient,
                        recipient,
                        request.configurationFingerprint,
                        request.expiresAtEpochMillis
                    )
                )
            ).kind
        )
        assertEquals(
            WarmupControlDecisionKind.ENDED,
            protocol.evaluate(record(8, ended(request))).kind
        )
        assertEquals(null, protocol.activeSessionId())
        assertEquals(
            WarmupControlDecisionKind.IGNORED_STALE,
            protocol.evaluate(record(9, accepted(request, local))).kind
        )
    }

    @Test
    fun `router sends broadcast and local messages to registered handlers only`() = runBlocking {
        val router = WarmupControlRouter(local)
        val first = mutableListOf<Long>()
        val second = mutableListOf<Long>()
        val registration = router.register { first += it.offset }
        router.register { second += it.offset }

        router.onMessage(record(1, request("broadcast", 10_000L)))
        router.onMessage(record(2, accepted(request("local", 10_000L), local)))
        router.onMessage(record(3, accepted(request("foreign", 10_000L), donor)))
        registration.close()
        router.onMessage(record(4, request("after-close", 10_000L)))

        assertEquals(listOf(1L, 2L), first)
        assertEquals(listOf(1L, 2L, 4L), second)
    }

    private fun request(id: String, deadline: Long) = WarmupRequested(
        WarmupSessionId(id), recipient, listOf(mapping), 2_000L, "fingerprint", deadline
    )

    private fun accepted(request: WarmupRequested, target: WarmupInstanceId) = DonorAccepted(
        request.sessionId,
        donor,
        target,
        request.configurationFingerprint,
        request.expiresAtEpochMillis
    )

    private fun ended(request: WarmupRequested) = SessionEnded(
        request.sessionId,
        recipient,
        WarmupSessionEndState.COMPLETED,
        null,
        request.configurationFingerprint,
        request.expiresAtEpochMillis
    )

    private fun record(offset: Long, message: WarmupControlMessage) = WarmupControlRecord(offset, message)
}
