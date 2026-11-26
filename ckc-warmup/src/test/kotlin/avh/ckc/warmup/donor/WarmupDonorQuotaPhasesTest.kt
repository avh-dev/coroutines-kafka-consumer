package avh.ckc.warmup.donor

import avh.ckc.core.processing.RecordProcessingOutcome
import avh.ckc.warmup.dataplane.WarmupDestination
import avh.ckc.warmup.dataplane.WarmupParticipantDataPlane
import avh.ckc.warmup.dataplane.WarmupParticipantDonorLane
import avh.ckc.warmup.dataplane.WarmupRecordSender
import avh.ckc.warmup.dataplane.WarmupSendAcknowledgement
import avh.ckc.warmup.protocol.WarmupMappingQuota
import avh.ckc.warmup.protocol.WarmupParticipantId
import avh.ckc.warmup.protocol.WarmupQuotaProfileParser
import avh.ckc.warmup.protocol.WarmupRecordCompleted
import avh.ckc.warmup.protocol.WarmupRecordId
import avh.ckc.warmup.protocol.WarmupRemoteOutcome
import avh.ckc.warmup.protocol.WarmupSessionId
import avh.ckc.warmup.protocol.WarmupTopicMapping
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.sync.Semaphore
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

class WarmupDonorQuotaPhasesTest {
    @Test
    fun `four sessions admitted at the same instant can delegate at their respective phase boundaries`() = runTest {
        val donor = Donor()
        for (session in listOf("first", "second", "third", "fourth")) {
            donor.activate(session, mapping(session, quota = "2"))
        }

        repeat(2) { donor.expect("first", session = "first") }
        donor.expect("first", session = null)

        for ((session, phase) in listOf("third" to 250L, "second" to 500L, "fourth" to 750L)) {
            donor.at(phase - 1)
            donor.expect(session, session = null)
            donor.at(phase)
            repeat(2) { donor.expect(session, session = session) }
            donor.expect(session, session = null)
        }
    }

    @Test
    fun `idle periods do not accumulate quota or move boundaries`() = runTest {
        val donor = Donor()
        donor.at(123)
        donor.activate("first", mapping("orders", quota = "2"))
        donor.expect("orders", session = "first")

        donor.at(4_923)
        repeat(2) { donor.expect("orders", session = "first") }
        donor.at(5_122)
        donor.expect("orders", session = null)
        donor.at(5_123)
        repeat(2) { donor.expect("orders", session = "first") }
        donor.expect("orders", session = null)
    }

    @Test
    fun `a replacement uses the largest available gap after normal close`() = verifyReplacement(cancelled = false)

    @Test
    fun `a replacement uses the largest available gap after cancellation`() = verifyReplacement(cancelled = true)

    private fun verifyReplacement(cancelled: Boolean) = runTest {
        val donor = Donor()
        donor.activate("first", mapping("first"))
        donor.activate("departing", mapping("departing"))
        donor.activate("third", mapping("third"))
        donor.expect("first", session = "first")
        donor.at(250)
        donor.expect("third", session = "third")

        donor.at(400)
        donor.close("departing", cancelled)
        donor.activate("replacement", mapping("replacement"))
        donor.expect("first", session = null)
        donor.expect("third", session = null)
        donor.at(624)
        donor.expect("replacement", session = null)
        donor.at(625)
        donor.expect("replacement", session = "replacement")
        donor.at(999)
        donor.expect("first", session = null)
        donor.at(1_000)
        donor.expect("first", session = "first")
        donor.at(1_249)
        donor.expect("third", session = null)
        donor.at(1_250)
        donor.expect("third", session = "third")
    }

    @Test
    fun `equal intervals share a phase across participants with independent limits and ramps`() = runTest {
        val donor = Donor()
        donor.activate("first", mapping("first-fast"), mapping("first-slow", intervalMillis = 2_000))
        donor.at(700)
        donor.activate(
            "second",
            mapping("orders", quota = "1 -> (400ms) -> 2"),
            mapping("batches", quota = "2"),
            mapping("telemetry", intervalMillis = 2_000)
        )
        donor.expect("orders", session = null)
        donor.at(999)
        donor.expect("telemetry", session = null)
        donor.at(1_000)
        donor.expect("telemetry", session = "second")
        donor.at(1_499)
        donor.expect("orders", session = null)
        donor.expect("batches", session = null)
        donor.at(1_500)
        donor.expect("orders", session = "second")
        donor.expect("orders", session = null)
        repeat(2) { donor.expect("batches", session = "second") }
        donor.expect("batches", session = null)
        donor.at(1_900)
        donor.expect("orders", session = "second")
        donor.expect("orders", session = null)
    }

    @Test
    fun `second batch can use all places released by the first batch`() = verifySecondBatch(released = 300)

    @Test
    fun `second batch can use only the 80 released places without consuming blocked quota`() =
        verifySecondBatch(released = 80)

    private fun verifySecondBatch(released: Int) = runTest {
        val donor = Donor(inflightLimit = 300)
        val mapping = mapping("orders", quota = "300")
        donor.activate("first", mapping)
        donor.activate("second", mapping)

        val first = List(300) { async { donor.process("orders", holdResult = true) } }
        testScheduler.runCurrent()
        val firstResults = donor.forwarded.toList()
        assertEquals(List(300) { WarmupSessionId("first") }, firstResults.map { it.sessionId })
        assertEquals(0, donor.availablePlaces)

        donor.at(499)
        firstResults.take(released).forEach(donor::complete)
        first.take(released).forEach { assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, it.await()) }
        assertEquals(released, donor.availablePlaces)
        donor.expect("orders", session = null)

        donor.at(500)
        val second = List(300) { async { donor.process("orders", holdResult = true) } }
        testScheduler.runCurrent()
        val secondResults = donor.forwarded.drop(300)
        assertEquals(List(released) { WarmupSessionId("second") }, secondResults.map { it.sessionId })
        assertEquals(0, donor.availablePlaces)
        second.drop(released).forEach { assertEquals(RecordProcessingOutcome.HandlerSucceeded, it.await()) }
        donor.expect("orders", session = null)

        firstResults.drop(released).forEach(donor::complete)
        first.drop(released).forEach { assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, it.await()) }
        repeat(300 - released) { donor.expect("orders", session = "second") }
        donor.expect("orders", session = null)

        secondResults.forEach(donor::complete)
        second.take(released).forEach { assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, it.await()) }
        assertEquals(300, donor.availablePlaces)
    }

    private class Donor(inflightLimit: Int = 1) {
        private var now = 0L
        private var nextOffset = 0L
        private val phases = WarmupDonorQuotaPhases { now }
        private val store = LogicalWarmupDonorSessionStore(maxSessions = 4, maxAggregateBudgetPerSecond = 1_000)
        private val planes = mutableMapOf<WarmupParticipantId, WarmupParticipantDataPlane<String, String>>()
        private val inflight = Semaphore(inflightLimit)
        val availablePlaces get() = inflight.availablePermits
        val forwarded = mutableListOf<WarmupRecordId>()

        fun at(millis: Long) {
            val nanos = millis.milliseconds.inWholeNanoseconds
            require(nanos >= now)
            now = nanos
        }

        fun activate(name: String, vararg mappings: WarmupTopicMapping) {
            val id = WarmupSessionId(name)
            val windows = phases.Session(mappings.map { it.quota.quotaIntervalMillis.milliseconds }.toSet())
            val lanes = mappings.groupBy { it.participantId }.map { (participant, selected) ->
                val plane = planes.getOrPut(participant) { WarmupParticipantDataPlane() }
                WarmupParticipantDonorLane(id, plane, selected.associateWith { mapping ->
                    val interval = mapping.quota.quotaIntervalMillis.milliseconds
                    WarmupDonorSession(
                        sessionId = id,
                        quotaProfile = mapping.quota.profile,
                        quotaInterval = interval,
                        delegationTimeout = 5.seconds,
                        destinationResolver = WarmupDestinationResolver { WarmupDestination(mapping.warmupTopic) },
                        sender = WarmupRecordSender { record, destination, sessionId ->
                            val partition = TopicPartition(destination.topic, destination.partition)
                            val result = WarmupRecordId(sessionId, partition, record.offset())
                            forwarded += result
                            if (record.value() != "hold") complete(result)
                            WarmupSendAcknowledgement(partition, record.offset())
                        },
                        nanoTime = { now },
                        inflightDelegations = inflight,
                        firstQuotaWindowStart = { windows.firstWindowStart(interval) }
                    )
                })
            }
            val budget = lanes.sumOf { it.admissionBudgetPerSecond }
            val session = LogicalWarmupDonorSession(id, budget, lanes, windows)
            assertEquals(DonorSessionAdmission.ACCEPTED, store.tryAdmitPreparation(id, budget))
            assertTrue(store.installAdmittedSession(session))
            assertTrue(store.activate(id))
        }

        fun close(name: String, cancelled: Boolean) {
            val id = WarmupSessionId(name)
            if (cancelled) store.fail(id, CancellationException("Recipient cancelled"))
            else store.stopDelegating(id)
            assertNotNull(store.detachDrainedSession(id)).close()
        }

        fun complete(record: WarmupRecordId) {
            store.acceptResult(WarmupRecordCompleted(record, WarmupRemoteOutcome.Succeeded))
        }

        suspend fun process(source: String, holdResult: Boolean = false): RecordProcessingOutcome {
            val record = ConsumerRecord(source, 0, nextOffset++, "key", if (holdResult) "hold" else "complete")
            return planes.getValue(WarmupParticipantId(source)).process(record) { RecordProcessingOutcome.HandlerSucceeded }
        }

        suspend fun expect(source: String, session: String?) {
            val before = forwarded.size
            val outcome = process(source)
            if (session == null) {
                assertEquals(RecordProcessingOutcome.HandlerSucceeded, outcome)
                assertEquals(before, forwarded.size)
            } else {
                assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, outcome)
                assertEquals(listOf(WarmupSessionId(session)), forwarded.drop(before).map { it.sessionId })
            }
        }
    }

    private fun mapping(topic: String, quota: String = "1", intervalMillis: Long = 1_000) = WarmupTopicMapping(
        WarmupParticipantId(topic),
        topic,
        "warmup-$topic",
        "fingerprint-$topic",
        WarmupMappingQuota(WarmupQuotaProfileParser.parse(quota), intervalMillis)
    )
}
