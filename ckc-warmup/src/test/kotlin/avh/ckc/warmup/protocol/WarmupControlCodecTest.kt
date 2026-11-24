package avh.ckc.warmup.protocol

import java.security.MessageDigest
import java.util.UUID
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class WarmupControlCodecTest {
    private val namespace = WarmupControlNamespace("global5-shared-partition-v6-test")
    private val recipient = WarmupInstanceId("recipient")
    private val donor = WarmupInstanceId("donor")
    private val session = WarmupSessionId("session")
    private val mapping = WarmupTopicMapping(
        WarmupParticipantId("orders"),
        "orders",
        "warmup-orders",
        "mapping-fingerprint",
        WarmupMappingQuota(
            WarmupQuotaProfileParser.parse("1 -> (30s, ramp) -> 10"),
            quotaIntervalMillis = 1_000
        )
    )
    private val donorMapping = WarmupDonorMapping(mapping, setOf(WarmupSourcePartition(0, 7)))
    private val fingerprint = "context-fingerprint"

    @Test
    fun `every v6 control message retains its CORE-96 payload and round trips`() {
        val recordId = WarmupRecordId(session, TopicPartition("warmup-orders", 0), 91)
        val recordOutcomes = listOf(WarmupRemoteOutcome.Succeeded, WarmupRemoteOutcome.TerminalFailure) +
            WarmupDiscardReason.entries.map { WarmupRemoteOutcome.Discarded(it) }
        val completions = recordOutcomes.map { outcome ->
            ControlRecordCompleted(
                namespace, session, recipient, donor, WarmupRecordCompleted(recordId, outcome), fingerprint, 10_000
            )
        }
        val messages = listOf(
            WarmupRequested(namespace, session, recipient, listOf(mapping), 2_000, 10, fingerprint, 10_000),
            DonorOffered(namespace, session, donor, recipient, listOf(donorMapping), fingerprint, 10_000),
            SessionAccepted(namespace, session, recipient, donor, listOf(donorMapping), 10, fingerprint, 10_000),
            DonorAccepted(namespace, session, donor, recipient, listOf(donorMapping), 10, fingerprint, 10_000),
            SessionRejected(namespace, session, donor, recipient, "capacity_limit", fingerprint, 10_000),
            RecipientReady(
                namespace, session, recipient, donor, listOf(WarmupPartitionAssignment(mapping)), fingerprint, 10_000
            ),
            WarmupCompletionRequested(namespace, session, recipient, donor, fingerprint, 10_000),
            DonorClosed(namespace, session, donor, recipient, fingerprint, 10_000),
            SessionEnded(namespace, session, recipient, WarmupSessionEndState.COLD_FALLBACK, "metadata_failed", fingerprint, 10_000)
        ) + completions

        // SHA-256 of the payloads encoded by CORE-96 before the module extraction.
        val expectedHashes = listOf(
            "bbf05b113d2967d3edfa250a7778257cd113cf78f3f47bd16c3ba7102e334654",
            "01c0f26db545d179aec9305250efe658038c2dbec713d9656f514a1e10f3c4ba",
            "6ddb81463c99b98d36caceab9597f620ec057f2b7e72a133814e5e58353eb043",
            "573a4e028e236bffa64428161e88f07ca09d242a3d6e812f237c194acff9de97",
            "83008fd18839a4da7c1fc090ba3c040c74b6837fe36d7a88eee9da34026bee40",
            "20505f7e4a8b2d31c6473cd35b07e6bd58244ee9d6bea596dc1f313eb374bb95",
            "494c953f5774c3636293597f235c947301463366f9d6602b1cda8a33714d22a0",
            "f4619d3210c25b1500559a133c9aa0ecde3a06b0267bf09bd6124f90c2fc3dfd",
            "d1c3cf928e1452c3d8d05b21638b7198fcb83b8fcc7b0fe3c7d55ff7541c3d53",
            "59c50eaa85bf7cc00b067198a1a1c7ec5ca2410047344713fe9fc6ab02a439b8",
            "d7e5868d8afc6873cd81ad6500b0be1fda1d9f50d00a792badcb5837bcac9338",
            "1cad33ef03df0ea80ead815f2a004001ddaef8a9f1e5497370d44a74363ecc48",
            "33d27dd7bb4885139e0346f9c768ffc9d2141529f56fadc5ec5639c61ab3539f",
            "e45cbfa910cfe517c55590491157a5d2b3f504bfae09b61dfb1bb0d32c33f611",
            "df026ca62044d82a78643da7eccbf04edda1e1936a9188bfbeccd185e621f41b",
            "460655a2c2a819f2a66bb379e93fb05c6442a4b10f29a35395625e8b69babc84",
            "761f9b2f9ca32c7e687ce106647a30d1a921ceedf1d0560fdffb267c2b5c6a0d"
        )
        assertEquals(expectedHashes.size, messages.size)
        for ((message, expected) in messages.zip(expectedHashes)) {
            val bytes = WarmupControlCodec.encode(message)
            val hash = MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }
            assertEquals(expected, hash, message.toString())
            assertEquals(message, WarmupControlCodec.decode(bytes))
        }
    }

    @Test
    fun `capability and probes retain their exact legacy v6 wire envelopes`() {
        val accepted = DonorAccepted(namespace, session, donor, recipient, listOf(donorMapping), 10, fingerprint, 10_000)
        val ready = RecipientReady(
            namespace, session, recipient, donor, listOf(WarmupPartitionAssignment(mapping)), fingerprint, 10_000
        )
        val messages = listOf(
            accepted.copy(supportsLiveness = true) to accepted,
            WarmupLivenessProbe(ready, 500) to ready,
            WarmupLivenessProbe(accepted, 500) to accepted
        )

        messages.forEach { (modern, legacy) ->
            val wire = WarmupControlCodec.producerRecord("control", modern)
            assertEquals(legacy.sessionId.value, wire.key())
            assertTrue(WarmupControlCodec.encode(legacy).contentEquals(wire.value()))
            assertEquals(legacy, WarmupControlCodec.decode(wire.value()))
            assertEquals(modern, WarmupControlCodec.decode(wire.value(), wire.headers()))

            val header = wire.headers().single()
            if (modern is WarmupLivenessProbe) {
                assertEquals("ckc.warmup.liveness.probe", header.key())
                assertArrayEquals(byteArrayOf(0, 0, 0, 0, 0, 0, 1, -12), header.value())
            } else {
                assertEquals("ckc.warmup.liveness.supported", header.key())
                assertArrayEquals(byteArrayOf(1), header.value())
            }
        }
    }

    @Test
    fun `delegation correlation survives the Kafka round trip with the same v6 payload`() {
        val completed = WarmupRecordCompleted(
            WarmupRecordId(session, TopicPartition(mapping.warmupTopic, 0), 91), WarmupRemoteOutcome.Succeeded
        )
        val legacy = ControlRecordCompleted(namespace, session, recipient, donor, completed, fingerprint, 10_000)
        val modern = legacy.copy(completed = completed.copy(delegationId = UUID(93, 41)))
        val wire = WarmupControlCodec.producerRecord("control", modern)
        assertArrayEquals(WarmupControlCodec.encode(legacy), wire.value())
        assertEquals(legacy, WarmupControlCodec.decode(wire.value()))
        assertEquals(modern, WarmupControlCodec.decode(wire.value(), wire.headers()))
    }

    @Test
    fun `v5 envelope is rejected before dispatch`() {
        val encoded = WarmupControlCodec.encode(
            WarmupRequested(namespace, session, recipient, listOf(mapping), 2_000, 10, fingerprint, 10_000)
        )
        encoded[5] = 5

        assertThrows(IllegalArgumentException::class.java) { WarmupControlCodec.decode(encoded) }
    }
}
