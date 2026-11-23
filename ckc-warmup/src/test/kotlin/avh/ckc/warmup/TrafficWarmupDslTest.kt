package avh.ckc.warmup

import avh.ckc.warmup.protocol.WarmupMappingQuota
import avh.ckc.warmup.protocol.WarmupParticipantId
import avh.ckc.warmup.protocol.WarmupQuotaPhase
import avh.ckc.warmup.protocol.WarmupQuotaProfile
import avh.ckc.warmup.protocol.WarmupQuotaProfileParser
import org.apache.kafka.clients.CommonClientConfigs
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class TrafficWarmupDslTest {
    @Test
    fun `mapping quota profile is parsed while the DSL configuration is built`() {
        val configuration = TrafficWarmupConsumerConfiguration<String, String>().apply {
            mapTopic("orders", "warmup-orders") {
                quotaProfile = "5 -> (0s, step) -> 50"
            }
        }

        assertEquals(
            WarmupQuotaProfile(5, listOf(WarmupQuotaPhase(0, 50))),
            configuration.mappings.getValue("orders").quota.profile
        )
        assertThrows(IllegalArgumentException::class.java) {
            configuration.mapTopic("batch", "warmup-batch") { quotaProfile = "invalid" }
        }
    }

    @Test
    fun `mapping fingerprint identifies destination cluster independent of broker order`() {
        val participant = WarmupParticipantId("orders")
        val mapping = TrafficWarmupConsumerConfiguration.Mapping(
            "orders",
            "warmup-orders",
            WarmupMappingQuota(100)
        )

        val first = mappingFingerprint(
            participant,
            mapping,
            mapOf(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to "broker-b:9092,broker-a:9092")
        )
        val reordered = mappingFingerprint(
            participant,
            mapping,
            mapOf(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to listOf("broker-a:9092", "broker-b:9092"))
        )
        val anotherCluster = mappingFingerprint(
            participant,
            mapping,
            mapOf(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to "broker-c:9092")
        )

        assertEquals(first, reordered)
        assertNotEquals(first, anotherCluster)
    }

    @Test
    fun `mapping fingerprint uses normalized behavior rather than formatting or labels`() {
        val participant = WarmupParticipantId("orders")
        val connection = mapOf(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to "broker:9092")
        fun mapping(profile: String) = TrafficWarmupConsumerConfiguration.Mapping(
            "orders",
            "warmup-orders",
            WarmupMappingQuota(WarmupQuotaProfileParser.parse(profile))
        )

        val first = mapping("5 -> (10s, first) -> 100")
        val equivalent = mapping(" 5->(10S, ignored)->100 ")
        val different = mapping("5 -> (9s, first) -> 100")

        assertEquals(
            mappingFingerprint(participant, first, connection),
            mappingFingerprint(participant, equivalent, connection)
        )
        assertNotEquals(
            mappingFingerprint(participant, first, connection),
            mappingFingerprint(participant, different, connection)
        )
    }
}
