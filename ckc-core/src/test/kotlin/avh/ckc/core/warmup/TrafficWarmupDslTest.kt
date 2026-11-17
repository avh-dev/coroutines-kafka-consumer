package avh.ckc.core.warmup

import org.apache.kafka.clients.CommonClientConfigs
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Test

class TrafficWarmupDslTest {
    @Test
    fun `mapping quota defaults to quadratic ramp and accepts linear selection`() {
        val configuration = TrafficWarmupConsumerConfiguration().apply {
            mapTopic("orders", "warmup-orders")
            mapTopic("batch", "warmup-batch") {
                quotaRampCurve = QuotaRampCurve.LINEAR
            }
        }

        assertEquals(QuotaRampCurve.QUADRATIC, configuration.mappings.getValue("orders").quota.quotaRampCurve)
        assertEquals(QuotaRampCurve.LINEAR, configuration.mappings.getValue("batch").quota.quotaRampCurve)
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
    fun `mapping fingerprint includes the selected quota ramp curve`() {
        val participant = WarmupParticipantId("orders")
        val connection = mapOf(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to "broker:9092")
        val quadratic = TrafficWarmupConsumerConfiguration.Mapping(
            "orders",
            "warmup-orders",
            WarmupMappingQuota(100)
        )
        val linear = quadratic.copy(
            quota = quadratic.quota.copy(quotaRampCurve = QuotaRampCurve.LINEAR)
        )

        assertNotEquals(
            mappingFingerprint(participant, quadratic, connection),
            mappingFingerprint(participant, linear, connection)
        )
    }
}
