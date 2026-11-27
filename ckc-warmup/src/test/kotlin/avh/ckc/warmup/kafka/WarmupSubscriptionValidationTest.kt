package avh.ckc.warmup.kafka

import avh.ckc.warmup.protocol.WarmupParticipantId
import avh.ckc.warmup.protocol.WarmupTopicMapping
import avh.ckc.warmup.runtime.RegisteredTrafficWarmupParticipant
import java.util.concurrent.ExecutionException
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.apache.kafka.common.KafkaException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mockingDetails
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.slf4j.Logger
import org.slf4j.helpers.MessageFormatter

class WarmupSubscriptionValidationTest {
    @Test
    fun `cluster lookup warning identifies the route without logging Kafka exception details`() = runBlocking {
        val senderId = WarmupParticipantId("sender")
        val subscriberId = WarmupParticipantId("subscriber")
        val sender = mock<RegisteredTrafficWarmupParticipant>()
        whenever(sender.participantId).thenReturn(senderId)
        whenever(sender.mainTopics).thenReturn(listOf("orders"))
        whenever(sender.mappings).thenReturn(listOf(WarmupTopicMapping(senderId, "orders", "batches", "mapping")))
        val subscriber = mock<RegisteredTrafficWarmupParticipant>()
        whenever(subscriber.participantId).thenReturn(subscriberId)
        whenever(subscriber.mainTopics).thenReturn(listOf("batches"))
        val marker = "fake-auth-provider-config"
        val logger = mock<Logger>()
        val validation = WarmupSubscriptionValidation(
            2.seconds, Dispatchers.IO,
            adminFactory = { throw ExecutionException(KafkaException(marker)) },
            log = logger
        )

        assertEquals("warmup_cluster_identity_unavailable", validation.failureReason(listOf(sender, subscriber)))

        val warning = mockingDetails(logger).invocations.single()
        assertEquals("warn", warning.method.name)
        val formatted = MessageFormatter.arrayFormat(
            warning.arguments.first() as String, warning.arguments.drop(1).toTypedArray()
        )
        assertTrue(formatted.message.contains("warmup_cluster_identity_unavailable"))
        assertTrue(formatted.message.contains("participant 'sender', mapping 'orders' -> 'batches'"))
        assertTrue(formatted.message.contains("main subscription 'subscriber' to 'batches'"))
        assertTrue(formatted.message.contains("cause_type=KafkaException"))
        assertFalse(formatted.message.contains(marker))
        assertNull(formatted.throwable)
    }
}
