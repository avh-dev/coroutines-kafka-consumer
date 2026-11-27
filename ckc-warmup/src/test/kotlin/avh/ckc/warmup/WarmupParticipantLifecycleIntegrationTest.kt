package avh.ckc.warmup

import avh.ckc.warmup.protocol.SessionRejected
import avh.ckc.warmup.protocol.WarmupLivenessProbe
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.testcontainers.junit.jupiter.Testcontainers

@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
internal class WarmupParticipantLifecycleIntegrationTest : WarmupContextKafkaFixture() {
    @Test
    fun `closed donor session cannot answer probes after loss of participant rejection`() = runBlocking {
        val source = unique("closed-donor-source")
        val warmup = unique("closed-donor-warmup")
        val otherSource = unique("surviving-donor-source")
        val otherWarmup = unique("surviving-donor-warmup")
        val control = unique("closed-donor-control")
        val namespace = unique("closed-donor")
        createTopics(source, warmup, otherSource, otherWarmup, control)
        val controlAvailable = AtomicBoolean(true)
        val firstReply = CompletableDeferred<Unit>()
        val rejected = CompletableDeferred<Unit>()
        val probeAfterRecovery = CompletableDeferred<Unit>()
        val donorContext = faultInjectionContext(namespace, control, send = { message ->
            when {
                message is SessionRejected && message.reasonCode == "donor_participant_unavailable" -> {
                    assertFalse(controlAvailable.get())
                    rejected.complete(Unit)
                    mock()
                }
                message is WarmupLivenessProbe && message.reply -> {
                    if (controlAvailable.get()) send(message).also { firstReply.complete(Unit) } else mock()
                }
                else -> send(message)
            }
        })
        val donor = stringConsumer(
            donorContext, "records", source, warmup, unique("closed-donor-group"), CopyOnWriteArrayList()
        )
        val otherHandled = CopyOnWriteArrayList<String>()
        val other = stringConsumer(
            donorContext, "other", otherSource, otherWarmup, unique("surviving-donor-group"), otherHandled
        )
        val mainReady = CompletableDeferred<TrafficWarmupLifecycleEvent>()
        val recipientContext = faultInjectionContext(namespace, control, configure = {
            warmupDuration = 30.seconds
            donorLivenessTimeout = 6.seconds
            onLifecycleEvent { event ->
                if (event.phase == TrafficWarmupLifecyclePhase.MAIN_READY) mainReady.complete(event)
            }
        }, send = { message ->
            if (message is WarmupLivenessProbe && !controlAvailable.get()) mock()
            else send(message).also {
                if (message is WarmupLivenessProbe && rejected.isCompleted) probeAfterRecovery.complete(Unit)
            }
        })
        val lifecycle = recipientContext.register(ControllableRecipient(source, warmup, kafkaConnection()))
        try {
            donor.start()
            other.start()
            withTimeout(15.seconds) {
                donor.awaitReady()
                other.awaitReady()
            }
            lifecycle.start()
            withTimeout(15.seconds) { firstReply.await() }

            controlAvailable.set(false)
            donor.stop()
            withTimeout(2.seconds) { rejected.await() }
            controlAvailable.set(true)
            withTimeout(5.seconds) { probeAfterRecovery.await() }

            val ready = withTimeout(10.seconds) { mainReady.await() }
            assertEquals("donor_liveness_timeout", ready.reasonCode)
            produce(otherSource, "alive", "alive", StringSerializer())
            awaitFor { otherHandled.contains("alive") }
        } finally {
            controlAvailable.set(true)
            try {
                lifecycle.stop()
            } finally {
                try {
                    donor.stop()
                } finally {
                    other.stop()
                }
            }
        }
    }

    @Test
    fun `stopping one participant during warmup rolls back and starts requested neighbors cold`() = runBlocking {
        val topics = createTypedTopics()
        val namespace = unique("stop-during-warmup")
        val donor = typedApplication(topics, namespace)
        val warming = CompletableDeferred<Unit>()
        val recipient = typedApplication(topics, namespace) { event ->
            if (event.phase == TrafficWarmupLifecyclePhase.WARMING) warming.complete(Unit)
        }
        try {
            donor.startAll()
            donor.awaitAllReady()
            recipient.startAll()
            withTimeout(10.seconds) { warming.await() }

            recipient.order.stop()
            withTimeout(15.seconds) {
                recipient.batch.awaitReady()
                recipient.telemetry.awaitReady()
            }
            assertTrue(runCatching { recipient.order.awaitReady() }.exceptionOrNull() is CancellationException)
        } finally {
            stopApplications(recipient, donor)
        }
    }

    @Test
    fun `participant stop does not await a cold started neighbor main readiness`() = runBlocking {
        val firstSource = unique("independent-stop-source-one")
        val secondSource = unique("independent-stop-source-two")
        val firstWarmup = unique("independent-stop-warmup-one")
        val secondWarmup = unique("independent-stop-warmup-two")
        val control = unique("independent-stop-control")
        createTopics(firstSource, secondSource, firstWarmup, secondWarmup, control)
        val namespace = unique("independent-stop")
        val donorContext = context(namespace, control)
        val donorFirst = stringConsumer(
            donorContext, "first", firstSource, firstWarmup, unique("donor-first"), CopyOnWriteArrayList()
        )
        val donorSecond = stringConsumer(
            donorContext, "second", secondSource, secondWarmup, unique("donor-second"), CopyOnWriteArrayList()
        )
        val warming = CompletableDeferred<Unit>()
        val recipientContext = context(
            namespace,
            control,
            lifecycleObserver = { event ->
                if (event.phase == TrafficWarmupLifecyclePhase.WARMING) warming.complete(Unit)
            }
        )
        val recipientFirst = stringConsumer(
            recipientContext,
            "first",
            firstSource,
            firstWarmup,
            unique("recipient-first"),
            CopyOnWriteArrayList()
        )
        val unavailableMainProperties = consumerProperties(
            unique("recipient-second"), StringDeserializer::class.java
        ) + mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "127.0.0.1:1",
            ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG to 50,
            ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG to 100
        )
        val recipientSecond = stringConsumer(
            recipientContext,
            "second",
            secondSource,
            secondWarmup,
            unique("unused-recipient-second-group"),
            CopyOnWriteArrayList(),
            unavailableMainProperties,
            30.seconds
        )
        val secondReadiness = async { runCatching { recipientSecond.awaitReady() } }
        try {
            donorFirst.start()
            donorSecond.start()
            withTimeout(10.seconds) {
                donorFirst.awaitReady()
                donorSecond.awaitReady()
            }
            recipientFirst.start()
            recipientSecond.start()
            withTimeout(10.seconds) { warming.await() }

            withTimeout(15.seconds) { recipientFirst.stop() }

            val earlyReadiness = if (secondReadiness.isCompleted) secondReadiness.await() else null
            assertFalse(secondReadiness.isCompleted, "Neighbor readiness completed early: $earlyReadiness")
        } finally {
            val cleanupFailures = listOf(recipientSecond, recipientFirst, donorSecond, donorFirst)
                .mapNotNull { consumer ->
                    runCatching { withTimeout(10.seconds) { consumer.stop() } }.exceptionOrNull()
                }
                .toMutableList()
            runCatching { withTimeout(10.seconds) { secondReadiness.await() } }
                .exceptionOrNull()
                ?.let(cleanupFailures::add)
            if (cleanupFailures.isNotEmpty()) {
                throw IllegalStateException("Independent-stop cleanup failed").apply {
                    cleanupFailures.forEach(::addSuppressed)
                }
            }
        }
    }
}
