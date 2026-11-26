package avh.ckc.warmup

import avh.ckc.core.consumerContainer
import avh.ckc.warmup.protocol.WarmupLivenessProbe
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Testcontainers

@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
internal class WarmupContextStartupIntegrationTest : WarmupContextKafkaFixture() {
    @Test
    fun `one container cold starts participants from independent warmup contexts`() = runBlocking {
        val firstSource = unique("container-source-first")
        val firstWarmup = unique("container-warmup-first")
        val firstControl = unique("container-control-first")
        val secondSource = unique("container-source-second")
        val secondWarmup = unique("container-warmup-second")
        val secondControl = unique("container-control-second")
        createTopics(
            firstSource,
            firstWarmup,
            firstControl,
            secondSource,
            secondWarmup,
            secondControl
        )
        val firstHandled = CopyOnWriteArrayList<String>()
        val secondHandled = CopyOnWriteArrayList<String>()
        val first = stringConsumer(
            context(unique("container-first"), firstControl),
            "first",
            firstSource,
            firstWarmup,
            unique("container-first-group"),
            firstHandled
        )
        val second = stringConsumer(
            context(unique("container-second"), secondControl),
            "second",
            secondSource,
            secondWarmup,
            unique("container-second-group"),
            secondHandled
        )
        val container = consumerContainer(first, second)
        try {
            container.start()
            withTimeout(15.seconds) { container.awaitReady() }
            produce(firstSource, "first", "first-value", StringSerializer())
            produce(secondSource, "second", "second-value", StringSerializer())
            awaitFor {
                firstHandled.contains("first-value") && secondHandled.contains("second-value")
            }
        } finally {
            container.stop()
        }
    }

    @Test
    @Tag("without-micrometer")
    fun `public context warms three typed participants through one logical session`() = runBlocking {
        val topics = createTypedTopics()
        val namespace = unique("public-v6")
        val firstReply = CompletableDeferred<Unit>()
        val donorContext = faultInjectionContext(namespace, topics.control, send = { message ->
            send(message).also {
                if (message is WarmupLivenessProbe && message.reply) firstReply.complete(Unit)
            }
        })
        val donor = typedApplication(topics, namespace, applicationContext = donorContext)
        val lifecycle = CopyOnWriteArrayList<TrafficWarmupLifecycleEvent>()
        val recipient = typedApplication(topics, namespace, warmupDuration = 10.seconds) { event -> lifecycle += event }
        try {
            donor.startAll()
            donor.awaitAllReady()

            recipient.startAll()
            // Start the burst after the donor has activated the session and leave time for all three lanes.
            withTimeout(10.seconds) { firstReply.await() }
            produceMany(topics.orders, 300, StringSerializer()) { "order-$it" }
            produceMany(topics.batches, 300, ByteArraySerializer()) { byteArrayOf((it % 127).toByte()) }
            produceMany(topics.telemetry, 300, LongSerializer()) { it.toLong() }

            recipient.awaitAllReady()
            try {
                awaitFor {
                    recipient.ordersHandled.size == 300 &&
                        recipient.batchesHandled.size == 300 &&
                        recipient.telemetryHandled.size == 300
                }
            } catch (timeout: kotlinx.coroutines.TimeoutCancellationException) {
                throw AssertionError(
                    "recipient counts=${recipient.handledCounts()}, donor counts=${donor.handledCounts()}, " +
                        "lifecycle=$lifecycle", timeout
                )
            }

            assertTrue(donor.ordersHandled.isEmpty())
            assertTrue(donor.batchesHandled.isEmpty())
            assertTrue(donor.telemetryHandled.isEmpty())
            assertEquals(300, donor.orderMetrics.processed.count { it.processedRemotely })
            assertEquals(300, donor.batchMetrics.processed.count { it.processedRemotely })
            assertEquals(300, donor.telemetryMetrics.processed.count { it.processedRemotely })
        } finally {
            stopApplications(recipient, donor)
        }
    }

    @Test
    fun `missing participant times out while requested consumer starts cold and late start is cold`() = runBlocking {
        val sourceOne = unique("source-one")
        val sourceTwo = unique("source-two")
        val warmupOne = unique("warmup-one")
        val warmupTwo = unique("warmup-two")
        val control = unique("control")
        createTopics(sourceOne, sourceTwo, warmupOne, warmupTwo, control)
        val context = context(unique("late-start"), control, participantStartTimeout = 250.milliseconds)
        val firstHandled = CopyOnWriteArrayList<String>()
        val secondHandled = CopyOnWriteArrayList<String>()
        val first = stringConsumer(context, "first", sourceOne, warmupOne, unique("first-group"), firstHandled)
        val second = stringConsumer(context, "second", sourceTwo, warmupTwo, unique("second-group"), secondHandled)
        try {
            first.start()
            withTimeout(10.seconds) { first.awaitReady() }
            produce(sourceOne, "key", "first", StringSerializer())
            awaitFor { firstHandled.contains("first") }

            second.start()
            withTimeout(10.seconds) { second.awaitReady() }
            produce(sourceTwo, "key", "second", StringSerializer())
            awaitFor { secondHandled.contains("second") }

            assertEquals(listOf("first"), firstHandled)
            assertEquals(listOf("second"), secondHandled)
        } finally {
            second.stop()
            first.stop()
        }
    }

    @Test
    fun `first start freezes context registration`() = runBlocking {
        val sourceOne = unique("freeze-source-one")
        val sourceTwo = unique("freeze-source-two")
        val warmupOne = unique("freeze-warmup-one")
        val warmupTwo = unique("freeze-warmup-two")
        val control = unique("freeze-control")
        createTopics(sourceOne, sourceTwo, warmupOne, warmupTwo, control)
        val context = context(unique("freeze"), control)
        val first = stringConsumer(
            context, "first", sourceOne, warmupOne, unique("freeze-first-group"), CopyOnWriteArrayList()
        )
        try {
            first.start()
            assertThrows(IllegalStateException::class.java) {
                stringConsumer(
                    context, "late", sourceTwo, warmupTwo, unique("freeze-late-group"), CopyOnWriteArrayList()
                )
            }
            withTimeout(10.seconds) { first.awaitReady() }
        } finally {
            first.stop()
        }
    }

    @Test
    fun `invalid metadata for one public mapping cold starts the whole context`() = runBlocking {
        val topics = createTypedTopics(invalidTelemetryDestination = true)
        val namespace = unique("invalid-public-metadata")
        val donor = typedApplication(topics, namespace)
        val fallback = CompletableDeferred<String?>()
        val recipient = typedApplication(topics, namespace) { event ->
            if (event.phase == TrafficWarmupLifecyclePhase.COLD_FALLBACK) fallback.complete(event.reasonCode)
        }
        try {
            donor.startAll()
            donor.awaitAllReady()
            recipient.startAll()
            recipient.awaitAllReady()

            assertEquals("metadata_lookup_failed", withTimeout(10.seconds) { fallback.await() })
        } finally {
            stopApplications(recipient, donor)
        }
    }

    @Test
    fun `one public donor context concurrently warms two recipient contexts`() = runBlocking {
        val source = unique("multi-recipient-source")
        val warmup = unique("multi-recipient-warmup")
        val control = unique("multi-recipient-control")
        createTopics(source, warmup, control)
        val namespace = unique("multi-recipient")
        val group = unique("multi-recipient-group")
        val forwarded = CopyOnWriteArrayList<TrafficWarmupForwardedEvent>()
        val firstHandled = CopyOnWriteArrayList<String>()
        val secondHandled = CopyOnWriteArrayList<String>()
        val firstWarming = CompletableDeferred<String>()
        val secondWarming = CompletableDeferred<String>()
        val donor = stringConsumer(
            context(namespace, control), "records", source, warmup, group, CopyOnWriteArrayList(),
            forwarded = forwarded
        )
        val first = stringConsumer(
            context(namespace, control, warmupDuration = 6.seconds, lifecycleObserver = { event ->
                if (event.phase == TrafficWarmupLifecyclePhase.WARMING) {
                    firstWarming.complete(checkNotNull(event.sessionId))
                }
            }),
            "records", source, warmup, group, firstHandled
        )
        val second = stringConsumer(
            context(namespace, control, warmupDuration = 6.seconds, lifecycleObserver = { event ->
                if (event.phase == TrafficWarmupLifecyclePhase.WARMING) {
                    secondWarming.complete(checkNotNull(event.sessionId))
                }
            }),
            "records", source, warmup, group, secondHandled
        )
        try {
            donor.start()
            withTimeout(10.seconds) { donor.awaitReady() }
            listOf(launch { first.start() }, launch { second.start() }).joinAll()
            val recipientSessions = withTimeout(10.seconds) {
                setOf(firstWarming.await(), secondWarming.await())
            }
            assertEquals(2, recipientSessions.size)
            // Keep sending across the phase boundaries and verify the donor serves both live sessions.
            withTimeout(4.seconds) {
                var index = 0
                while (forwarded.map { it.sessionId }.toSet() != recipientSessions) {
                    produce(source, "key-$index", "value-$index", StringSerializer())
                    index++
                    delay(125.milliseconds)
                }
            }
            awaitFor { firstHandled.isNotEmpty() && secondHandled.isNotEmpty() }
            withTimeout(15.seconds) {
                first.awaitReady()
                second.awaitReady()
            }
        } finally {
            val failures = listOf(second, first, donor).mapNotNull { consumer ->
                runCatching { consumer.stop() }.exceptionOrNull()
            }
            if (failures.isNotEmpty()) {
                throw IllegalStateException("Multi-recipient cleanup failed").apply {
                    failures.forEach(::addSuppressed)
                }
            }
        }
    }
}
