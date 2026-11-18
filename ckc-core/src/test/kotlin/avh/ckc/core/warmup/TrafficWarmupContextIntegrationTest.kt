package avh.ckc.core.warmup

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.ConsumerContainer
import avh.ckc.core.RecordingMetrics
import avh.ckc.core.consumerContainer
import avh.ckc.core.coroutinesKafkaConsumer
import avh.ckc.core.metrics.ConsumerMetrics
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.LongDeserializer
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.net.ServerSocket
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class TrafficWarmupContextIntegrationTest {
    @Test
    fun `new recipient warms after complete warmup kafka data loss without restarting donor`() = runBlocking {
        val source = unique("transport-loss-source")
        val warmup = unique("transport-loss-warmup")
        val control = unique("transport-loss-control")
        createTopics(source)
        val warmupKafka = ReplaceableWarmupKafka()
        warmupKafka.start()
        createTopicsAt(warmupKafka.connection(), warmup, control)
        val namespace = unique("transport-loss")
        val group = unique("transport-loss-group")
        val donorHandled = CopyOnWriteArrayList<String>()
        val forwarded = CopyOnWriteArrayList<TrafficWarmupForwardedEvent>()
        val donorMetrics = RecordingMetrics<String, String>()
        val firstHandled = CopyOnWriteArrayList<String>()
        val secondHandled = CopyOnWriteArrayList<String>()
        val replacementHandled = CopyOnWriteArrayList<String>()
        val firstWarming = CompletableDeferred<Unit>()
        val secondWarming = CompletableDeferred<Unit>()
        val replacementWarming = CompletableDeferred<Unit>()
        val replacementEvents = CopyOnWriteArrayList<TrafficWarmupLifecycleEvent>()
        val firstFallback = CompletableDeferred<Unit>()
        val secondFallback = CompletableDeferred<Unit>()
        val releaseRemoteHandlers = CompletableDeferred<Unit>()
        val remoteHandlersStarted = CopyOnWriteArrayList<String>()
        val controlConnection = warmupKafka.connection()
        val donorContext = recoveryContext(namespace, control, controlConnection)
        val donor = stringConsumer(
            donorContext,
            "records",
            source,
            warmup,
            group,
            donorHandled,
            warmupProperties = controlConnection,
            forwarded = forwarded,
            consumerMetrics = donorMetrics
        )
        val first = stringConsumer(
            recoveryContext(namespace, control, controlConnection) { event ->
                when (event.phase) {
                    TrafficWarmupLifecyclePhase.WARMING -> firstWarming.complete(Unit)
                    TrafficWarmupLifecyclePhase.COLD_FALLBACK -> firstFallback.complete(Unit)
                    else -> Unit
                }
            },
            "records",
            source,
            warmup,
            group,
            firstHandled,
            warmupProperties = controlConnection,
            beforeHandle = { value ->
                remoteHandlersStarted += value
                releaseRemoteHandlers.await()
            }
        )
        val second = stringConsumer(
            recoveryContext(namespace, control, controlConnection) { event ->
                when (event.phase) {
                    TrafficWarmupLifecyclePhase.WARMING -> secondWarming.complete(Unit)
                    TrafficWarmupLifecyclePhase.COLD_FALLBACK -> secondFallback.complete(Unit)
                    else -> Unit
                }
            },
            "records",
            source,
            warmup,
            group,
            secondHandled,
            warmupProperties = controlConnection,
            beforeHandle = { value ->
                remoteHandlersStarted += value
                releaseRemoteHandlers.await()
            }
        )
        var replacement: CoroutinesKafkaConsumer<String, String>? = null
        val replacementMetrics = RecordingMetrics<String, String>()
        try {
            donor.start()
            withTimeout(10.seconds) { donor.awaitReady() }
            first.start()
            second.start()
            withTimeout(10.seconds) {
                firstWarming.await()
                secondWarming.await()
            }
            repeat(2) { index -> produce(source, "lost-$index", "lost-$index", StringSerializer()) }
            awaitFor { remoteHandlersStarted.size == 2 }

            warmupKafka.destroyData()
            releaseRemoteHandlers.complete(Unit)
            withTimeout(15.seconds) {
                first.awaitReady()
                second.awaitReady()
                firstFallback.await()
                secondFallback.await()
            }
            awaitFor { donorHandled.containsAll(listOf("lost-0", "lost-1")) }
            first.stop()
            second.stop()
            awaitFor { donor.currentWarmupSourceOwnership(source) != null }

            warmupKafka.startEmptyReplacement()
            createTopicsAt(warmupKafka.connection(), warmup, control)
            // The donor transport confirms the new topic identity before replacing stale control
            // clients; a new recipient starts after that bounded recovery window.
            kotlinx.coroutines.delay(5.seconds)
            val recoveredConnection = warmupKafka.connection()
            replacement = stringConsumer(
                recoveryContext(namespace, control, recoveredConnection) { event ->
                    replacementEvents += event
                    if (event.phase == TrafficWarmupLifecyclePhase.WARMING) replacementWarming.complete(Unit)
                },
                "records",
                source,
                warmup,
                group,
                replacementHandled,
                warmupProperties = recoveredConnection,
                consumerMetrics = replacementMetrics
            )
            replacement.start()
            val replacementReadiness = async { runCatching { replacement.awaitReady() } }
            withTimeout(20.seconds) {
                while (!replacementWarming.isCompleted && !replacementReadiness.isCompleted) {
                    kotlinx.coroutines.delay(20)
                }
            }
            assertTrue(
                replacementWarming.isCompleted,
                "Recovered recipient cold-started instead of warming: " +
                    "events=$replacementEvents, readinessCompleted=${replacementReadiness.isCompleted}"
            )
            val recoveredSession = requireNotNull(
                replacementEvents.last { it.phase == TrafficWarmupLifecyclePhase.WARMING }.sessionId
            )
            // WARMING means RecipientReady is durably published, while donor activation follows
            // asynchronously in its poll loop. Continue real traffic until the public forwarding
            // callback proves that this exact recovered session became active.
            KafkaProducer(kafkaConnection(), StringSerializer(), StringSerializer()).use { producer ->
                withTimeout(3.seconds) {
                    var index = 0
                    while (forwarded.none { it.sessionId == recoveredSession }) {
                        val value = "recovered-${index++}"
                        producer.send(ProducerRecord(source, value, value)).get(2, TimeUnit.SECONDS)
                        kotlinx.coroutines.delay(50.milliseconds)
                    }
                }
            }
            withTimeout(10.seconds) {
                while (replacementHandled.isEmpty()) kotlinx.coroutines.delay(20)
            }
            assertTrue(
                replacementHandled.isNotEmpty(),
                "Recovered session never delegated to the new recipient; events=$replacementEvents, " +
                    "forwarded=$forwarded, donor=$donorHandled"
            )
            withTimeout(15.seconds) { replacementReadiness.await().getOrThrow() }

            assertFalse(donor.stateSnapshot().failed)
            assertTrue(replacementHandled.all { it.startsWith("recovered-") })
            assertTrue(
                donorMetrics.processed.any { it.processedRemotely && it.value in replacementHandled },
                "Donor did not report the recovered remote success"
            )
            assertTrue(replacementMetrics.processed.all { !it.processedRemotely })
            assertTrue(
                forwarded.filter { it.sourceOffset >= 2 }.all { it.sessionId == recoveredSession },
                "An old session resumed after transport recreation: $forwarded"
            )
            assertEquals(0, donorMetrics.warmupPending.lastOrNull())
        } finally {
            releaseRemoteHandlers.complete(Unit)
            val cleanupFailures = listOfNotNull(replacement, second, first, donor).mapNotNull { consumer ->
                runCatching { withTimeout(10.seconds) { consumer.stop() } }.exceptionOrNull()
            }.toMutableList()
            runCatching { warmupKafka.stop() }.exceptionOrNull()?.let(cleanupFailures::add)
            if (cleanupFailures.isNotEmpty()) {
                throw IllegalStateException("Transport-recovery cleanup failed").apply {
                    cleanupFailures.forEach(::addSuppressed)
                }
            }
        }
    }

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
    fun `public context warms three typed participants through one logical session`() = runBlocking {
        val topics = TypedTopics.create()
        val namespace = unique("public-v6")
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
            produce(topics.orders, "order", "one", StringSerializer())
            produce(topics.batches, "batch", byteArrayOf(1, 2, 3), ByteArraySerializer())
            produce(topics.telemetry, "telemetry", 42L, LongSerializer())

            recipient.awaitAllReady()
            awaitFor {
                recipient.ordersHandled.contains("one") &&
                    recipient.batchesHandled.any { it.contentEquals(byteArrayOf(1, 2, 3)) } &&
                    recipient.telemetryHandled.contains(42L)
            }

            assertTrue(donor.ordersHandled.isEmpty())
            assertTrue(donor.batchesHandled.isEmpty())
            assertTrue(donor.telemetryHandled.isEmpty())
            assertEquals(1, donor.orderMetrics.processed.count { it.processedRemotely })
            assertEquals(1, donor.batchMetrics.processed.count { it.processedRemotely })
            assertEquals(1, donor.telemetryMetrics.processed.count { it.processedRemotely })
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
    fun `stopping one participant during warmup rolls back and starts requested neighbors cold`() = runBlocking {
        val topics = TypedTopics.create()
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
        val topics = TypedTopics.create(invalidTelemetryDestination = true)
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
        val donorHandled = CopyOnWriteArrayList<String>()
        val firstHandled = CopyOnWriteArrayList<String>()
        val secondHandled = CopyOnWriteArrayList<String>()
        val firstWarming = CompletableDeferred<Unit>()
        val secondWarming = CompletableDeferred<Unit>()
        val donor = stringConsumer(context(namespace, control), "records", source, warmup, group, donorHandled)
        val first = stringConsumer(
            context(namespace, control, lifecycleObserver = { event ->
                if (event.phase == TrafficWarmupLifecyclePhase.WARMING) firstWarming.complete(Unit)
            }),
            "records", source, warmup, group, firstHandled
        )
        val second = stringConsumer(
            context(namespace, control, lifecycleObserver = { event ->
                if (event.phase == TrafficWarmupLifecyclePhase.WARMING) secondWarming.complete(Unit)
            }),
            "records", source, warmup, group, secondHandled
        )
        try {
            donor.start()
            withTimeout(10.seconds) { donor.awaitReady() }
            listOf(launch { first.start() }, launch { second.start() }).joinAll()
            withTimeout(10.seconds) {
                firstWarming.await()
                secondWarming.await()
            }
            repeat(12) { index -> produce(source, "key-$index", "value-$index", StringSerializer()) }
            withTimeout(15.seconds) {
                first.awaitReady()
                second.awaitReady()
            }
            awaitFor { firstHandled.isNotEmpty() && secondHandled.isNotEmpty() }
            assertTrue(donorHandled.isEmpty())
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

    @Test
    fun `one public participant creates independent metric bindings for two mappings`() = runBlocking {
        val firstSource = unique("binding-source-one")
        val secondSource = unique("binding-source-two")
        val firstWarmup = unique("binding-warmup-one")
        val secondWarmup = unique("binding-warmup-two")
        val control = unique("binding-control")
        createTopics(firstSource, secondSource, firstWarmup, secondWarmup, control)
        val namespace = unique("binding-namespace")
        val group = unique("binding-group")
        val donor = multiTopicStringConsumer(
            context(namespace, control), group,
            firstSource, firstWarmup, secondSource, secondWarmup
        )
        val recipientMetrics = RuntimeBindingMetrics()
        val recipient = multiTopicStringConsumer(
            context(namespace, control), group,
            firstSource, firstWarmup, secondSource, secondWarmup,
            recipientMetrics
        )
        try {
            donor.start()
            withTimeout(10.seconds) { donor.awaitReady() }
            recipient.start()
            withTimeout(15.seconds) { recipient.awaitReady() }

            assertEquals(2, recipientMetrics.bindingIds.size)
            assertTrue(recipientMetrics.bindingIds.all { it.startsWith("warmup-") })
        } finally {
            recipient.stop()
            donor.stop()
        }
    }

    private suspend fun stopApplications(vararg applications: TypedApplication) {
        val failures = applications.mapNotNull { application ->
            runCatching { application.stopAll() }.exceptionOrNull()
        }
        if (failures.isNotEmpty()) {
            throw IllegalStateException("Application cleanup had ${failures.size} failure(s)").apply {
                failures.forEach(::addSuppressed)
            }
        }
    }

    private fun typedApplication(
        topics: TypedTopics,
        namespace: String,
        lifecycleObserver: (TrafficWarmupLifecycleEvent) -> Unit = {}
    ): TypedApplication {
        val context = context(namespace, topics.control, lifecycleObserver = lifecycleObserver)
        val orderMetrics = RecordingMetrics<String, String>()
        val batchMetrics = RecordingMetrics<String, ByteArray>()
        val telemetryMetrics = RecordingMetrics<String, Long>()
        val ordersHandled = CopyOnWriteArrayList<String>()
        val batchesHandled = CopyOnWriteArrayList<ByteArray>()
        val telemetryHandled = CopyOnWriteArrayList<Long>()
        val order = coroutinesKafkaConsumer<String, String>(
            consumerProperties("orders-group", StringDeserializer::class.java)
        ) {
            topics(topics.orders)
            metrics = orderMetrics
            commitIntervalMs = 50
            commitRecordsThreshold = 1
            handle { ordersHandled += it.value() }
            trafficWarmup(context, "orders") {
                mapTopic(topics.orders, topics.warmupOrders) { quotaProfile = "100" }
                warmupConsumerProperties = warmupConsumerConnection()
                forwardingProducerProperties = forwardingConnection(StringSerializer::class.java)
            }
        }
        val batch = coroutinesKafkaConsumer<String, ByteArray>(
            consumerProperties("batches-group", ByteArrayDeserializer::class.java)
        ) {
            topics(topics.batches)
            metrics = batchMetrics
            commitIntervalMs = 50
            commitRecordsThreshold = 1
            handle { batchesHandled += it.value() }
            trafficWarmup(context, "batches") {
                mapTopic(topics.batches, topics.warmupBatches) { quotaProfile = "100" }
                warmupConsumerProperties = warmupConsumerConnection()
                forwardingProducerProperties = forwardingConnection(ByteArraySerializer::class.java)
            }
        }
        val telemetry = coroutinesKafkaConsumer<String, Long>(
            consumerProperties("telemetry-group", LongDeserializer::class.java)
        ) {
            topics(topics.telemetry)
            metrics = telemetryMetrics
            commitIntervalMs = 50
            commitRecordsThreshold = 1
            handle { telemetryHandled += it.value() }
            trafficWarmup(context, "telemetry") {
                mapTopic(topics.telemetry, topics.warmupTelemetry) { quotaProfile = "100" }
                warmupConsumerProperties = warmupConsumerConnection()
                forwardingProducerProperties = forwardingConnection(LongSerializer::class.java)
            }
        }
        return TypedApplication(
            order, batch, telemetry,
            ordersHandled, batchesHandled, telemetryHandled,
            orderMetrics, batchMetrics, telemetryMetrics
        )
    }

    private fun stringConsumer(
        context: TrafficWarmupContext,
        participantId: String,
        sourceTopic: String,
        warmupTopic: String,
        groupId: String,
        handled: MutableList<String>,
        mainProperties: Map<String, Any?> = consumerProperties(groupId, StringDeserializer::class.java),
        mainRecoveryTimeout: kotlin.time.Duration? = null,
        warmupProperties: Map<String, Any?> = warmupConsumerConnection(),
        beforeHandle: suspend (String) -> Unit = {},
        forwarded: MutableList<TrafficWarmupForwardedEvent>? = null,
        consumerMetrics: ConsumerMetrics<String, String> = object : ConsumerMetrics<String, String> {}
    ): CoroutinesKafkaConsumer<String, String> = coroutinesKafkaConsumer(
        mainProperties
    ) {
        topics(sourceTopic)
        commitIntervalMs = 50
        commitRecordsThreshold = 1
        metrics = consumerMetrics
        mainRecoveryTimeout?.let { recoveryTimeout = it }
        handle {
            beforeHandle(it.value())
            handled += it.value()
        }
        trafficWarmup(context, participantId) {
            mapTopic(sourceTopic, warmupTopic)
            warmupConsumerProperties = warmupProperties
            forwardingProducerProperties = warmupProperties + mapOf(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
            )
            forwarded?.let { events -> onForwarded(events::add) }
        }
    }

    private fun recoveryContext(
        namespace: String,
        controlTopic: String,
        connection: Map<String, Any?>,
        lifecycleObserver: (TrafficWarmupLifecycleEvent) -> Unit = {}
    ) = trafficWarmupContext {
        this.namespace = namespace
        this.controlTopic = controlTopic
        controlProducerProperties = connection
        controlConsumerProperties = connection
        warmupDuration = 5.seconds
        offerWindow = 250.milliseconds
        participantStartTimeout = 1.seconds
        preparationTimeout = 1.seconds
        completionTimeout = 2.seconds
        delegationTimeout = 500.milliseconds
        maxDonorSessions = 2
        maxAggregateQuotaPerSecond = 1_000
        onLifecycleEvent(lifecycleObserver)
    }

    private fun multiTopicStringConsumer(
        context: TrafficWarmupContext,
        groupId: String,
        firstSource: String,
        firstWarmup: String,
        secondSource: String,
        secondWarmup: String,
        consumerMetrics: ConsumerMetrics<String, String> = object : ConsumerMetrics<String, String> {}
    ): CoroutinesKafkaConsumer<String, String> = coroutinesKafkaConsumer(
        consumerProperties(groupId, StringDeserializer::class.java)
    ) {
        topics(firstSource, secondSource)
        metrics = consumerMetrics
        commitIntervalMs = 50
        commitRecordsThreshold = 1
        handle { }
        trafficWarmup(context, "two-mappings") {
            mapTopic(firstSource, firstWarmup)
            mapTopic(secondSource, secondWarmup)
            warmupConsumerProperties = warmupConsumerConnection()
            forwardingProducerProperties = forwardingConnection(StringSerializer::class.java)
        }
    }

    private class RuntimeBindingMetrics : ConsumerMetrics<String, String> {
        val bindingIds = CopyOnWriteArrayList<String>()

        override fun runtimeBinding(bindingId: String): ConsumerMetrics<String, String> {
            bindingIds += bindingId
            return object : ConsumerMetrics<String, String> {}
        }
    }

    private fun context(
        namespace: String,
        controlTopic: String,
        participantStartTimeout: kotlin.time.Duration = 2.seconds,
        lifecycleObserver: (TrafficWarmupLifecycleEvent) -> Unit = {}
    ) = trafficWarmupContext {
        this.namespace = namespace
        this.controlTopic = controlTopic
        controlProducerProperties = kafkaConnection()
        controlConsumerProperties = kafkaConnection()
        warmupDuration = 750.milliseconds
        offerWindow = 250.milliseconds
        this.participantStartTimeout = participantStartTimeout
        preparationTimeout = 3.seconds
        completionTimeout = 10.seconds
        delegationTimeout = 3.seconds
        maxDonorSessions = 4
        maxAggregateQuotaPerSecond = 1_000
        onLifecycleEvent(lifecycleObserver)
    }

    private data class TypedApplication(
        val order: CoroutinesKafkaConsumer<String, String>,
        val batch: CoroutinesKafkaConsumer<String, ByteArray>,
        val telemetry: CoroutinesKafkaConsumer<String, Long>,
        val ordersHandled: CopyOnWriteArrayList<String>,
        val batchesHandled: CopyOnWriteArrayList<ByteArray>,
        val telemetryHandled: CopyOnWriteArrayList<Long>,
        val orderMetrics: RecordingMetrics<String, String>,
        val batchMetrics: RecordingMetrics<String, ByteArray>,
        val telemetryMetrics: RecordingMetrics<String, Long>
    ) {
        private val consumers: ConsumerContainer = consumerContainer(order, batch, telemetry)

        fun startAll() {
            consumers.start()
        }

        suspend fun awaitAllReady() {
            withTimeout(15.seconds) { consumers.awaitReady() }
        }

        suspend fun stopAll() = consumers.stop()
    }

    private data class TypedTopics(
        val orders: String,
        val batches: String,
        val telemetry: String,
        val warmupOrders: String,
        val warmupBatches: String,
        val warmupTelemetry: String,
        val control: String
    ) {
        companion object {
            fun create(invalidTelemetryDestination: Boolean = false): TypedTopics {
                val topics = TypedTopics(
                    unique("orders"), unique("batches"), unique("telemetry"),
                    unique("warmup-orders"), unique("warmup-batches"), unique("warmup-telemetry"),
                    unique("control")
                )
                createTopicsWithPartitions(
                    mapOf(
                        topics.orders to 1,
                        topics.batches to 1,
                        topics.telemetry to 1,
                        topics.warmupOrders to 1,
                        topics.warmupBatches to 1,
                        topics.warmupTelemetry to if (invalidTelemetryDestination) 2 else 1,
                        topics.control to 1
                    )
                )
                return topics
            }
        }
    }

    private fun consumerProperties(groupId: String, valueDeserializer: Class<*>): Map<String, Any?> =
        kafkaConnection() + mapOf(
            ConsumerConfig.GROUP_ID_CONFIG to groupId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to valueDeserializer
        )

    private fun warmupConsumerConnection(): Map<String, Any?> = kafkaConnection()

    private fun forwardingConnection(valueSerializer: Class<*>): Map<String, Any?> =
        kafkaConnection() + mapOf(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to valueSerializer
        )

    private fun kafkaConnection(): Map<String, Any?> =
        mapOf("bootstrap.servers" to kafka.bootstrapServers)

    private fun <V> produce(topic: String, key: String, value: V, serializer: org.apache.kafka.common.serialization.Serializer<V>) {
        KafkaProducer(
            kafkaConnection(),
            StringSerializer(),
            serializer
        ).use { producer -> producer.send(ProducerRecord(topic, key, value)).get(10, TimeUnit.SECONDS) }
    }

    private suspend fun awaitFor(assertion: () -> Boolean) {
        withTimeout(10.seconds) {
            while (!assertion()) kotlinx.coroutines.delay(20)
        }
    }

    companion object {
        @Container
        @JvmStatic
        val kafka = KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"))

        private fun unique(prefix: String): String = "$prefix-${UUID.randomUUID()}"

        private fun createTopics(vararg topics: String) {
            createTopicsWithPartitions(topics.associateWith { 1 })
        }

        private fun createTopicsWithPartitions(topics: Map<String, Int>) {
            AdminClient.create(mapOf("bootstrap.servers" to kafka.bootstrapServers)).use { admin ->
                admin.createTopics(topics.map { (topic, partitions) ->
                    NewTopic(topic, partitions, 1.toShort())
                }).all().get(10, TimeUnit.SECONDS)
            }
        }

        private fun createTopicsAt(connection: Map<String, Any?>, vararg topics: String) {
            AdminClient.create(connection).use { admin ->
                admin.createTopics(topics.map { topic ->
                    NewTopic(topic, 1, 1.toShort())
                }).all().get(10, TimeUnit.SECONDS)
            }
        }
    }
}

private class ReplaceableWarmupKafka {
    private val port = ServerSocket(0).use { it.localPort }
    private var kafka = newContainer()

    fun start() = kafka.start()

    fun connection(): Map<String, Any?> = mapOf("bootstrap.servers" to kafka.bootstrapServers)

    fun destroyData() {
        kafka.stop()
        kafka = newContainer()
    }

    fun startEmptyReplacement() {
        kafka.start()
    }

    fun stop() = kafka.stop()

    private fun newContainer(): KafkaContainer = FixedPortKafkaContainer(
        DockerImageName.parse("apache/kafka-native:3.8.0")
    ).withFixedKafkaPort(port)
}

private class FixedPortKafkaContainer(imageName: DockerImageName) : KafkaContainer(imageName) {
    fun withFixedKafkaPort(hostPort: Int): KafkaContainer =
        apply { addFixedExposedPort(hostPort, 9092) }
}
