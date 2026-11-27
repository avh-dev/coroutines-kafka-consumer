package avh.ckc.warmup

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.consumerContainer
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.LifecycleOutcome
import avh.ckc.warmup.broker.BROKER_DRAINING
import avh.ckc.warmup.broker.BrokerLifecycleEvent
import avh.ckc.warmup.broker.KafkaBrokerLifecycleLog
import avh.ckc.warmup.broker.launchBrokerLifecycleCli
import avh.ckc.warmup.protocol.ControlRecordCompleted
import avh.ckc.warmup.protocol.DonorClosed
import avh.ckc.warmup.protocol.RecipientReady
import avh.ckc.warmup.protocol.SessionEnded
import avh.ckc.warmup.protocol.WarmupControlCodec
import avh.ckc.warmup.protocol.WarmupLivenessProbe
import avh.ckc.warmup.protocol.WarmupRecordId
import avh.ckc.warmup.protocol.WarmupSessionEndState
import avh.ckc.warmup.protocol.WarmupSessionId
import java.time.Duration as JavaDuration
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException as KafkaTimeoutException
import org.apache.kafka.common.serialization.ByteArrayDeserializer
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
internal class WarmupBrokerRecoveryIntegrationTest : WarmupContextKafkaFixture() {
    @Test
    fun `donor lost at end of warming retains the liveness deadline and main recovery context`() = runBlocking {
        val source = unique("late-loss-source")
        val warmup = unique("late-loss-warmup")
        val control = unique("late-loss-control")
        val namespace = unique("late-loss")
        val group = unique("late-loss-group")
        createTopics(source, warmup, control)
        val firstReply = CompletableDeferred<Unit>()
        val donorLost = AtomicBoolean()
        val donorContext = faultInjectionContext(namespace, control, send = { message ->
            when {
                message is WarmupLivenessProbe && message.reply -> {
                    if (donorLost.get()) mock() else send(message).also { firstReply.complete(Unit) }
                }
                message is DonorClosed && donorLost.get() -> mock()
                else -> send(message)
            }
        })
        val donor = stringConsumer(donorContext, "records", source, warmup, group, CopyOnWriteArrayList())
        val warming = CompletableDeferred<TrafficWarmupLifecycleEvent>()
        val completing = CompletableDeferred<Unit>()
        val interrupted = CompletableDeferred<TrafficWarmupLifecycleEvent>()
        val mainReady = CompletableDeferred<TrafficWarmupLifecycleEvent>()
        val recipient = ControllableRecipient(source, warmup, kafkaConnection())
        val recipientContext = faultInjectionContext(
            namespace, control,
            configure = {
                warmupDuration = 3.seconds
                donorLivenessTimeout = 6.seconds
                completionTimeout = 20.seconds
                onLifecycleEvent { event ->
                    when (event.phase) {
                        TrafficWarmupLifecyclePhase.WARMING -> warming.complete(event)
                        TrafficWarmupLifecyclePhase.COMPLETING -> completing.complete(Unit)
                        TrafficWarmupLifecyclePhase.WARMUP_INTERRUPTED -> interrupted.complete(event)
                        TrafficWarmupLifecyclePhase.MAIN_READY -> mainReady.complete(event)
                        else -> Unit
                    }
                }
            }
        )
        val lifecycle = recipientContext.register(recipient)
        try {
            donor.start()
            withTimeout(15.seconds) { donor.awaitReady() }
            lifecycle.start()
            val attempt = withTimeout(15.seconds) { warming.await() }
            delay(2_400.milliseconds)
            assertTrue(firstReply.isCompleted, "expected a successful control round trip before donor loss")
            assertFalse(completing.isCompleted)
            donorLost.set(true)

            withTimeout(2.seconds) { completing.await() }
            val event = withTimeout(8.seconds) { mainReady.await() }
            lifecycle.awaitReady()
            assertEquals("donor_liveness_timeout", event.reasonCode)
            assertEquals(attempt.sessionId, event.sessionId)
            assertEquals(event.sessionId, interrupted.await().sessionId)
            assertTrue(requireNotNull(event.recoveryDurationMillis) < 8_000)
        } finally {
            try {
                lifecycle.stop()
            } finally {
                donor.stop()
            }
        }
    }

    @Test
    fun `liveness deadline interrupts before a stalled probe sender can unwind`() = runBlocking {
        val source = unique("stalled-probe-source")
        val warmup = unique("stalled-probe-warmup")
        val control = unique("stalled-probe-control")
        val namespace = unique("stalled-probe")
        createTopics(source, warmup, control)
        val warming = CompletableDeferred<Unit>()
        val interrupted = CompletableDeferred<TrafficWarmupLifecycleEvent>()
        val mainReady = CompletableDeferred<TrafficWarmupLifecycleEvent>()
        val probeStarted = CompletableDeferred<Unit>()
        val releaseProbe = CompletableDeferred<Unit>()
        val resultStarted = CompletableDeferred<Unit>()
        val resultStopped = CompletableDeferred<Unit>()
        val recipient = ControllableRecipient(source, warmup, kafkaConnection())
        val recipientContext = faultInjectionContext(namespace, control, configure = {
            serviceThreadLimit = 3
            warmupDuration = 30.seconds
            donorLivenessTimeout = 3.seconds
            completionTimeout = 4.seconds
            onLifecycleEvent { event ->
                when (event.phase) {
                    TrafficWarmupLifecyclePhase.WARMING -> warming.complete(Unit)
                    TrafficWarmupLifecyclePhase.WARMUP_INTERRUPTED -> interrupted.complete(event)
                    TrafficWarmupLifecyclePhase.MAIN_READY -> mainReady.complete(event)
                    else -> Unit
                }
            }
        }, send = { message ->
            when (message) {
                is WarmupLivenessProbe -> {
                    probeStarted.complete(Unit)
                    try {
                        awaitCancellation()
                    } finally {
                        // Model a cancelled send whose Kafka dispatcher still has to unwind it.
                        withContext(NonCancellable) { releaseProbe.await() }
                    }
                }
                is ControlRecordCompleted -> {
                    resultStarted.complete(Unit)
                    try {
                        awaitCancellation()
                    } finally {
                        resultStopped.complete(Unit)
                    }
                }
                else -> send(message)
            }
        })
        val donor = stringConsumer(
            context(namespace, control), "records", source, warmup, unique("probe-group"), CopyOnWriteArrayList()
        )
        val lifecycle = recipientContext.register(recipient)
        try {
            donor.start()
            withTimeout(15.seconds) { donor.awaitReady() }
            lifecycle.start()
            withTimeout(15.seconds) {
                warming.await()
                probeStarted.await()
            }
            recipient.publishResult()
            withTimeout(2.seconds) { resultStarted.await() }

            val failure = withTimeout(4.seconds) { interrupted.await() }
            assertEquals("donor_liveness_timeout", failure.reasonCode)
            withTimeout(2.seconds) { resultStopped.await() }
            assertFalse(mainReady.isCompleted)
            releaseProbe.complete(Unit)

            val ready = withTimeout(5.seconds) { mainReady.await() }
            assertEquals(failure.sessionId, ready.sessionId)
            assertEquals(failure.reasonCode, ready.reasonCode)
            assertTrue(requireNotNull(ready.recoveryDurationMillis) >= 0)
        } finally {
            releaseProbe.complete(Unit)
            try {
                lifecycle.stop()
            } finally {
                donor.stop()
            }
        }
    }

    @Test
    fun `idle recipient starts main consumers promptly after losing its control path`() = runBlocking {
        val source = unique("liveness-source")
        val warmup = unique("liveness-warmup")
        val control = unique("liveness-control")
        val namespace = unique("liveness")
        val group = unique("liveness-group")
        createTopics(source, warmup)
        val controlKafka = ReplaceableWarmupKafka()
        controlKafka.start()
        val connection = controlKafka.connection()
        val warming = CompletableDeferred<Unit>()
        val interrupted = CompletableDeferred<TrafficWarmupLifecycleEvent>()
        val mainReady = CompletableDeferred<TrafficWarmupLifecycleEvent>()
        val donorHandled = CopyOnWriteArrayList<String>()
        val recipientHandled = CopyOnWriteArrayList<String>()

        fun livenessContext(observe: (TrafficWarmupLifecycleEvent) -> Unit = {}) = trafficWarmupContext {
            this.namespace = namespace
            this.controlTopic = control
            controlProducerProperties = connection
            controlConsumerProperties = connection
            warmupDuration = 90.seconds
            donorLivenessTimeout = 6.seconds
            completionTimeout = 2.seconds
            offerWindow = 500.milliseconds
            preparationTimeout = 3.seconds
            onLifecycleEvent(observe)
        }
        val donor = stringConsumer(livenessContext(), "records", source, warmup, group, donorHandled)
        val recipientContext = livenessContext { event ->
            when (event.phase) {
                TrafficWarmupLifecyclePhase.WARMING -> warming.complete(Unit)
                TrafficWarmupLifecyclePhase.WARMUP_INTERRUPTED -> interrupted.complete(event)
                TrafficWarmupLifecyclePhase.MAIN_READY -> mainReady.complete(event)
                else -> Unit
            }
        }
        val recipient = stringConsumer(recipientContext, "records", source, warmup, group, recipientHandled)
        try {
            createTopicsAt(connection, control)
            donor.start()
            withTimeout(20.seconds) { donor.awaitReady() }
            recipient.start()
            withTimeout(20.seconds) { warming.await() }
            kotlinx.coroutines.delay(8.seconds)
            assertFalse(interrupted.isCompleted, "idle donors must keep the control round trip alive")
            assertFalse(mainReady.isCompleted)

            controlKafka.destroyData()
            val failure = withTimeout(8.seconds) { interrupted.await() }
            assertEquals("donor_liveness_timeout", failure.reasonCode)
            val recovered = withTimeout(10.seconds) { mainReady.await() }
            assertEquals(failure.sessionId, recovered.sessionId)
            assertEquals(failure.reasonCode, recovered.reasonCode)
            assertTrue(requireNotNull(recovered.recoveryDurationMillis) < 10_000)
            recipient.awaitReady()
        } finally {
            try {
                recipient.stop()
            } finally {
                try {
                    donor.stop()
                } finally {
                    controlKafka.stop()
                }
            }
        }
    }

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
        val donorMetrics = RecordingWarmupMetrics<String, String>()
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
        val replacementMetrics = RecordingWarmupMetrics<String, String>()
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
            awaitFor { donor.runtimeControl.partitionOwnership(source).isNotEmpty() }

            warmupKafka.startEmptyReplacement()
            createTopicsAt(warmupKafka.connection(), warmup, control)
            produceForeignControlHistory(
                warmupKafka.connection(),
                control,
                source,
                warmup,
                count = 100
            )
            // The donor transport confirms the new topic identity before replacing stale control
            // clients. The fresh log is deliberately grown beyond the previous numeric cursor:
            // offset-range checks alone cannot distinguish the two topic generations.
            kotlinx.coroutines.delay(15.seconds)
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
    fun `broker drain switches an in-progress recipient stop after DonorClosed to the broker deadline`() = runBlocking {
        val source = unique("drain-stop-source")
        val warmup = unique("drain-stop-destination")
        val control = unique("drain-stop-control")
        val lifecycle = unique("drain-stop-lifecycle")
        val namespace = unique("drain-stop")
        val epoch = UUID.randomUUID().toString()
        createTopics(source, warmup, control, lifecycle)
        val stopEntered = CompletableDeferred<Unit>()
        val abortEntered = CompletableDeferred<Unit>()
        val finishAbort = CompletableDeferred<Unit>()
        val recipientContext = faultInjectionContext(namespace, control, configure = {
            warmupDuration = 1.seconds
            completionTimeout = 30.seconds
            brokerLifecycle {
                topic = lifecycle
                kafkaProperties = kafkaConnection()
            }
        })
        val participant = ControllableRecipient(
            source, warmup, kafkaConnection(),
            stopConsumer = {
                stopEntered.complete(Unit)
                awaitCancellation()
            },
            abortConsumer = {
                abortEntered.complete(Unit)
                finishAbort.await()
                true
            }
        )
        val recipient = recipientContext.register(participant)
        val donor = stringConsumer(
            context(namespace, control, brokerLifecycleTopic = lifecycle),
            "records", source, warmup, unique("drain-stop-group"), CopyOnWriteArrayList()
        )
        val witness = startBrokerWitness(kafka.bootstrapServers, lifecycle, epoch)
        var cli: Process? = null
        try {
            donor.start()
            withTimeout(15.seconds) { donor.awaitReady() }
            KafkaBrokerLifecycleLog(lifecycle, kafkaConnection(), 3_000).use { observer ->
                observer.seek(0)
                val donorInstance = (awaitLifecycle(observer) { it is BrokerLifecycleEvent.JoinConfirmed }
                    as BrokerLifecycleEvent.JoinConfirmed).context
                recipient.start()
                withTimeout(15.seconds) { stopEntered.await() }

                val draining = launchBrokerLifecycleCli("drain", kafka.bootstrapServers, lifecycle, epoch, "9000")
                cli = draining
                withTimeout(7.seconds) { abortEntered.await() }
                assertFalse(participant.warmupStopped.isCompleted)
                assertTrue(draining.isAlive)
                finishAbort.complete(Unit)
                val recipientDrain = awaitLifecycle(observer) {
                    it is BrokerLifecycleEvent.ContextDrained && it.context != donorInstance
                } as BrokerLifecycleEvent.ContextDrained
                assertTrue(participant.warmupStopped.isCompleted)
                val session = WarmupSessionId(recipientDrain.sessions.single())
                assertEquals(WarmupSessionEndState.COMPLETED, sessionEndRecords(control, session).single().state)
                val output = withContext(Dispatchers.IO) {
                    assertTrue(draining.waitFor(10, TimeUnit.SECONDS))
                    draining.inputStream.bufferedReader().readText()
                }
                assertEquals(0, draining.exitValue(), output)
                withTimeout(2.seconds) { recipient.awaitReady() }
            }
        } finally {
            finishAbort.complete(Unit)
            cli?.destroyForcibly()
            try {
                recipient.stop()
            } finally {
                try {
                    donor.stop()
                } finally {
                    witness.close()
                }
            }
        }
    }

    @Test
    fun `broker drain bounds a held RecordCompleted ACK after DonorClosed`() = runBlocking {
        verifyHeldResultAckDuringBrokerDrain(drainAfterDonorClosed = false)
    }

    @Test
    fun `broker drain takes over an existing result ACK wait after DonorClosed`() = runBlocking {
        verifyHeldResultAckDuringBrokerDrain(drainAfterDonorClosed = true)
    }

    @Test
    fun `broker drain preserves the result timeout when a participant stops during sender cleanup`() = runBlocking {
        verifyHeldResultAckDuringBrokerDrain(drainAfterDonorClosed = false, stopSibling = true)
    }

    @Test
    fun `broker drain during COMPLETED acknowledgement preserves the recorded session outcome`() = runBlocking {
        val source = unique("broker-completed-source")
        val warmup = unique("broker-completed-destination")
        val control = unique("broker-completed-control")
        val lifecycle = unique("broker-completed-lifecycle")
        val namespace = unique("broker-completed")
        val group = unique("broker-completed-group")
        val epoch = UUID.randomUUID().toString()
        createTopics(source, warmup, control, lifecycle)
        val completed = CompletableDeferred<SessionEnded>()
        val events = CopyOnWriteArrayList<TrafficWarmupLifecycleEvent>()
        val recipientContext = faultInjectionContext(namespace, control, configure = {
            warmupDuration = 1.seconds
            donorLivenessTimeout = 30.seconds
            brokerLifecycle {
                topic = lifecycle
                kafkaProperties = kafkaConnection()
            }
            onLifecycleEvent { events += it }
        }, send = { message ->
            val ack = send(message)
            if (message is SessionEnded) {
                completed.complete(message)
                awaitCancellation()
            }
            ack
        })
        val donor = stringConsumer(
            context(namespace, control, brokerLifecycleTopic = lifecycle),
            "records", source, warmup, group, CopyOnWriteArrayList()
        )
        val recipient = stringConsumer(recipientContext, "records", source, warmup, group, CopyOnWriteArrayList())
        val witness = startBrokerWitness(kafka.bootstrapServers, lifecycle, epoch)
        var cli: Process? = null
        try {
            donor.start()
            withTimeout(15.seconds) { donor.awaitReady() }
            recipient.start()
            val ending = withTimeout(15.seconds) { completed.await() }
            assertEquals(WarmupSessionEndState.COMPLETED, ending.state)
            val drain = launchBrokerLifecycleCli("drain", kafka.bootstrapServers, lifecycle, epoch, "5000")
            cli = drain
            val output = withContext(Dispatchers.IO) {
                assertTrue(drain.waitFor(12, TimeUnit.SECONDS))
                drain.inputStream.bufferedReader().readText()
            }
            assertEquals(0, drain.exitValue(), output)
            withTimeout(12.seconds) { recipient.awaitReady() }
            assertEquals(listOf(ending), sessionEndRecords(control, ending.sessionId))
            assertFalse(events.any {
                it.phase == TrafficWarmupLifecyclePhase.WARMUP_INTERRUPTED ||
                    it.phase == TrafficWarmupLifecyclePhase.COLD_FALLBACK
            })
        } finally {
            cli?.destroyForcibly()
            try {
                consumerContainer(recipient, donor).stop()
            } finally {
                witness.close()
            }
        }
    }

    @Test
    fun `broker drain keeps Kafka open for both recipients and fences new source delegations`() = runBlocking {
        val source = unique("broker-drain-source")
        val warmup = unique("broker-drain-destination")
        val control = unique("broker-drain-control")
        val lifecycle = unique("broker-drain-lifecycle")
        createTopics(source, warmup, control, lifecycle)
        val namespace = unique("broker-drain")
        val group = unique("broker-drain-group")
        val epoch = UUID.randomUUID().toString()
        val donorHandled = CopyOnWriteArrayList<String>()
        val forwarded = CopyOnWriteArrayList<TrafficWarmupForwardedEvent>()
        val donorMetrics = RecordingWarmupMetrics<String, String>()
        val donor = stringConsumer(
            context(namespace, control, brokerLifecycleTopic = lifecycle, delegationTimeout = 45.seconds),
            "records", source, warmup, group, donorHandled, unorderedWorkers = 4,
            forwarded = forwarded, consumerMetrics = donorMetrics
        )
        val entered = List(2) { CompletableDeferred<Unit>() }
        val release = List(2) { CompletableDeferred<Unit>() }
        val recipientEvents = List(2) { CopyOnWriteArrayList<TrafficWarmupLifecycleEvent>() }
        val recipients = entered.indices.map { index ->
            stringConsumer(
                context(namespace, control, warmupDuration = 60.seconds, brokerLifecycleTopic = lifecycle) {
                    recipientEvents[index] += it
                },
                "records", source, warmup, group, CopyOnWriteArrayList(),
                beforeHandle = { value ->
                    if (value.startsWith("hold")) {
                        withContext(NonCancellable) {
                            entered[index].complete(Unit)
                            release[index].await()
                        }
                    }
                }
            )
        }
        val lateEvents = CopyOnWriteArrayList<TrafficWarmupLifecycleEvent>()
        val late = stringConsumer(
            context(namespace, control, brokerLifecycleTopic = lifecycle) { lateEvents += it },
            "records", source, warmup, unique("late-drain-group"), CopyOnWriteArrayList()
        )
        val witness = startBrokerWitness(kafka.bootstrapServers, lifecycle, epoch)
        var cli: Process? = null
        try {
            donor.start()
            withTimeout(15.seconds) { donor.awaitReady() }
            recipients.forEach { it.start() }
            awaitActiveDonorSessions(control, 2)
            produce(source, "first", "hold-1", StringSerializer())
            produce(source, "second", "hold-2", StringSerializer())
            withTimeout(10.seconds) { entered.forEach { it.await() } }

            val draining = launchBrokerLifecycleCli("drain", kafka.bootstrapServers, lifecycle, epoch, "20000")
            cli = draining
            KafkaBrokerLifecycleLog(lifecycle, kafkaConnection(), 3_000).use { observer ->
                observer.seek(0)
                val accepted = linkedSetOf<String>()
                awaitLifecycle(observer) { event ->
                    if (event is BrokerLifecycleEvent.DrainAccepted) accepted += event.context
                    accepted.size == 3
                }
                produce(source, "local", "local-during-drain", StringSerializer())
                awaitFor { "local-during-drain" in donorHandled }
                assertTrue(draining.isAlive)

                late.start()
                withTimeout(10.seconds) { late.awaitReady() }
                assertTrue(lateEvents.any {
                    it.phase == TrafficWarmupLifecyclePhase.COLD_FALLBACK && it.reasonCode == "broker_draining"
                })
                assertFalse(lateEvents.any { it.phase == TrafficWarmupLifecyclePhase.DISCOVERING })

                release[0].complete(Unit)
                awaitLifecycle(observer) { it is BrokerLifecycleEvent.ContextDrained }
                assertTrue(draining.isAlive)
                release[1].complete(Unit)
            }
            val output = withContext(Dispatchers.IO) {
                assertTrue(draining.waitFor(22, TimeUnit.SECONDS), "Broker drain CLI did not exit")
                draining.inputStream.bufferedReader().readText()
            }
            assertEquals(0, draining.exitValue(), output)
            assertTrue(output.contains("status=DRAINED"), output)
            withTimeout(15.seconds) { recipients.forEach { it.awaitReady() } }
            assertEquals(listOf("local-during-drain"), donorHandled.toList())
            assertEquals(setOf("hold-1", "hold-2"), donorMetrics.processed.filter { it.processedRemotely }.map { it.value }.toSet())
            assertEquals(2, forwarded.size)
            forwarded.forEach { delivery ->
                val messages = sessionControlRecords(control, WarmupSessionId(delivery.sessionId))
                assertEquals(listOf(delivery.recordId()), messages.filterIsInstance<ControlRecordCompleted>()
                    .map { it.completed.recordId })
                val terminal = messages.filterIsInstance<SessionEnded>().single()
                assertEquals(WarmupSessionEndState.COMPLETED, terminal.state)
                val resultIndex = messages.indexOfFirst { it is ControlRecordCompleted }
                val closedIndex = messages.indexOfFirst { it is DonorClosed }
                assertTrue(resultIndex < closedIndex && closedIndex < messages.indexOf(terminal))
            }
            assertTrue(recipientEvents.all { events ->
                events.none { it.phase == TrafficWarmupLifecyclePhase.WARMUP_INTERRUPTED }
            })
        } finally {
            release.forEach { it.complete(Unit) }
            cli?.destroyForcibly()
            consumerContainer(*((recipients + late + donor).toTypedArray())).stop()
            witness.close()
        }
    }

    @Test
    fun `broker drain delivers acknowledged records and fences the unresolved remainder before donor cleanup`() = runBlocking {
        val source = unique("partial-drain-source")
        val warmup = unique("partial-drain-destination")
        val control = unique("partial-drain-control")
        val lifecycle = unique("partial-drain-lifecycle")
        createTopics(source, warmup, control, lifecycle)
        val namespace = unique("partial-drain")
        val group = unique("partial-drain-group")
        val epoch = UUID.randomUUID().toString()
        val resultQueued = CompletableDeferred<WarmupRecordId>()
        val releaseResult = CompletableDeferred<Unit>()
        val processingGate = CompletableDeferred<Unit>()
        val releaseProcessing = CompletableDeferred<Unit>()
        val unfinishedEntered = CompletableDeferred<Unit>()
        val unfinishedStopped = CompletableDeferred<Unit>()
        val fallbackStarted = CompletableDeferred<Unit>()
        val finishFallback = CompletableDeferred<Unit>()
        val donorHandled = CopyOnWriteArrayList<String>()
        val recipientHandled = CopyOnWriteArrayList<String>()
        val forwarded = CopyOnWriteArrayList<TrafficWarmupForwardedEvent>()
        val metrics = RecordingWarmupMetrics<String, String>()
        val recipientContext = faultInjectionContext(
            namespace, control,
            configure = {
                warmupDuration = 60.seconds
                completionTimeout = 4.seconds
                brokerLifecycle {
                    topic = lifecycle
                    kafkaProperties = kafkaConnection()
                }
            },
            send = { message ->
                if (message is ControlRecordCompleted && resultQueued.complete(message.completed.recordId)) {
                    releaseResult.await()
                }
                send(message)
            }
        )
        val recipient = stringConsumer(
            recipientContext, "records", source, warmup, group, recipientHandled, unorderedWorkers = 1,
            beforeHandle = { value ->
                when (value) {
                    "gate" -> {
                        processingGate.complete(Unit)
                        releaseProcessing.await()
                    }
                    "unfinished" -> {
                        unfinishedEntered.complete(Unit)
                        try {
                            awaitCancellation()
                        } finally {
                            unfinishedStopped.complete(Unit)
                        }
                    }
                }
            }
        )
        val donor = stringConsumer(
            context(
                namespace, control, brokerLifecycleTopic = lifecycle,
                delegationTimeout = 45.seconds, completionTimeout = 45.seconds
            ),
            "records", source, warmup, group, donorHandled,
            forwarded = forwarded, consumerMetrics = metrics, unorderedWorkers = 4,
            beforeHandle = { value ->
                if (value == "unfinished") {
                    fallbackStarted.complete(Unit)
                    finishFallback.await()
                }
            }
        )
        val witness = startBrokerWitness(kafka.bootstrapServers, lifecycle, epoch)
        var cli: Process? = null
        try {
            donor.start()
            withTimeout(15.seconds) { donor.awaitReady() }
            KafkaBrokerLifecycleLog(lifecycle, kafkaConnection(), 3_000).use { observer ->
                observer.seek(0)
                val donorContext = (awaitLifecycle(observer) { it is BrokerLifecycleEvent.JoinConfirmed }
                    as BrokerLifecycleEvent.JoinConfirmed).context
                recipient.start()
                awaitActiveDonorSessions(control, 1)
                val deliveries = listOf("queued-result", "gate", "queued-record", "unfinished").associateWith { value ->
                    val count = forwarded.size
                    val sourceRecord = produce(source, value, value, StringSerializer())
                    awaitFor { forwarded.size == count + 1 }
                    forwarded.single { it.sourceOffset == sourceRecord.offset() }
                }
                val sessionId = WarmupSessionId(deliveries.getValue("queued-result").sessionId)
                withTimeout(5.seconds) {
                    assertEquals(deliveries.getValue("queued-result").recordId(), resultQueued.await())
                    processingGate.await()
                }
                assertEquals(listOf("queued-result"), recipientHandled.toList())
                assertFalse(unfinishedEntered.isCompleted)
                assertTrue(metrics.processed.isEmpty())

                val draining = launchBrokerLifecycleCli("drain", kafka.bootstrapServers, lifecycle, epoch, "15000")
                cli = draining
                val accepted = linkedSetOf<String>()
                awaitLifecycle(observer) { event ->
                    if (event is BrokerLifecycleEvent.DrainAccepted) accepted += event.context
                    accepted.size == 2
                }
                assertEquals(listOf("queued-result"), recipientHandled.toList())
                releaseResult.complete(Unit)
                releaseProcessing.complete(Unit)
                withTimeout(12.seconds) {
                    unfinishedEntered.await()
                    fallbackStarted.await()
                }
                assertTrue(unfinishedStopped.isCompleted)
                assertTrue(donorHandled.isEmpty())
                assertTrue(draining.isAlive)
                val recipientDrain = awaitLifecycle(observer) { it is BrokerLifecycleEvent.ContextDrained }
                    as BrokerLifecycleEvent.ContextDrained
                assertTrue(recipientDrain.context != donorContext)
                assertEquals(setOf(sessionId.value), recipientDrain.sessions)

                val messages = sessionControlRecords(control, sessionId)
                val terminal = messages.filterIsInstance<SessionEnded>().single()
                assertEquals(WarmupSessionEndState.COLD_FALLBACK, terminal.state)
                assertEquals(BROKER_DRAINING, terminal.reasonCode)
                val expectedRemote = setOf("queued-result", "gate", "queued-record")
                val completed = messages.filterIsInstance<ControlRecordCompleted>()
                assertEquals(expectedRemote.map { deliveries.getValue(it).recordId() }.toSet(),
                    completed.map { it.completed.recordId }.toSet())
                assertEquals(expectedRemote.size, completed.size)
                assertEquals(expectedRemote.size, completed.mapNotNull { it.completed.delegationId }.toSet().size)
                assertTrue(completed.all { messages.indexOf(it) < messages.indexOf(terminal) })
                awaitFor { metrics.processed.count { it.processedRemotely } == expectedRemote.size }
                assertEquals(expectedRemote, recipientHandled.toSet())
                assertEquals(expectedRemote, metrics.processed.filter { it.processedRemotely }.map { it.value }.toSet())
                assertEquals(listOf("unfinished"), metrics.warmupFallbacks.map { it.first.value() })

                finishFallback.complete(Unit)
                val donorDrain = awaitLifecycle(observer) {
                    it is BrokerLifecycleEvent.ContextDrained && it.context == donorContext
                } as BrokerLifecycleEvent.ContextDrained
                assertEquals(setOf(sessionId.value), donorDrain.sessions)
                assertEquals(listOf("unfinished"), donorHandled.toList())
                val output = withContext(Dispatchers.IO) {
                    assertTrue(draining.waitFor(5, TimeUnit.SECONDS), "Broker drain CLI did not exit")
                    draining.inputStream.bufferedReader().readText()
                }
                assertEquals(0, draining.exitValue(), output)
            }
        } finally {
            releaseResult.complete(Unit)
            releaseProcessing.complete(Unit)
            finishFallback.complete(Unit)
            cli?.destroyForcibly()
            try {
                consumerContainer(recipient, donor).stop()
            } finally {
                witness.close()
            }
        }
    }

    @Test
    fun `broker drain confirms donor cleanup when only SessionEnded publication fails`() = runBlocking {
        val source = unique("missing-fence-source")
        val warmup = unique("missing-fence-destination")
        val control = unique("missing-fence-control")
        val lifecycle = unique("missing-fence-lifecycle")
        val namespace = unique("missing-fence")
        val group = unique("missing-fence-group")
        val epoch = UUID.randomUUID().toString()
        createTopics(source, warmup, control, lifecycle)
        val rejectedFence = CompletableDeferred<SessionEnded>()
        val recipientEntered = CompletableDeferred<Unit>()
        val fallbackAt = CompletableDeferred<Long>()
        val donorHandled = CopyOnWriteArrayList<String>()
        val recipientHandled = CopyOnWriteArrayList<String>()
        val metrics = RecordingWarmupMetrics<String, String>()

        val donor = stringConsumer(
            context(
                namespace, control, brokerLifecycleTopic = lifecycle,
                completionTimeout = 45.seconds, delegationTimeout = 45.seconds
            ),
            "records", source, warmup, group, donorHandled, consumerMetrics = metrics,
            beforeHandle = { fallbackAt.complete(System.currentTimeMillis()) }
        )
        val recipientContext = faultInjectionContext(namespace, control, configure = {
            warmupDuration = 60.seconds
            completionTimeout = 3.seconds
            brokerLifecycle {
                topic = lifecycle
                kafkaProperties = kafkaConnection()
            }
        }, send = { message ->
            if (message is SessionEnded) {
                rejectedFence.complete(message)
                throw KafkaTimeoutException("SessionEnded send failed before submission")
            }
            send(message)
        })
        val recipient = stringConsumer(
            recipientContext, "records", source, warmup, group, recipientHandled,
            beforeHandle = {
                recipientEntered.complete(Unit)
                awaitCancellation()
            }
        )
        val witness = startBrokerWitness(kafka.bootstrapServers, lifecycle, epoch)
        var cli: Process? = null
        try {
            donor.start()
            withTimeout(15.seconds) { donor.awaitReady() }
            KafkaBrokerLifecycleLog(lifecycle, kafkaConnection(), 3_000).use { observer ->
                observer.seek(0)
                val donorInstance = (awaitLifecycle(observer) { it is BrokerLifecycleEvent.JoinConfirmed }
                    as BrokerLifecycleEvent.JoinConfirmed).context
                recipient.start()
                awaitActiveDonorSessions(control, 1)
                produce(source, "held", "held", StringSerializer())
                withTimeout(5.seconds) { recipientEntered.await() }
                awaitFor { metrics.warmupPending.lastOrNull() == 1 }

                val draining = launchBrokerLifecycleCli("drain", kafka.bootstrapServers, lifecycle, epoch, "10000")
                cli = draining
                val request = awaitLifecycle(observer) { it is BrokerLifecycleEvent.DrainRequested }
                    as BrokerLifecycleEvent.DrainRequested
                val fence = withTimeout(10.seconds) { rejectedFence.await() }
                assertEquals(WarmupSessionEndState.COLD_FALLBACK, fence.state)
                assertEquals(BROKER_DRAINING, fence.reasonCode)

                val donorDrain = awaitLifecycle(observer) {
                    it is BrokerLifecycleEvent.ContextDrained && it.context == donorInstance
                } as BrokerLifecycleEvent.ContextDrained
                assertEquals(setOf(fence.sessionId.value), donorDrain.sessions)
                assertTrue(withTimeout(2.seconds) { fallbackAt.await() } < request.deadlineMillis)
                assertEquals(listOf("held"), donorHandled.toList())
                assertEquals(listOf("held"), metrics.warmupFallbacks.map { it.first.value() })
                assertTrue(recipientHandled.isEmpty())
                assertTrue(sessionEndRecords(control, fence.sessionId).isEmpty())
                val output = withContext(Dispatchers.IO) {
                    assertTrue(draining.waitFor(5, TimeUnit.SECONDS))
                    draining.inputStream.bufferedReader().readText()
                }
                assertEquals(0, draining.exitValue(), output)
                assertTrue(output.contains("status=DRAINED"), output)
            }
        } finally {
            cli?.destroyForcibly()
            try {
                consumerContainer(recipient, donor).stop()
            } finally {
                witness.close()
            }
        }
    }

    @Test
    fun `broker loss during broker drain releases the donor by its emergency deadline without a cleanup confirmation`() =
        runBlocking {
            PersistedWarmupKafka().use { temporary ->
                temporary.start()
                val connection = mapOf("bootstrap.servers" to temporary.bootstrapServers())
                val source = unique("drain-loss-source")
                val warmup = unique("drain-loss-warmup")
                val control = unique("drain-loss-control")
                val lifecycle = unique("drain-loss-lifecycle")
                val namespace = unique("drain-loss")
                val group = unique("drain-loss-group")
                val epoch = UUID.randomUUID().toString()
                createTopics(source)
                createTopicsAt(connection, warmup, control, lifecycle)
                val recipientEntered = CompletableDeferred<Unit>()
                val recipientStopped = CompletableDeferred<Unit>()
                val fallbackEntered = CompletableDeferred<Unit>()
                val finishFallback = CompletableDeferred<Unit>()
                val donorHandled = CopyOnWriteArrayList<String>()
                val recipientHandled = CopyOnWriteArrayList<String>()
                val metrics = RecordingWarmupMetrics<String, String>()
                fun drainingContext() = faultInjectionContext(namespace, control, configure = {
                    controlConsumerProperties = connection
                    controlProducerProperties = connection
                    warmupDuration = 60.seconds
                    completionTimeout = 4.seconds
                    delegationTimeout = 30.seconds
                    donorLivenessTimeout = 4.seconds
                    brokerLifecycle {
                        topic = lifecycle
                        kafkaProperties = connection
                        operationTimeout = 1.seconds
                    }
                })
                val donor = stringConsumer(
                    drainingContext(), "records", source, warmup, group, donorHandled,
                    consumerMetrics = metrics, warmupProperties = connection,
                    beforeHandle = {
                        fallbackEntered.complete(Unit)
                        finishFallback.await()
                    }
                )
                val recipient = stringConsumer(
                    drainingContext(), "records", source, warmup, group, recipientHandled,
                    warmupProperties = connection,
                    beforeHandle = {
                        recipientEntered.complete(Unit)
                        try {
                            awaitCancellation()
                        } finally {
                            recipientStopped.complete(Unit)
                        }
                    }
                )
                val witness = startBrokerWitness(temporary.bootstrapServers(), lifecycle, epoch)
                try {
                    donor.start()
                    withTimeout(15.seconds) { donor.awaitReady() }
                    recipient.start()
                    awaitActiveDonorSessions(control, 1, connection)
                    produce(source, "key", "held", StringSerializer())
                    withTimeout(5.seconds) { recipientEntered.await() }
                    awaitFor { metrics.warmupPending.lastOrNull() == 1 }

                    val drain = BrokerLifecycleEvent.DrainRequested(epoch, "loss", System.currentTimeMillis() + 10_000)
                    KafkaBrokerLifecycleLog(lifecycle, connection, 1_000).use { observer ->
                        observer.seek(0)
                        observer.submit(drain).get(3, TimeUnit.SECONDS)
                        val accepted = linkedSetOf<String>()
                        awaitLifecycle(observer) { event ->
                            assertFalse(event is BrokerLifecycleEvent.ContextDrained)
                            if (event is BrokerLifecycleEvent.DrainAccepted) accepted += event.context
                            accepted.size == 2
                        }
                    }
                    withContext(Dispatchers.IO) { temporary.kill() }
                    withTimeout(15.seconds) {
                        recipientStopped.await()
                        fallbackEntered.await()
                    }
                    assertTrue(donorHandled.isEmpty())
                    assertTrue(recipientHandled.isEmpty())
                    assertEquals(listOf("held"), metrics.warmupFallbacks.map { it.first.value() })

                    withContext(Dispatchers.IO) { temporary.restart() }
                    KafkaBrokerLifecycleLog(lifecycle, connection, 2_000).use { observer ->
                        val end = observer.end(JavaDuration.ofSeconds(2))
                        observer.seek(0)
                        withTimeout(5.seconds) {
                            while (observer.position(JavaDuration.ofSeconds(1)) < end) {
                                assertTrue(observer.poll(JavaDuration.ofMillis(100)).none {
                                    it.event is BrokerLifecycleEvent.ContextDrained
                                })
                                yield()
                            }
                        }
                    }
                    finishFallback.complete(Unit)
                    awaitFor { donorHandled == listOf("held") }
                } finally {
                    finishFallback.complete(Unit)
                    try {
                        consumerContainer(recipient, donor).stop()
                    } finally {
                        witness.close()
                    }
                }
            }
        }

    @Test
    fun `surviving donor reopens only after a new broker process confirms registration with retained history`() =
        runBlocking {
            val temporary = PersistedWarmupKafka()
            temporary.start()
            val connection = mapOf("bootstrap.servers" to temporary.bootstrapServers())
            val source = unique("epoch-source")
            val warmup = unique("epoch-destination")
            val control = unique("epoch-control")
            val lifecycle = unique("epoch-lifecycle")
            createTopics(source, control)
            createTopicsAt(connection, warmup, lifecycle)
            val namespace = unique("epoch")
            val group = unique("epoch-group")
            val donorHandled = CopyOnWriteArrayList<String>()
            val recipientHandled = CopyOnWriteArrayList<String>()
            val staleEvents = CopyOnWriteArrayList<TrafficWarmupLifecycleEvent>()
            val donor = stringConsumer(
                context(namespace, control, brokerLifecycleTopic = lifecycle, brokerLifecycleConnection = connection),
                "records", source, warmup, group, donorHandled, warmupProperties = connection
            )
            val newcomer = stringConsumer(
                context(namespace, control, brokerLifecycleTopic = lifecycle, brokerLifecycleConnection = connection) {
                    staleEvents += it
                },
                "records", source, warmup, unique("epoch-stale-group"), CopyOnWriteArrayList(),
                warmupProperties = connection
            )
            val recipient = stringConsumer(
                context(
                    namespace, control, warmupDuration = 5.seconds,
                    brokerLifecycleTopic = lifecycle, brokerLifecycleConnection = connection
                ),
                "records", source, warmup, group, recipientHandled, warmupProperties = connection
            )
            val epochBefore = UUID.randomUUID().toString()
            val epochAfter = UUID.randomUUID().toString()
            val original = startBrokerWitness(temporary.bootstrapServers(), lifecycle, epochBefore)
            var replacement: AutoCloseable? = null
            try {
                donor.start()
                withTimeout(15.seconds) { donor.awaitReady() }
                val instanceBefore = KafkaBrokerLifecycleLog(lifecycle, connection, 3_000).use { observer ->
                    observer.seek(0)
                    (awaitLifecycle(observer) {
                        it is BrokerLifecycleEvent.JoinConfirmed && it.epoch == epochBefore
                    } as BrokerLifecycleEvent.JoinConfirmed).context
                }

                original.close()
                withContext(Dispatchers.IO) {
                    temporary.restart()
                }
                KafkaBrokerLifecycleLog(lifecycle, connection, 3_000).use { observer ->
                    observer.seek(0)
                    awaitLifecycle(observer) { it == BrokerLifecycleEvent.Started(epochBefore) }
                }
                newcomer.start()
                withTimeout(15.seconds) { newcomer.awaitReady() }
                assertFalse(staleEvents.any { it.phase == TrafficWarmupLifecyclePhase.DISCOVERING })
                newcomer.stop()
                produce(source, "survivor", "main-still-running", StringSerializer())
                awaitFor { "main-still-running" in donorHandled }

                replacement = startBrokerWitness(temporary.bootstrapServers(), lifecycle, epochAfter)
                KafkaBrokerLifecycleLog(lifecycle, connection, 3_000).use { observer ->
                    observer.seek(0)
                    awaitLifecycle(observer) {
                        it is BrokerLifecycleEvent.JoinConfirmed && it.epoch == epochAfter && it.context == instanceBefore
                    }
                }
                recipient.start()
                awaitActiveDonorSessions(control, 1)
                produce(source, "new-epoch", "delegated-in-new-epoch", StringSerializer())
                awaitFor { "delegated-in-new-epoch" in recipientHandled }
                withTimeout(15.seconds) { recipient.awaitReady() }
                assertFalse("delegated-in-new-epoch" in donorHandled)
            } finally {
                try {
                    consumerContainer(recipient, newcomer, donor).stop()
                } finally {
                    replacement?.close()
                    original.close()
                    temporary.close()
                }
            }
        }

    private suspend fun verifyHeldResultAckDuringBrokerDrain(
        drainAfterDonorClosed: Boolean,
        stopSibling: Boolean = false
    ) {
        val source = unique("drain-held-ack-source")
        val warmup = unique("drain-held-ack-destination")
        val control = unique("drain-held-ack-control")
        val lifecycle = unique("drain-held-ack-lifecycle")
        val namespace = unique("drain-held-ack")
        val group = unique("drain-held-ack-group")
        val siblingSource = unique("drain-held-ack-sibling-source")
        val siblingWarmup = unique("drain-held-ack-sibling-warmup")
        val siblingGroup = unique("drain-held-ack-sibling-group")
        val epoch = UUID.randomUUID().toString()
        createTopics(source, warmup, control, lifecycle)
        if (stopSibling) createTopics(siblingSource, siblingWarmup)

        val resultSent = CompletableDeferred<WarmupRecordId>()
        val resultSenderStopping = CompletableDeferred<Unit>()
        val releaseResultSender = CompletableDeferred<Unit>()
        val resultSenderStopped = CompletableDeferred<Unit>()
        val donorClosed = CompletableDeferred<DonorClosed>()
        val warmupStopped = CompletableDeferred<Unit>()
        val recipientFinished = CompletableDeferred<TrafficWarmupLifecycleEvent>()
        val mainReady = CompletableDeferred<TrafficWarmupLifecycleEvent>()
        val donorHandled = CopyOnWriteArrayList<String>()
        val donorMetrics = RecordingWarmupMetrics<String, String>()
        val recipientHandled = CopyOnWriteArrayList<String>()
        val recipientMetrics = object : ConsumerMetrics<String, String> {
            override fun onShutdown(durationNanos: Long, outcome: LifecycleOutcome) {
                warmupStopped.complete(Unit)
            }
        }
        val donorContext = faultInjectionContext(namespace, control, configure = {
            brokerLifecycle {
                topic = lifecycle
                kafkaProperties = kafkaConnection()
            }
        }, send = { message ->
            send(message).also {
                if (message is DonorClosed) donorClosed.complete(message)
            }
        })
        val recipientContext = faultInjectionContext(namespace, control, configure = {
            warmupDuration = if (drainAfterDonorClosed) 4.seconds else 60.seconds
            completionTimeout = 30.seconds
            brokerLifecycle {
                topic = lifecycle
                kafkaProperties = kafkaConnection()
            }
            onLifecycleEvent { event ->
                when (event.phase) {
                    TrafficWarmupLifecyclePhase.RECIPIENT_FINISHED -> recipientFinished.complete(event)
                    TrafficWarmupLifecyclePhase.MAIN_READY -> if (event.participantId == "records") {
                        mainReady.complete(event)
                    }
                    else -> Unit
                }
            }
        }, send = { message ->
            val ack = send(message)
            if (message is ControlRecordCompleted) {
                resultSent.complete(message.completed.recordId)
                try {
                    awaitCancellation()
                } finally {
                    resultSenderStopping.complete(Unit)
                    if (stopSibling) withContext(NonCancellable) { releaseResultSender.await() }
                    resultSenderStopped.complete(Unit)
                }
            }
            ack
        })
        val donor = stringConsumer(
            donorContext, "records", source, warmup, group, donorHandled, consumerMetrics = donorMetrics
        )
        val recipient = stringConsumer(
            recipientContext, "records", source, warmup, group, recipientHandled, consumerMetrics = recipientMetrics
        )
        val siblingDonor = if (stopSibling) stringConsumer(
            donorContext, "sibling", siblingSource, siblingWarmup, siblingGroup, CopyOnWriteArrayList()
        ) else null
        val siblingRecipient = if (stopSibling) stringConsumer(
            recipientContext, "sibling", siblingSource, siblingWarmup, siblingGroup, CopyOnWriteArrayList()
        ) else null
        val donors = consumerContainer(*listOfNotNull(donor, siblingDonor).toTypedArray())
        val recipients = consumerContainer(*listOfNotNull(recipient, siblingRecipient).toTypedArray())
        val witness = startBrokerWitness(kafka.bootstrapServers, lifecycle, epoch)
        var cli: Process? = null
        try {
            donors.start()
            withTimeout(15.seconds) { donors.awaitReady() }
            KafkaBrokerLifecycleLog(lifecycle, kafkaConnection(), 3_000).use { observer ->
                observer.seek(0)
                val donorInstance = (awaitLifecycle(observer) { it is BrokerLifecycleEvent.JoinConfirmed }
                    as BrokerLifecycleEvent.JoinConfirmed).context
                recipients.start()
                awaitActiveDonorSessions(control, 1)
                produce(source, "held", "held", StringSerializer())
                val record = withTimeout(5.seconds) { resultSent.await() }
                awaitFor { donorMetrics.processed.any { it.value == "held" && it.processedRemotely } }
                if (drainAfterDonorClosed) {
                    withTimeout(10.seconds) {
                        donorClosed.await()
                        warmupStopped.await()
                    }
                } else assertFalse(donorClosed.isCompleted)
                assertFalse(resultSenderStopped.isCompleted)

                val draining = launchBrokerLifecycleCli("drain", kafka.bootstrapServers, lifecycle, epoch, "9000")
                cli = draining
                if (siblingRecipient != null) {
                    withTimeout(8.seconds) { resultSenderStopping.await() }
                    assertTrue(donorClosed.isCompleted)
                    assertTrue(warmupStopped.isCompleted)
                    assertFalse(resultSenderStopped.isCompleted)
                    assertFalse(mainReady.isCompleted)
                    coroutineScope {
                        val stop = async(start = CoroutineStart.UNDISPATCHED) { siblingRecipient.stop() }
                        try {
                            assertFalse(stop.isCompleted)
                        } finally {
                            releaseResultSender.complete(Unit)
                        }
                        withTimeout(5.seconds) { stop.await() }
                    }
                }
                val recipientDrain = awaitLifecycle(observer) {
                    it is BrokerLifecycleEvent.ContextDrained && it.context != donorInstance
                } as BrokerLifecycleEvent.ContextDrained
                assertTrue(resultSenderStopped.isCompleted)
                assertTrue(warmupStopped.isCompleted)
                assertEquals(setOf(record.sessionId.value), recipientDrain.sessions)
                val output = withContext(Dispatchers.IO) {
                    assertTrue(draining.waitFor(12, TimeUnit.SECONDS))
                    draining.inputStream.bufferedReader().readText()
                }
                assertEquals(0, draining.exitValue(), output)
                val messages = sessionControlRecords(control, record.sessionId)
                assertEquals(
                    listOf(record),
                    messages.filterIsInstance<ControlRecordCompleted>().map { it.completed.recordId }
                )
                val closed = messages.filterIsInstance<DonorClosed>().single()
                assertEquals(withTimeout(2.seconds) { donorClosed.await() }, closed)
                val terminal = messages.filterIsInstance<SessionEnded>().single()
                assertEquals(WarmupSessionEndState.COLD_FALLBACK, terminal.state)
                assertEquals("result_delivery_timeout", terminal.reasonCode)
                assertTrue(messages.indexOf(closed) < messages.indexOf(terminal))
                withTimeout(10.seconds) {
                    for (event in listOf(recipientFinished.await(), mainReady.await())) {
                        assertEquals(terminal.sessionId.value, event.sessionId)
                        assertEquals(terminal.reasonCode, event.reasonCode)
                    }
                }
                assertEquals(listOf("held"), recipientHandled.toList())
                assertTrue(donorHandled.isEmpty())
            }
        } finally {
            releaseResultSender.complete(Unit)
            cli?.destroyForcibly()
            try {
                recipients.stop()
            } finally {
                try {
                    donors.stop()
                } finally {
                    witness.close()
                }
            }
        }
    }

    private fun TrafficWarmupForwardedEvent.recordId() = WarmupRecordId(
        WarmupSessionId(sessionId), TopicPartition(destinationTopic, destinationPartition), destinationOffset
    )

    private suspend fun awaitActiveDonorSessions(
        control: String,
        count: Int,
        connection: Map<String, Any?> = kafkaConnection()
    ) = withTimeout(15.seconds) {
        val properties = consumerProperties(unique("activation-observer"), ByteArrayDeserializer::class.java) + connection
        KafkaConsumer<String, ByteArray>(properties).use { observer ->
            val partition = TopicPartition(control, 0)
            observer.assign(listOf(partition))
            observer.seekToBeginning(listOf(partition))
            val active = linkedSetOf<WarmupSessionId>()
            while (active.size < count) {
                observer.poll(JavaDuration.ofMillis(100)).forEach { record ->
                    val event = WarmupControlCodec.decode(record.value(), record.headers())
                    if (event is WarmupLivenessProbe && event.reply) active += event.sessionId
                }
                yield()
            }
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
}
