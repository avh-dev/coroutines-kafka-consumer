package avh.ckc.warmup

import avh.ckc.core.ConsumerContainer
import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.ProcessingMode
import avh.ckc.core.consumerContainer
import avh.ckc.core.coroutinesKafkaConsumer
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.LifecycleOutcome
import avh.ckc.warmup.broker.BrokerLifecycleEvent
import avh.ckc.warmup.broker.KafkaBrokerLifecycleLog
import avh.ckc.warmup.broker.launchBrokerLifecycleCli
import avh.ckc.warmup.protocol.ControlRecordCompleted
import avh.ckc.warmup.protocol.DonorClosed
import avh.ckc.warmup.protocol.RecipientReady
import avh.ckc.warmup.protocol.SessionAccepted
import avh.ckc.warmup.protocol.SessionEnded
import avh.ckc.warmup.protocol.SessionRejected
import avh.ckc.warmup.protocol.WarmupControlCodec
import avh.ckc.warmup.protocol.WarmupControlMessage
import avh.ckc.warmup.protocol.WarmupControlNamespace
import avh.ckc.warmup.protocol.WarmupInstanceId
import avh.ckc.warmup.protocol.WarmupLivenessProbe
import avh.ckc.warmup.protocol.WarmupMappingQuota
import avh.ckc.warmup.protocol.WarmupParticipantId
import avh.ckc.warmup.protocol.WarmupRecordCompleted
import avh.ckc.warmup.protocol.WarmupRecordId
import avh.ckc.warmup.protocol.WarmupRemoteOutcome
import avh.ckc.warmup.protocol.WarmupRequested
import avh.ckc.warmup.protocol.WarmupSessionEndState
import avh.ckc.warmup.protocol.WarmupSessionId
import avh.ckc.warmup.protocol.WarmupTopicMapping
import avh.ckc.warmup.protocol.normalizedAdmissionBudget
import com.github.dockerjava.api.model.Bind
import com.github.dockerjava.api.model.Volume
import java.net.ServerSocket
import java.nio.file.Files
import java.time.Duration as JavaDuration
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException as KafkaTimeoutException
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
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.testcontainers.DockerClientFactory
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class TrafficWarmupContextIntegrationTest {
    @Test
    fun `recipient consumer termination cancels a held result ACK before starting main`() = runBlocking {
        val source = unique("held-result-source")
        val warmup = unique("held-result-warmup")
        val control = unique("held-result-control")
        val namespace = unique("held-result")
        val group = unique("held-result-group")
        createTopics(source, warmup, control)
        val warming = CompletableDeferred<Unit>()
        val mainReady = CompletableDeferred<TrafficWarmupLifecycleEvent>()
        val sendStarted = CompletableDeferred<Unit>()
        val sendStopped = CompletableDeferred<Unit>()
        val recipient = ControllableRecipient(source, warmup, kafkaConnection())
        val recipientContext = faultInjectionContext(
            namespace, control,
            configure = {
                warmupDuration = 30.seconds
                completionTimeout = 1.seconds
                onLifecycleEvent { event ->
                    if (event.phase == TrafficWarmupLifecyclePhase.WARMING) warming.complete(Unit)
                    if (event.phase == TrafficWarmupLifecyclePhase.MAIN_READY) mainReady.complete(event)
                }
            },
            send = { message ->
                if (message is ControlRecordCompleted) {
                    sendStarted.complete(Unit)
                    try {
                        awaitCancellation()
                    } finally {
                        sendStopped.complete(Unit)
                    }
                } else send(message)
            }
        )
        val donor = stringConsumer(
            context(namespace, control), "records", source, warmup, group, CopyOnWriteArrayList()
        )
        val lifecycle = recipientContext.register(recipient)
        try {
            donor.start()
            withTimeout(15.seconds) { donor.awaitReady() }
            lifecycle.start()
            withTimeout(15.seconds) { warming.await() }
            recipient.publishResult()
            withTimeout(2.seconds) { sendStarted.await() }

            recipient.terminated.complete(Unit)

            val event = withTimeout(5.seconds) { mainReady.await() }
            lifecycle.awaitReady()
            assertTrue(sendStopped.isCompleted)
            assertTrue(recipient.warmupStopped.isCompleted)
            assertEquals("recipient_consumer_terminated", event.reasonCode)
            assertTrue(event.recoveryDurationMillis != null)
        } finally {
            try {
                lifecycle.stop()
            } finally {
                donor.stop()
            }
        }
    }

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
    fun `outbox ACK timeout after DonorClosed publishes one cold fallback`() = runBlocking {
        verifyLateResultTimeout()
    }

    @Test
    fun `Kafka producer timeout after DonorClosed publishes one cold fallback`() = runBlocking {
        verifyLateResultTimeout(KafkaTimeoutException("result delivery expired"))
    }

    @Test
    fun `recipient recovers when a result sender stops during the cleanup retry`() = runBlocking {
        verifyLateResultTimeout(cleanup = LateResultCleanup.RETRY)
    }

    @Test
    fun `participant stop during sender cleanup retains the result timeout for the surviving participant`() = runBlocking {
        verifyLateResultTimeout(cleanup = LateResultCleanup.STOP_PARTICIPANT)
    }

    @Test
    fun `participant stop during COMPLETED acknowledgement keeps the selected outcome`() = runBlocking {
        val source = unique("terminal-ack-source")
        val warmup = unique("terminal-ack-warmup")
        val control = unique("terminal-ack-control")
        val namespace = unique("terminal-ack")
        val group = unique("terminal-ack-group")
        createTopics(source, warmup, control)
        val fenceAccepted = CompletableDeferred<SessionEnded>()
        val publicationStopped = CompletableDeferred<Unit>()
        val finished = CompletableDeferred<TrafficWarmupLifecycleEvent>()
        val events = CopyOnWriteArrayList<TrafficWarmupLifecycleEvent>()
        val recipientContext = faultInjectionContext(namespace, control, configure = {
            warmupDuration = 1.seconds
            donorLivenessTimeout = 30.seconds
            onLifecycleEvent { event ->
                events += event
                if (event.phase == TrafficWarmupLifecyclePhase.RECIPIENT_FINISHED) finished.complete(event)
            }
        }, send = { message ->
            val ack = send(message)
            if (message is SessionEnded) {
                fenceAccepted.complete(message)
                try {
                    awaitCancellation()
                } finally {
                    publicationStopped.complete(Unit)
                }
            }
            ack
        })
        val donor = stringConsumer(
            context(namespace, control), "records", source, warmup, group, CopyOnWriteArrayList()
        )
        val recipient = stringConsumer(recipientContext, "records", source, warmup, group, CopyOnWriteArrayList())
        try {
            donor.start()
            withTimeout(15.seconds) { donor.awaitReady() }
            recipient.start()
            val fence = withTimeout(15.seconds) { fenceAccepted.await() }
            assertEquals(WarmupSessionEndState.COMPLETED, fence.state)
            assertFalse(finished.isCompleted)

            withTimeout(8.seconds) { recipient.stop() }

            val outcome = withTimeout(2.seconds) { finished.await() }
            assertTrue(publicationStopped.isCompleted)
            assertEquals(fence.sessionId.value, outcome.sessionId)
            assertEquals(fence.reasonCode, outcome.reasonCode)
            assertFalse(events.any { it.phase == TrafficWarmupLifecyclePhase.COLD_FALLBACK })
            assertEquals(listOf(fence), sessionEndRecords(control, fence.sessionId))
        } finally {
            try {
                recipient.stop()
            } finally {
                donor.stop()
            }
        }
    }

    private enum class LateResultCleanup { IMMEDIATE, RETRY, STOP_PARTICIPANT }

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

    private suspend fun verifyLateResultTimeout(
        kafkaFailure: KafkaTimeoutException? = null,
        cleanup: LateResultCleanup = LateResultCleanup.IMMEDIATE
    ) {
        val source = unique("late-result-ack-source")
        val warmup = unique("late-result-ack-warmup")
        val control = unique("late-result-ack-control")
        val namespace = unique("late-result-ack")
        val group = unique("late-result-ack-group")
        val siblingSource = unique("late-result-sibling-source")
        val siblingWarmup = unique("late-result-sibling-warmup")
        val siblingGroup = unique("late-result-sibling-group")
        val stopSibling = cleanup == LateResultCleanup.STOP_PARTICIPANT
        if (stopSibling) createTopics(source, warmup, control, siblingSource, siblingWarmup)
        else createTopics(source, warmup, control)
        val firstReply = CompletableDeferred<Unit>()
        val donorClosed = CompletableDeferred<DonorClosed>()
        val donorContext = faultInjectionContext(namespace, control, send = { message ->
            send(message).also {
                if (message is WarmupLivenessProbe && message.reply) firstReply.complete(Unit)
                if (message is DonorClosed) donorClosed.complete(message)
            }
        })
        val donorMetrics = RecordingWarmupMetrics<String, String>()
        val donor = stringConsumer(
            donorContext, "records", source, warmup, group, CopyOnWriteArrayList(), consumerMetrics = donorMetrics
        )
        val siblingDonor = if (stopSibling) stringConsumer(
            donorContext, "sibling", siblingSource, siblingWarmup, siblingGroup, CopyOnWriteArrayList()
        ) else null
        val donors = consumerContainer(*listOfNotNull(donor, siblingDonor).toTypedArray())
        val resultSent = CompletableDeferred<Unit>()
        val resultStopped = CompletableDeferred<Unit>()
        val sendCancellationStarted = CompletableDeferred<Unit>()
        val releaseCancelledSend = CompletableDeferred<Unit>()
        val sendCleanupTimeout = 2.seconds
        val warmupStopped = CompletableDeferred<Unit>()
        val releaseKafkaFailure = CompletableDeferred<Unit>()
        val drainOutcome = CompletableDeferred<LifecycleOutcome>()
        val sendFailure = CompletableDeferred<WarmupResultDeliveryFailureReason>()
        val recipientMetrics = object : ConsumerMetrics<String, String>, WarmupMetrics<String, String> {
            override fun onShutdown(durationNanos: Long, outcome: LifecycleOutcome) {
                warmupStopped.complete(Unit)
            }

            override fun onWarmupResultDrain(durationNanos: Long, outcome: LifecycleOutcome) {
                drainOutcome.complete(outcome)
            }

            override fun onWarmupResultDeliveryFailure(reason: WarmupResultDeliveryFailureReason) {
                sendFailure.complete(reason)
            }
        }
        val interruption = CompletableDeferred<TrafficWarmupLifecycleEvent>()
        val mainReady = CompletableDeferred<TrafficWarmupLifecycleEvent>()
        val finished = CompletableDeferred<TrafficWarmupLifecycleEvent>()
        val recipientContext = faultInjectionContext(namespace, control, configure = {
            warmupDuration = 4.seconds
            completionTimeout = sendCleanupTimeout
            onLifecycleEvent { event ->
                when (event.phase) {
                    TrafficWarmupLifecyclePhase.WARMUP_INTERRUPTED -> interruption.complete(event)
                    TrafficWarmupLifecyclePhase.RECIPIENT_FINISHED -> finished.complete(event)
                    TrafficWarmupLifecyclePhase.MAIN_READY -> if (event.participantId == "records") {
                        mainReady.complete(event)
                    }
                    else -> Unit
                }
            }
        }, send = { message ->
            val ack = send(message)
            if (message is ControlRecordCompleted) {
                resultSent.complete(Unit)
                try {
                    if (kafkaFailure == null) awaitCancellation()
                    releaseKafkaFailure.await()
                    throw kafkaFailure
                } finally {
                    sendCancellationStarted.complete(Unit)
                    if (cleanup != LateResultCleanup.IMMEDIATE) {
                        withContext(NonCancellable) { releaseCancelledSend.await() }
                    }
                    resultStopped.complete(Unit)
                }
            }
            ack
        })
        val recipient = stringConsumer(
            recipientContext, "records", source, warmup, group, CopyOnWriteArrayList(), consumerMetrics = recipientMetrics
        )
        val siblingRecipient = if (stopSibling) stringConsumer(
            recipientContext, "sibling", siblingSource, siblingWarmup, siblingGroup, CopyOnWriteArrayList()
        ) else null
        val recipients = consumerContainer(*listOfNotNull(recipient, siblingRecipient).toTypedArray())
        try {
            donors.start()
            withTimeout(15.seconds) { donors.awaitReady() }
            recipients.start()
            withTimeout(15.seconds) { firstReply.await() }
            produce(source, "delegated", "delegated", StringSerializer())
            withTimeout(4.seconds) { resultSent.await() }
            awaitFor { donorMetrics.processed.any { it.processedRemotely } }
            val closed = withTimeout(8.seconds) {
                val message = donorClosed.await()
                warmupStopped.await()
                message
            }
            assertFalse(resultStopped.isCompleted)
            releaseKafkaFailure.complete(Unit)
            val interrupted = withTimeout(5.seconds) { interruption.await() }
            if (cleanup != LateResultCleanup.IMMEDIATE) {
                withTimeout(5.seconds) { sendCancellationStarted.await() }
                // Let the first cleanup wait expire while keeping the sender within the retry budget.
                delay(sendCleanupTimeout + 400.milliseconds)
                assertFalse(mainReady.isCompleted)
                if (siblingRecipient != null) coroutineScope {
                    // stop() cancels the shared startup before suspending on its completion.
                    val stop = async(start = CoroutineStart.UNDISPATCHED) { siblingRecipient.stop() }
                    try {
                        assertFalse(stop.isCompleted)
                    } finally {
                        releaseCancelledSend.complete(Unit)
                    }
                    withTimeout(5.seconds) { stop.await() }
                } else releaseCancelledSend.complete(Unit)
            }

            val ready = withTimeout(10.seconds) {
                recipient.awaitReady()
                mainReady.await()
            }
            val outcome = withTimeout(2.seconds) { finished.await() }
            assertTrue(resultStopped.isCompleted)
            assertEquals("result_delivery_timeout", interrupted.reasonCode)
            assertEquals(interrupted.reasonCode, ready.reasonCode)
            assertEquals(interrupted.sessionId, ready.sessionId)
            assertEquals(ready.reasonCode, outcome.reasonCode)
            assertEquals(ready.sessionId, outcome.sessionId)
            assertTrue(requireNotNull(ready.recoveryDurationMillis) >= 0)
            assertEquals(closed.sessionId.value, ready.sessionId)
            assertEquals(LifecycleOutcome.TIMEOUT, withTimeout(2.seconds) { drainOutcome.await() })
            assertEquals(WarmupResultDeliveryFailureReason.TIMEOUT, withTimeout(2.seconds) { sendFailure.await() })
            val fences = sessionEndRecords(control, closed.sessionId)
            assertEquals(listOf(WarmupSessionEndState.COLD_FALLBACK), fences.map { it.state })
            val fence = fences.single()
            assertEquals(ready.sessionId, fence.sessionId.value)
            assertEquals(ready.reasonCode, fence.reasonCode)
        } finally {
            releaseCancelledSend.complete(Unit)
            try {
                recipients.stop()
            } finally {
                donors.stop()
            }
        }
    }

    private suspend fun sessionEndRecords(controlTopic: String, sessionId: WarmupSessionId): List<SessionEnded> =
        sessionControlRecords(controlTopic, sessionId).filterIsInstance<SessionEnded>()

    private suspend fun sessionControlRecords(
        controlTopic: String,
        sessionId: WarmupSessionId
    ): List<WarmupControlMessage> {
        val properties = consumerProperties(unique("control-audit"), ByteArrayDeserializer::class.java)
        return KafkaConsumer<String, ByteArray>(properties).use { consumer ->
            val partition = TopicPartition(controlTopic, 0)
            consumer.assign(listOf(partition))
            consumer.seekToBeginning(listOf(partition))
            val end = consumer.endOffsets(listOf(partition), JavaDuration.ofSeconds(5)).getValue(partition)
            withTimeout(5.seconds) {
                val messages = mutableListOf<WarmupControlMessage>()
                while (consumer.position(partition, JavaDuration.ofSeconds(1)) < end) {
                    consumer.poll(JavaDuration.ofMillis(100)).forEach { record ->
                        if (record.offset() < end && record.key() == sessionId.value) {
                            messages += WarmupControlCodec.decode(record.value(), record.headers())
                        }
                    }
                }
                messages
            }
        }
    }

    private fun faultInjectionContext(
        namespace: String,
        controlTopic: String,
        configure: TrafficWarmupContextConfiguration.() -> Unit = {},
        send: suspend WarmupControlChannel.(WarmupControlMessage) -> RecordMetadata = { send(it) }
    ): TrafficWarmupContext {
        val configuration = TrafficWarmupContextConfiguration().apply {
            this.namespace = namespace
            this.controlTopic = controlTopic
            controlProducerProperties = kafkaConnection()
            controlConsumerProperties = kafkaConnection()
            offerWindow = 500.milliseconds
            preparationTimeout = 3.seconds
            configure()
        }
        return TrafficWarmupContext(configuration) { config, handler, scope, dispatcher ->
            val transport = WarmupControlTransport(
                config, handler, parentContext = scope.coroutineContext, dispatcher = dispatcher
            )
            object : WarmupControlChannel by transport {
                override suspend fun send(message: WarmupControlMessage) = send(transport, message)
            }
        }
    }

    private class ControllableRecipient(
        source: String,
        private val warmup: String,
        override val warmupConsumerProperties: Map<String, Any?>,
        private val stopConsumer: suspend () -> Unit = {},
        private val abortConsumer: suspend () -> Boolean = { false }
    ) : RegisteredTrafficWarmupParticipant {
        override val participantId = WarmupParticipantId("records")
        private val quota = WarmupMappingQuota(10)
        override val mappings = listOf(
            WarmupTopicMapping(
                participantId, source, warmup,
                mappingFingerprint(
                    participantId,
                    TrafficWarmupConsumerConfiguration.Mapping(source, warmup, quota),
                    warmupConsumerProperties
                ),
                quota
            )
        )
        override val mainConsumer = mock<CoroutinesKafkaConsumer<String, String>>()
        val terminated = CompletableDeferred<Unit>()
        val warmupStopped = CompletableDeferred<Unit>()
        private val resultPublisher = CompletableDeferred<Pair<WarmupSessionId, WarmupResultLane>>()

        init {
            val runtimeControl = mock<avh.ckc.core.extension.ConsumerRuntimeControl>()
            whenever(runtimeControl.isShutdownComplete()).thenReturn(true)
            whenever(mainConsumer.runtimeControl).thenReturn(runtimeControl)
        }

        override fun prepareDonorLane(
            acceptance: SessionAccepted,
            quotaPhases: WarmupDonorQuotaPhases.Session
        ): WarmupDonorSessionLifecycle =
            error("The recipient is never selected as a donor in this test")

        override suspend fun prepareRecipient(
            sessionId: WarmupSessionId,
            start: WarmupDestinationStart,
            completionOutbox: WarmupCompletionOutbox
        ): PreparedWarmupRecipientConsumer {
            resultPublisher.complete(sessionId to completionOutbox.lane("records"))
            return object : PreparedWarmupRecipientConsumer {
                override val assignment = start.assignment
                override val startOffset = start.offset
                override fun start() = Unit
                override suspend fun awaitReady() = Unit
                override suspend fun awaitTermination() = terminated.await()
                override suspend fun stop() {
                    stopConsumer()
                    warmupStopped.complete(Unit)
                }
                override suspend fun abort(): Boolean = abortConsumer().also { stopped ->
                    if (stopped) warmupStopped.complete(Unit)
                }
            }
        }

        suspend fun publishResult() {
            val (session, lane) = resultPublisher.await()
            assertTrue(lane.tryAcquire(ConsumerRecord(warmup, 0, 0, "key", "value")))
            lane.publish(
                WarmupRecordCompleted(WarmupRecordId(session, TopicPartition(warmup, 0), 0),
                    WarmupRemoteOutcome.Succeeded)
            )
        }

        override fun startMainRuntime() = Unit
        override suspend fun awaitMainReady() = Unit
        override suspend fun stopMain() = Unit
        override suspend fun awaitMainTermination(): Unit = awaitCancellation()
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
        val topics = TypedTopics.create()
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

    @Test
    fun `public donor limit sends a saturated participant local and then resumes delegation`() = runBlocking {
        val firstSource = unique("inflight-first-source")
        val firstWarmup = unique("inflight-first-warmup")
        val secondSource = unique("inflight-second-source")
        val secondWarmup = unique("inflight-second-warmup")
        val control = unique("inflight-control")
        val namespace = unique("inflight-namespace")
        createTopics(firstSource, firstWarmup, secondSource, secondWarmup, control)
        val donorContext = trafficWarmupContext {
            this.namespace = namespace
            controlTopic = control
            controlProducerProperties = kafkaConnection()
            controlConsumerProperties = kafkaConnection()
            offerWindow = 250.milliseconds
            delegationTimeout = 20.seconds
            maxInflightDelegations = 1
        }
        val recipientContext = context(namespace, control, warmupDuration = 30.seconds)
        val firstGroup = unique("inflight-first-group")
        val secondGroup = unique("inflight-second-group")
        val firstEntered = CompletableDeferred<Unit>()
        val releaseFirst = CompletableDeferred<Unit>()
        val firstDonorHandled = CopyOnWriteArrayList<String>()
        val secondDonorHandled = CopyOnWriteArrayList<String>()
        val secondRecipientHandled = CopyOnWriteArrayList<String>()
        val firstMetrics = RecordingWarmupMetrics<String, String>()
        val secondMetrics = RecordingWarmupMetrics<String, String>()
        val donor = consumerContainer(
            stringConsumer(
                donorContext, "first", firstSource, firstWarmup, firstGroup, firstDonorHandled,
                consumerMetrics = firstMetrics
            ),
            stringConsumer(
                donorContext, "second", secondSource, secondWarmup, secondGroup, secondDonorHandled,
                consumerMetrics = secondMetrics
            )
        )
        val recipient = consumerContainer(
            stringConsumer(
                recipientContext, "first", firstSource, firstWarmup, firstGroup, CopyOnWriteArrayList(),
                beforeHandle = {
                    firstEntered.complete(Unit)
                    releaseFirst.await()
                }
            ),
            stringConsumer(
                recipientContext, "second", secondSource, secondWarmup, secondGroup, secondRecipientHandled
            )
        )
        try {
            donor.start()
            withTimeout(15.seconds) { donor.awaitReady() }
            recipient.start()
            // A probe reply establishes that the donor has processed readiness and activated its lanes.
            val controlProperties = consumerProperties(unique("inflight-control-observer"), ByteArrayDeserializer::class.java)
            KafkaConsumer<String, ByteArray>(controlProperties).use { observer ->
                val partition = TopicPartition(control, 0)
                observer.assign(listOf(partition))
                observer.seekToBeginning(listOf(partition))
                withTimeout(15.seconds) {
                    while (true) {
                        val active = observer.poll(JavaDuration.ofMillis(100)).any { record ->
                            val message = WarmupControlCodec.decode(record.value(), record.headers())
                            message is WarmupLivenessProbe && message.reply
                        }
                        if (active) break
                        yield()
                    }
                }
            }

            produce(firstSource, "key", "held", StringSerializer())
            withTimeout(10.seconds) { firstEntered.await() }
            awaitFor { firstMetrics.warmupPending.lastOrNull() == 1 }
            produce(secondSource, "key", "local", StringSerializer())
            awaitFor { secondMetrics.processed.any { it.value == "local" && !it.processedRemotely } }
            assertEquals(listOf("local"), secondDonorHandled.toList())
            assertTrue(secondMetrics.warmupDelegated.isEmpty())
            assertTrue(secondMetrics.warmupFallbacks.isEmpty())
            assertTrue(firstMetrics.processed.isEmpty())

            releaseFirst.complete(Unit)
            awaitFor { firstMetrics.processed.any { it.value == "held" && it.processedRemotely } }
            produce(secondSource, "key", "remote", StringSerializer())
            awaitFor { secondMetrics.processed.any { it.value == "remote" && it.processedRemotely } }
            assertEquals(listOf("remote"), secondRecipientHandled.toList())
            assertEquals(listOf("local"), secondDonorHandled.toList())
            assertTrue(firstDonorHandled.isEmpty())
            assertEquals(listOf("remote"), secondMetrics.warmupDelegated.map { it.first.value() })
        } finally {
            releaseFirst.complete(Unit)
            try {
                recipient.stop()
            } finally {
                donor.stop()
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

    private fun TrafficWarmupForwardedEvent.recordId() = WarmupRecordId(
        WarmupSessionId(sessionId), TopicPartition(destinationTopic, destinationPartition), destinationOffset
    )

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

    private suspend fun awaitLifecycle(
        observer: KafkaBrokerLifecycleLog,
        matches: (BrokerLifecycleEvent) -> Boolean
    ): BrokerLifecycleEvent = withTimeout(12.seconds) {
        while (true) {
            val records = observer.poll(JavaDuration.ofMillis(100))
            records.firstOrNull { matches(it.event) }?.let { return@withTimeout it.event }
            yield()
        }
        error("Unreachable")
    }

    private suspend fun startBrokerWitness(bootstrap: String, topic: String, epoch: String): AutoCloseable {
        val directory = Files.createTempDirectory("ckc-broker-witness")
        val ready = directory.resolve("ready")
        val process = launchBrokerLifecycleCli("serve", bootstrap, topic, epoch, "10000", ready.toString())
        fun close() {
            process.destroyForcibly().waitFor(5, TimeUnit.SECONDS)
            Files.deleteIfExists(ready)
            Files.deleteIfExists(directory)
        }
        try {
            awaitFor {
                check(process.isAlive) { process.inputStream.bufferedReader().readText() }
                Files.exists(ready)
            }
        } catch (error: Throwable) {
            close()
            throw error
        }
        return AutoCloseable { close() }
    }

    @Test
    @Tag("without-micrometer")
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
        warmupDuration: kotlin.time.Duration = 750.milliseconds,
        applicationContext: TrafficWarmupContext? = null,
        lifecycleObserver: (TrafficWarmupLifecycleEvent) -> Unit = {}
    ): TypedApplication {
        val context = applicationContext ?: context(
            namespace,
            topics.control,
            warmupDuration = warmupDuration,
            lifecycleObserver = lifecycleObserver
        )
        val orderMetrics = RecordingWarmupMetrics<String, String>()
        val batchMetrics = RecordingWarmupMetrics<String, ByteArray>()
        val telemetryMetrics = RecordingWarmupMetrics<String, Long>()
        val ordersHandled = CopyOnWriteArrayList<String>()
        val batchesHandled = CopyOnWriteArrayList<ByteArray>()
        val telemetryHandled = CopyOnWriteArrayList<Long>()
        val order = coroutinesKafkaConsumer<String, String>(
            consumerProperties("$namespace-orders", StringDeserializer::class.java)
        ) {
            topics(topics.orders)
            metrics = orderMetrics
            commitIntervalMs = 50
            commitRecordsThreshold = 1
            handle { ordersHandled += it.value() }
            trafficWarmup(context, "orders") {
                mapTopic(topics.orders, topics.warmupOrders) {
                    quotaProfile = "1 -> (0s, step) -> 300"
                }
                warmupConsumerProperties = warmupConsumerConnection()
                forwardingProducerProperties = forwardingConnection(StringSerializer::class.java)
            }
        }
        val batch = coroutinesKafkaConsumer<String, ByteArray>(
            consumerProperties("$namespace-batches", ByteArrayDeserializer::class.java)
        ) {
            topics(topics.batches)
            metrics = batchMetrics
            commitIntervalMs = 50
            commitRecordsThreshold = 1
            handle { batchesHandled += it.value() }
            trafficWarmup(context, "batches") {
                mapTopic(topics.batches, topics.warmupBatches) {
                    quotaProfile = "1 -> (0s, step) -> 300"
                }
                warmupConsumerProperties = warmupConsumerConnection()
                forwardingProducerProperties = forwardingConnection(ByteArraySerializer::class.java)
            }
        }
        val telemetry = coroutinesKafkaConsumer<String, Long>(
            consumerProperties("$namespace-telemetry", LongDeserializer::class.java)
        ) {
            topics(topics.telemetry)
            metrics = telemetryMetrics
            commitIntervalMs = 50
            commitRecordsThreshold = 1
            handle { telemetryHandled += it.value() }
            trafficWarmup(context, "telemetry") {
                mapTopic(topics.telemetry, topics.warmupTelemetry) {
                    quotaProfile = "1 -> (0s, step) -> 300"
                }
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
        consumerMetrics: ConsumerMetrics<String, String> = object : ConsumerMetrics<String, String> {},
        unorderedWorkers: Int? = null
    ): CoroutinesKafkaConsumer<String, String> = coroutinesKafkaConsumer(
        mainProperties
    ) {
        topics(sourceTopic)
        commitIntervalMs = 50
        commitRecordsThreshold = 1
        metrics = consumerMetrics
        if (unorderedWorkers != null) {
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
            workerConcurrency = unorderedWorkers
        }
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
        warmupDuration: kotlin.time.Duration = 750.milliseconds,
        brokerLifecycleTopic: String? = null,
        brokerLifecycleConnection: Map<String, Any?> = kafkaConnection(),
        delegationTimeout: kotlin.time.Duration = 3.seconds,
        completionTimeout: kotlin.time.Duration = 10.seconds,
        lifecycleObserver: (TrafficWarmupLifecycleEvent) -> Unit = {}
    ) = trafficWarmupContext {
        this.namespace = namespace
        this.controlTopic = controlTopic
        controlProducerProperties = kafkaConnection()
        controlConsumerProperties = kafkaConnection()
        this.warmupDuration = warmupDuration
        offerWindow = 250.milliseconds
        this.participantStartTimeout = participantStartTimeout
        preparationTimeout = 3.seconds
        this.completionTimeout = completionTimeout
        this.delegationTimeout = delegationTimeout
        maxDonorSessions = 4
        maxAggregateQuotaPerSecond = 1_000
        if (brokerLifecycleTopic != null) {
            brokerLifecycle {
                topic = brokerLifecycleTopic
                kafkaProperties = brokerLifecycleConnection
            }
        }
        onLifecycleEvent(lifecycleObserver)
    }

    private data class TypedApplication(
        val order: CoroutinesKafkaConsumer<String, String>,
        val batch: CoroutinesKafkaConsumer<String, ByteArray>,
        val telemetry: CoroutinesKafkaConsumer<String, Long>,
        val ordersHandled: CopyOnWriteArrayList<String>,
        val batchesHandled: CopyOnWriteArrayList<ByteArray>,
        val telemetryHandled: CopyOnWriteArrayList<Long>,
        val orderMetrics: RecordingWarmupMetrics<String, String>,
        val batchMetrics: RecordingWarmupMetrics<String, ByteArray>,
        val telemetryMetrics: RecordingWarmupMetrics<String, Long>
    ) {
        private val consumers: ConsumerContainer = consumerContainer(order, batch, telemetry)

        fun handledCounts() = listOf(ordersHandled.size, batchesHandled.size, telemetryHandled.size)

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

    private fun <V> produce(
        topic: String,
        key: String,
        value: V,
        serializer: org.apache.kafka.common.serialization.Serializer<V>
    ): RecordMetadata = KafkaProducer(kafkaConnection(), StringSerializer(), serializer).use { producer ->
        producer.send(ProducerRecord(topic, key, value)).get(10, TimeUnit.SECONDS)
    }

    private fun <V> produceMany(
        topic: String,
        count: Int,
        serializer: org.apache.kafka.common.serialization.Serializer<V>,
        value: (Int) -> V
    ) {
        KafkaProducer(kafkaConnection(), StringSerializer(), serializer).use { producer ->
            val futures = List(count) { index ->
                producer.send(ProducerRecord(topic, "key-$index", value(index)))
            }
            futures.forEach { it.get(10, TimeUnit.SECONDS) }
        }
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

        private fun produceForeignControlHistory(
            connection: Map<String, Any?>,
            controlTopic: String,
            sourceTopic: String,
            warmupTopic: String,
            count: Int
        ) {
            val mapping = WarmupTopicMapping(
                WarmupParticipantId("foreign-participant"),
                sourceTopic,
                warmupTopic,
                "foreign-mapping-fingerprint"
            )
            val now = System.currentTimeMillis()
            KafkaProducer(connection, StringSerializer(), ByteArraySerializer()).use { producer ->
                repeat(count) { index ->
                    val sessionId = WarmupSessionId("foreign-$index")
                    val message = WarmupRequested(
                        namespace = WarmupControlNamespace("foreign-v6-namespace"),
                        sessionId = sessionId,
                        senderInstanceId = WarmupInstanceId("foreign-sender"),
                        mappings = listOf(mapping),
                        offerDeadlineEpochMillis = now + 60_000,
                        admissionBudgetPerSecond = normalizedAdmissionBudget(listOf(mapping)),
                        configurationFingerprint = "foreign-configuration-fingerprint",
                        expiresAtEpochMillis = now + 120_000
                    )
                    producer.send(
                        ProducerRecord(controlTopic, sessionId.value, WarmupControlCodec.encode(message))
                    ).get(5, TimeUnit.SECONDS)
                }
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

private class PersistedWarmupKafka : AutoCloseable {
    private val docker = DockerClientFactory.instance().client()
    private val volume = docker.createVolumeCmd().exec().name
    private val port = ServerSocket(0).use { it.localPort }
    private var kafka = newContainer()

    fun start() = kafka.start()

    fun bootstrapServers(): String = kafka.bootstrapServers

    fun kill() {
        docker.killContainerCmd(kafka.containerId).exec()
    }

    fun restart() {
        kafka.stop()
        kafka = newContainer()
        kafka.start()
    }

    override fun close() {
        try {
            kafka.stop()
        } finally {
            docker.removeVolumeCmd(volume).exec()
        }
    }

    private fun newContainer(): KafkaContainer = FixedPortKafkaContainer(
        DockerImageName.parse("apache/kafka:3.8.0")
    ).withFixedKafkaPort(port)
        .withEnv("CLUSTER_ID", "M8gW-iVjQkGqC1rH0vM4xA")
        .withEnv("KAFKA_LOG_DIRS", "/var/lib/kafka/data")
        .withCreateContainerCmdModifier { command ->
            val config = requireNotNull(command.hostConfig)
            config.withBinds(config.binds.orEmpty().toList() + Bind(volume, Volume("/var/lib/kafka/data")))
        }
}
