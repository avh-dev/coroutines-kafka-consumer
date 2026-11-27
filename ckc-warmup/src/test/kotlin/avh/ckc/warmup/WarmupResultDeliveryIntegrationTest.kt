package avh.ckc.warmup

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.consumerContainer
import avh.ckc.core.coroutinesKafkaConsumer
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.LifecycleOutcome
import avh.ckc.warmup.protocol.ControlRecordCompleted
import avh.ckc.warmup.protocol.DonorClosed
import avh.ckc.warmup.protocol.SessionEnded
import avh.ckc.warmup.protocol.WarmupControlCodec
import avh.ckc.warmup.protocol.WarmupLivenessProbe
import avh.ckc.warmup.protocol.WarmupSessionEndState
import java.time.Duration as JavaDuration
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
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
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException as KafkaTimeoutException
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Testcontainers

@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
internal class WarmupResultDeliveryIntegrationTest : WarmupContextKafkaFixture() {
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

    private enum class LateResultCleanup { IMMEDIATE, RETRY, STOP_PARTICIPANT }

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
}
