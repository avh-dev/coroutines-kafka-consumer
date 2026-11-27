package avh.ckc.warmup.dataplane

import avh.ckc.core.KafkaRecordHandler
import avh.ckc.core.ProcessingMode
import avh.ckc.core.awaitFor
import avh.ckc.core.createTestConsumer
import avh.ckc.core.processing.RecordProcessingOutcome
import avh.ckc.core.stringSerdeProperties
import avh.ckc.core.testRuntime
import avh.ckc.warmup.WarmupDonorDataPlaneFixture
import avh.ckc.warmup.donor.DonorSessionAdmission
import avh.ckc.warmup.donor.WarmupDestinationResolver
import avh.ckc.warmup.donor.WarmupDonorSession
import avh.ckc.warmup.protocol.WarmupMappingQuota
import avh.ckc.warmup.protocol.WarmupParticipantId
import avh.ckc.warmup.protocol.WarmupQuotaProfile
import avh.ckc.warmup.protocol.WarmupRecordCompleted
import avh.ckc.warmup.protocol.WarmupRecordId
import avh.ckc.warmup.protocol.WarmupRemoteOutcome
import avh.ckc.warmup.protocol.WarmupSessionId
import avh.ckc.warmup.protocol.WarmupTopicMapping
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.spy
import org.mockito.kotlin.whenever

class WarmupParticipantDataPlaneTest {
    @Test
    fun `active session cannot be detached while activation returns`() {
        val activationCommitted = CountDownLatch(1)
        val releaseActivation = CountDownLatch(1)
        val session = spy(idleSession("activation-wins"))
        doAnswer { invocation ->
            val activated = invocation.callRealMethod()
            activationCommitted.countDown()
            check(releaseActivation.await(2, TimeUnit.SECONDS)) { "Timed out releasing session activation" }
            activated
        }.whenever(session).activate()

        val plane = WarmupDonorDataPlaneFixture<String, String>(1, 10)
        assertEquals(DonorSessionAdmission.ACCEPTED, plane.admit(mappingFor(session), session))
        val executor = Executors.newSingleThreadExecutor()
        try {
            val activation = executor.submit<Boolean> { plane.store.activate(session.sessionId) }
            assertTrue(activationCommitted.await(2, TimeUnit.SECONDS))
            assertFalse(plane.removeDrainedSession(session.sessionId))
            assertEquals(1, plane.store.admittedSessionCount())

            releaseActivation.countDown()
            assertTrue(activation.get(2, TimeUnit.SECONDS))
            closeSessions(plane, session)
        } finally {
            releaseActivation.countDown()
            executor.shutdownNow()
        }
    }

    @Test
    fun `detached session cannot activate after lookup`() {
        val activationEntered = CountDownLatch(1)
        val releaseActivation = CountDownLatch(1)
        val session = spy(idleSession("detachment-wins"))
        doAnswer { invocation ->
            activationEntered.countDown()
            check(releaseActivation.await(2, TimeUnit.SECONDS)) { "Timed out releasing session activation" }
            invocation.callRealMethod()
        }.whenever(session).activate()

        val plane = WarmupDonorDataPlaneFixture<String, String>(1, 10)
        assertEquals(DonorSessionAdmission.ACCEPTED, plane.admit(mappingFor(session), session))
        val executor = Executors.newSingleThreadExecutor()
        try {
            val activation = executor.submit<Boolean> { plane.store.activate(session.sessionId) }
            assertTrue(activationEntered.await(2, TimeUnit.SECONDS))
            assertTrue(plane.removeDrainedSession(session.sessionId))
            assertEquals(0, plane.store.admittedSessionCount())

            releaseActivation.countDown()
            assertFalse(activation.get(2, TimeUnit.SECONDS))
        } finally {
            releaseActivation.countDown()
            executor.shutdownNow()
        }
    }

    @Test
    fun `rejected inflight removal leaves main runtime and sibling session operational`() = runBlocking {
        val plane = WarmupDonorDataPlaneFixture<String, String>(maxSessions = 2, maxAggregateBudgetPerSecond = 20)
        val firstForwarded = CompletableDeferred<WarmupForwardedEvent>()
        val secondForwarded = CompletableDeferred<WarmupForwardedEvent>()
        val first = pendingSession("first-runtime", 1, firstForwarded)
        val second = pendingSession("second-runtime", 2, secondForwarded)
        plane.admit(mappingFor(first), first)
        plane.admit(mappingFor(second), second)
        plane.store.activate(first.sessionId)
        plane.store.activate(second.sessionId)

        val localHandled = CompletableDeferred<Long>()
        val allowSecond = CompletableDeferred<Unit>()
        val allowThird = CompletableDeferred<Unit>()
        val consumer = createTestConsumer(
            records = listOf(record(1), record(2), record(3)),
            consumerProperties = stringSerdeProperties(),
            handler = KafkaRecordHandler { localHandled.complete(it.offset()) },
            workerConcurrency = 2,
            interceptor = plane.records,
            beforeAdmission = {
                when (it.offset()) {
                    2L -> allowSecond.await()
                    3L -> allowThird.await()
                    else -> Unit
                }
            }
        )
        consumer.start()

        val firstEvent = firstForwarded.await()
        awaitFor(2_000) { Unit.takeIf { first.pendingResultCount() == 1 } }
        assertFalse(plane.removeDrainedSession(first.sessionId))

        allowSecond.complete(Unit)
        val secondEvent = secondForwarded.await()
        awaitFor(2_000) { Unit.takeIf { second.pendingResultCount() == 1 } }
        assertEquals(WarmupResultTracker.Completion.DELIVERED, plane.store.acceptResult(secondEvent.completed()))
        plane.store.stopDelegatingAndAwaitInflight(second.sessionId, 1.seconds)
        assertTrue(plane.removeDrainedSession(second.sessionId))

        assertEquals(WarmupResultTracker.Completion.DELIVERED, plane.store.acceptResult(firstEvent.completed()))
        plane.store.stopDelegatingAndAwaitInflight(first.sessionId, 1.seconds)
        assertTrue(plane.removeDrainedSession(first.sessionId))

        allowThird.complete(Unit)
        assertEquals(3L, withTimeout(2_000) { localHandled.await() })
        consumer.stop()
        Unit
    }

    @Test
    fun `early removal with inflight is rejected while sibling session continues`() = runBlocking {
        val plane = WarmupDonorDataPlaneFixture<String, String>(maxSessions = 2, maxAggregateBudgetPerSecond = 20)
        val firstForwarded = CompletableDeferred<WarmupForwardedEvent>()
        val secondForwarded = CompletableDeferred<WarmupForwardedEvent>()
        val first = WarmupDonorSession(
            sessionId = WarmupSessionId("first-pending"),
            quotaProfile = WarmupQuotaProfile.constant(10),
            quotaInterval = 1.seconds,
            delegationTimeout = 5.seconds,
            destinationResolver = WarmupDestinationResolver { WarmupDestination("warmup") },
            sender = WarmupRecordSender { _, destination, _ ->
                WarmupSendAcknowledgement(TopicPartition(destination.topic, destination.partition), 1)
            },
            observer = forwardingObserver(firstForwarded)
        )
        val second = WarmupDonorSession(
            sessionId = WarmupSessionId("second-ready"),
            quotaProfile = WarmupQuotaProfile.constant(10),
            quotaInterval = 1.seconds,
            delegationTimeout = 5.seconds,
            destinationResolver = WarmupDestinationResolver { WarmupDestination("warmup") },
            sender = WarmupRecordSender { _, destination, _ ->
                WarmupSendAcknowledgement(TopicPartition(destination.topic, destination.partition), 2)
            },
            observer = forwardingObserver(secondForwarded)
        )
        plane.admit(mappingFor(first), first)
        plane.admit(mappingFor(second), second)
        plane.store.activate(first.sessionId)
        plane.store.activate(second.sessionId)

        val firstResult = async { plane.records.process(record(1)) { error("fallback not expected") } }
        val firstEvent = firstForwarded.await()
        assertFalse(firstResult.isCompleted)
        assertFalse(plane.removeDrainedSession(first.sessionId))

        val secondResult = async { plane.records.process(record(2)) { error("fallback not expected") } }
        val secondEvent = secondForwarded.await()
        assertEquals(WarmupResultTracker.Completion.DELIVERED, plane.store.acceptResult(secondEvent.completed()))
        assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, secondResult.await())
        assertFalse(firstResult.isCompleted)

        assertEquals(WarmupResultTracker.Completion.DELIVERED, plane.store.acceptResult(firstEvent.completed()))
        assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, firstResult.await())
        plane.store.stopDelegatingAndAwaitInflight(first.sessionId, 1.seconds)
        assertTrue(plane.removeDrainedSession(first.sessionId))
        closeSessions(plane, second)
    }

    @Test
    fun `external timeout cancels delegation without local fallback`() {
        val plane = WarmupDonorDataPlaneFixture<String, String>(maxSessions = 1, maxAggregateBudgetPerSecond = 10)
        val sent = CompletableDeferred<Unit>()
        val localCalls = AtomicInteger()
        val session = idleSession(
            id = "outer-timeout",
            sender = WarmupRecordSender { _, destination, _ ->
                sent.complete(Unit)
                WarmupSendAcknowledgement(TopicPartition(destination.topic, destination.partition), 1)
            }
        )
        plane.admit(mappingFor(session), session)
        plane.store.activate(session.sessionId)

        assertThrows(TimeoutCancellationException::class.java) {
            runBlocking {
                withTimeout(250) {
                    plane.records.process(record(1)) {
                        localCalls.incrementAndGet()
                        RecordProcessingOutcome.HandlerSucceeded
                    }
                }
            }
        }

        assertTrue(sent.isCompleted)
        assertEquals(0, localCalls.get())
        runBlocking { plane.store.stopDelegatingAndAwaitInflight(session.sessionId, 1.seconds) }
        assertTrue(plane.removeDrainedSession(session.sessionId))
    }

    @Test
    fun `remote wait keeps the partition ordering slot occupied`() = runBlocking {
        val plane = WarmupDonorDataPlaneFixture<String, String>(maxSessions = 1, maxAggregateBudgetPerSecond = 10)
        val events = CopyOnWriteArrayList<WarmupForwardedEvent>()
        val sends = AtomicInteger()
        val session = WarmupDonorSession(
            sessionId = WarmupSessionId("ordered"),
            quotaProfile = WarmupQuotaProfile.constant(10),
            quotaInterval = 1.seconds,
            delegationTimeout = 5.seconds,
            destinationResolver = WarmupDestinationResolver { WarmupDestination("warmup") },
            sender = WarmupRecordSender { _, destination, _ ->
                WarmupSendAcknowledgement(
                    TopicPartition(destination.topic, destination.partition),
                    sends.getAndIncrement().toLong()
                )
            },
            observer = object : WarmupDataPlaneObserver<String, String> {
                override fun onForwarded(
                    record: ConsumerRecord<String, String>,
                    event: WarmupForwardedEvent,
                    durationNanos: Long
                ) {
                    events += event
                }
            }
        )
        plane.admit(mappingFor(session), session)
        plane.store.activate(session.sessionId)
        val consumer = createTestConsumer(
            records = listOf(record(1), record(2)),
            consumerProperties = stringSerdeProperties(),
            handler = KafkaRecordHandler { error("local handler must not run") },
            workerConcurrency = 2,
            interceptor = plane.records,
            runtime = testRuntime(ProcessingMode.AT_LEAST_ONCE_PARTITION_ORDERING)
        )
        consumer.start()
        withTimeout(2_000) { while (events.size != 1) kotlinx.coroutines.yield() }
        assertEquals(1, sends.get())

        plane.store.acceptResult(events[0].completed())
        withTimeout(2_000) { while (events.size != 2) kotlinx.coroutines.yield() }
        plane.store.acceptResult(events[1].completed())

        consumer.stop()
        assertEquals(2, sends.get())
        closeSessions(plane, session)
    }

    @Test
    fun `one donor selects two active sessions fairly and delegates each record once`() = runBlocking {
        val plane = WarmupDonorDataPlaneFixture<String, String>(maxSessions = 2, maxAggregateBudgetPerSecond = 20)
        val firstSends = AtomicInteger()
        val secondSends = AtomicInteger()
        val localCalls = AtomicInteger()
        val first = successfulSession("first", firstSends, plane)
        val second = successfulSession("second", secondSends, plane)
        assertEquals(DonorSessionAdmission.ACCEPTED, plane.admit(mappingFor(first), first))
        assertEquals(DonorSessionAdmission.ACCEPTED, plane.admit(mappingFor(second), second))
        assertTrue(plane.store.activate(first.sessionId))
        assertTrue(plane.store.activate(second.sessionId))

        repeat(8) { offset ->
            val outcome = plane.records.process(record(offset.toLong())) {
                localCalls.incrementAndGet()
                RecordProcessingOutcome.HandlerSucceeded
            }
            assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, outcome)
        }

        assertEquals(4, firstSends.get())
        assertEquals(4, secondSends.get())
        assertEquals(8, firstSends.get() + secondSends.get())
        assertEquals(0, localCalls.get())
        closeSessions(plane, first, second)
    }

    @Test
    fun `admission checks session and aggregate budget limits atomically`() = runBlocking {
        val plane = WarmupDonorDataPlaneFixture<String, String>(maxSessions = 3, maxAggregateBudgetPerSecond = 10)
        val candidates = List(20) { index -> idleSession("session-$index", intervalBudget = 4) }

        val results = candidates.map { session ->
            async(Dispatchers.Default) { session to plane.admit(mappingFor(session), session) }
        }.map { it.await() }

        assertEquals(2, results.count { it.second == DonorSessionAdmission.ACCEPTED })
        assertEquals(18, results.count { it.second == DonorSessionAdmission.BUDGET_LIMIT })
        assertEquals(2, plane.store.admittedSessionCount())
        assertEquals(8, plane.store.admittedBudgetPerSecond())
        results.filter { it.second != DonorSessionAdmission.ACCEPTED }
            .forEach { it.first.close() }
        results.filter { it.second == DonorSessionAdmission.ACCEPTED }
            .forEach { closeSessions(plane, it.first) }
    }

    @Test
    fun `failure in one session falls back locally and leaves sibling active`() = runBlocking {
        val plane = WarmupDonorDataPlaneFixture<String, String>(maxSessions = 2, maxAggregateBudgetPerSecond = 20)
        val failedSends = AtomicInteger()
        val successfulSends = AtomicInteger()
        val localCalls = AtomicInteger()
        val failed = idleSession(
            id = "failed",
            sender = WarmupRecordSender { _, _, _ ->
                failedSends.incrementAndGet()
                throw IllegalStateException("send failed")
            }
        )
        val successful = successfulSession("healthy", successfulSends, plane)
        plane.admit(mappingFor(failed), failed)
        plane.admit(mappingFor(successful), successful)
        plane.store.activate(failed.sessionId)
        plane.store.activate(successful.sessionId)

        val fallback = plane.records.process(record(1)) {
            localCalls.incrementAndGet()
            RecordProcessingOutcome.HandlerSucceeded
        }
        val remote = plane.records.process(record(2)) {
            localCalls.incrementAndGet()
            RecordProcessingOutcome.HandlerSucceeded
        }

        assertEquals(RecordProcessingOutcome.HandlerSucceeded, fallback)
        assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, remote)
        assertEquals(1, failedSends.get())
        assertEquals(1, successfulSends.get())
        assertEquals(1, localCalls.get())
        assertFalse(failed.isActive())
        assertTrue(successful.isActive())
        closeSessions(plane, failed, successful)
    }

    @Test
    fun `cancellation while waiting for a result never invokes local fallback`() = runBlocking {
        val plane = WarmupDonorDataPlaneFixture<String, String>(maxSessions = 1, maxAggregateBudgetPerSecond = 10)
        val sent = CompletableDeferred<Unit>()
        val localCalls = AtomicInteger()
        val session = idleSession(
            id = "cancelled",
            sender = WarmupRecordSender { _, destination, _ ->
                sent.complete(Unit)
                WarmupSendAcknowledgement(TopicPartition(destination.topic, destination.partition), 1)
            }
        )
        plane.admit(mappingFor(session), session)
        plane.store.activate(session.sessionId)
        val processing = async {
            plane.records.process(record(1)) {
                localCalls.incrementAndGet()
                RecordProcessingOutcome.HandlerSucceeded
            }
        }
        sent.await()

        processing.cancelAndJoin()
        plane.store.stopDelegatingAndAwaitInflight(session.sessionId, 1.seconds)

        assertEquals(0, localCalls.get())
        assertEquals(0, session.pendingResultCount())
        assertTrue(plane.removeDrainedSession(session.sessionId))
        Unit
    }

    private fun mappingFor(session: WarmupDonorSession<String, String>) = WarmupTopicMapping(
        WarmupParticipantId("source"), "source", "warmup", "source-to-warmup",
        WarmupMappingQuota(session.admissionBudgetPerSecond)
    )

    private fun closeSessions(
        plane: WarmupDonorDataPlaneFixture<String, String>,
        vararg sessions: WarmupDonorSession<String, String>
    ) {
        sessions.forEach { session ->
            plane.store.stopDelegating(session.sessionId)
            assertTrue(plane.removeDrainedSession(session.sessionId))
        }
    }

    private fun successfulSession(
        id: String,
        sends: AtomicInteger,
        plane: WarmupDonorDataPlaneFixture<String, String>
    ): WarmupDonorSession<String, String> {
        val sessionId = WarmupSessionId(id)
        return idleSession(
            id = id,
            sender = WarmupRecordSender { _, destination, actualSessionId ->
                val offset = sends.getAndIncrement().toLong()
                val topicPartition = TopicPartition(destination.topic, destination.partition)
                plane.store.acceptResult(
                    WarmupRecordCompleted(
                        WarmupRecordId(actualSessionId, topicPartition, offset),
                        WarmupRemoteOutcome.Succeeded
                    )
                )
                WarmupSendAcknowledgement(topicPartition, offset)
            }
        ).also { assertEquals(sessionId, it.sessionId) }
    }

    private fun idleSession(
        id: String,
        intervalBudget: Int = 10,
        sender: WarmupRecordSender<String, String> = WarmupRecordSender { _, destination, _ ->
            WarmupSendAcknowledgement(TopicPartition(destination.topic, destination.partition), 0)
        }
    ): WarmupDonorSession<String, String> = WarmupDonorSession(
        sessionId = WarmupSessionId(id),
        quotaProfile = WarmupQuotaProfile.constant(intervalBudget),
        quotaInterval = 1.seconds,
        delegationTimeout = 5.seconds,
        destinationResolver = WarmupDestinationResolver { WarmupDestination("warmup") },
        sender = sender
    )

    private fun pendingSession(
        id: String,
        destinationOffset: Long,
        forwarded: CompletableDeferred<WarmupForwardedEvent>
    ): WarmupDonorSession<String, String> = WarmupDonorSession(
        sessionId = WarmupSessionId(id),
        quotaProfile = WarmupQuotaProfile.constant(10),
        quotaInterval = 1.seconds,
        delegationTimeout = 5.seconds,
        destinationResolver = WarmupDestinationResolver { WarmupDestination("warmup") },
        sender = WarmupRecordSender { _, destination, _ ->
            WarmupSendAcknowledgement(
                TopicPartition(destination.topic, destination.partition),
                destinationOffset
            )
        },
        observer = forwardingObserver(forwarded)
    )

    private fun record(offset: Long): ConsumerRecord<String, String> =
        ConsumerRecord("source", 0, offset, "key-$offset", "value-$offset")

    private fun forwardingObserver(target: CompletableDeferred<WarmupForwardedEvent>) =
        object : WarmupDataPlaneObserver<String, String> {
            override fun onForwarded(
                record: ConsumerRecord<String, String>,
                event: WarmupForwardedEvent,
                durationNanos: Long
            ) {
                target.complete(event)
            }
        }

    private fun WarmupForwardedEvent.completed() = WarmupRecordCompleted(
        WarmupRecordId(sessionId, destinationTopicPartition, destinationOffset),
        WarmupRemoteOutcome.Succeeded
    )
}
