package avh.ckc.core.warmup

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class TrafficWarmupContextTest {
    private val local = WarmupInstanceId("recipient")
    private val donor = WarmupInstanceId("donor")
    private val consumerId = WarmupConsumerId("orders")
    private val mapping = WarmupTopicMapping(consumerId, "orders", "warmup-orders")

    @Test
    fun `production bootstrap quarantine is fixed across local warmup settings`() {
        val short = context(warmupDuration = 20.milliseconds) { handler -> FakeControlChannel(handler) }
        val long = context(warmupDuration = 5.minutes) { handler -> FakeControlChannel(handler) }
        val testOverride = context(
            bootstrapRecoveryWindowOverride = 10.milliseconds
        ) { handler -> FakeControlChannel(handler) }

        assertEquals(605.seconds, short.bootstrapRecoveryWindow())
        assertEquals(605.seconds, long.bootstrapRecoveryWindow())
        assertEquals(10.milliseconds, testOverride.bootstrapRecoveryWindow())
    }

    @Test
    fun `authorization lifetime above namespace maximum is rejected`() {
        assertThrows(IllegalArgumentException::class.java) {
            context(warmupDuration = 10.minutes) { handler -> FakeControlChannel(handler) }
        }
    }

    @Test
    fun `no-source bootstrap retains observed reservation and becomes snapshot donor after quarantine`() = runBlocking {
        val participant = DonorParticipant(consumerId, listOf(mapping))
        lateinit var channel: FakeControlChannel
        val context = context(
            enforceDistributedProtocol = true,
            bootstrapRecoveryWindowOverride = 500.milliseconds
        ) { handler -> FakeControlChannel(handler).also { channel = it } }
        context.register(participant)
        context.start()
        withTimeout(2_000) { context.awaitReady() }
        assertTrue(participant.mainStarted)
        val localRequest = channel.sent.filterIsInstance<WarmupRequested>().single()
        assertFalse(channel.sent.any { it is ReservationRequested && it.senderInstanceId == local })

        val observedSession = WarmupSessionId("observed-during-quarantine")
        val observedRecipient = WarmupInstanceId("observed-recipient")
        val observedReservation = WarmupReservationId("observed-reservation")
        val observedExpiry = System.currentTimeMillis() + 60_000
        channel.deliver(
            WarmupRequested(
                observedSession, observedRecipient, listOf(mapping), System.currentTimeMillis() + 100,
                localRequest.configurationFingerprint, observedExpiry
            )
        )
        channel.deliver(
            ReservationRequested(
                observedSession, observedRecipient, observedReservation,
                listOf(WarmupPartitionAssignment(mapping, 0)),
                localRequest.configurationFingerprint, observedExpiry
            )
        )

        val earlyProbe = WarmupSessionId("early-probe")
        channel.deliver(
            WarmupRequested(
                earlyProbe, WarmupInstanceId("early-recipient"), listOf(mapping), System.currentTimeMillis() + 100,
                localRequest.configurationFingerprint, observedExpiry
            )
        )
        delay(50.milliseconds)
        assertFalse(channel.sent.filterIsInstance<DonorOffered>().any { it.sessionId == earlyProbe })
        assertFalse(channel.sent.any { it is ReservationRequested })

        delay(600.milliseconds)
        val lateProbe = WarmupSessionId("late-probe")
        channel.deliver(
            WarmupRequested(
                lateProbe, WarmupInstanceId("late-recipient"), listOf(mapping), System.currentTimeMillis() + 100,
                localRequest.configurationFingerprint, observedExpiry
            )
        )
        val lateOffer = withTimeout(2_000) {
            while (true) {
                channel.sent.filterIsInstance<DonorOffered>().firstOrNull { it.sessionId == lateProbe }
                    ?.let { return@withTimeout it }
                kotlinx.coroutines.yield()
            }
            error("unreachable")
        }
        val snapshot = requireNotNull(lateOffer.stateSnapshot)
        val observed = snapshot.sessions.single { it.sessionId == observedSession }
        assertEquals(observedReservation, observed.reservationId)
        assertEquals(listOf(WarmupPartitionAssignment(mapping, 0)), observed.assignments)
        assertTrue(lateOffer.mappings.isNotEmpty())
        context.stop()
    }

    @Test
    fun `metadata timeout performs cold fallback without reserving`() = runBlocking {
        val participant = FakeParticipant(consumerId, listOf(mapping))
        lateinit var channel: FakeControlChannel
        val context = context(enforceDistributedProtocol = true) { handler ->
            FakeControlChannel(
                handler = handler,
                response = { message, control ->
                    if (message is WarmupRequested) {
                        control.deliver(authoritativeOffer(message))
                    }
                },
                metadata = {
                    delay(1.seconds)
                    mapOf("control" to setOf(0), "warmup-orders" to setOf(0))
                }
            ).also { channel = it }
        }
        context.register(participant)

        context.start()
        withTimeout(2_000) { context.awaitReady() }

        assertEquals(listOf("main"), participant.events)
        assertFalse(channel.sent.any { it is ReservationRequested })
        context.stop()
    }

    @Test
    fun `metadata with no unreserved partition performs cold fallback`() = runBlocking {
        val participant = FakeParticipant(consumerId, listOf(mapping))
        lateinit var channel: FakeControlChannel
        val context = context(enforceDistributedProtocol = true) { handler ->
            FakeControlChannel(
                handler = handler,
                response = { message, control ->
                    if (message is WarmupRequested) {
                        val occupied = WarmupSessionSnapshot(
                            WarmupSessionId("occupied"),
                            WarmupInstanceId("other-recipient"),
                            message.configurationFingerprint,
                            WarmupReservationId("occupied-reservation"),
                            listOf(WarmupPartitionAssignment(mapping, 0)),
                            setOf(donor),
                            WarmupSessionLifecycle.READY,
                            message.expiresAtEpochMillis,
                            0
                        )
                        val joining = WarmupSessionSnapshot(
                            message.sessionId, local, message.configurationFingerprint, null, emptyList(), emptySet(),
                            WarmupSessionLifecycle.DISCOVERING, message.expiresAtEpochMillis, 0
                        )
                        control.deliver(
                            DonorOffered(
                                message.sessionId, donor, local,
                                listOf(WarmupDonorMapping(mapping, setOf(0))),
                                message.configurationFingerprint, message.expiresAtEpochMillis,
                                stateSnapshot = WarmupStateSnapshot(0, listOf(occupied, joining))
                            )
                        )
                    }
                },
                metadata = { mapOf("control" to setOf(0), "warmup-orders" to setOf(0)) }
            ).also { channel = it }
        }
        context.register(participant)

        context.start()
        withTimeout(2_000) { context.awaitReady() }

        assertEquals(listOf("main"), participant.events)
        assertFalse(channel.sent.any { it is ReservationRequested })
        context.stop()
    }

    @Test
    fun `recipient requests donor close before stopping warmup and starting main`() = runBlocking {
        val participant = FakeParticipant(consumerId, listOf(mapping))
        lateinit var channel: FakeControlChannel
        val context = context { handler ->
            FakeControlChannel(handler) { message, control ->
                when (message) {
                    is WarmupRequested -> control.deliver(
                        DonorOffered(
                            message.sessionId,
                            donor,
                            local,
                            listOf(WarmupDonorMapping(mapping, setOf(0))),
                            message.configurationFingerprint,
                            message.expiresAtEpochMillis
                        )
                    )
                    is SessionAccepted -> control.deliver(
                        DonorAccepted(
                            message.sessionId,
                            donor,
                            local,
                            message.configurationFingerprint,
                            message.expiresAtEpochMillis
                        )
                    )
                    is WarmupCompletionRequested -> control.deliver(
                        DonorClosed(
                            message.sessionId,
                            donor,
                            local,
                            message.configurationFingerprint,
                            message.expiresAtEpochMillis
                        )
                    )
                    else -> Unit
                }
            }.also { channel = it }
        }
        context.register(participant)

        context.start()
        context.start()
        withTimeout(2_000) { context.awaitReady() }

        assertEquals(listOf("recipient-start", "recipient-complete", "recipient-main"), participant.events)
        assertTrue(channel.sent.any { it is WarmupCompletionRequested })
        assertTrue(channel.sent.any { it is SessionEnded && it.state == WarmupSessionEndState.COMPLETED })
        context.stop()
    }

    @Test
    fun `recipient keeps all warmup consumers running until multiple donors close`() = runBlocking {
        val secondMapping = WarmupTopicMapping(WarmupConsumerId("batches"), "batches", "warmup-batches")
        val firstParticipant = FakeParticipant(consumerId, listOf(mapping))
        val secondParticipant = FakeParticipant(secondMapping.consumerId, listOf(secondMapping))
        val firstDonor = WarmupInstanceId("donor-a")
        val secondDonor = WarmupInstanceId("donor-b")
        val secondCloseRequested = CompletableDeferred<WarmupCompletionRequested>()
        val releaseSecondClose = CompletableDeferred<Unit>()
        lateinit var channel: FakeControlChannel
        val context = context { handler ->
            FakeControlChannel(handler) { message, control ->
                when (message) {
                    is WarmupRequested -> {
                        control.deliver(
                            DonorOffered(
                                message.sessionId, firstDonor, local,
                                listOf(WarmupDonorMapping(mapping, setOf(0))),
                                message.configurationFingerprint, message.expiresAtEpochMillis
                            )
                        )
                        control.deliver(
                            DonorOffered(
                                message.sessionId, secondDonor, local,
                                listOf(WarmupDonorMapping(secondMapping, setOf(0))),
                                message.configurationFingerprint, message.expiresAtEpochMillis
                            )
                        )
                    }
                    is SessionAccepted -> control.deliver(
                        DonorAccepted(
                            message.sessionId, message.targetInstanceId, local,
                            message.configurationFingerprint, message.expiresAtEpochMillis
                        )
                    )
                    is WarmupCompletionRequested -> if (message.targetInstanceId == firstDonor) {
                        control.deliver(
                            DonorClosed(
                                message.sessionId, firstDonor, local,
                                message.configurationFingerprint, message.expiresAtEpochMillis
                            )
                        )
                    } else {
                        secondCloseRequested.complete(message)
                        releaseSecondClose.await()
                        control.deliver(
                            DonorClosed(
                                message.sessionId, secondDonor, local,
                                message.configurationFingerprint, message.expiresAtEpochMillis
                            )
                        )
                    }
                    else -> Unit
                }
            }.also { channel = it }
        }
        context.register(firstParticipant)
        context.register(secondParticipant)

        context.start()
        val becomingReady = async { context.awaitReady() }
        withTimeout(2_000) { secondCloseRequested.await() }

        assertFalse(becomingReady.isCompleted)
        assertEquals(listOf("recipient-start"), firstParticipant.events)
        assertEquals(listOf("recipient-start"), secondParticipant.events)
        assertEquals(2, channel.sent.count { it is WarmupCompletionRequested })

        releaseSecondClose.complete(Unit)
        withTimeout(2_000) { becomingReady.await() }
        assertEquals(listOf("recipient-start", "recipient-complete", "recipient-main"), firstParticipant.events)
        assertEquals(listOf("recipient-start", "recipient-complete", "recipient-main"), secondParticipant.events)
        context.stop()
    }

    @Test
    fun `partial recipient preparation releases earlier resources and cold starts`() = runBlocking {
        val secondMapping = WarmupTopicMapping(WarmupConsumerId("batches"), "batches", "warmup-batches")
        val first = FakeParticipant(consumerId, listOf(mapping))
        val failing = FakeParticipant(secondMapping.consumerId, listOf(secondMapping), failRecipientPreparation = true)
        val context = context { handler ->
            FakeControlChannel(handler) { message, control ->
                when (message) {
                    is WarmupRequested -> control.deliver(
                        DonorOffered(
                            message.sessionId, donor, local,
                            listOf(
                                WarmupDonorMapping(mapping, setOf(0)),
                                WarmupDonorMapping(secondMapping, setOf(0))
                            ),
                            message.configurationFingerprint, message.expiresAtEpochMillis
                        )
                    )
                    is SessionAccepted -> control.deliver(
                        DonorAccepted(
                            message.sessionId, donor, local,
                            message.configurationFingerprint, message.expiresAtEpochMillis
                        )
                    )
                    else -> Unit
                }
            }
        }
        context.register(first)
        context.register(failing)

        context.start()
        withTimeout(2_000) { context.awaitReady() }

        assertEquals(listOf("recipient-stop", "main"), first.events)
        assertEquals(listOf("main"), failing.events)
        context.stop()
    }

    @Test
    fun `early donor close failure stops warmup and cold starts main`() = runBlocking {
        val participant = FakeParticipant(consumerId, listOf(mapping))
        lateinit var channel: FakeControlChannel
        val context = context { handler ->
            FakeControlChannel(handler) { message, control ->
                when (message) {
                    is WarmupRequested -> control.deliver(
                        DonorOffered(
                            message.sessionId, donor, local,
                            listOf(WarmupDonorMapping(mapping, setOf(0))),
                            message.configurationFingerprint, message.expiresAtEpochMillis
                        )
                    )
                    is SessionAccepted -> control.deliver(
                        DonorAccepted(
                            message.sessionId, donor, local,
                            message.configurationFingerprint, message.expiresAtEpochMillis
                        )
                    )
                    is WarmupCompletionRequested -> control.deliver(
                        SessionRejected(
                            message.sessionId, donor, local, "donor failed",
                            message.configurationFingerprint, message.expiresAtEpochMillis
                        )
                    )
                    else -> Unit
                }
            }.also { channel = it }
        }
        context.register(participant)

        context.start()
        withTimeout(2_000) { context.awaitReady() }

        assertEquals(
            listOf("recipient-start", "recipient-stop", "main"),
            participant.events,
            channel.sent.filterIsInstance<SessionEnded>().joinToString { it.reason.orEmpty() }
        )
        assertTrue(channel.sent.any {
            it is SessionEnded && it.state == WarmupSessionEndState.COLD_FALLBACK
        })
        context.stop()
    }

    @Test
    fun `donor rejection after readiness interrupts warmup duration`() = runBlocking {
        val participant = FakeParticipant(consumerId, listOf(mapping))
        lateinit var channel: FakeControlChannel
        val context = context(warmupDuration = 5.seconds) { handler ->
            FakeControlChannel(handler) { message, control ->
                when (message) {
                    is WarmupRequested -> control.deliver(
                        DonorOffered(
                            message.sessionId, donor, local,
                            listOf(WarmupDonorMapping(mapping, setOf(0))),
                            message.configurationFingerprint, message.expiresAtEpochMillis
                        )
                    )
                    is SessionAccepted -> control.deliver(
                        DonorAccepted(
                            message.sessionId, donor, local,
                            message.configurationFingerprint, message.expiresAtEpochMillis
                        )
                    )
                    is RecipientReady -> control.deliver(
                        SessionRejected(
                            message.sessionId, donor, local, "activation failed",
                            message.configurationFingerprint, message.expiresAtEpochMillis
                        )
                    )
                    else -> Unit
                }
            }.also { channel = it }
        }
        context.register(participant)

        context.start()
        withTimeout(1_000) { context.awaitReady() }

        assertEquals(listOf("recipient-start", "recipient-stop", "main"), participant.events)
        assertTrue(channel.sent.none { it is WarmupCompletionRequested })
        context.stop()
    }

    @Test
    fun `missing donor performs bounded cold start`() = runBlocking {
        val participant = FakeParticipant(consumerId, listOf(mapping))
        val context = context { handler -> FakeControlChannel(handler) }
        context.register(participant)

        context.start()
        withTimeout(2_000) { context.awaitReady() }

        assertEquals(listOf("main"), participant.events)
        context.stop()
    }

    @Test
    fun `unready control transport falls back to main within readiness deadline`() = runBlocking {
        val participant = FakeParticipant(consumerId, listOf(mapping))
        val neverReady = CompletableDeferred<Unit>()
        val context = context { handler -> FakeControlChannel(handler, readiness = neverReady) }
        context.register(participant)

        context.start()
        withTimeout(2_000) { context.awaitReady() }

        assertEquals(listOf("main"), participant.events)
        context.stop()
    }

    @Test
    fun `application stop while discovering never starts main`() = runBlocking {
        val participant = FakeParticipant(consumerId, listOf(mapping))
        lateinit var channel: FakeControlChannel
        val context = context { handler -> FakeControlChannel(handler).also { channel = it } }
        context.register(participant)
        context.start()
        withTimeout(2_000) { channel.firstRequest.await() }

        context.stop()

        assertFalse(participant.mainStarted)
        assertFalse(participant.recipientMainStarted)
    }

    @Test
    fun `cold started context remains available as donor for a later pod`() = runBlocking {
        val participant = DonorParticipant(consumerId, listOf(mapping))
        lateinit var channel: FakeControlChannel
        val context = context { handler -> FakeControlChannel(handler).also { channel = it } }
        context.register(participant)
        context.start()
        withTimeout(2_000) { context.awaitReady() }
        val localRequest = channel.sent.filterIsInstance<WarmupRequested>().single()
        val session = WarmupSessionId("later-recipient")
        val laterRecipient = WarmupInstanceId("later-recipient")
        val expires = System.currentTimeMillis() + 2_000

        channel.deliver(
            WarmupRequested(
                session,
                laterRecipient,
                listOf(mapping),
                System.currentTimeMillis() + 100,
                localRequest.configurationFingerprint,
                expires
            )
        )
        withTimeout(2_000) {
            while (channel.sent.none { it is DonorOffered && it.sessionId == session }) kotlinx.coroutines.yield()
        }
        val assignment = WarmupPartitionAssignment(mapping, 0)
        channel.deliver(
            ReservationRequested(
                session, laterRecipient, WarmupReservationId("later-reservation"), listOf(assignment),
                localRequest.configurationFingerprint, expires
            )
        )
        channel.deliver(
            SessionAccepted(
                session,
                laterRecipient,
                local,
                listOf(assignment),
                localRequest.configurationFingerprint,
                expires
            )
        )
        withTimeout(2_000) {
            while (channel.sent.none { it is DonorAccepted && it.sessionId == session }) kotlinx.coroutines.yield()
        }
        channel.deliver(
            RecipientReady(
                session,
                laterRecipient,
                local,
                listOf(assignment),
                localRequest.configurationFingerprint,
                expires
            )
        )
        withTimeout(2_000) {
            while ("activate" !in participant.donorEvents) kotlinx.coroutines.yield()
        }
        channel.deliver(
            WarmupCompletionRequested(
                session,
                laterRecipient,
                local,
                localRequest.configurationFingerprint,
                expires
            )
        )
        withTimeout(2_000) {
            while (channel.sent.none { it is DonorClosed && it.sessionId == session }) kotlinx.coroutines.yield()
        }

        assertEquals(listOf("prepare", "activate", "stop-inflight"), participant.donorEvents)
        context.stop()
    }

    @Test
    fun `released reservation rejects late acceptance and readiness`() = runBlocking {
        val participant = DonorParticipant(consumerId, listOf(mapping))
        lateinit var channel: FakeControlChannel
        val context = context { handler -> FakeControlChannel(handler).also { channel = it } }
        context.register(participant)
        context.start()
        withTimeout(2_000) { context.awaitReady() }
        val localRequest = channel.sent.filterIsInstance<WarmupRequested>().single()
        val session = WarmupSessionId("released-runtime")
        val recipient = WarmupInstanceId("released-recipient")
        val reservation = WarmupReservationId("released-generation")
        val assignment = WarmupPartitionAssignment(mapping, 0)
        val expires = System.currentTimeMillis() + 5_000
        channel.deliver(WarmupRequested(
            session, recipient, listOf(mapping), System.currentTimeMillis() + 100,
            localRequest.configurationFingerprint, expires
        ))
        channel.deliver(ReservationRequested(
            session, recipient, reservation, listOf(assignment), localRequest.configurationFingerprint, expires
        ))
        channel.deliver(ReservationReleased(
            session, recipient, reservation, WarmupReservationReleaseKind.NORMAL,
            System.currentTimeMillis(), localRequest.configurationFingerprint, expires
        ))
        channel.deliver(SessionAccepted(
            session, recipient, local, listOf(assignment), localRequest.configurationFingerprint, expires
        ))
        channel.deliver(RecipientReady(
            session, recipient, local, listOf(assignment), localRequest.configurationFingerprint, expires
        ))
        delay(50.milliseconds)

        assertTrue(participant.donorEvents.isEmpty())
        context.stop()
    }

    @Test
    fun `closing during donor preparation cleans resources and never activates`() = runBlocking {
        val dispatcher = Executors.newSingleThreadExecutor().asCoroutineDispatcher()
        val preparationEntered = CompletableDeferred<Unit>()
        val releasePreparation = CountDownLatch(1)
        try {
            val participant = DonorParticipant(
                consumerId, listOf(mapping), preparationEntered = preparationEntered,
                releasePreparation = releasePreparation
            )
            lateinit var channel: FakeControlChannel
            val context = context(parentContext = dispatcher) { handler ->
                FakeControlChannel(handler).also { channel = it }
            }
            context.register(participant)
            context.start()
            withTimeout(2_000) { context.awaitReady() }
            val localRequest = channel.sent.filterIsInstance<WarmupRequested>().single()
            val session = WarmupSessionId("closed-during-prepare")
            val recipient = WarmupInstanceId("preparing-recipient")
            val reservation = WarmupReservationId("preparing-generation")
            val assignment = WarmupPartitionAssignment(mapping, 0)
            val expires = System.currentTimeMillis() + 5_000
            channel.deliver(WarmupRequested(
                session, recipient, listOf(mapping), System.currentTimeMillis() + 100,
                localRequest.configurationFingerprint, expires
            ))
            channel.deliver(ReservationRequested(
                session, recipient, reservation, listOf(assignment), localRequest.configurationFingerprint, expires
            ))
            channel.deliver(SessionAccepted(
                session, recipient, local, listOf(assignment), localRequest.configurationFingerprint, expires
            ))
            withTimeout(2_000) { preparationEntered.await() }
            channel.deliver(SessionEnded(
                session, recipient, WarmupSessionEndState.COLD_FALLBACK, "recipient stopped",
                localRequest.configurationFingerprint, expires
            ))
            releasePreparation.countDown()
            withTimeout(2_000) {
                while ("finish" !in participant.donorEvents) kotlinx.coroutines.yield()
            }
            channel.deliver(RecipientReady(
                session, recipient, local, listOf(assignment), localRequest.configurationFingerprint, expires
            ))

            assertEquals(1, participant.donorEvents.count { it == "prepare" })
            assertEquals(1, participant.donorEvents.count { it == "finish" })
            assertTrue("activate" !in participant.donorEvents)
            context.stop()
        } finally {
            releasePreparation.countDown()
            dispatcher.close()
        }
    }

    @Test
    fun `duplicate and invalid donor control messages do not duplicate runtime`() = runBlocking {
        val participant = DonorParticipant(consumerId, listOf(mapping))
        lateinit var channel: FakeControlChannel
        val context = context { handler -> FakeControlChannel(handler).also { channel = it } }
        context.register(participant)
        context.start()
        withTimeout(2_000) { context.awaitReady() }
        val localRequest = channel.sent.filterIsInstance<WarmupRequested>().single()
        val session = WarmupSessionId("idempotent-runtime")
        val recipient = WarmupInstanceId("idempotent-recipient")
        val assignment = WarmupPartitionAssignment(mapping, 0)
        val expires = System.currentTimeMillis() + 5_000
        channel.deliver(WarmupRequested(
            session, recipient, listOf(mapping), System.currentTimeMillis() + 100,
            localRequest.configurationFingerprint, expires
        ))
        channel.deliver(ReservationRequested(
            session, recipient, WarmupReservationId("idempotent-generation"), listOf(assignment),
            localRequest.configurationFingerprint, expires
        ))
        val accepted = SessionAccepted(
            session, recipient, local, listOf(assignment), localRequest.configurationFingerprint, expires
        )
        channel.deliver(accepted)
        channel.deliver(accepted)
        withTimeout(2_000) {
            while (channel.sent.none { it is DonorAccepted && it.sessionId == session }) kotlinx.coroutines.yield()
        }
        channel.deliver(accepted.copy(senderInstanceId = WarmupInstanceId("wrong-recipient")))
        channel.deliver(accepted.copy(assignments = listOf(WarmupPartitionAssignment(mapping, 1))))
        channel.deliver(accepted.copy(expiresAtEpochMillis = expires + 1))
        val ready = RecipientReady(
            session, recipient, local, listOf(assignment), localRequest.configurationFingerprint, expires
        )
        channel.deliver(ready.copy(assignments = listOf(WarmupPartitionAssignment(mapping, 1))))
        channel.deliver(ready)
        channel.deliver(ready)
        withTimeout(2_000) {
            while ("activate" !in participant.donorEvents) kotlinx.coroutines.yield()
        }
        channel.deliver(accepted)
        val completedId = WarmupRecordId(session, assignment.destination, 17)
        channel.deliver(
            ControlRecordCompleted(
                session,
                recipient,
                local,
                WarmupRecordCompleted(completedId, WarmupRemoteOutcome.Succeeded),
                localRequest.configurationFingerprint,
                expires
            )
        )
        withTimeout(2_000) {
            while (completedId !in participant.accepted) kotlinx.coroutines.yield()
        }

        assertEquals(1, participant.donorEvents.count { it == "prepare" })
        assertEquals(1, participant.donorEvents.count { it == "activate" })
        assertEquals(listOf(completedId), participant.accepted)
        context.stop()
    }

    @Test
    fun `expired reservation never starts delayed donor runtime`() = runBlocking {
        val participant = DonorParticipant(consumerId, listOf(mapping))
        lateinit var channel: FakeControlChannel
        val context = context { handler -> FakeControlChannel(handler).also { channel = it } }
        context.register(participant)
        context.start()
        withTimeout(2_000) { context.awaitReady() }
        val localRequest = channel.sent.filterIsInstance<WarmupRequested>().single()
        val session = WarmupSessionId("expired-runtime")
        val recipient = WarmupInstanceId("expired-recipient")
        val assignment = WarmupPartitionAssignment(mapping, 0)
        val expired = System.currentTimeMillis() - 1
        channel.deliver(WarmupRequested(
            session, recipient, listOf(mapping), expired,
            localRequest.configurationFingerprint, expired
        ))
        channel.deliver(ReservationRequested(
            session, recipient, WarmupReservationId("expired-generation"), listOf(assignment),
            localRequest.configurationFingerprint, expired
        ))
        channel.deliver(SessionAccepted(
            session, recipient, local, listOf(assignment), localRequest.configurationFingerprint, expired
        ))
        channel.deliver(RecipientReady(
            session, recipient, local, listOf(assignment), localRequest.configurationFingerprint, expired
        ))
        delay(50.milliseconds)

        assertTrue(participant.donorEvents.isEmpty())
        context.stop()
    }

    @Test
    fun `rejecting stale session leaves another donor session operational`() = runBlocking {
        val participant = DonorParticipant(consumerId, listOf(mapping))
        lateinit var channel: FakeControlChannel
        val context = context { handler -> FakeControlChannel(handler).also { channel = it } }
        context.register(participant)
        context.start()
        withTimeout(2_000) { context.awaitReady() }
        val localRequest = channel.sent.filterIsInstance<WarmupRequested>().single()
        val recipient = WarmupInstanceId("parallel-recipient")
        val expires = System.currentTimeMillis() + 5_000
        val assignment = WarmupPartitionAssignment(mapping, 0)
        val first = WarmupSessionId("stale-parallel-session")
        val second = WarmupSessionId("live-parallel-session")
        suspend fun accept(session: WarmupSessionId, reservation: WarmupReservationId, partition: Int) {
            val assigned = WarmupPartitionAssignment(mapping, partition)
            channel.deliver(WarmupRequested(
                session, recipient, listOf(mapping), System.currentTimeMillis() + 100,
                localRequest.configurationFingerprint, expires
            ))
            channel.deliver(ReservationRequested(
                session, recipient, reservation, listOf(assigned), localRequest.configurationFingerprint, expires
            ))
            channel.deliver(SessionAccepted(
                session, recipient, local, listOf(assigned), localRequest.configurationFingerprint, expires
            ))
        }
        accept(first, WarmupReservationId("stale-parallel-generation"), 0)
        accept(second, WarmupReservationId("live-parallel-generation"), 1)
        withTimeout(2_000) {
            while (participant.preparedSessions.size < 2) kotlinx.coroutines.yield()
        }
        channel.deliver(ReservationReleased(
            first, recipient, WarmupReservationId("stale-parallel-generation"),
            WarmupReservationReleaseKind.NORMAL, System.currentTimeMillis(),
            localRequest.configurationFingerprint, expires
        ))
        channel.deliver(RecipientReady(
            first, recipient, local, listOf(assignment), localRequest.configurationFingerprint, expires
        ))
        channel.deliver(RecipientReady(
            second, recipient, local, listOf(WarmupPartitionAssignment(mapping, 1)),
            localRequest.configurationFingerprint, expires
        ))
        withTimeout(2_000) {
            while (second !in participant.activatedSessions) kotlinx.coroutines.yield()
        }

        assertTrue(first !in participant.activatedSessions)
        assertTrue(second in participant.activatedSessions)
        assertTrue(participant.mainStarted)
        context.stop()
    }

    @Test
    fun `retirement aborts only donor sessions through cutoff`() = runBlocking {
        val participant = DonorParticipant(consumerId, listOf(mapping))
        lateinit var channel: FakeControlChannel
        val context = context { handler -> FakeControlChannel(handler).also { channel = it } }
        context.register(participant)
        context.start()
        withTimeout(2_000) { context.awaitReady() }
        val localRequest = channel.sent.filterIsInstance<WarmupRequested>().single()
        val recipient = WarmupInstanceId("retirement-recipient")
        val retiredSession = WarmupSessionId("retired-runtime-session")
        val liveSession = WarmupSessionId("live-runtime-session")
        val retiredExpiry = System.currentTimeMillis() + 5_000
        val liveExpiry = retiredExpiry + 5_000

        suspend fun activate(
            session: WarmupSessionId,
            reservation: WarmupReservationId,
            partition: Int,
            expiresAt: Long
        ) {
            val assignment = WarmupPartitionAssignment(mapping, partition)
            channel.deliver(WarmupRequested(
                session, recipient, listOf(mapping), System.currentTimeMillis() + 100,
                localRequest.configurationFingerprint, expiresAt
            ))
            channel.deliver(ReservationRequested(
                session, recipient, reservation, listOf(assignment),
                localRequest.configurationFingerprint, expiresAt
            ))
            channel.deliver(SessionAccepted(
                session, recipient, local, listOf(assignment),
                localRequest.configurationFingerprint, expiresAt
            ))
            withTimeout(2_000) {
                while (channel.sent.none { it is DonorAccepted && it.sessionId == session }) kotlinx.coroutines.yield()
            }
            channel.deliver(RecipientReady(
                session, recipient, local, listOf(assignment),
                localRequest.configurationFingerprint, expiresAt
            ))
            withTimeout(2_000) {
                while (session !in participant.activatedSessions) kotlinx.coroutines.yield()
            }
        }

        activate(retiredSession, WarmupReservationId("retired-runtime-generation"), 0, retiredExpiry)
        activate(liveSession, WarmupReservationId("live-runtime-generation"), 1, liveExpiry)
        channel.deliver(RetireSessionsThrough(
            WarmupInstanceId("retirement-publisher"), retiredExpiry, localRequest.configurationFingerprint
        ))
        withTimeout(2_000) {
            while (participant.donorEvents.count { it == "finish" } < 1) kotlinx.coroutines.yield()
        }

        val retiredResult = WarmupRecordId(retiredSession, TopicPartition(mapping.warmupTopic, 0), 1)
        val liveResult = WarmupRecordId(liveSession, TopicPartition(mapping.warmupTopic, 1), 2)
        channel.deliver(ControlRecordCompleted(
            retiredSession, recipient, local,
            WarmupRecordCompleted(retiredResult, WarmupRemoteOutcome.Succeeded),
            localRequest.configurationFingerprint, retiredExpiry
        ))
        channel.deliver(ControlRecordCompleted(
            liveSession, recipient, local,
            WarmupRecordCompleted(liveResult, WarmupRemoteOutcome.Succeeded),
            localRequest.configurationFingerprint, liveExpiry
        ))
        withTimeout(2_000) {
            while (liveResult !in participant.accepted) kotlinx.coroutines.yield()
        }

        assertTrue(retiredResult !in participant.accepted)
        assertTrue(liveResult in participant.accepted)
        assertTrue(participant.mainStarted)
        context.stop()
    }

    @Test
    fun `surviving synchronized context publishes emergency reservation release`() = runBlocking {
        val participant = FakeParticipant(consumerId, listOf(mapping))
        lateinit var channel: FakeControlChannel
        val context = context { handler -> FakeControlChannel(handler).also { channel = it } }
        context.register(participant)
        context.start()
        withTimeout(2_000) { context.awaitReady() }
        val localRequest = channel.sent.filterIsInstance<WarmupRequested>().single()
        val session = WarmupSessionId("abandoned-session")
        val recipient = WarmupInstanceId("lost-recipient")
        val reservation = WarmupReservationId("abandoned-reservation")
        val expiredAt = System.currentTimeMillis() - WARMUP_MAX_PAIRWISE_CLOCK_SKEW_MILLIS - 1_000

        channel.deliver(
            WarmupRequested(
                session, recipient, listOf(mapping), expiredAt,
                localRequest.configurationFingerprint, expiredAt
            )
        )
        channel.deliver(
            ReservationRequested(
                session, recipient, reservation, listOf(WarmupPartitionAssignment(mapping, 0)),
                localRequest.configurationFingerprint, expiredAt
            )
        )

        val release = withTimeout(2_000) {
            while (true) {
                channel.sent.filterIsInstance<ReservationReleased>().firstOrNull {
                    it.sessionId == session && it.kind == WarmupReservationReleaseKind.EMERGENCY_TIMEOUT
                }?.let { return@withTimeout it }
                kotlinx.coroutines.yield()
            }
            error("unreachable")
        }
        assertEquals(reservation, release.reservationId)
        assertEquals(local, release.senderInstanceId)
        assertTrue(release.releasedAtEpochMillis >= expiredAt + WARMUP_MAX_PAIRWISE_CLOCK_SKEW_MILLIS)
        context.stop()
    }

    @Test
    fun `synchronized context periodically publishes a clock safe retirement boundary`() = runBlocking {
        val participant = FakeParticipant(consumerId, listOf(mapping))
        lateinit var channel: FakeControlChannel
        val context = context(retirementIntervalOverride = 20.milliseconds) { handler ->
            FakeControlChannel(handler).also { channel = it }
        }
        context.register(participant)
        context.start()
        withTimeout(2_000) { context.awaitReady() }

        val before = System.currentTimeMillis()
        val retirement = withTimeout(2_000) {
            while (true) {
                channel.sent.filterIsInstance<RetireSessionsThrough>().firstOrNull()
                    ?.let { return@withTimeout it }
                kotlinx.coroutines.yield()
            }
            error("unreachable")
        }

        assertEquals(local, retirement.senderInstanceId)
        assertTrue(retirement.deadlineCutoffEpochMillis <= before)
        assertTrue(
            retirement.deadlineCutoffEpochMillis <=
                System.currentTimeMillis() - WARMUP_MAX_PAIRWISE_CLOCK_SKEW_MILLIS
        )
        context.stop()
    }

    @Test
    fun `completion requested during donor activation is applied after activation`() = runBlocking {
        val activationEntered = CompletableDeferred<Unit>()
        val releaseActivation = CountDownLatch(1)
        val participant = DonorParticipant(
            consumerId,
            listOf(mapping),
            activationEntered,
            releaseActivation
        )
        lateinit var channel: FakeControlChannel
        val context = context { handler -> FakeControlChannel(handler).also { channel = it } }
        context.register(participant)
        context.start()
        withTimeout(2_000) { context.awaitReady() }
        val localRequest = channel.sent.filterIsInstance<WarmupRequested>().single()
        val session = WarmupSessionId("completion-during-activation")
        val laterRecipient = WarmupInstanceId("later-recipient")
        val expires = System.currentTimeMillis() + 2_000
        val assignment = WarmupPartitionAssignment(mapping, 0)

        channel.deliver(
            WarmupRequested(
                session, laterRecipient, listOf(mapping), System.currentTimeMillis() + 100,
                localRequest.configurationFingerprint, expires
            )
        )
        withTimeout(2_000) {
            while (channel.sent.none { it is DonorOffered && it.sessionId == session }) kotlinx.coroutines.yield()
        }
        channel.deliver(
            ReservationRequested(
                session, laterRecipient, WarmupReservationId("activation-reservation"), listOf(assignment),
                localRequest.configurationFingerprint, expires
            )
        )
        channel.deliver(
            SessionAccepted(
                session, laterRecipient, local, listOf(assignment),
                localRequest.configurationFingerprint, expires
            )
        )
        withTimeout(2_000) {
            while (channel.sent.none { it is DonorAccepted && it.sessionId == session }) kotlinx.coroutines.yield()
        }
        channel.deliver(
            RecipientReady(
                session, laterRecipient, local, listOf(assignment),
                localRequest.configurationFingerprint, expires
            )
        )
        withTimeout(2_000) { activationEntered.await() }
        channel.deliver(
            WarmupCompletionRequested(
                session, laterRecipient, local, localRequest.configurationFingerprint, expires
            )
        )

        releaseActivation.countDown()
        withTimeout(2_000) {
            while (channel.sent.none { it is DonorClosed && it.sessionId == session }) kotlinx.coroutines.yield()
        }

        assertEquals(listOf("prepare", "activate", "stop-inflight"), participant.donorEvents)
        context.stop()
    }

    @Test
    fun `completion before queued donor activation closes prepared session`() = runBlocking {
        val dispatcher = Executors.newSingleThreadExecutor().asCoroutineDispatcher()
        val releaseDispatcher = CountDownLatch(1)
        try {
            val participant = DonorParticipant(consumerId, listOf(mapping))
            lateinit var channel: FakeControlChannel
            val context = context(parentContext = dispatcher) { handler ->
                FakeControlChannel(handler).also { channel = it }
            }
            context.register(participant)
            context.start()
            withTimeout(2_000) { context.awaitReady() }
            val localRequest = channel.sent.filterIsInstance<WarmupRequested>().single()
            val session = WarmupSessionId("completion-before-activation")
            val recipient = WarmupInstanceId("queued-recipient")
            val expires = System.currentTimeMillis() + 5_000
            val assignment = WarmupPartitionAssignment(mapping, 0)
            channel.deliver(
                WarmupRequested(
                    session, recipient, listOf(mapping), System.currentTimeMillis() + 100,
                    localRequest.configurationFingerprint, expires
                )
            )
            channel.deliver(
                ReservationRequested(
                    session, recipient, WarmupReservationId("queued-activation-reservation"), listOf(assignment),
                    localRequest.configurationFingerprint, expires
                )
            )
            channel.deliver(
                SessionAccepted(
                    session, recipient, local, listOf(assignment),
                    localRequest.configurationFingerprint, expires
                )
            )
            withTimeout(2_000) {
                while (channel.sent.none { it is DonorAccepted && it.sessionId == session }) kotlinx.coroutines.yield()
            }

            val dispatcherBlocked = CompletableDeferred<Unit>()
            CoroutineScope(dispatcher).launch {
                dispatcherBlocked.complete(Unit)
                releaseDispatcher.await()
            }
            withTimeout(2_000) { dispatcherBlocked.await() }
            channel.deliver(
                RecipientReady(
                    session, recipient, local, listOf(assignment),
                    localRequest.configurationFingerprint, expires
                )
            )
            channel.deliver(
                WarmupCompletionRequested(
                    session, recipient, local, localRequest.configurationFingerprint, expires
                )
            )
            releaseDispatcher.countDown()

            withTimeout(2_000) {
                while (channel.sent.none { it is DonorClosed && it.sessionId == session }) kotlinx.coroutines.yield()
            }
            assertEquals(listOf("prepare", "stop-inflight"), participant.donorEvents)
            context.stop()
        } finally {
            releaseDispatcher.countDown()
            dispatcher.close()
        }
    }

    @Test
    fun `remote outcomes are routed only to the donor that owns their destination`() = runBlocking {
        val mappings = listOf(
            WarmupTopicMapping(WarmupConsumerId("orders"), "orders", "warmup-orders"),
            WarmupTopicMapping(WarmupConsumerId("batches"), "batches", "warmup-batches"),
            WarmupTopicMapping(WarmupConsumerId("cauldrons"), "cauldrons", "warmup-cauldrons")
        )
        val participants = mappings.map { mapping -> DonorParticipant(mapping.consumerId, listOf(mapping)) }
        lateinit var channel: FakeControlChannel
        val context = context { handler -> FakeControlChannel(handler).also { channel = it } }
        participants.forEach(context::register)
        context.start()
        withTimeout(2_000) { context.awaitReady() }
        val localRequest = channel.sent.filterIsInstance<WarmupRequested>().single()
        val session = WarmupSessionId("multi-consumer-recipient")
        val recipient = WarmupInstanceId("multi-consumer-recipient")
        val expires = System.currentTimeMillis() + 3_000

        channel.deliver(
            WarmupRequested(
                session,
                recipient,
                mappings,
                System.currentTimeMillis() + 100,
                localRequest.configurationFingerprint,
                expires
            )
        )
        withTimeout(2_000) {
            while (channel.sent.none { it is DonorOffered && it.sessionId == session }) kotlinx.coroutines.yield()
        }
        val assignments = mappings.map { mapping -> WarmupPartitionAssignment(mapping, 0) }
        channel.deliver(
            ReservationRequested(
                session, recipient, WarmupReservationId("multi-reservation"), assignments,
                localRequest.configurationFingerprint, expires
            )
        )
        channel.deliver(
            SessionAccepted(
                session,
                recipient,
                local,
                assignments,
                localRequest.configurationFingerprint,
                expires
            )
        )
        withTimeout(2_000) {
            while (participants.any { "prepare" !in it.donorEvents }) kotlinx.coroutines.yield()
        }
        channel.deliver(
            RecipientReady(
                session,
                recipient,
                local,
                assignments,
                localRequest.configurationFingerprint,
                expires
            )
        )
        withTimeout(2_000) {
            while (participants.any { "activate" !in it.donorEvents }) kotlinx.coroutines.yield()
        }

        assignments.forEachIndexed { index, assignment ->
            channel.deliver(
                ControlRecordCompleted(
                    session,
                    recipient,
                    local,
                    WarmupRecordCompleted(
                        WarmupRecordId(session, assignment.destination, index.toLong()),
                        WarmupRemoteOutcome.Succeeded
                    ),
                    localRequest.configurationFingerprint,
                    expires
                )
            )
        }
        channel.deliver(
            ControlRecordCompleted(
                session,
                recipient,
                local,
                WarmupRecordCompleted(
                    WarmupRecordId(session, TopicPartition("unknown-warmup-topic", 0), 0),
                    WarmupRemoteOutcome.Succeeded
                ),
                localRequest.configurationFingerprint,
                expires
            )
        )

        participants.zip(assignments).forEachIndexed { index, (participant, assignment) ->
            assertEquals(
                listOf(WarmupRecordId(session, assignment.destination, index.toLong())),
                participant.accepted
            )
        }
        context.stop()
    }

    @Test
    fun `cancelled stop caller does not cancel shared context cleanup`() = runBlocking {
        val stopEntered = CompletableDeferred<Unit>()
        val releaseStop = CompletableDeferred<Unit>()
        val participant = BlockingStopParticipant(consumerId, listOf(mapping), stopEntered, releaseStop)
        val context = context { handler -> FakeControlChannel(handler) }
        context.register(participant)
        context.start()
        withTimeout(2_000) { context.awaitReady() }

        val firstStop = async { runCatching { context.stop() } }
        withTimeout(2_000) { stopEntered.await() }
        firstStop.cancelAndJoin()
        val repeatedStop = async { context.stop() }
        assertFalse(repeatedStop.isCompleted)

        releaseStop.complete(Unit)
        withTimeout(2_000) { repeatedStop.await() }
        assertEquals(1, participant.stopCalls)
    }

    @Test
    fun `main failure is preserved when cleanup reports the same error`() = runBlocking {
        val failure = IllegalStateException("main failed")
        val participant = FailingParticipant(consumerId, listOf(mapping), failure)
        val context = context { handler -> FakeControlChannel(handler) }
        context.register(participant)
        context.start()
        withTimeout(2_000) { context.awaitReady() }

        val terminal = withTimeout(2_000) { runCatching { context.awaitTermination() }.exceptionOrNull() }
        val stopFailure = runCatching { context.stop() }.exceptionOrNull()

        assertSame(failure, terminal)
        assertSame(failure, stopFailure)
    }

    private fun context(
        warmupDuration: Duration = 20.milliseconds,
        enforceDistributedProtocol: Boolean = false,
        bootstrapRecoveryWindowOverride: Duration? = null,
        retirementIntervalOverride: Duration? = null,
        parentContext: CoroutineContext = Dispatchers.Default,
        factory: (WarmupControlMessageHandler) -> WarmupControlChannel
    ): TrafficWarmupContext {
        val configuration = TrafficWarmupContextConfiguration().apply {
            instanceId = local
            controlTopic = "control"
            offerWindow = 20.milliseconds
            negotiationTimeout = 500.milliseconds
            reservationInitialJitterMillis = 1L..2L
            reservationRetryJitterMillis = 1L..2L
            this.warmupDuration = warmupDuration
            delegationTimeout = 500.milliseconds
            cleanupTimeout = 500.milliseconds
            controlReadinessTimeout = 100.milliseconds
            this.parentContext = parentContext
        }
        return TrafficWarmupContext(
            configuration,
            factory,
            enforceDistributedProtocol,
            bootstrapRecoveryWindowOverride,
            retirementIntervalOverride
        )
    }

    private fun authoritativeOffer(request: WarmupRequested) = DonorOffered(
        request.sessionId,
        donor,
        local,
        listOf(WarmupDonorMapping(mapping, setOf(0))),
        request.configurationFingerprint,
        request.expiresAtEpochMillis,
        stateSnapshot = WarmupStateSnapshot(
            0,
            listOf(
                WarmupSessionSnapshot(
                    request.sessionId, local, request.configurationFingerprint, null, emptyList(), emptySet(),
                    WarmupSessionLifecycle.DISCOVERING, request.expiresAtEpochMillis, 0
                )
            )
        )
    )

    private class FakeControlChannel(
        private val handler: WarmupControlMessageHandler,
        private val readiness: CompletableDeferred<Unit>? = null,
        private val metadata: suspend (Set<String>) -> Map<String, Set<Int>>? = { null },
        private val response: suspend (WarmupControlMessage, FakeControlChannel) -> Unit = { _, _ -> }
    ) : WarmupControlChannel {
        private val offset = AtomicLong()
        val sent = CopyOnWriteArrayList<WarmupControlMessage>()
        val firstRequest = CompletableDeferred<Unit>()
        private var started = false

        override fun start() { started = true }
        override suspend fun awaitReady() {
            check(started)
            readiness?.await()
        }
        override suspend fun send(message: WarmupControlMessage): RecordMetadata {
            sent += message
            deliver(message)
            if (message is WarmupRequested) firstRequest.complete(Unit)
            response(message, this)
            return mock()
        }

        suspend fun deliver(message: WarmupControlMessage) {
            handler.onMessage(WarmupControlRecord(offset.getAndIncrement(), message))
        }

        override suspend fun topicPartitions(topics: Set<String>): Map<String, Set<Int>>? = metadata(topics)

        override suspend fun awaitTermination() = Unit
        override suspend fun stop() = Unit
    }

    private class FakeParticipant(
        override val consumerId: WarmupConsumerId,
        override val mappings: List<WarmupTopicMapping>,
        private val failRecipientPreparation: Boolean = false
    ) : RegisteredWarmupParticipant {
        val events = mutableListOf<String>()
        var mainStarted = false
        var recipientMainStarted = false

        override fun donorMappings(requested: Collection<WarmupTopicMapping>) = emptyList<WarmupDonorMapping>()
        override fun prepareDonor(sessionId: WarmupSessionId, assignments: List<WarmupPartitionAssignment>) =
            error("recipient test must not prepare a donor")

        override fun prepareRecipient(
            sessionId: WarmupSessionId,
            assignments: List<WarmupPartitionAssignment>,
            donorByDestination: Map<TopicPartition, WarmupInstanceId>,
            sendCompletion: suspend (WarmupInstanceId, WarmupRecordCompleted) -> Unit
        ): PreparedWarmupRecipient {
            if (failRecipientPreparation) error("recipient preparation failed")
            return object : PreparedWarmupRecipient {
            override val assignments = assignments
            override suspend fun start() { events += "recipient-start" }
            override suspend fun completeAfterDonorsClosed() { events += "recipient-complete" }
            override suspend fun startMain() {
                recipientMainStarted = true
                events += "recipient-main"
            }
            override suspend fun stop() { events += "recipient-stop" }
            }
        }

        override suspend fun startMain() {
            mainStarted = true
            events += "main"
        }
        override suspend fun stop() = Unit
        override suspend fun awaitTermination() = CompletableDeferred<Unit>().await()
    }

    private class DonorParticipant(
        override val consumerId: WarmupConsumerId,
        override val mappings: List<WarmupTopicMapping>,
        private val activationEntered: CompletableDeferred<Unit>? = null,
        private val releaseActivation: CountDownLatch? = null,
        private val preparationEntered: CompletableDeferred<Unit>? = null,
        private val releasePreparation: CountDownLatch? = null
    ) : RegisteredWarmupParticipant {
        var mainStarted = false
        val donorEvents = CopyOnWriteArrayList<String>()
        val accepted = CopyOnWriteArrayList<WarmupRecordId>()
        val preparedSessions = CopyOnWriteArrayList<WarmupSessionId>()
        val activatedSessions = CopyOnWriteArrayList<WarmupSessionId>()

        override fun donorMappings(requested: Collection<WarmupTopicMapping>): List<WarmupDonorMapping> =
            if (mainStarted && mappings.single() in requested) {
                listOf(WarmupDonorMapping(mappings.single(), setOf(0)))
            } else {
                emptyList()
            }

        override fun prepareDonor(
            sessionId: WarmupSessionId,
            assignments: List<WarmupPartitionAssignment>
        ): PreparedWarmupDonor {
            donorEvents += "prepare"
            preparedSessions += sessionId
            preparationEntered?.complete(Unit)
            releasePreparation?.await()
            return object : PreparedWarmupDonor {
                override val sessionId = sessionId
                override val assignments = assignments
                override fun activate() {
                    donorEvents += "activate"
                    activatedSessions += sessionId
                    activationEntered?.complete(Unit)
                    releaseActivation?.await()
                }
                override fun accept(completed: WarmupRecordCompleted): WarmupResultTracker.Completion {
                    accepted += completed.recordId
                    return WarmupResultTracker.Completion.DELIVERED
                }
                override fun stopDelegating() {
                    donorEvents += "stop-inflight"
                }
                override fun failDelegation(error: Throwable) {
                    donorEvents += "fail-inflight"
                }
                override suspend fun awaitInflight(timeout: kotlin.time.Duration) = Unit
                override fun finish() { donorEvents += "finish" }
            }
        }

        override fun prepareRecipient(
            sessionId: WarmupSessionId,
            assignments: List<WarmupPartitionAssignment>,
            donorByDestination: Map<TopicPartition, WarmupInstanceId>,
            sendCompletion: suspend (WarmupInstanceId, WarmupRecordCompleted) -> Unit
        ): PreparedWarmupRecipient = error("not expected")

        override suspend fun startMain() { mainStarted = true }
        override suspend fun stop() = Unit
        override suspend fun awaitTermination() = CompletableDeferred<Unit>().await()
    }

    private class BlockingStopParticipant(
        override val consumerId: WarmupConsumerId,
        override val mappings: List<WarmupTopicMapping>,
        private val stopEntered: CompletableDeferred<Unit>,
        private val releaseStop: CompletableDeferred<Unit>
    ) : RegisteredWarmupParticipant {
        var stopCalls = 0
        override fun donorMappings(requested: Collection<WarmupTopicMapping>) = emptyList<WarmupDonorMapping>()
        override fun prepareDonor(sessionId: WarmupSessionId, assignments: List<WarmupPartitionAssignment>) =
            error("not expected")
        override fun prepareRecipient(
            sessionId: WarmupSessionId,
            assignments: List<WarmupPartitionAssignment>,
            donorByDestination: Map<TopicPartition, WarmupInstanceId>,
            sendCompletion: suspend (WarmupInstanceId, WarmupRecordCompleted) -> Unit
        ): PreparedWarmupRecipient = error("not expected")
        override suspend fun startMain() = Unit
        override suspend fun stop() {
            stopCalls++
            stopEntered.complete(Unit)
            releaseStop.await()
        }
        override suspend fun awaitTermination() = CompletableDeferred<Unit>().await()
    }

    private class FailingParticipant(
        override val consumerId: WarmupConsumerId,
        override val mappings: List<WarmupTopicMapping>,
        private val failure: Throwable
    ) : RegisteredWarmupParticipant {
        override fun donorMappings(requested: Collection<WarmupTopicMapping>) = emptyList<WarmupDonorMapping>()
        override fun prepareDonor(sessionId: WarmupSessionId, assignments: List<WarmupPartitionAssignment>) =
            error("not expected")
        override fun prepareRecipient(
            sessionId: WarmupSessionId,
            assignments: List<WarmupPartitionAssignment>,
            donorByDestination: Map<TopicPartition, WarmupInstanceId>,
            sendCompletion: suspend (WarmupInstanceId, WarmupRecordCompleted) -> Unit
        ): PreparedWarmupRecipient = error("not expected")
        override suspend fun startMain() = Unit
        override suspend fun stop(): Unit = throw failure
        override suspend fun awaitTermination(): Unit = throw failure
    }
}
