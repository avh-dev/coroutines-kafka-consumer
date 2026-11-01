package avh.ckc.core.warmup

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.mockito.kotlin.mock
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds

class WarmupCoordinatorTest {
    @Test
    fun `assignment uses partitions discovered for the warm-up topic`() = runBlocking {
        val bus = ControlBus()
        val donor = coordinator("donor", bus, availableWarmupPartitions = mapOf("warmup-orders" to setOf(3, 5)))
            .also { it.register(TestParticipant()) }
        val recipient = coordinator(
            "recipient", bus, availableWarmupPartitions = mapOf("warmup-orders" to setOf(3, 5))
        ).also { it.register(TestParticipant()) }
        try {
            donor.start()
            assertEquals(WarmupStartupResult.WARMED, withTimeout(3_000) { recipient.start() })
            val assignment = bus.messages.filterIsInstance<AssignmentProposed>().first().assignment
            assertEquals(3, assignment.partitions.single().warmupPartition)
        } finally {
            recipient.stop(); donor.stop()
        }
    }

    @Test
    fun `active coordinator owns assignment and ambiguous decision send still completes from journal`() = runBlocking {
        val bus = ControlBus(throwAfterPublish = { it is AssignmentProposed })
        val donorParticipant = TestParticipant()
        val recipientParticipant = TestParticipant()
        val donor = coordinator("donor", bus).also { it.register(donorParticipant) }
        val recipient = coordinator("recipient", bus).also { it.register(recipientParticipant) }
        try {
            assertEquals(WarmupStartupResult.NO_DONOR_FALLBACK, donor.start())
            assertEquals(
                WarmupStartupResult.WARMED,
                withTimeout(3_000) { recipient.start() },
                (bus.messages.map { it::class.simpleName }.toString() + " view=" + recipient.controlPlaneView())
            )
            assertEquals(2, bus.messages.count { it is AssignmentProposed })
            assertEquals(listOf("fallback-prepared", "main", "reserved", "forwarding", "boundary", "released"), donorParticipant.events)
            assertEquals(listOf("recipient", "drained", "main-after-drain"), recipientParticipant.events)
        } finally {
            recipient.stop(); donor.stop()
        }
    }

    @Test
    fun `two recipients cannot acquire the same journal reserved partition`() = runBlocking {
        val bus = ControlBus()
        val donor = coordinator("donor", bus).also { it.register(TestParticipant(hold = 100.milliseconds)) }
        val recipients = listOf("one", "two").map { id ->
            coordinator("recipient-$id", bus).also { it.register(TestParticipant(hold = 100.milliseconds)) }
        }
        try {
            donor.start()
            val results = recipients.map { async { it.start() } }.awaitAll()
            assertEquals(1, results.count { it == WarmupStartupResult.WARMED })
            assertEquals(1, results.count { it == WarmupStartupResult.NO_DONOR_FALLBACK })
            val assignments = bus.messages.filterIsInstance<AssignmentProposed>()
            assertTrue(assignments.isNotEmpty())
        } finally {
            recipients.forEach { it.stop() }; donor.stop()
        }
    }

    @Test
    fun `recipient startup failure performs cleanup and cold starts main before forwarding`() = runBlocking {
        val bus = ControlBus()
        val donorParticipant = TestParticipant()
        val recipientParticipant = TestParticipant(recipientFailure = IllegalStateException("recipient failed"))
        val donor = coordinator("donor", bus).also { it.register(donorParticipant) }
        val recipient = coordinator("recipient", bus).also { it.register(recipientParticipant) }
        try {
            donor.start()
            assertEquals(WarmupStartupResult.NO_DONOR_FALLBACK, withTimeout(3_000) { recipient.start() })
            assertEquals(listOf("fallback-prepared", "main"), recipientParticipant.events)
            assertTrue("forwarding" !in donorParticipant.events)
            delay(200)
            assertTrue(
                "reservation-released" in donorParticipant.events,
                "events=${donorParticipant.events}, messages=${bus.messages.map { it::class.simpleName }}"
            )
        } finally {
            recipient.stop(); donor.stop()
        }
    }

    @Test
    fun `long warm-up does not fall back after forwarding was accepted`() = runBlocking {
        val bus = ControlBus()
        val donorParticipant = TestParticipant()
        val recipientParticipant = TestParticipant(hold = 350.milliseconds)
        val donor = coordinator("donor", bus, stageTimeout = 80.milliseconds).also { it.register(donorParticipant) }
        val recipient = coordinator("recipient", bus, stageTimeout = 80.milliseconds).also {
            it.register(recipientParticipant)
        }
        try {
            donor.start()
            assertEquals(WarmupStartupResult.WARMED, withTimeout(3_000) { recipient.start() })
            assertTrue("fallback-prepared" !in recipientParticipant.events)
            assertEquals(1, recipientParticipant.events.count { it == "recipient" })
        } finally {
            recipient.stop(); donor.stop()
        }
    }

    @Test
    fun `one donor serves at most one session when two warm-up partitions are free`() = runBlocking {
        val bus = ControlBus()
        val donor = coordinator("donor", bus, warmupPartitions = listOf(0, 1)).also {
            it.register(TestParticipant(hold = 150.milliseconds))
        }
        val recipients = listOf("one", "two").map { id ->
            coordinator("recipient-$id", bus, warmupPartitions = listOf(0, 1)).also {
                it.register(TestParticipant(hold = 150.milliseconds))
            }
        }
        try {
            donor.start()
            val results = recipients.map { async { it.start() } }.awaitAll()
            assertEquals(1, results.count { it == WarmupStartupResult.WARMED })
            assertEquals(1, results.count { it == WarmupStartupResult.NO_DONOR_FALLBACK })
        } finally {
            recipients.forEach { it.stop() }; donor.stop()
        }
    }

    @Test
    fun `blocked drain does not block applying a new coordinator generation`() = runBlocking {
        val bus = ControlBus()
        val drainGate = CompletableDeferred<Unit>()
        val drainEntered = CompletableDeferred<Unit>()
        val donor = coordinator("donor", bus).also { it.register(TestParticipant()) }
        val recipient = coordinator("recipient", bus).also {
            it.register(TestParticipant(drainGate = drainGate, drainEntered = drainEntered))
        }
        try {
            donor.start()
            val startup = async { recipient.start() }
            withTimeout(2_000) { drainEntered.await() }
            bus.pausedHeartbeatSender = recipient.controlPlaneView().activeCoordinator
            delay(30)
            val before = recipient.controlPlaneView()
            bus.publish(CoordinatorClaim(
                WarmupSessionId("re-election"), requireNotNull(before.activeCoordinator),
                before.generation, before.activeCoordinator, before.lastHeartbeatOffset
            ))
            withTimeout(1_000) {
                while (recipient.controlPlaneView().generation == before.generation) delay(5)
            }
            drainGate.complete(Unit)
            val result = withTimeoutOrNull(2_000) { startup.await() }
            assertEquals(
                WarmupStartupResult.WARMED,
                result,
                "messages=${bus.messages.map { it::class.simpleName to it.senderInstanceId }}, " +
                    "view=${recipient.controlPlaneView()}"
            )
        } finally {
            drainGate.complete(Unit)
            recipient.stop(); donor.stop()
        }
    }

    @Test
    fun `new active stops forwarding using replicated deadline`() = runBlocking {
        val bus = ControlBus()
        val donor = coordinator("donor", bus).also { it.register(TestParticipant()) }
        val standby = coordinator("standby", bus).also { it.register(TestParticipant()) }
        val recipient = coordinator("recipient", bus).also {
            it.register(TestParticipant(hold = 1_000.milliseconds))
        }
        try {
            donor.start()
            assertEquals(WarmupStartupResult.WARMED, withTimeout(2_000) { standby.start() })
            val startsBefore = bus.messages.count { it is StartForwarding }
            val startup = async { recipient.start() }
            withTimeout(2_000) {
                while (bus.messages.count { it is StartForwarding } == startsBefore) delay(5)
            }
            bus.pausedHeartbeatSender = WarmupInstanceId("donor")
            delay(30)
            var view = recipient.controlPlaneView()
            repeat(5) {
                if (view.activeCoordinator == WarmupInstanceId("standby")) return@repeat
                bus.publish(CoordinatorClaim(
                    WarmupSessionId("takeover-$it"), WarmupInstanceId("standby"),
                    view.generation, view.activeCoordinator, view.lastHeartbeatOffset
                ))
                delay(10)
                view = recipient.controlPlaneView()
            }
            assertEquals(WarmupInstanceId("standby"), recipient.controlPlaneView().activeCoordinator)
            withTimeout(2_000) {
                while (bus.messages.none {
                        it is StopForwarding && it.senderInstanceId == WarmupInstanceId("standby")
                    }) delay(5)
            }
            val result = withTimeoutOrNull(3_000) { startup.await() }
            assertEquals(
                WarmupStartupResult.WARMED,
                result,
                "messages=${bus.messages.map { it::class.simpleName to it.senderInstanceId }}, " +
                    "view=${recipient.controlPlaneView()}"
            )
        } finally {
            recipient.stop(); standby.stop(); donor.stop()
        }
    }

    @Test
    fun `donor participant failure after forwarding notifies and quarantines session`() = runBlocking {
        val bus = ControlBus()
        val donorParticipant = TestParticipant(hold = 500.milliseconds)
        val donor = coordinator("donor", bus).also { it.register(donorParticipant) }
        val recipient = coordinator("recipient", bus).also { it.register(TestParticipant(hold = 500.milliseconds)) }
        supervisorScope {
            try {
                donor.start()
                val startup = async { recipient.start() }
                withTimeout(2_000) { while ("forwarding" !in donorParticipant.events) delay(5) }
                donor.onParticipantFailure(WarmupConsumerId("orders"), IllegalStateException("donor failed"))
                assertFailsWith<IllegalStateException> { withTimeout(2_000) { startup.await() } }
                assertTrue(bus.messages.any {
                    it is SessionFailed && it.senderInstanceId == WarmupInstanceId("donor")
                })
                assertTrue(recipient.controlPlaneView().sessions.values.any {
                    it.stage == WarmupReplicatedStage.QUARANTINED
                })
            } finally {
                recipient.stop(); donor.stop()
            }
        }
    }

    @Test
    fun `next donor reservation waits for previous donor cleanup`() = runBlocking {
        val bus = ControlBus()
        val finishGate = CompletableDeferred<Unit>()
        val finishEntered = CompletableDeferred<Unit>()
        val donorParticipant = TestParticipant(finishGate = finishGate, finishEntered = finishEntered)
        val donor = coordinator("donor", bus, stageTimeout = 1_000.milliseconds).also {
            it.register(donorParticipant)
        }
        val recipient = coordinator("recipient", bus, stageTimeout = 1_000.milliseconds).also {
            it.register(TestParticipant())
        }
        try {
            donor.start()
            assertEquals(WarmupStartupResult.WARMED, withTimeout(3_000) { recipient.start() })
            withTimeout(1_000) { finishEntered.await() }

            val view = donor.controlPlaneView()
            val session = WarmupSessionId("next-session")
            val assignment = WarmupSessionAssignment(
                session, WarmupInstanceId("donor"), WarmupInstanceId("next-recipient"),
                listOf(WarmupPartitionAssignment(
                    WarmupConsumerId("orders"), "orders", "warmup-orders", 0
                ))
            )
            bus.publish(AssignmentProposed(
                session, requireNotNull(view.activeCoordinator), view.generation, assignment, 100
            ))
            delay(100)
            assertEquals(1, donorParticipant.events.count { it == "reserved" })
            assertFalse(donor.state() == WarmupCoordinatorState.FAILED)

            finishGate.complete(Unit)
            withTimeout(1_000) {
                while (donorParticipant.events.count { it == "reserved" } < 2) delay(5)
            }
            assertFalse(donor.state() == WarmupCoordinatorState.FAILED)
        } finally {
            finishGate.complete(Unit)
            recipient.stop(); donor.stop()
        }
    }

    @Test
    fun `session failure interrupts blocked drain before main starts`() = runBlocking {
        val bus = ControlBus()
        val drainGate = CompletableDeferred<Unit>()
        val drainEntered = CompletableDeferred<Unit>()
        val recipientParticipant = TestParticipant(drainGate = drainGate, drainEntered = drainEntered)
        val donor = coordinator("donor", bus).also { it.register(TestParticipant()) }
        val recipient = coordinator("recipient", bus).also { it.register(recipientParticipant) }
        supervisorScope {
            donor.start()
            val startup = async { recipient.start() }
            try {
                withTimeout(2_000) { drainEntered.await() }
                val session = recipient.controlPlaneView().sessions.keys.single()
                bus.publish(SessionFailed(
                    session, WarmupInstanceId("donor"), recipient.controlPlaneView().activeCoordinator,
                    "forced failure"
                ))

                withTimeout(2_000) {
                    assertFailsWith<IllegalStateException> { recipient.awaitTermination() }
                }
                assertFalse("main-after-drain" in recipientParticipant.events)
                assertFalse(bus.messages.any { it is SessionCompleted && it.sessionId == session })
                assertFailsWith<IllegalStateException> { startup.await() }
                Unit
            } finally {
                drainGate.complete(Unit)
                recipient.stop(); donor.stop()
            }
        }
    }

    @Test
    fun `unassigned requests finish without accumulating session executors`() = runBlocking {
        val bus = ControlBus()
        val coordinator = coordinator("active", bus).also { it.register(TestParticipant()) }
        try {
            coordinator.start()
            val incompatible = WarmupParticipantConfiguration(
                WarmupConsumerId("missing"),
                listOf(WarmupTopicMapping("missing", "warmup-missing")),
                WarmupForwardingProfile(0.0, 0.1, 10.milliseconds)
            )
            repeat(20) { index ->
                bus.publish(WarmupRequest(
                    WarmupSessionId("unassigned-$index"), WarmupInstanceId("requester-$index"),
                    listOf(incompatible)
                ))
            }

            assertEquals(0, coordinator.localSessionExecutorCount())
            withTimeout(1_000) {
                while (coordinator.pendingRequestCount() != 0) delay(5)
            }
            assertEquals(0, coordinator.localSessionExecutorCount())
        } finally {
            coordinator.stop()
        }
    }

    @Test
    fun `session executor count is bounded and overflow fails closed`() = runBlocking {
        val bus = ControlBus()
        val startGate = CompletableDeferred<Unit>()
        val startEntered = CompletableDeferred<Unit>()
        val coordinator = coordinator(
            "recipient", bus, warmupPartitions = listOf(0, 1),
            limits = WarmupControlPlaneLimits(maximumSessionExecutors = 1)
        ).also {
            it.register(TestParticipant(recipientStartGate = startGate, recipientStartEntered = startEntered))
        }
        supervisorScope {
            try {
                coordinator.start()
                withTimeout(1_000) {
                    while (coordinator.controlPlaneView().activeCoordinator == null) delay(5)
                }
                val view = coordinator.controlPlaneView()
                fun assignment(id: String, donor: String, partition: Int) = WarmupSessionAssignment(
                    WarmupSessionId(id), WarmupInstanceId(donor), WarmupInstanceId("recipient"),
                    listOf(WarmupPartitionAssignment(
                        WarmupConsumerId("orders"), "orders", "warmup-orders", partition
                    ))
                )
                bus.publish(AssignmentProposed(
                    WarmupSessionId("first"), requireNotNull(view.activeCoordinator), view.generation,
                    assignment("first", "donor-one", 0), 100
                ))
                withTimeout(1_000) { startEntered.await() }
                bus.publish(AssignmentProposed(
                    WarmupSessionId("second"), requireNotNull(view.activeCoordinator), view.generation,
                    assignment("second", "donor-two", 1), 100
                ))

                withTimeout(2_000) {
                    assertFailsWith<IllegalStateException> { coordinator.awaitTermination() }
                }
                assertTrue(coordinator.localSessionExecutorCount() <= 1)
            } finally {
                startGate.complete(Unit)
                coordinator.stop()
            }
        }
    }

    @Test
    fun `failure during partial donor activation stops already activated participants`() = runBlocking {
        val bus = ControlBus()
        val activationGate = CompletableDeferred<Unit>()
        val activationEntered = CompletableDeferred<Unit>()
        val firstDonor = TestParticipant(
            consumerId = "orders-a", sourceTopic = "orders-a", warmupTopic = "warmup-orders-a"
        )
        val secondDonor = TestParticipant(
            consumerId = "orders-b", sourceTopic = "orders-b", warmupTopic = "warmup-orders-b",
            activationGate = activationGate, activationEntered = activationEntered
        )
        val donor = coordinator("donor", bus).also {
            it.register(firstDonor)
            it.register(secondDonor)
        }
        val recipient = coordinator("recipient", bus).also {
            it.register(TestParticipant(
                consumerId = "orders-a", sourceTopic = "orders-a", warmupTopic = "warmup-orders-a"
            ))
            it.register(TestParticipant(
                consumerId = "orders-b", sourceTopic = "orders-b", warmupTopic = "warmup-orders-b"
            ))
        }
        supervisorScope {
            donor.start()
            val startup = async { recipient.start() }
            try {
                withTimeout(2_000) { activationEntered.await() }
                assertTrue(firstDonor.forwardingActive)
                val session = donor.controlPlaneView().sessions.keys.single()
                bus.publish(SessionFailed(
                    session, WarmupInstanceId("recipient"), donor.controlPlaneView().activeCoordinator,
                    "activation failed"
                ))

                withTimeout(2_000) {
                    assertFailsWith<IllegalStateException> { donor.awaitTermination() }
                }
                assertFalse(firstDonor.forwardingActive)
                assertFalse(secondDonor.forwardingActive)
                assertFailsWith<IllegalStateException> { startup.await() }
                Unit
            } finally {
                activationGate.complete(Unit)
                recipient.stop(); donor.stop()
            }
        }
    }

    @Test
    fun `active releases successful executor before next sequential warmup`() = runBlocking {
        val bus = ControlBus()
        val limits = WarmupControlPlaneLimits(maximumSessionExecutors = 1)
        val active = coordinator("active", bus, limits = limits).also { it.register(TestParticipant()) }
        val first = coordinator("first", bus, limits = limits).also { it.register(TestParticipant()) }
        val second = coordinator("second", bus, limits = limits).also { it.register(TestParticipant()) }
        try {
            active.start()
            assertEquals(WarmupStartupResult.WARMED, withTimeout(3_000) { first.start() })
            withTimeout(1_000) {
                while (active.localSessionExecutorCount() != 0) delay(5)
            }

            assertEquals(WarmupStartupResult.WARMED, withTimeout(3_000) { second.start() })
            withTimeout(1_000) {
                while (active.localSessionExecutorCount() != 0) delay(5)
            }
            assertFalse(active.state() == WarmupCoordinatorState.FAILED)
        } finally {
            second.stop(); first.stop(); active.stop()
        }
    }

    private fun coordinator(
        id: String,
        bus: ControlBus,
        warmupPartitions: Collection<Int> = listOf(0),
        stageTimeout: kotlin.time.Duration = 120.milliseconds,
        limits: WarmupControlPlaneLimits = WarmupControlPlaneLimits(),
        availableWarmupPartitions: Map<String, Set<Int>>? = null
    ) = WarmupCoordinator(
        WarmupCoordinatorConfiguration(
            WarmupInstanceId(id), warmupPartitions, donorSearchTimeout = 20.milliseconds,
            stageTimeout = stageTimeout, heartbeatInterval = 20.milliseconds,
            electionTimeout = 80.milliseconds, controlPlaneLimits = limits,
            availableWarmupPartitions = availableWarmupPartitions
        ), Dispatchers.Default, bus::channel
    )

    private class ControlBus(private val throwAfterPublish: (WarmupControlMessage) -> Boolean = { false }) {
        private val channels = CopyOnWriteArrayList<TestChannel>()
        val messages = CopyOnWriteArrayList<WarmupControlMessage>()
        private val publication = Mutex()
        private var offset = 0L
        @Volatile var pausedHeartbeatSender: WarmupInstanceId? = null

        fun channel(handler: WarmupControlMessageHandler): WarmupControlChannel = synchronized(this) {
            TestChannel(handler, this).also(channels::add)
        }

        @Synchronized fun endOffset(): Long = offset

        suspend fun publish(message: WarmupControlMessage): RecordMetadata = publication.withLock {
            if (message is CoordinatorHeartbeat && message.senderInstanceId == pausedHeartbeatSender) {
                return@withLock mock()
            }
            val record = synchronized(this) {
                messages += message
                WarmupControlRecord(offset++, message)
            }
            channels.filter(TestChannel::active).forEach { it.handler.onMessage(record) }
            if (throwAfterPublish(message)) throw IllegalStateException("ambiguous send")
            mock()
        }
    }

    private class TestChannel(
        val handler: WarmupControlMessageHandler,
        private val bus: ControlBus
    ) : WarmupControlChannel {
        private val ready = CompletableDeferred<Unit>()
        private val terminated = CompletableDeferred<Unit>()
        private var startOffset = -1L
        @Volatile var active = false
        override fun start() { startOffset = bus.endOffset(); active = true; ready.complete(Unit) }
        override suspend fun awaitReady() = ready.await()
        override fun logPosition() = WarmupControlLogPosition(TopicPartition("control", 0), 0, startOffset)
        override suspend fun send(message: WarmupControlMessage) = bus.publish(message)
        override suspend fun awaitTermination() = terminated.await()
        override suspend fun stop() { active = false; terminated.complete(Unit) }
    }

    private class TestParticipant(
        consumerId: String = "orders",
        sourceTopic: String = "orders",
        warmupTopic: String = "warmup-orders",
        private val hold: kotlin.time.Duration = 0.milliseconds,
        private val recipientFailure: Throwable? = null,
        private val drainGate: CompletableDeferred<Unit>? = null,
        private val drainEntered: CompletableDeferred<Unit>? = null,
        private val finishGate: CompletableDeferred<Unit>? = null,
        private val finishEntered: CompletableDeferred<Unit>? = null,
        private val recipientStartGate: CompletableDeferred<Unit>? = null,
        private val recipientStartEntered: CompletableDeferred<Unit>? = null,
        private val activationGate: CompletableDeferred<Unit>? = null,
        private val activationEntered: CompletableDeferred<Unit>? = null
    ) : CoordinatorParticipant {
        override val configuration = WarmupParticipantConfiguration(
            WarmupConsumerId(consumerId), listOf(WarmupTopicMapping(sourceTopic, warmupTopic)),
            WarmupForwardingProfile(1.0, 1.0, 0.milliseconds, hold)
        )
        val events = CopyOnWriteArrayList<String>()
        @Volatile var forwardingActive = false
        private lateinit var assignment: List<WarmupPartitionAssignment>
        override suspend fun startMain() { events += "main" }
        override fun capability(requested: WarmupParticipantConfiguration) = WarmupParticipantCapability(
            configuration.consumerId,
            listOf(WarmupTopicCapability(configuration.topicMappings.single().sourceTopic, true, setOf(0), 10))
        )
        override suspend fun reserveDonor(assignment: List<WarmupPartitionAssignment>) {
            this.assignment = assignment; events += "reserved"
        }
        override suspend fun releaseDonorReservation() {
            forwardingActive = false
            events += "reservation-released"
        }
        override suspend fun activateDonor(sessionId: WarmupSessionId) {
            forwardingActive = true
            events += "forwarding"
            activationEntered?.complete(Unit)
            activationGate?.await()
        }
        override suspend fun completeForwarding(): WarmupForwardingBoundary {
            events += "boundary"
            return WarmupForwardingBoundary(assignment.associate { it.topicPartition to 4L })
        }
        override suspend fun finishDonor(sessionId: WarmupSessionId) {
            finishEntered?.complete(Unit)
            finishGate?.await()
            forwardingActive = false
            events += "released"
        }
        override suspend fun startRecipient(assignment: List<WarmupPartitionAssignment>) {
            this.assignment = assignment
            recipientFailure?.let { throw it }
            recipientStartEntered?.complete(Unit)
            recipientStartGate?.await()
            events += "recipient"
        }
        override suspend fun drainRecipient(boundary: WarmupForwardingBoundary) {
            drainEntered?.complete(Unit)
            drainGate?.await()
            events += "drained"
        }
        override suspend fun startMainAfterDrain() { events += "main-after-drain" }
        override suspend fun prepareFallback(knownFailure: Throwable) { events += "fallback-prepared" }
        override suspend fun stop() { forwardingActive = false }
    }
}
