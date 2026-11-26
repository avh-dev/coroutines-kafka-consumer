package avh.ckc.warmup.recipient

import avh.ckc.warmup.protocol.DonorAccepted
import avh.ckc.warmup.protocol.DonorOffered
import avh.ckc.warmup.protocol.SessionAccepted
import avh.ckc.warmup.protocol.SessionRejected
import avh.ckc.warmup.protocol.WarmupControlNamespace
import avh.ckc.warmup.protocol.WarmupDonorMapping
import avh.ckc.warmup.protocol.WarmupInstanceId
import avh.ckc.warmup.protocol.WarmupParticipantId
import avh.ckc.warmup.protocol.WarmupPartitionAssignment
import avh.ckc.warmup.protocol.WarmupRequested
import avh.ckc.warmup.protocol.WarmupSessionId
import avh.ckc.warmup.protocol.WarmupSourcePartition
import avh.ckc.warmup.protocol.WarmupTopicMapping
import avh.ckc.warmup.protocol.normalizedAdmissionBudget
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class WarmupRecipientSessionTest {
    private val namespace = WarmupControlNamespace("global5-shared-partition-v6-test")
    private val recipient = WarmupInstanceId("recipient")
    private val donor = WarmupInstanceId("donor")
    private val mappings = listOf(
        WarmupTopicMapping(WarmupParticipantId("orders"), "orders", "warmup-orders", "orders-fp"),
        WarmupTopicMapping(WarmupParticipantId("payments"), "payments", "warmup-payments", "payments-fp")
    )

    @Test
    fun `recipient publishes readiness only after every accepted mapping consumer is ready`() = runBlocking {
        val gates = mappings.associateWith { CompletableDeferred<Unit>() }
        val starts = CopyOnWriteArrayList<WarmupDestinationStart>()
        val stopped = CopyOnWriteArrayList<String>()
        val discovery = discovery(
            metadata = WarmupDestinationMetadata { mapping ->
                mapOf(0 to 100 + mappings.indexOf(mapping).toLong())
            },
            preparation = WarmupRecipientConsumerPreparation { _, start ->
                starts += start
                preparedConsumer(start, gates.getValue(start.assignment.mapping), stopped)
            }
        )
        assertTrue(discovery.onOffer(offer(mappings)))
        val selection = assertInstanceOf(WarmupRecipientSelection.Accepted::class.java, discovery.selectDonors())
        assertEquals(1, selection.messages.size)
        assertTrue(discovery.onDonorAccepted(accepted(selection.messages.single())))

        val result = async { discovery.prepareConsumers() }
        withTimeout(2_000) {
            while (starts.size != 2) kotlinx.coroutines.yield()
        }
        assertFalse(result.isCompleted)
        gates.getValue(mappings[0]).complete(Unit)
        assertFalse(result.isCompleted)
        gates.getValue(mappings[1]).complete(Unit)

        val ready = assertInstanceOf(WarmupRecipientPreparationResult.Ready::class.java, result.await())
        assertEquals(mappings.map(::WarmupPartitionAssignment).toSet(), ready.messages.single().assignments.toSet())
        assertEquals(listOf(100L, 101L), starts.map { it.offset })
        assertTrue(stopped.isEmpty())
    }

    @Test
    fun `one failed mapping cold starts the complete context and stops every prepared consumer`() = runBlocking {
        val stopped = CopyOnWriteArrayList<String>()
        val discovery = discovery(
            metadata = WarmupDestinationMetadata { mapping ->
                if (mapping == mappings[1]) throw IllegalStateException("metadata unavailable")
                mapOf(0 to 100L)
            },
            preparation = WarmupRecipientConsumerPreparation { _, start ->
                preparedConsumer(start, CompletableDeferred(Unit), stopped)
            }
        )
        discovery.onOffer(offer(mappings))
        val selection = assertInstanceOf(WarmupRecipientSelection.Accepted::class.java, discovery.selectDonors())
        discovery.onDonorAccepted(accepted(selection.messages.single()))

        val result = discovery.prepareConsumers()

        assertEquals(
            WarmupRecipientPreparationResult.ColdFallback("metadata_lookup_failed"),
            result
        )
        assertTrue(stopped.isEmpty())
        assertEquals(WarmupRecipientSession.State.COLD_FALLBACK, discovery.state())
    }

    @Test
    fun `warmup topic with incompatible partition metadata cold starts the complete context`() = runBlocking {
        val discovery = discovery(
            metadata = WarmupDestinationMetadata { mapOf(0 to 10L, 1 to 10L) }
        )
        discovery.onOffer(offer(mappings))
        val selection = assertInstanceOf(WarmupRecipientSelection.Accepted::class.java, discovery.selectDonors())
        discovery.onDonorAccepted(accepted(selection.messages.single()))

        assertEquals(
            WarmupRecipientPreparationResult.ColdFallback("metadata_lookup_failed"),
            discovery.prepareConsumers()
        )
    }

    @Test
    fun `metadata failure reports safe local diagnostics without provider details`() = runBlocking {
        val diagnostics = CopyOnWriteArrayList<String>()
        val discovery = discovery(
            metadata = WarmupDestinationMetadata { error("password=do-not-log") },
            diagnosticObserver = diagnostics::add
        )
        discovery.onOffer(offer(mappings))
        val selection = assertInstanceOf(WarmupRecipientSelection.Accepted::class.java, discovery.selectDonors())
        discovery.onDonorAccepted(accepted(selection.messages.single()))

        assertEquals(
            WarmupRecipientPreparationResult.ColdFallback("metadata_lookup_failed"),
            discovery.prepareConsumers()
        )
        val diagnostic = diagnostics.single()
        assertTrue(diagnostic.contains("event_code=CKC_WARMUP_METADATA_COLD_FALLBACK"))
        assertTrue(diagnostic.contains("reason=metadata_lookup_failed"))
        assertTrue(diagnostic.contains("session_id=session"))
        assertTrue(diagnostic.contains("cause_type=IllegalStateException"))
        assertFalse(diagnostic.contains("do-not-log"))
    }

    @Test
    fun `one metadata deadline bounds all mapping lookups and reports unfinished mappings`() = runBlocking {
        val diagnostics = CopyOnWriteArrayList<String>()
        val discovery = discovery(
            metadata = WarmupDestinationMetadata {
                delay(1.seconds)
                mapOf(0 to 0L)
            },
            preparationTimeout = 20.milliseconds,
            diagnosticObserver = diagnostics::add
        )
        discovery.onOffer(offer(mappings))
        val selection = assertInstanceOf(WarmupRecipientSelection.Accepted::class.java, discovery.selectDonors())
        discovery.onDonorAccepted(accepted(selection.messages.single()))

        assertEquals(
            WarmupRecipientPreparationResult.ColdFallback("metadata_timeout"),
            discovery.prepareConsumers()
        )
        val diagnostic = diagnostics.single()
        assertTrue(diagnostic.contains("reason=metadata_timeout"))
        assertTrue(diagnostic.contains("warmup-orders"))
        assertTrue(diagnostic.contains("warmup-payments"))
    }

    @Test
    fun `consumer returned with an invalid assignment is still stopped`() = runBlocking {
        val stopped = CopyOnWriteArrayList<String>()
        val discovery = discovery(
            preparation = WarmupRecipientConsumerPreparation { _, start ->
                val invalidStart = start.copy(assignment = WarmupPartitionAssignment(mappings.last()))
                preparedConsumer(invalidStart, CompletableDeferred(Unit), stopped)
            }
        )
        discovery.onOffer(offer(mappings))
        val selection = assertInstanceOf(WarmupRecipientSelection.Accepted::class.java, discovery.selectDonors())
        discovery.onDonorAccepted(accepted(selection.messages.single()))

        assertInstanceOf(WarmupRecipientPreparationResult.ColdFallback::class.java, discovery.prepareConsumers())
        assertEquals(listOf("warmup-payments"), stopped)
    }

    @Test
    fun `external cancellation waits for non cancellable bounded consumer cleanup`() = runBlocking {
        val readiness = CompletableDeferred<Unit>()
        val consumerCreated = CompletableDeferred<Unit>()
        val stopStarted = CompletableDeferred<Unit>()
        val releaseStop = CompletableDeferred<Unit>()
        val discovery = discovery(
            preparation = WarmupRecipientConsumerPreparation { _, start ->
                consumerCreated.complete(Unit)
                object : PreparedWarmupRecipientConsumer {
                    override val assignment = start.assignment
                    override val startOffset = start.offset
                    override fun start() = Unit
                    override suspend fun awaitReady() = readiness.await()
                    override suspend fun stop() {
                        stopStarted.complete(Unit)
                        releaseStop.await()
                    }
                }
            }
        )
        discovery.onOffer(offer(mappings))
        val selection = assertInstanceOf(WarmupRecipientSelection.Accepted::class.java, discovery.selectDonors())
        discovery.onDonorAccepted(accepted(selection.messages.single()))
        val preparation = async { discovery.prepareConsumers() }
        withTimeout(2_000) { consumerCreated.await() }

        preparation.cancel()
        withTimeout(2_000) { stopStarted.await() }
        assertFalse(preparation.isCompleted)
        releaseStop.complete(Unit)
        assertThrows(CancellationException::class.java) { runBlocking { preparation.await() } }
        assertEquals(WarmupRecipientSession.State.COLD_FALLBACK, discovery.state())
    }

    @Test
    fun `late donor rejection cannot prevent cleanup of already prepared consumers`() = runBlocking {
        val stopped = CopyOnWriteArrayList<String>()
        val discovery = discovery(
            preparation = WarmupRecipientConsumerPreparation { _, start ->
                preparedConsumer(start, CompletableDeferred(Unit), stopped)
            }
        )
        discovery.onOffer(offer(mappings))
        val selection = assertInstanceOf(WarmupRecipientSelection.Accepted::class.java, discovery.selectDonors())
        val acceptance = selection.messages.single()
        discovery.onDonorAccepted(accepted(acceptance))
        assertInstanceOf(WarmupRecipientPreparationResult.Ready::class.java, discovery.prepareConsumers())
        assertTrue(
            discovery.onDonorRejected(
                SessionRejected(
                    acceptance.namespace, acceptance.sessionId, acceptance.targetInstanceId,
                    acceptance.senderInstanceId, "late_failure", acceptance.configurationFingerprint,
                    acceptance.expiresAtEpochMillis
                )
            )
        )

        assertEquals(WarmupRecipientCleanupResult.Complete, discovery.terminate())
        assertEquals(mappings.map { it.warmupTopic }.reversed(), stopped)
    }

    @Test
    fun `cleanup timeout is reported and pending consumers remain available for another wait`() = runBlocking {
        val releaseStop = CompletableDeferred<Unit>()
        val discovery = discovery(
            preparation = WarmupRecipientConsumerPreparation { _, start ->
                object : PreparedWarmupRecipientConsumer {
                    override val assignment = start.assignment
                    override val startOffset = start.offset
                    override fun start() = Unit
                    override suspend fun awaitReady() = Unit
                    override suspend fun stop() = releaseStop.await()
                }
            },
            preparationTimeout = 20.milliseconds
        )
        discovery.onOffer(offer(mappings))
        val selection = assertInstanceOf(WarmupRecipientSelection.Accepted::class.java, discovery.selectDonors())
        discovery.onDonorAccepted(accepted(selection.messages.single()))
        assertInstanceOf(WarmupRecipientPreparationResult.Ready::class.java, discovery.prepareConsumers())

        val incomplete = assertInstanceOf(
            WarmupRecipientCleanupResult.Incomplete::class.java,
            discovery.terminate()
        )
        assertEquals(mappings.map(::WarmupPartitionAssignment).toSet(), incomplete.pendingAssignments.toSet())
        assertTrue("recipient_consumer_cleanup_timeout" in incomplete.diagnostics)

        releaseStop.complete(Unit)
        assertEquals(WarmupRecipientCleanupResult.Complete, discovery.terminate())
    }

    @Test
    fun `cleanup deadline force closes every prepared recipient consumer`() = runBlocking {
        val aborts = AtomicInteger()
        val discovery = discovery(
            preparation = WarmupRecipientConsumerPreparation { _, start ->
                object : PreparedWarmupRecipientConsumer {
                    override val assignment = start.assignment
                    override val startOffset = start.offset
                    override fun start() = Unit
                    override suspend fun awaitReady() = Unit
                    override suspend fun stop(): Unit = awaitCancellation()
                    override suspend fun abort(): Boolean {
                        aborts.incrementAndGet()
                        return true
                    }
                }
            },
            preparationTimeout = 20.milliseconds
        )
        discovery.onOffer(offer(mappings))
        val selection = assertInstanceOf(WarmupRecipientSelection.Accepted::class.java, discovery.selectDonors())
        discovery.onDonorAccepted(accepted(selection.messages.single()))
        assertInstanceOf(WarmupRecipientPreparationResult.Ready::class.java, discovery.prepareConsumers())

        val cleanup = assertInstanceOf(
            WarmupRecipientCleanupResult.CompleteWithDiagnostics::class.java,
            discovery.terminate()
        )

        assertEquals(mappings.size, aborts.get())
        assertTrue("recipient_consumer_cleanup_timeout" in cleanup.diagnostics)
        assertTrue(cleanup.diagnostics.count { "forced_after_cleanup_timeout" in it } == mappings.size)
    }

    @Test
    fun `cleanup failure is diagnosed and failed resource remains available for retry`() = runBlocking {
        val stopAttempts = AtomicInteger()
        val completeDiscovery = discovery(
            preparation = WarmupRecipientConsumerPreparation { _, start ->
                object : PreparedWarmupRecipientConsumer {
                    override val assignment = start.assignment
                    override val startOffset = start.offset
                    override fun start() = Unit
                    override suspend fun awaitReady() = Unit
                    override suspend fun stop() {
                        if (stopAttempts.incrementAndGet() == 1) error("stop failed")
                    }
                }
            }
        )
        completeDiscovery.onOffer(offer(mappings))
        val selection = assertInstanceOf(
            WarmupRecipientSelection.Accepted::class.java,
            completeDiscovery.selectDonors()
        )
        completeDiscovery.onDonorAccepted(accepted(selection.messages.single()))
        assertInstanceOf(WarmupRecipientPreparationResult.Ready::class.java, completeDiscovery.prepareConsumers())

        val incomplete = assertInstanceOf(
            WarmupRecipientCleanupResult.Incomplete::class.java,
            completeDiscovery.terminate()
        )
        assertTrue(incomplete.diagnostics.any { "stop failed" in it })
        assertEquals(WarmupRecipientCleanupResult.Complete, completeDiscovery.terminate())
    }

    @Test
    fun `incomplete offers cold start the entire recipient without acceptance`() {
        val discovery = discovery()
        assertTrue(discovery.onOffer(offer(listOf(mappings.first()))))

        assertEquals(
            WarmupRecipientSelection.ColdFallback("incomplete_donor_coverage"),
            discovery.selectDonors()
        )
    }

    private fun discovery(
        metadata: WarmupDestinationMetadata = WarmupDestinationMetadata { mapping ->
            mapOf(0 to 0L)
        },
        preparation: WarmupRecipientConsumerPreparation = WarmupRecipientConsumerPreparation { _, start ->
            preparedConsumer(start, CompletableDeferred(Unit), CopyOnWriteArrayList())
        },
        preparationTimeout: kotlin.time.Duration = 2.seconds,
        diagnosticObserver: ((String) -> Unit)? = null
    ) = WarmupRecipientSession(
        request(),
        metadata,
        preparation,
        preparationTimeout,
        nowEpochMillis = { 1_000 },
        diagnosticObserver = diagnosticObserver
    )

    private fun request() = WarmupRequested(
        namespace, WarmupSessionId("session"), recipient, mappings, 2_000,
        normalizedAdmissionBudget(mappings), "fingerprint", 10_000
    )

    private fun offer(selected: List<WarmupTopicMapping>) = DonorOffered(
        namespace, WarmupSessionId("session"), donor, recipient,
        selected.map { WarmupDonorMapping(it, setOf(WarmupSourcePartition(0, 4))) },
        "fingerprint", 10_000
    )

    private fun accepted(message: SessionAccepted) = DonorAccepted(
        message.namespace, message.sessionId, message.targetInstanceId, message.senderInstanceId,
        message.mappings, message.admissionBudgetPerSecond,
        message.configurationFingerprint, message.expiresAtEpochMillis
    )

    private fun preparedConsumer(
        start: WarmupDestinationStart,
        ready: CompletableDeferred<Unit>,
        stopped: MutableList<String>
    ) = object : PreparedWarmupRecipientConsumer {
        override val assignment = start.assignment
        override val startOffset = start.offset
        override fun start() = Unit
        override suspend fun awaitReady() = ready.await()
        override suspend fun stop() {
            stopped += assignment.mapping.warmupTopic
        }
    }
}
