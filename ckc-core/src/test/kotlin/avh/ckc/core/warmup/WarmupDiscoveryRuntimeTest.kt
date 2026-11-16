package avh.ckc.core.warmup

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.producer.RecordMetadata
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class WarmupDiscoveryRuntimeTest {
    private val namespace = WarmupControlNamespace("global5-shared-partition-v6-test")
    private val recipientId = WarmupInstanceId("recipient")
    private val remoteDonor = WarmupInstanceId("remote-donor")
    private val mapping = WarmupTopicMapping(
        WarmupParticipantId("orders"), "orders", "warmup-orders", "mapping-fingerprint"
    )

    @Test
    fun `runtime follows request offer acceptance preparation readiness sequence`() = runBlocking {
        val router = WarmupControlRouter(recipientId, namespace)
        val channel = InMemoryControlChannel(router, remoteDonor, mapping)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val plane = MultiSessionWarmupDataPlane<String, String>(1, 10)
        val activation = WarmupDonorActivation(
            localInstanceId = recipientId,
            namespace = namespace,
            supportedMappings = listOf(mapping),
            dataPlane = plane,
            eligibility = WarmupDonorEligibility { null },
            preparation = WarmupDonorSessionPreparation { error("local donor is not selected") },
            scope = scope,
            onPreparationFailed = { channel.send(it) }
        )
        val runtime = WarmupDiscoveryRuntime(
            localInstanceId = recipientId,
            namespace = namespace,
            router = router,
            channel = channel,
            donorDiscovery = StatelessWarmupDonorDiscovery(recipientId, namespace, emptyList()) { null },
            donorActivation = activation,
            offerCollectionWindow = 1.milliseconds,
            donorAcceptanceTimeout = 1.seconds
        )
        val request = request()
        val stopped = AtomicInteger()
        val recipient = WarmupRecipientDiscovery(
            request = request,
            metadata = WarmupDestinationMetadata { mapOf(0 to 42L) },
            consumerPreparation = WarmupRecipientConsumerPreparation { _, start -> readyConsumer(start, stopped) },
            preparationTimeout = 1.seconds
        )

        runtime.startControl()
        val result = runtime.discoverAndPrepare(recipient)

        assertInstanceOf(WarmupDiscoveryResult.Ready::class.java, result)
        assertEquals(
            listOf(WarmupRequested::class, SessionAccepted::class, RecipientReady::class),
            channel.sent.map { it::class }
        )
        assertEquals(WarmupCompletionResult.Completed, runtime.completeWarmup(recipient))
        assertEquals(1, stopped.get())
        assertEquals(
            listOf(
                WarmupRequested::class,
                SessionAccepted::class,
                RecipientReady::class,
                WarmupCompletionRequested::class,
                SessionEnded::class
            ),
            channel.sent.map { it::class }
        )
        val sentAfterCompletion = channel.sent.toList()
        assertEquals(WarmupCompletionResult.Completed, runtime.completeWarmup(recipient))
        assertEquals(1, stopped.get())
        assertEquals(sentAfterCompletion, channel.sent)
        runtime.stop()
        runtime.close()
        scope.cancel()
    }

    @Test
    fun `partial readiness publication rolls back prepared consumers and broadcasts session end`() = runBlocking {
        val secondMapping = mapping.copy(
            participantId = WarmupParticipantId("payments"),
            sourceTopic = "payments",
            warmupTopic = "warmup-payments",
            mappingFingerprint = "payments-fingerprint"
        )
        val requestedMappings = listOf(mapping, secondMapping)
        val router = WarmupControlRouter(recipientId, namespace)
        val channel = PartialReadinessFailureChannel(router, requestedMappings)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val plane = MultiSessionWarmupDataPlane<String, String>(1, 10)
        val activation = WarmupDonorActivation(
            localInstanceId = recipientId,
            namespace = namespace,
            supportedMappings = emptyList(),
            dataPlane = plane,
            eligibility = WarmupDonorEligibility { null },
            preparation = WarmupDonorSessionPreparation { error("local donor is not selected") },
            scope = scope,
            onPreparationFailed = { channel.send(it) }
        )
        val runtime = WarmupDiscoveryRuntime(
            recipientId, namespace, router, channel,
            StatelessWarmupDonorDiscovery(recipientId, namespace, emptyList()) { null },
            activation, 1.milliseconds, 1.seconds
        )
        val now = System.currentTimeMillis()
        val request = WarmupRequested(
            namespace, WarmupSessionId("partial-ready"), recipientId, requestedMappings,
            now + 500, normalizedAdmissionBudget(requestedMappings), "fingerprint", now + 5_000
        )
        val releaseStop = kotlinx.coroutines.CompletableDeferred<Unit>()
        val stopped = mutableListOf<String>()
        val recipient = WarmupRecipientDiscovery(
            request,
            WarmupDestinationMetadata { mapOf(0 to 0L) },
            WarmupRecipientConsumerPreparation { _, start ->
                object : PreparedWarmupRecipientConsumer {
                    override val assignment = start.assignment
                    override val startOffset = start.offset
                    override fun start() = Unit
                    override suspend fun awaitReady() = Unit
                    override suspend fun stop() {
                        releaseStop.await()
                        stopped += assignment.mapping.warmupTopic
                    }
                }
            },
            20.milliseconds
        )

        runtime.startControl()
        val result = runtime.discoverAndPrepare(recipient)

        val incomplete = assertInstanceOf(WarmupDiscoveryResult.CleanupIncomplete::class.java, result)
        assertEquals("discovery_failed", incomplete.reasonCode)
        assertEquals(2, incomplete.cleanup.pendingAssignments.size)
        assertTrue(stopped.isEmpty())
        assertEquals(1, channel.sent.count { it is RecipientReady })
        assertTrue(channel.sent.last() is SessionEnded)
        releaseStop.complete(Unit)
        assertEquals(WarmupRecipientCleanupResult.Complete, recipient.terminate())
        assertEquals(requestedMappings.map { it.warmupTopic }.reversed(), stopped)
        runtime.stop()
        runtime.close()
        scope.cancel()
    }

    @Test
    fun `donor rejection interrupts active warmup and permits bounded rollback`() = runBlocking {
        val router = WarmupControlRouter(recipientId, namespace)
        val channel = InMemoryControlChannel(router, remoteDonor, mapping)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val activation = WarmupDonorActivation(
            recipientId,
            namespace,
            listOf(mapping),
            MultiSessionWarmupDataPlane<String, String>(1, 10),
            WarmupDonorEligibility { null },
            WarmupDonorSessionPreparation { error("local donor is not selected") },
            scope,
            onPreparationFailed = { channel.send(it) }
        )
        val runtime = WarmupDiscoveryRuntime(
            recipientId, namespace, router, channel,
            StatelessWarmupDonorDiscovery(recipientId, namespace, emptyList()) { null },
            activation, 1.milliseconds, 1.seconds
        )
        val request = request()
        val stopped = AtomicInteger()
        val recipient = WarmupRecipientDiscovery(
            request,
            WarmupDestinationMetadata { mapOf(0 to 0L) },
            WarmupRecipientConsumerPreparation { _, start -> readyConsumer(start, stopped) },
            1.seconds
        )

        runtime.startControl()
        assertInstanceOf(WarmupDiscoveryResult.Ready::class.java, runtime.discoverAndPrepare(recipient))
        router.onMessage(
            WarmupControlRecord(
                100,
                SessionRejected(
                    namespace, request.sessionId, remoteDonor, recipientId,
                    "donor_participant_unavailable", request.configurationFingerprint,
                    request.expiresAtEpochMillis
                )
            )
        )

        assertTrue(runtime.awaitWarmupInterruption(recipient, 100.milliseconds))
        assertInstanceOf(
            WarmupDiscoveryResult.ColdFallback::class.java,
            runtime.abortWarmup(recipient, "donor_session_ended")
        )
        assertEquals(1, stopped.get())
        runtime.stop()
        runtime.close()
        scope.cancel()
    }

    @Test
    fun `recipient consumer termination interrupts warmup and permits bounded rollback`() = runBlocking {
        val router = WarmupControlRouter(recipientId, namespace)
        val channel = InMemoryControlChannel(router, remoteDonor, mapping)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val activation = WarmupDonorActivation(
            recipientId,
            namespace,
            listOf(mapping),
            MultiSessionWarmupDataPlane<String, String>(1, 10),
            WarmupDonorEligibility { null },
            WarmupDonorSessionPreparation { error("local donor is not selected") },
            scope,
            onPreparationFailed = { channel.send(it) }
        )
        val runtime = WarmupDiscoveryRuntime(
            recipientId, namespace, router, channel,
            StatelessWarmupDonorDiscovery(recipientId, namespace, emptyList()) { null },
            activation, 1.milliseconds, 1.seconds
        )
        val termination = kotlinx.coroutines.CompletableDeferred<Unit>()
        val stopped = AtomicInteger()
        val recipient = WarmupRecipientDiscovery(
            request(),
            WarmupDestinationMetadata { mapOf(0 to 0L) },
            WarmupRecipientConsumerPreparation { _, start ->
                object : PreparedWarmupRecipientConsumer {
                    override val assignment = start.assignment
                    override val startOffset = start.offset
                    override fun start() = Unit
                    override suspend fun awaitReady() = Unit
                    override suspend fun awaitTermination() = termination.await()
                    override suspend fun stop() {
                        stopped.incrementAndGet()
                    }
                }
            },
            1.seconds
        )

        runtime.startControl()
        assertInstanceOf(WarmupDiscoveryResult.Ready::class.java, runtime.discoverAndPrepare(recipient))
        termination.completeExceptionally(IllegalStateException("warm-up topic was recreated"))

        assertTrue(runtime.awaitWarmupInterruption(recipient, 100.milliseconds))
        assertInstanceOf(
            WarmupDiscoveryResult.ColdFallback::class.java,
            runtime.abortWarmup(recipient, "recipient_consumer_terminated")
        )
        assertEquals(1, stopped.get())
        runtime.stop()
        runtime.close()
        scope.cancel()
    }

    @Test
    fun `missing donor close acknowledgement cold falls back and stops recipient consumer`() = runBlocking {
        val router = WarmupControlRouter(recipientId, namespace)
        val channel = InMemoryControlChannel(router, remoteDonor, mapping, acknowledgeCompletion = false)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val plane = MultiSessionWarmupDataPlane<String, String>(1, 10)
        val activation = WarmupDonorActivation(
            localInstanceId = recipientId,
            namespace = namespace,
            supportedMappings = listOf(mapping),
            dataPlane = plane,
            eligibility = WarmupDonorEligibility { null },
            preparation = WarmupDonorSessionPreparation { error("local donor is not selected") },
            scope = scope,
            onPreparationFailed = { channel.send(it) }
        )
        val runtime = WarmupDiscoveryRuntime(
            recipientId, namespace, router, channel,
            StatelessWarmupDonorDiscovery(recipientId, namespace, emptyList()) { null },
            activation, 1.milliseconds, 1.seconds, 10.milliseconds
        )
        val stopped = AtomicInteger()
        val recipient = WarmupRecipientDiscovery(
            request(),
            WarmupDestinationMetadata { mapOf(0 to 0L) },
            WarmupRecipientConsumerPreparation { _, start -> readyConsumer(start, stopped) },
            1.seconds
        )

        runtime.startControl()
        assertInstanceOf(WarmupDiscoveryResult.Ready::class.java, runtime.discoverAndPrepare(recipient))
        val result = runtime.completeWarmup(recipient)

        val fallback = assertInstanceOf(WarmupCompletionResult.ColdFallback::class.java, result)
        assertEquals("donor_completion_failed", fallback.reasonCode)
        assertEquals(1, stopped.get())
        assertEquals(WarmupRecipientDiscovery.State.COLD_FALLBACK, recipient.state())
        assertTrue(channel.sent[channel.sent.lastIndex - 1] is WarmupCompletionRequested)
        assertTrue(channel.sent.last() is SessionEnded)
        runtime.stop()
        runtime.close()
        scope.cancel()
    }

    private fun request(): WarmupRequested {
        val now = System.currentTimeMillis()
        return WarmupRequested(
            namespace, WarmupSessionId("session"), recipientId, listOf(mapping),
            now + 500, normalizedAdmissionBudget(listOf(mapping)), "fingerprint", now + 5_000
        )
    }

    private fun readyConsumer(
        start: WarmupDestinationStart,
        stopped: AtomicInteger = AtomicInteger()
    ) = object : PreparedWarmupRecipientConsumer {
        override val assignment = start.assignment
        override val startOffset = start.offset
        override fun start() = Unit
        override suspend fun awaitReady() = Unit
        override suspend fun stop() {
            stopped.incrementAndGet()
        }
    }

    private class InMemoryControlChannel(
        private val router: WarmupControlRouter,
        private val donor: WarmupInstanceId,
        private val mapping: WarmupTopicMapping,
        private val acknowledgeCompletion: Boolean = true
    ) : WarmupControlChannel {
        val sent = mutableListOf<WarmupControlMessage>()

        override fun start() = Unit
        override suspend fun awaitReady() = Unit
        override fun initialOffsetBoundary(): Long = -1
        override suspend fun awaitTermination() = Unit
        override suspend fun stop() = Unit

        override suspend fun send(message: WarmupControlMessage): RecordMetadata {
            sent += message
            when (message) {
                is WarmupRequested -> router.onMessage(
                    WarmupControlRecord(
                        sent.size.toLong(),
                        DonorOffered(
                            message.namespace, message.sessionId, donor, message.senderInstanceId,
                            listOf(WarmupDonorMapping(mapping, setOf(WarmupSourcePartition(0, 1)))),
                            message.configurationFingerprint, message.expiresAtEpochMillis
                        )
                    )
                )
                is SessionAccepted -> router.onMessage(
                    WarmupControlRecord(
                        sent.size.toLong(),
                        DonorAccepted(
                            message.namespace, message.sessionId, donor, message.senderInstanceId,
                            message.mappings, message.admissionBudgetPerSecond,
                            message.configurationFingerprint, message.expiresAtEpochMillis
                        )
                    )
                )
                is WarmupCompletionRequested -> if (acknowledgeCompletion) router.onMessage(
                    WarmupControlRecord(
                        sent.size.toLong(),
                        DonorClosed(
                            message.namespace, message.sessionId, donor, message.senderInstanceId,
                            message.configurationFingerprint, message.expiresAtEpochMillis
                        )
                    )
                )
                else -> Unit
            }
            return mock()
        }
    }

    private class PartialReadinessFailureChannel(
        private val router: WarmupControlRouter,
        private val mappings: List<WarmupTopicMapping>
    ) : WarmupControlChannel {
        val sent = mutableListOf<WarmupControlMessage>()
        private val donors = mappings.indices.map { WarmupInstanceId("donor-$it") }
        private var readinessCount = 0

        override fun start() = Unit
        override suspend fun awaitReady() = Unit
        override fun initialOffsetBoundary(): Long = -1
        override suspend fun awaitTermination() = Unit
        override suspend fun stop() = Unit

        override suspend fun send(message: WarmupControlMessage): RecordMetadata {
            if (message is RecipientReady && ++readinessCount == 2) {
                throw IllegalStateException("second readiness send failed")
            }
            sent += message
            when (message) {
                is WarmupRequested -> mappings.forEachIndexed { index, mapping ->
                    router.onMessage(
                        WarmupControlRecord(
                            sent.size.toLong() + index,
                            DonorOffered(
                                message.namespace, message.sessionId, donors[index], message.senderInstanceId,
                                listOf(WarmupDonorMapping(mapping, setOf(WarmupSourcePartition(0, 1)))),
                                message.configurationFingerprint, message.expiresAtEpochMillis
                            )
                        )
                    )
                }
                is SessionAccepted -> router.onMessage(
                    WarmupControlRecord(
                        sent.size.toLong(),
                        DonorAccepted(
                            message.namespace, message.sessionId, message.targetInstanceId,
                            message.senderInstanceId, message.mappings, message.admissionBudgetPerSecond,
                            message.configurationFingerprint, message.expiresAtEpochMillis
                        )
                    )
                )
                else -> Unit
            }
            return mock()
        }
    }
}
