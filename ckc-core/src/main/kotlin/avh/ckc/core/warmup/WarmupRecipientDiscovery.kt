package avh.ckc.core.warmup

import avh.ckc.core.isFatalJvmError
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration

internal data class WarmupDestinationStart(
    val assignment: WarmupPartitionAssignment,
    val offset: Long
) {
    init {
        require(offset >= 0) { "Warm-up start offset must not be negative" }
    }
}

/** Metadata is resolved through the participant connection belonging to each mapping. */
internal fun interface WarmupDestinationMetadata {
    suspend fun endOffsets(mapping: WarmupTopicMapping): Map<Int, Long>
}

internal interface PreparedWarmupRecipientConsumer {
    val assignment: WarmupPartitionAssignment
    val startOffset: Long
    fun start()
    suspend fun awaitReady()
    suspend fun stop()
}

internal fun interface WarmupRecipientConsumerPreparation {
    suspend fun prepare(
        sessionId: WarmupSessionId,
        start: WarmupDestinationStart
    ): PreparedWarmupRecipientConsumer
}

internal sealed interface WarmupRecipientSelection {
    data class Accepted(val messages: List<SessionAccepted>) : WarmupRecipientSelection
    data class ColdFallback(val reasonCode: String) : WarmupRecipientSelection
}

internal sealed interface WarmupRecipientPreparationResult {
    data class Ready(val messages: List<RecipientReady>) : WarmupRecipientPreparationResult
    data class ColdFallback(
        val reasonCode: String,
        val cleanup: WarmupRecipientCleanupResult = WarmupRecipientCleanupResult.Complete
    ) : WarmupRecipientPreparationResult
}

internal sealed interface WarmupRecipientCleanupResult {
    data object Complete : WarmupRecipientCleanupResult
    data class CompleteWithDiagnostics(val diagnostics: List<String>) : WarmupRecipientCleanupResult
    data class Incomplete(
        val pendingAssignments: List<WarmupPartitionAssignment>,
        val diagnostics: List<String>
    ) : WarmupRecipientCleanupResult
}

private class WarmupRecipientPreparationStoppedException :
    IllegalStateException("Warm-up recipient preparation was stopped")

private class WarmupMetadataLookupException(
    mapping: WarmupTopicMapping,
    cause: Throwable
) : IllegalStateException("Warm-up metadata lookup failed for ${mapping.warmupTopic}", cause)

/**
 * Owns one recipient session only. Offers remain observations until [selectDonors] chooses complete
 * coverage. Preparation captures every destination end before readiness and publishes no readiness
 * until every independently owned consumer reports its explicit assignment and position ready.
 */
internal class WarmupRecipientDiscovery(
    internal val request: WarmupRequested,
    private val metadata: WarmupDestinationMetadata,
    private val consumerPreparation: WarmupRecipientConsumerPreparation,
    private val preparationTimeout: Duration,
    private val nowEpochMillis: () -> Long = System::currentTimeMillis,
    private val diagnosticObserver: ((String) -> Unit)? = null
) {
    internal enum class State {
        DISCOVERING,
        ADMITTING_DONORS,
        PREPARING_CONSUMERS,
        READY,
        DRAINING,
        COMPLETED,
        COLD_FALLBACK
    }

    private val lock = Any()
    private val log = LoggerFactory.getLogger(javaClass)
    private val cleanupMutex = Mutex()
    private var state = State.DISCOVERING
    private val offers = linkedMapOf<WarmupInstanceId, DonorOffered>()
    private var acceptances = emptyMap<WarmupInstanceId, SessionAccepted>()
    private val confirmations = linkedMapOf<WarmupInstanceId, DonorAccepted>()
    private val closedDonors = linkedMapOf<WarmupInstanceId, DonorClosed>()
    private val prepared = mutableListOf<PreparedWarmupRecipientConsumer>()

    init {
        require(preparationTimeout.isPositive()) { "Warm-up preparation timeout must be > 0" }
        require(request.protocolVersion == WARMUP_CONTROL_PROTOCOL_VERSION) {
            "Recipient request must use warm-up control protocol v$WARMUP_CONTROL_PROTOCOL_VERSION"
        }
    }

    fun onOffer(offer: DonorOffered): Boolean = synchronized(lock) {
        if (state != State.DISCOVERING || !matchesRequest(offer)) return@synchronized false
        if (nowEpochMillis() > request.offerDeadlineEpochMillis) return@synchronized false
        if (offer.mappings.any { it.mapping !in request.mappings }) return@synchronized false
        val existing = offers[offer.senderInstanceId]
        if (existing != null && existing != offer) return@synchronized false
        offers[offer.senderInstanceId] = offer
        true
    }

    fun selectDonors(): WarmupRecipientSelection = synchronized(lock) {
        check(state == State.DISCOVERING) { "Recipient discovery has already selected donors" }
        val uncovered = request.mappings.toMutableSet()
        val selected = linkedMapOf<WarmupInstanceId, MutableList<WarmupDonorMapping>>()
        offers.values
            .sortedWith(compareByDescending<DonorOffered> { offer ->
                offer.mappings.count { it.mapping in uncovered }
            }.thenBy { it.senderInstanceId.value })
            .forEach { offer ->
                val accepted = offer.mappings.filter { it.mapping in uncovered }
                if (accepted.isNotEmpty()) {
                    selected.getOrPut(offer.senderInstanceId, ::mutableListOf).addAll(accepted)
                    uncovered.removeAll(accepted.map { it.mapping }.toSet())
                }
            }
        if (uncovered.isNotEmpty()) {
            state = State.COLD_FALLBACK
            return@synchronized WarmupRecipientSelection.ColdFallback("incomplete_donor_coverage")
        }
        acceptances = selected.mapValues { (donor, mappings) ->
            SessionAccepted(
                namespace = request.namespace,
                sessionId = request.sessionId,
                senderInstanceId = request.senderInstanceId,
                targetInstanceId = donor,
                mappings = mappings,
                intervalBudget = request.intervalBudget,
                initialQuota = request.initialQuota,
                configurationFingerprint = request.configurationFingerprint,
                expiresAtEpochMillis = request.expiresAtEpochMillis
            )
        }
        state = State.ADMITTING_DONORS
        WarmupRecipientSelection.Accepted(acceptances.values.toList())
    }

    fun onDonorAccepted(accepted: DonorAccepted): Boolean = synchronized(lock) {
        if (state != State.ADMITTING_DONORS) return@synchronized false
        val expected = acceptances[accepted.senderInstanceId] ?: return@synchronized false
        if (!matches(expected, accepted)) return@synchronized false
        val existing = confirmations[accepted.senderInstanceId]
        if (existing != null && existing != accepted) return@synchronized false
        confirmations[accepted.senderInstanceId] = accepted
        true
    }

    fun onDonorRejected(rejected: SessionRejected): Boolean = synchronized(lock) {
        if (state !in setOf(State.ADMITTING_DONORS, State.PREPARING_CONSUMERS, State.READY, State.DRAINING)) {
            return@synchronized false
        }
        val expected = acceptances[rejected.senderInstanceId] ?: return@synchronized false
        if (rejected.protocolVersion != WARMUP_CONTROL_PROTOCOL_VERSION ||
            rejected.namespace != expected.namespace || rejected.sessionId != expected.sessionId ||
            rejected.senderInstanceId != expected.targetInstanceId ||
            rejected.targetInstanceId != expected.senderInstanceId ||
            rejected.configurationFingerprint != expected.configurationFingerprint ||
            rejected.expiresAtEpochMillis != expected.expiresAtEpochMillis
        ) {
            return@synchronized false
        }
        state = State.COLD_FALLBACK
        true
    }

    /** Starts completion once and returns the same immutable requests for duplicate local calls. */
    fun completionRequests(): List<WarmupCompletionRequested> = synchronized(lock) {
        check(state == State.READY || state == State.DRAINING) {
            "Recipient warm-up is not ready for completion"
        }
        state = State.DRAINING
        acceptances.map { (donor, acceptance) ->
            WarmupCompletionRequested(
                namespace = request.namespace,
                sessionId = request.sessionId,
                senderInstanceId = request.senderInstanceId,
                targetInstanceId = donor,
                configurationFingerprint = acceptance.configurationFingerprint,
                expiresAtEpochMillis = acceptance.expiresAtEpochMillis
            )
        }
    }

    /** Unknown, forged, or parameter-mismatched acknowledgements cannot advance completion. */
    fun onDonorClosed(closed: DonorClosed): Boolean = synchronized(lock) {
        if (state != State.DRAINING) return@synchronized false
        val expected = acceptances[closed.senderInstanceId] ?: return@synchronized false
        if (closed.protocolVersion != WARMUP_CONTROL_PROTOCOL_VERSION ||
            closed.namespace != expected.namespace || closed.sessionId != expected.sessionId ||
            closed.senderInstanceId != expected.targetInstanceId ||
            closed.targetInstanceId != expected.senderInstanceId ||
            closed.configurationFingerprint != expected.configurationFingerprint ||
            closed.expiresAtEpochMillis != expected.expiresAtEpochMillis
        ) {
            return@synchronized false
        }
        val existing = closedDonors[closed.senderInstanceId]
        if (existing != null && existing != closed) return@synchronized false
        closedDonors[closed.senderInstanceId] = closed
        true
    }

    fun allDonorsClosed(): Boolean = synchronized(lock) {
        state == State.DRAINING && closedDonors.keys == acceptances.keys
    }

    /** Stops recipient consumers only after every selected donor has fenced new delegation. */
    suspend fun completeAfterDonorsClosed(): WarmupRecipientCleanupResult {
        synchronized(lock) {
            check(state == State.DRAINING && closedDonors.keys == acceptances.keys) {
                "Recipient donors have not all closed"
            }
        }
        val cleanup = cleanupPreparedConsumers()
        if (cleanup !is WarmupRecipientCleanupResult.Incomplete) {
            synchronized(lock) {
                if (state == State.DRAINING) state = State.COMPLETED
            }
        }
        return cleanup
    }

    suspend fun prepareConsumers(): WarmupRecipientPreparationResult {
        synchronized(lock) {
            check(state == State.ADMITTING_DONORS) { "Recipient donors are not being admitted" }
            if (confirmations.keys != acceptances.keys) {
                state = State.COLD_FALLBACK
                return WarmupRecipientPreparationResult.ColdFallback("incomplete_donor_acceptance")
            }
            state = State.PREPARING_CONSUMERS
        }
        val unresolvedMappings = ConcurrentHashMap.newKeySet<WarmupTopicMapping>().apply {
            addAll(request.mappings)
        }
        val metadataComplete = AtomicBoolean(false)
        return try {
            val remainingMillis = request.expiresAtEpochMillis - nowEpochMillis()
            if (remainingMillis <= 0) return coldFallback("session_expired")
            val timeoutMillis = minOf(preparationTimeout.inWholeMilliseconds, remainingMillis).coerceAtLeast(1)
            val completed = withTimeoutOrNull(timeoutMillis) {
                val starts = coroutineScope {
                    request.mappings.map { mapping ->
                        async {
                            try {
                                val endOffsets = metadata.endOffsets(mapping)
                                require(endOffsets.keys == setOf(0) && endOffsets.getValue(0) >= 0) {
                                    "Warm-up topic ${mapping.warmupTopic} must expose exactly partition 0"
                                }
                                unresolvedMappings.remove(mapping)
                                WarmupDestinationStart(
                                    WarmupPartitionAssignment(mapping),
                                    endOffsets.getValue(0)
                                )
                            } catch (error: CancellationException) {
                                throw error
                            } catch (error: Throwable) {
                                if (error.isFatalJvmError()) throw error
                                throw WarmupMetadataLookupException(mapping, error)
                            }
                        }
                    }.awaitAll()
                }
                metadataComplete.set(true)
                starts.forEach { start ->
                    val consumer = consumerPreparation.prepare(request.sessionId, start)
                    // Transfer ownership before validation so every returned resource participates in rollback.
                    val retained = synchronized(lock) {
                        prepared.add(consumer)
                        state == State.PREPARING_CONSUMERS
                    }
                    if (!retained) {
                        stopAll(listOf(consumer))
                        throw WarmupRecipientPreparationStoppedException()
                    }
                    require(consumer.assignment == start.assignment && consumer.startOffset == start.offset) {
                        "Prepared warm-up consumer does not match its captured start"
                    }
                }
                val consumers = synchronized(lock) { prepared.toList() }
                consumers.forEach(PreparedWarmupRecipientConsumer::start)
                coroutineScope { consumers.map { async { it.awaitReady() } }.awaitAll() }
                true
            }
            if (completed == null) {
                val reason = if (metadataComplete.get()) "consumer_preparation_timeout" else "metadata_timeout"
                if (!metadataComplete.get()) logMetadataFallback(reason, unresolvedMappings, null)
                return coldFallback(reason)
            }
            val readyMessages = synchronized(lock) {
                if (state != State.PREPARING_CONSUMERS) return@synchronized null
                acceptances.map { (donor, acceptance) ->
                    RecipientReady(
                        namespace = request.namespace,
                        sessionId = request.sessionId,
                        senderInstanceId = request.senderInstanceId,
                        targetInstanceId = donor,
                        assignments = acceptance.mappings.map { WarmupPartitionAssignment(it.mapping) },
                        configurationFingerprint = request.configurationFingerprint,
                        expiresAtEpochMillis = request.expiresAtEpochMillis
                    )
                }
            }
            if (readyMessages == null) coldFallback("preparation_cancelled")
            else WarmupRecipientPreparationResult.Ready(readyMessages)
        } catch (error: CancellationException) {
            coldFallback("preparation_cancelled")
            throw error
        } catch (error: WarmupMetadataLookupException) {
            logMetadataFallback("metadata_lookup_failed", unresolvedMappings, error.cause)
            coldFallback("metadata_lookup_failed")
        } catch (error: Exception) {
            coldFallback("consumer_preparation_failed")
        }
    }

    /** Commits activation only after readiness was published to every selected donor. */
    fun confirmReadyPublished(): Boolean = synchronized(lock) {
        if (state != State.PREPARING_CONSUMERS) return@synchronized false
        state = State.READY
        true
    }

    /**
     * Terminates this recipient session from any non-terminal stage, including prepared readiness.
     * An incomplete result retains the unresolved consumers; another call waits for them again.
     */
    suspend fun terminate(): WarmupRecipientCleanupResult = withContext(NonCancellable) {
        cleanupMutex.withLock {
            val resources = synchronized(lock) {
                state = State.COLD_FALLBACK
                prepared.toList()
            }
            stopAll(resources)
        }
    }

    private suspend fun cleanupPreparedConsumers(): WarmupRecipientCleanupResult = withContext(NonCancellable) {
        cleanupMutex.withLock {
            stopAll(synchronized(lock) { prepared.toList() })
        }
    }

    internal fun state(): State = synchronized(lock) { state }

    private fun logMetadataFallback(
        reason: String,
        unfinishedMappings: Collection<WarmupTopicMapping>,
        cause: Throwable?
    ) {
        val mappings = unfinishedMappings.sortedWith(
            compareBy({ it.participantId.value }, WarmupTopicMapping::sourceTopic)
        )
        val diagnostic = buildString {
            append("event_code=CKC_WARMUP_METADATA_COLD_FALLBACK")
            append(" reason=").append(reason)
            append(" session_id=").append(request.sessionId.value)
            append(" participant_ids=")
            append(mappings.map { it.participantId.value }.distinct().joinToString(","))
            append(" topics=").append(mappings.map { it.warmupTopic }.distinct().sorted().joinToString(","))
            append(" unfinished_mappings=")
            append(mappings.joinToString(",") { mapping ->
                "${mapping.participantId.value}:${mapping.sourceTopic}->${mapping.warmupTopic}"
            })
            append(" cause_type=").append(cause?.javaClass?.simpleName ?: "timeout")
        }
        log.warn(diagnostic)
        runCatching { diagnosticObserver?.invoke(diagnostic) }
    }

    internal fun allDonorsAccepted(): Boolean = synchronized(lock) {
        state == State.ADMITTING_DONORS && confirmations.keys == acceptances.keys
    }

    private suspend fun coldFallback(reasonCode: String): WarmupRecipientPreparationResult.ColdFallback {
        val cleanup = terminate()
        return WarmupRecipientPreparationResult.ColdFallback(reasonCode, cleanup)
    }

    private suspend fun stopAll(
        resources: List<PreparedWarmupRecipientConsumer>
    ): WarmupRecipientCleanupResult {
        if (resources.isEmpty()) return WarmupRecipientCleanupResult.Complete
        val completed = mutableListOf<PreparedWarmupRecipientConsumer>()
        val diagnostics = mutableListOf<String>()
        // Cleanup owns a separate bounded cancellation scope so caller cancellation cannot leak clients.
        val finished = withContext(NonCancellable) {
            withTimeoutOrNull(preparationTimeout.inWholeMilliseconds.coerceAtLeast(1)) {
                resources.asReversed().forEach { resource ->
                    try {
                        resource.stop()
                        completed += resource
                    } catch (error: CancellationException) {
                        throw error
                    } catch (error: WarmupRecipientConsumerStoppedWithFailureException) {
                        diagnostics += "${resource.assignment.destination}: ${error.cause?.javaClass?.name}: ${error.cause?.message}"
                        completed += resource
                    } catch (error: Throwable) {
                        if (error.isFatalJvmError()) throw error
                        diagnostics += "${resource.assignment.destination}: ${error.javaClass.name}: ${error.message}"
                    }
                }
                true
            }
        }
        if (finished != true) diagnostics += "recipient_consumer_cleanup_timeout"
        val pending = synchronized(lock) {
            prepared.removeAll(completed.toSet())
            prepared.map(PreparedWarmupRecipientConsumer::assignment)
        }
        return if (pending.isEmpty()) {
            if (diagnostics.isEmpty()) {
                WarmupRecipientCleanupResult.Complete
            } else {
                WarmupRecipientCleanupResult.CompleteWithDiagnostics(diagnostics.toList())
            }
        } else {
            WarmupRecipientCleanupResult.Incomplete(pending, diagnostics.toList())
        }
    }

    private fun matchesRequest(message: WarmupControlMessage): Boolean =
        message.namespace == request.namespace &&
            message.protocolVersion == WARMUP_CONTROL_PROTOCOL_VERSION &&
            message.sessionId == request.sessionId &&
            message.targetInstanceId == request.senderInstanceId &&
            message.configurationFingerprint == request.configurationFingerprint &&
            message.expiresAtEpochMillis == request.expiresAtEpochMillis &&
            message.senderInstanceId != request.senderInstanceId

    private fun matches(expected: SessionAccepted, accepted: DonorAccepted): Boolean =
        accepted.namespace == expected.namespace &&
            accepted.protocolVersion == WARMUP_CONTROL_PROTOCOL_VERSION &&
            accepted.sessionId == expected.sessionId &&
            accepted.senderInstanceId == expected.targetInstanceId &&
            accepted.targetInstanceId == expected.senderInstanceId &&
            accepted.mappings == expected.mappings &&
            accepted.intervalBudget == expected.intervalBudget &&
            accepted.initialQuota == expected.initialQuota &&
            accepted.configurationFingerprint == expected.configurationFingerprint &&
            accepted.expiresAtEpochMillis == expected.expiresAtEpochMillis
}
