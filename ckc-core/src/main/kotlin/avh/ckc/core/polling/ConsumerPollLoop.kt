package avh.ckc.core.polling

import avh.ckc.core.AssignedPartitionSnapshot
import avh.ckc.core.kafka.KafkaConsumerConfigAdapter
import avh.ckc.core.PollLoopStateSnapshot
import avh.ckc.core.ProcessingMode
import avh.ckc.core.metrics.BackpressureAction
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.LifecycleOutcome
import avh.ckc.core.metrics.RecordHandoffReason
import avh.ckc.core.polling.partition.OffsetCommitData
import avh.ckc.core.polling.partition.PartitionRegistry
import avh.ckc.core.polling.partition.PartitionState
import avh.ckc.core.polling.partition.offset.OffsetTrackerMetadata
import avh.ckc.core.polling.partition.offset.OffsetTrackerMetadataContext
import avh.ckc.core.processing.PolledRecordSink
import avh.ckc.core.isFreshnessFirst
import avh.ckc.core.tracksProcessedOffsets
import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.AuthorizationException
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.errors.WakeupException
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.regex.Pattern
import kotlin.concurrent.Volatile
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.toKotlinDuration

/**
 * Single-thread Kafka poll loop + dispatch into [recordSink].
 *
 * Design notes:
 * - KafkaConsumer is confined to a dedicated single thread (not thread-safe).
 * - Kafka performs configured key and value deserialization inside poll().
 *
 * Processing modes:
 * - AT_LEAST_ONCE_NO_ORDERING: non-blocking dispatch via `trySend` + `pause/resume`
 *   with bounded local stash and explicit contiguous commits.
 * - Freshness-first modes: best-effort dispatch with explicit commits of the latest
 *   accepted position, independent of processing completion.
 *
 * Invariants (AT_LEAST_ONCE_NO_ORDERING):
 * - Poll loop never suspends on dispatch.
 * - Pause/resume applies to all assigned partitions.
 * - Offsets are committed only when contiguous-ready (via [PartitionState]).
 */
internal class ConsumerPollLoop<K, V>(
    val id: Int,
    parentContext: CoroutineContext,
    private val processingMode: ProcessingMode,
    private val commitIntervalMs: Long,
    private val commitRecordsThreshold: Int,
    private val metrics: ConsumerMetrics<K, V>,
    private val consumerProperties: Map<String, Any?>,
    private val consumerConfigAdapter: KafkaConsumerConfigAdapter,
    private val topics: List<String>?,
    private val topicsPattern: Pattern?,
    private val recordSink: PolledRecordSink<K, V>,
    private val partitionStateRegistry: PartitionRegistry,
    private val kafkaConsumerFactory:
        (consumerProperties: Map<String, Any?>) -> KafkaConsumer<K, V> = {
            KafkaConsumer(it)
        },
) : ConsumerPollLoopControl {
    private var activeStash: ArrayDeque<ConsumerRecord<K, V>>? = null

    @Volatile
    private var partitionHandoffTimeoutNanos = PARTITION_HANDOFF_TIMEOUT_NANOS

    @Volatile
    private var processingCancellationTimeoutNanos = PARTITION_HANDOFF_TIMEOUT_NANOS

    @Volatile
    private var shutdownDrainTimeoutNanos = PARTITION_HANDOFF_TIMEOUT_NANOS

    @Volatile
    private var shutdownDeadlineNanos: Long? = null

    @Volatile
    private var recoveryTimeoutNanos = RECOVERY_TIMEOUT_NANOS

    @Volatile
    private var recoveryRetryBackoffMillis = RECOVERY_RETRY_BACKOFF_MILLIS

    @Volatile
    private var recoveryDeadlineNanos: Long? = null

    @Volatile
    private var recoveringLostAssignment = false

    @Volatile
    private var lastRecoveryReason: String? = null

    private var recoveryStartedNanos: Long? = null
    private var recoveryMetricActive = false
    /** Dedicated poll thread (KafkaConsumer thread-safety). */
    private val dispatcher = Executors
        .newSingleThreadExecutor { r -> Thread(r, "kafka-poll-$id").apply { isDaemon = true } }
        .asCoroutineDispatcher()

    /**
     * Poll loop scope confined to [dispatcher].
     *
     * - SupervisorJob: failure in a sibling coroutine should not automatically cancel this loop.
     * - Inherits parent context (except Job) to preserve structured concurrency boundaries.
     */
    private val lifecycleJob = SupervisorJob(parentContext[Job])
    private val scope = CoroutineScope(
        lifecycleJob +
                parentContext.minusKey(Job) +
                dispatcher +
                CoroutineName("ConsumerPollLoop-$id")
    )
    private val resourceReleaseStarted = AtomicBoolean(false)
    private val resourceReleaseCompletion = CompletableDeferred<Unit>()

    private var job: Job? = null

    /** Assigned partition states (mutated only on poll thread via rebalance callbacks). */
    private val assignedPartitions = mutableSetOf<PartitionState>()

    @Volatile
    private var assignedPartitionSnapshot: List<AssignedPartitionSnapshot> = emptyList()

    private val log = LoggerFactory.getLogger(this::class.java)

    /** Set from outside poll thread to initiate graceful shutdown. */
    @Volatile
    private var shutdownRequested = false

    @Volatile
    private var started = false

    @Volatile
    private var running = false

    @Volatile
    private var lastPollEpochMillis: Long? = null

    @Volatile
    private var lastPollRecordCount: Int? = null

    @Volatile
    private var lastCommitAttemptEpochMillis: Long? = null

    @Volatile
    private var lastCommitSucceeded: Boolean? = null

    /** Used to call wakeup() from outside poll thread. */
    @Volatile
    private var consumerRef: Consumer<K, V>? = null

    /** Completed when tracked at-least-once shutdown tail is drained (caller should cancel job afterwards). */
    private val readyForShutdownSignal = CompletableDeferred<Unit>()
    private val finalizedShutdownSignal = CompletableDeferred<Unit>()

    @Volatile
    private var finalizationRequested = false

    @Volatile
    private var shutdownPaused = false

    private val consumerGroupId: String by lazy {
        requireNotNull(consumerConfigAdapter.getString(GROUP_ID_CONFIG)) {
            "Kafka consumer group.id is required for tracked offset metadata"
        }
    }

    override fun start(): Job {
        check(job == null)
        started = true
        job = scope.launch { runLoop() }
        return job!!
    }

    /**
     * Phase 1 shutdown: request draining and pause poll().
     * Phase 2: loop completes [readyForShutdownSignal] once stash drained and poll returns empty.
     */
    override fun prepareForShutdown(): Deferred<Unit> {
        return prepareForShutdown(System.nanoTime() + shutdownDrainTimeoutNanos)
    }

    override fun prepareForShutdown(deadlineNanos: Long): Deferred<Unit> {
        shutdownDeadlineNanos = deadlineNanos
        shutdownRequested = true
        consumerRef?.wakeup()
        return readyForShutdownSignal
    }

    override fun finalizeShutdown(): Deferred<Unit> {
        finalizationRequested = true
        // The drained loop rechecks this flag after its short empty-poll delay. A wakeup here could
        // remain pending when the loop exits and interrupt the final commit instead of a poll.
        return finalizedShutdownSignal
    }

    override fun stateSnapshot(): PollLoopStateSnapshot =
        PollLoopStateSnapshot(
            id = id,
            started = started,
            running = running,
            shutdownRequested = shutdownRequested,
            assignedPartitions = assignedPartitionSnapshot,
            lastPollEpochMillis = lastPollEpochMillis,
            lastPollRecordCount = lastPollRecordCount,
            lastCommitAttemptEpochMillis = lastCommitAttemptEpochMillis,
            lastCommitSucceeded = lastCommitSucceeded,
            recovering = recoveringLostAssignment,
            lastRecoveryReason = lastRecoveryReason
        )

    override fun configurePartitionHandoffTimeout(timeout: kotlin.time.Duration) {
        partitionHandoffTimeoutNanos = timeout.inWholeNanoseconds
    }

    override fun configureProcessingCancellationTimeout(timeout: kotlin.time.Duration) {
        processingCancellationTimeoutNanos = timeout.inWholeNanoseconds
    }

    override fun configureShutdownDrainTimeout(timeout: kotlin.time.Duration) {
        shutdownDrainTimeoutNanos = timeout.inWholeNanoseconds
    }

    override fun configureRecovery(timeout: kotlin.time.Duration, retryBackoff: kotlin.time.Duration) {
        recoveryTimeoutNanos = timeout.inWholeNanoseconds
        recoveryRetryBackoffMillis = retryBackoff.inWholeMilliseconds.coerceAtLeast(1L)
    }

    override suspend fun releaseResources() {
        if (resourceReleaseStarted.compareAndSet(false, true)) {
            try {
                withContext(NonCancellable) {
                    try {
                        lifecycleJob.complete()
                        lifecycleJob.join()
                    } finally {
                        dispatcher.close()
                    }
                }
                resourceReleaseCompletion.complete(Unit)
            } catch (error: Throwable) {
                resourceReleaseCompletion.completeExceptionally(error)
                throw error
            }
        } else {
            resourceReleaseCompletion.await()
        }
    }

    private suspend fun runLoop() {
        running = true
        try {
            while (!shutdownRequested) {
                var consumer: KafkaConsumer<K, V>? = null
                var kafkaClientMetricsBound = false
                try {
                    if (recoveryDeadlineNanos == null) recoveringLostAssignment = false
                    if (recoveringLostAssignment) {
                        ensureRecoveryTimeRemaining("before creating a Kafka client")
                        metrics.onRecoveryAttempt()
                    }
                    consumer = kafkaConsumerFactory(consumerProperties)
                    consumerRef = consumer
                    metrics.bindKafkaClientMetrics(id, consumer)
                    kafkaClientMetricsBound = true
                    subscribe(consumer)
                    consumerLoop(consumer)
                    break
                } catch (_: LostAssignmentException) {
                    if (shutdownRequested) break
                    // The old generation was closed before this point. Recreate the non-thread-safe
                    // Kafka client rather than requiring the application to call start() again.
                    ensureRecoveryDeadline()
                    delayRecoveryBackoff()
                } catch (error: Throwable) {
                    if (error.hasCause<PartitionHandoffTimeoutException>() || error.hasCause<RecoveryTimeoutException>() ||
                        !recoveringLostAssignment || error is CancellationException || error.isFatalKafkaError()
                    ) {
                        throw error
                    }
                    ensureRecoveryDeadline()
                    log.warn("Kafka consumer #$id recovery attempt failed; retrying", error)
                    delayRecoveryBackoff()
                } finally {
                    consumerRef = null
                    withContext(NonCancellable) {
                        try {
                            if (kafkaClientMetricsBound) metrics.unbindKafkaClientMetrics(id)
                        } finally {
                            try {
                                if (recoveringLostAssignment) {
                                    consumer?.close(remainingRecoveryCloseTimeout())
                                } else {
                                    consumer?.close()
                                }
                            } catch (_: Exception) { }
                        }
                    }
                }
            }
        } finally {
            if (recoveringLostAssignment) finishRecovery(LifecycleOutcome.FAILURE)
            running = false
            assignedPartitionSnapshot = emptyList()
            // A terminal loop failure cannot perform the graceful drain handshake,
            // but it is already safe for the owner to join the completed job.
            readyForShutdownSignal.complete(Unit)
            finalizedShutdownSignal.complete(Unit)
        }
    }

    private fun subscribe(consumer: KafkaConsumer<K, V>) {
        if (topicsPattern != null) {
            consumer.subscribe(topicsPattern, createRebalanceListener(consumer, processingMode))
        } else {
            consumer.subscribe(topics, createRebalanceListener(consumer, processingMode))
        }
    }

    /**
     * Rebalance listener selection.
     *
     * Every mode maintains assignment state and commits on orderly revoke.
     */
    private fun createRebalanceListener(
        consumer: KafkaConsumer<K, V>,
        processingMode: ProcessingMode
    ): ConsumerRebalanceListener = atLeastOnceRebalanceListener(consumer)

    private fun atLeastOnceRebalanceListener(consumer: KafkaConsumer<K, V>): ConsumerRebalanceListener =
        object : ConsumerRebalanceListener {
            override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) =
                handlePartitionsRevoked(consumer, partitions)

            override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) =
                handlePartitionsAssigned(consumer, partitions)

            override fun onPartitionsLost(partitions: Collection<TopicPartition>) {
                handlePartitionsLost(partitions)
                throw LostAssignmentException()
            }
        }

    /**
     * Rebalance callback: partitions are being revoked from this consumer.
     *
     * Responsibilities:
     * - Remove revoked partitions from [assignedPartitions].
     * - Commit any contiguous-ready offsets for those partitions (best-effort).
     *
     * Rationale:
     * - Committing on revoke reduces duplicate processing after rebalance.
     * - The commit is best-effort; in failure scenarios Kafka may re-deliver.
     */
    private fun handlePartitionsRevoked(
        consumer: KafkaConsumer<K, V>,
        partitions: Collection<TopicPartition>
    ) {
        val startedAt = System.nanoTime()
        var outcome = LifecycleOutcome.SUCCESS
        try {
        // Closing admission happens before removing any queued records. A worker racing with
        // this callback either registered before the close and is awaited below, or cannot start.
        val revokedPartitionStates = partitionStateRegistry.revoke(partitions).toMutableSet()
        discardStashedRecords(partitions)
        for (tp in partitions) {
            val partitionState = partitionStateRegistry.partitionStateFor(tp)
            if (partitionState == null) {
                log.warn("Kafka consumer #$id: revoking unassigned partition $tp")
            } else {
                assignedPartitions.remove(partitionState)
            }
        }
        refreshAssignedPartitionSnapshot()
        val deadline = System.nanoTime() + partitionHandoffTimeoutNanos
        if (revokedPartitionStates.any { !it.awaitPipelines(deadline) }) {
            throw PartitionHandoffTimeoutException("Timed out waiting for revoked partitions: $partitions")
        }
        // Force metadata emission: snapshots can change even when the contiguous frontier does not.
        val result = commitOffsets(
            consumer = consumer,
            partitionStates = revokedPartitionStates,
            forceMetadata = processingMode.tracksProcessedOffsets(),
            deadlineNanos = deadline
        )
        if (result == CommitAttemptResult.FAILED) {
            throw PartitionHandoffCommitException("Final commit failed for revoked partitions: $partitions")
        }
            revokedPartitionStates.forEach {
                metrics.unbindPartitionMetrics(it.topic, it.partition)
            }
        } catch (error: Throwable) {
            outcome = if (error.hasCause<PartitionHandoffTimeoutException>()) {
                LifecycleOutcome.TIMEOUT
            } else {
                LifecycleOutcome.FAILURE
            }
            throw error
        } finally {
            metrics.onPartitionHandoff(System.nanoTime() - startedAt, outcome)
        }
    }

    private fun handlePartitionsLost(partitions: Collection<TopicPartition>) {
        val lostStates = partitionStateRegistry.revoke(partitions)
        discardStashedRecords(partitions)
        assignedPartitions.removeAll(lostStates.toSet())
        refreshAssignedPartitionSnapshot()
        startRecovery("partitions lost: ${partitions.joinToString()}")
        recoveryDeadlineNanos = System.nanoTime() + recoveryTimeoutNanos
        partitionStateRegistry.cancelActive(partitions)
        val deadline = System.nanoTime() + processingCancellationTimeoutNanos
        if (lostStates.any { !it.awaitPipelines(deadline) }) {
            throw PartitionHandoffTimeoutException("Timed out waiting for lost partitions: $partitions")
        }
        // Never commit a lost generation. Kafka will re-deliver from the authoritative group offset.
        lostStates.forEach { metrics.unbindPartitionMetrics(it.topic, it.partition) }
    }

    /**
     * Rebalance callback: partitions were assigned to this consumer.
     *
     * Responsibilities:
     * - Register assigned partitions in [PartitionRegistry].
     * - Initialize each partition's offset tracker from committed metadata when available.
     * - Fall back to the committed/assigned Kafka position when CKC metadata is absent or invalid.
     */
    private fun handlePartitionsAssigned(
        consumer: KafkaConsumer<K, V>,
        partitions: Collection<TopicPartition>
    ) {
        val assignedPartitionStates = partitionStateRegistry.onPartitionsAssigned(partitions)
        val recoveryCallTimeout = remainingRecoveryCallTimeout()
        val committedOffsets = if (recoveryCallTimeout == null) {
            consumer.committed(partitions.toSet())
        } else {
            consumer.committed(partitions.toSet(), recoveryCallTimeout)
        }
        assignedPartitionStates.forEach {
            val committed = committedOffsets[it.topicPartition]
            if (committed == null) {
                val positionTimeout = remainingRecoveryCallTimeout()
                val position = if (positionTimeout == null) {
                    consumer.position(it.topicPartition)
                } else {
                    consumer.position(it.topicPartition, positionTimeout)
                }
                it.init(position, forceReset = true)
            } else if (processingMode.tracksProcessedOffsets()) {
                initPartitionState(it, committed)
            } else {
                it.init(committed.offset(), forceReset = true)
            }
            metrics.bindPartitionMetrics(it)
        }
        assignedPartitions += assignedPartitionStates
        if (recoveringLostAssignment) finishRecovery(LifecycleOutcome.SUCCESS)
        refreshAssignedPartitionSnapshot()
    }

    private fun remainingRecoveryCallTimeout(): Duration? {
        if (!recoveringLostAssignment) return null
        val deadline = recoveryDeadlineNanos
            ?: throw RecoveryTimeoutException("Kafka consumer #$id has no active recovery deadline")
        val remaining = deadline - System.nanoTime()
        if (remaining <= 0L) {
            throw RecoveryTimeoutException("Kafka consumer #$id exceeded the recovery deadline during assignment")
        }
        return Duration.ofNanos(remaining)
    }

    // One absolute deadline belongs to the whole recovery cycle; retries must never refresh it.
    private fun ensureRecoveryDeadline(): Long = recoveryDeadlineNanos
        ?: (System.nanoTime() + recoveryTimeoutNanos).also { recoveryDeadlineNanos = it }

    private fun ensureRecoveryTimeRemaining(operation: String): Long {
        val remaining = ensureRecoveryDeadline() - System.nanoTime()
        if (remaining <= 0L) {
            throw RecoveryTimeoutException("Kafka consumer #$id exceeded the recovery deadline $operation")
        }
        return remaining
    }

    private suspend fun delayRecoveryBackoff() {
        val remaining = ensureRecoveryTimeRemaining("before retry backoff")
        val backoffNanos = recoveryRetryBackoffMillis * 1_000_000L
        delay(minOf(remaining, backoffNanos).nanoseconds)
        ensureRecoveryTimeRemaining("during retry backoff")
    }

    private fun remainingRecoveryCloseTimeout(): Duration {
        val deadline = recoveryDeadlineNanos ?: return Duration.ZERO
        return Duration.ofNanos((deadline - System.nanoTime()).coerceAtLeast(0L))
    }

    private fun startRecovery(reason: String) {
        recoveringLostAssignment = true
        lastRecoveryReason = reason
        recoveryStartedNanos = System.nanoTime()
        if (!recoveryMetricActive) {
            recoveryMetricActive = true
            metrics.onRecoveryStateChanged(true)
        }
    }

    private fun finishRecovery(outcome: LifecycleOutcome) {
        val startedAt = recoveryStartedNanos
        if (startedAt != null) {
            metrics.onRecoveryCompleted(System.nanoTime() - startedAt, outcome)
        }
        if (recoveryMetricActive) {
            recoveryMetricActive = false
            metrics.onRecoveryStateChanged(false)
        }
        recoveringLostAssignment = false
        recoveryDeadlineNanos = null
        recoveryStartedNanos = null
    }

    private fun refreshAssignedPartitionSnapshot() {
        assignedPartitionSnapshot = assignedPartitions
            .map { AssignedPartitionSnapshot(it.topic, it.partition) }
            .sortedWith(compareBy(AssignedPartitionSnapshot::topic, AssignedPartitionSnapshot::partition))
    }

    private fun initPartitionState(
        partitionState: PartitionState,
        committed: OffsetAndMetadata
    ) {
        val metadata = committed.metadata()
        if (metadata.isNullOrEmpty()) {
            partitionState.init(committed.offset(), forceReset = true)
            return
        }
        try {
            partitionState.init(
                committedOffset = committed.offset(),
                snapshot = OffsetTrackerMetadata.decode(
                    metadata = metadata,
                    context = metadataContext(partitionState.topicPartition, committed.offset())
                ),
                forceReset = true
            )
        } catch (e: Exception) {
            log.warn(
                "Kafka consumer #$id: failed to restore offset metadata for ${partitionState.topicPartition}",
                e
            )
            partitionState.init(committed.offset(), forceReset = true)
        }
    }

    private suspend fun consumerLoop(consumer: KafkaConsumer<K, V>) = try {
        when (processingMode) {
            ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
            ProcessingMode.AT_LEAST_ONCE_KEY_ORDERING,
            ProcessingMode.AT_LEAST_ONCE_PARTITION_ORDERING -> consumerLoopAtLeastOnceUnordered(consumer)
            ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST,
            ProcessingMode.FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY -> consumerLoopFreshnessFirst(consumer)
        }
    } catch (_: CancellationException) {
        log.info("Kafka consumer loop #$id cancelled")
    } catch (ex: Throwable) {
        log.error("Kafka consumer loop #$id failed", ex)
        throw ex
    } finally {
        if (shutdownRequested && !recoveringLostAssignment) {
            // Final commit happens only after the owner has drained workers. At-least-once modes
            // preserve tracker metadata; freshness-first modes commit their accepted frontier.
            val result = commitOffsets(
                consumer = consumer,
                partitionStates = assignedPartitions,
                forceMetadata = processingMode.tracksProcessedOffsets(),
                deadlineNanos = shutdownDeadlineNanos
            )
            if (result == CommitAttemptResult.FAILED) {
                throw IllegalStateException("Final offset commit failed for Kafka consumer #$id")
            }
            assignedPartitions.forEach {
                metrics.unbindPartitionMetrics(it.topic, it.partition)
            }
            assignedPartitions.clear()
        }
    }

    /** Commit the mode-specific ready position for every partition. */
    private fun commitOffsets(
        consumer: KafkaConsumer<K, V>,
        partitionStates: Set<PartitionState>,
        forceMetadata: Boolean = false,
        deadlineNanos: Long? = null
    ): CommitAttemptResult {
        val offsets = mutableMapOf<TopicPartition, OffsetAndMetadata>()
        val commitDataByPartition = mutableMapOf<PartitionState, OffsetCommitData>()
        var offsetsCount = 0L
        for (partitionState in partitionStates) {
            val commitData = if (processingMode.tracksProcessedOffsets()) {
                partitionState.advanceAndGetCommitData(forceMetadata)
            } else {
                partitionState.getAcceptedCommitData()
            }
            if (commitData != null) {
                commitDataByPartition[partitionState] = commitData
                val kafkaOffset = commitData.offset + 1
                val snapshot = commitData.offsetTrackerSnapshot
                val metadata = if (snapshot == null) {
                    null
                } else {
                    val encodedMetadata = OffsetTrackerMetadata.encode(
                        snapshot = snapshot,
                        context = metadataContext(partitionState.topicPartition, kafkaOffset)
                    )
                    metrics.onOffsetCommitMetadataEncoded(encodedMetadata.toCommitMetadataStats(partitionState.topic))
                    encodedMetadata.metadata
                }
                offsets[partitionState.topicPartition] = if (metadata == null) {
                    OffsetAndMetadata(kafkaOffset)
                } else {
                    OffsetAndMetadata(kafkaOffset, metadata)
                }
                offsetsCount += commitData.advancedOffsetsCount
            }
        }
        if (offsets.isNotEmpty()) {
            val startedAt = System.nanoTime()
            lastCommitAttemptEpochMillis = System.currentTimeMillis()
            try {
                if (deadlineNanos == null) {
                    consumer.commitSync(offsets)
                } else {
                    val remainingNanos = deadlineNanos - System.nanoTime()
                    if (remainingNanos <= 0L) return CommitAttemptResult.FAILED
                    consumer.commitSync(offsets, Duration.ofNanos(remainingNanos))
                }
                commitDataByPartition.forEach { (partitionState, commitData) ->
                    partitionState.markCommitted(commitData.offset)
                }
                lastCommitSucceeded = true
                metrics.onCommit(offsets.size, offsetsCount, System.nanoTime() - startedAt, true)
                return CommitAttemptResult.SUCCEEDED
            } catch (e: Exception) {
                lastCommitSucceeded = false
                metrics.onCommit(offsets.size, offsetsCount, System.nanoTime() - startedAt, false)
                log.warn("Error committing offsets in manager #$id", e)
                return CommitAttemptResult.FAILED
            }
        }
        return CommitAttemptResult.NO_OFFSETS
    }

    private fun metadataContext(
        topicPartition: TopicPartition,
        committedOffset: Long
    ): OffsetTrackerMetadataContext =
        OffsetTrackerMetadataContext(
            groupId = consumerGroupId,
            topicPartition = topicPartition,
            committedOffset = committedOffset
        )

    /**
     * Tracked at-least-once mode loop.
     *
     * Key properties:
     * - Dispatch to workers via `trySend()` (never suspends poll thread).
     * - When downstream is saturated, pause partitions and spill into bounded stash.
     * - Resume only after stash is fully drained.
     *
     * State machine overview:
     *
     * ACTIVE
     *   ├─(channel full)→ PAUSED
     *   └─(shutdown requested)→ DRAINING_TAIL
     *
     * PAUSED
     *   ├─(stash drained)→ ACTIVE
     *   └─(shutdown requested)→ DRAINING_TAIL
     *
     * DRAINING_TAIL
     *   └─(stash drained AND poll returns empty)→ TAIL_DRAINED
     */
    private suspend fun consumerLoopAtLeastOnceUnordered(consumer: KafkaConsumer<K, V>) {

        val maxPollRecords = consumerConfigAdapter.getInt(MAX_POLL_RECORDS_CONFIG)!!

        // Bounded spill queue for records already fetched when channel is saturated.
        val stash = ArrayDeque<ConsumerRecord<K, V>>(maxPollRecords)
        activeStash = stash

        // Time remains the low-traffic fallback; the record threshold can trigger an earlier commit.
        var lastCommitAt = System.currentTimeMillis()
        var pendingRecordsAtLastAttempt = 0L
        var state = State.ACTIVE

        try {
            while (currentCoroutineContext().isActive) {

            if (recoveringLostAssignment) {
                val deadline = recoveryDeadlineNanos
                if (deadline != null && System.nanoTime() >= deadline) {
                    throw RecoveryTimeoutException("Kafka consumer #$id did not receive an assignment before the recovery deadline")
                }
            }

            if (state == State.TAIL_DRAINED && finalizationRequested) {
                break
            }

            /**
             * Shutdown gate:
             * - On first observation of shutdownRequested, stop intake and pause partitions.
             * - Continue looping to drain local tail.
             */
            if (shutdownRequested && !state.shuttingDown) {
                state = State.DRAINING_TAIL
                pause(consumer, recordBackpressureMetric = false)
            }

            val pendingOffsets = countPendingCommitRecords()
            if (pendingOffsets < pendingRecordsAtLastAttempt) {
                pendingRecordsAtLastAttempt = pendingOffsets
            }

            val now = System.currentTimeMillis()
            val intervalReached = now - lastCommitAt >= commitIntervalMs
            val recordsThresholdReached =
                pendingOffsets - pendingRecordsAtLastAttempt >= commitRecordsThreshold.toLong()
            if (intervalReached || recordsThresholdReached) {
                val result = commitOffsets(consumer, assignedPartitions)
                lastCommitAt = now
                pendingRecordsAtLastAttempt = when (result) {
                    CommitAttemptResult.FAILED -> pendingOffsets
                    CommitAttemptResult.NO_OFFSETS,
                    CommitAttemptResult.SUCCEEDED -> 0L
                }
            }

            val records = pollRecords(consumer, state)
            val iterator = records.iterator()

            /**
             * In ACTIVE state we attempt immediate dispatch.
             * If channel is full, we transition to PAUSED and start stashing.
             */
            if (state == State.ACTIVE) {
                while (iterator.hasNext()) {
                    val record = iterator.next()
                    observeRecordOffset(record)
                    val accepted = recordSink.tryEmit(record)

                    if (!accepted) {
                        // Processing runtime saturated -> pause Kafka and stash the rest of this poll batch.
                        state = State.PAUSED
                        stash.addLast(record)
                        pause(consumer)
                        break
                    }
                }
            }

            // Always stash the remainder of the current poll batch.
            while (iterator.hasNext()) {
                val record = iterator.next()
                observeRecordOffset(record)
                stash.addLast(record)
            }

            /**
             * Drain attempt:
             * If we can flush the entire stash, we can resume intake (or complete shutdown).
             */
            if (drainStash(stash)) {
                when (state) {
                    State.PAUSED -> {
                        state = State.ACTIVE
                        resume(consumer)
                    }
                    State.DRAINING_TAIL -> {
                        // Tail drained: stash empty AND no new records in this poll.
                        if (records.isEmpty) {
                            state = State.TAIL_DRAINED
                            readyForShutdownSignal.complete(Unit)
                        }
                    }
                    else -> Unit
                }
            }
            }
        } finally {
            activeStash = null
        }
    }

    /** Marks offsets Kafka skipped before a record while leaving the delivered record pending for a worker. */
    private fun observeRecordOffset(record: ConsumerRecord<K, V>) {
        partitionStateRegistry.observe(
            record,
            observeOffset = processingMode.tracksProcessedOffsets(),
            acceptOffset = processingMode.isFreshnessFirst()
        )
    }

    private fun countPendingCommitRecords(): Long {
        var total = 0L
        for (partitionState in assignedPartitions) {
            val pending = if (processingMode.tracksProcessedOffsets()) {
                partitionState.advanceAndGetPendingOffsetsCount()
            } else {
                partitionState.acceptedRecordsPendingCommit()
            }
            total = if (Long.MAX_VALUE - total < pending) Long.MAX_VALUE else total + pending
        }
        return total
    }

    /**
     * poll wrapper:
     * - In PAUSED/draining states poll timeout is zero; add a delay on empty to avoid busy spin.
     * - WakeupException is expected during shutdown; translate to an empty batch.
     */
    private suspend fun pollRecords(
        consumer: KafkaConsumer<K, V>,
        state: State
    ): ConsumerRecords<K, V> {
        return try {
            val startedAt = System.nanoTime()
            val records = consumer.poll(state.pollTimeout)
            lastPollEpochMillis = System.currentTimeMillis()
            lastPollRecordCount = records.count()
            metrics.onPoll(records.count(), System.nanoTime() - startedAt)
            if (state.pollTimeout == Duration.ZERO && records.isEmpty) {
                delay(State.ACTIVE.pollTimeout.toKotlinDuration())
            }
            records
        } catch (_: WakeupException) {
            ConsumerRecords<K, V>(emptyMap())
        }
    }

    /** Drain stash to processing runtime; returns true if fully drained. */
    private fun drainStash(stash: ArrayDeque<ConsumerRecord<K, V>>): Boolean {
        while (stash.isNotEmpty()) {
            val record = stash.first()
            if (!recordSink.tryEmit(record)) {
                return false
            }
            stash.removeFirst()
        }
        return true
    }

    private fun discardStashedRecords(partitions: Collection<TopicPartition>) {
        val revoked = partitions.toHashSet()
        val stash = activeStash ?: return
        val retained = stash.filterTo(ArrayDeque()) { record ->
            val discard = TopicPartition(record.topic(), record.partition()) in revoked
            if (discard) {
                metrics.onRecordHandedOff(record, RecordHandoffReason.ASSIGNMENT_ENDED)
                partitionStateRegistry.discard(record)
            }
            !discard
        }
        stash.clear()
        stash.addAll(retained)
    }

    /** Pause all assigned partitions (best-effort). */
    private fun pause(
        consumer: KafkaConsumer<K, V>,
        recordBackpressureMetric: Boolean = true
    ) {
        try {
            val assigned = consumer.assignment()
            if (assigned.isNotEmpty()) {
                consumer.pause(assigned)
                if (recordBackpressureMetric) {
                    metrics.onBackpressurePauseResume(BackpressureAction.PAUSE, assigned.size)
                }
            }
        } catch (e: Exception) {
            log.warn("Error pausing consumer in manager #$id", e)
        }
    }

    /** Resume all assigned partitions (best-effort). */
    private fun resume(consumer: KafkaConsumer<K, V>) {
        try {
            val assigned = consumer.assignment()
            if (assigned.isNotEmpty()) {
                consumer.resume(assigned)
                metrics.onBackpressurePauseResume(BackpressureAction.RESUME, assigned.size)
            }
        } catch (e: Exception) {
            log.warn("Error resuming consumer in manager #$id", e)
        }
    }

    /**
     * Freshness-first modes:
     * - Minimal poll-loop mode; uses best-effort dispatch into a runtime that may drop records.
     * - Explicitly commits the latest accepted offsets by time or record count.
     */
    private suspend fun consumerLoopFreshnessFirst(consumer: KafkaConsumer<K, V>) {
        var lastCommitAt = System.currentTimeMillis()
        var pendingRecordsAtLastAttempt = 0L
        while (currentCoroutineContext().isActive) {
            if (recoveringLostAssignment) {
                val deadline = recoveryDeadlineNanos
                if (deadline != null && System.nanoTime() >= deadline) {
                    throw RecoveryTimeoutException("Kafka consumer #$id did not receive an assignment before the recovery deadline")
                }
            }
            if (shutdownRequested) {
                if (!shutdownPaused) {
                    pause(consumer, recordBackpressureMetric = false)
                    shutdownPaused = true
                }
                readyForShutdownSignal.complete(Unit)
                // Keep the group member/client alive while the runtime drains accepted records.
                // No new records are admitted once shutdown starts.
                if (finalizationRequested) break
                pollRecords(consumer, State.DRAINING_TAIL).forEach { record ->
                    metrics.onRecordDropped(record, avh.ckc.core.metrics.RecordDropReason.LIFECYCLE_DISCARD)
                }
                continue
            }

            val pendingRecords = countPendingCommitRecords()
            if (pendingRecords < pendingRecordsAtLastAttempt) {
                pendingRecordsAtLastAttempt = pendingRecords
            }
            val now = System.currentTimeMillis()
            val intervalReached = now - lastCommitAt >= commitIntervalMs
            val recordsThresholdReached =
                pendingRecords - pendingRecordsAtLastAttempt >= commitRecordsThreshold.toLong()
            if (intervalReached || recordsThresholdReached) {
                val result = commitOffsets(consumer, assignedPartitions)
                lastCommitAt = now
                pendingRecordsAtLastAttempt = when (result) {
                    CommitAttemptResult.FAILED -> pendingRecords
                    CommitAttemptResult.NO_OFFSETS,
                    CommitAttemptResult.SUCCEEDED -> 0L
                }
            }

            val records = pollRecords(consumer, State.ACTIVE)

            for (record in records) {
                observeRecordOffset(record)
                recordSink.tryEmit(record)
            }
        }
    }

    /**
     * Poll loop states for tracked at-least-once processing.
     *
     * - ACTIVE: normal poll and dispatch.
     * - PAUSED: downstream saturated; Kafka paused; draining stash.
     * - DRAINING_TAIL: shutdown requested; Kafka paused; draining stash; no new intake.
     * - TAIL_DRAINED: tail drained and ready signal emitted.
     */
    private enum class State(
        val pollTimeout: Duration,
        val shuttingDown: Boolean
    ) {
        ACTIVE(Duration.ofMillis(250L), false),
        PAUSED(Duration.ZERO, false),
        DRAINING_TAIL(Duration.ZERO, true),
        TAIL_DRAINED(Duration.ZERO, true)
    }

    private enum class CommitAttemptResult {
        NO_OFFSETS,
        SUCCEEDED,
        FAILED
    }

    private class PartitionHandoffTimeoutException(message: String) : RuntimeException(message)

    private class PartitionHandoffCommitException(message: String) : RuntimeException(message)

    private class LostAssignmentException : RuntimeException()

    private class RecoveryTimeoutException(message: String) : RuntimeException(message)

    private fun Throwable.isFatalKafkaError(): Boolean =
        this is AuthenticationException || this is AuthorizationException || this is InvalidConfigurationException

    private inline fun <reified T : Throwable> Throwable.hasCause(): Boolean {
        var current: Throwable? = this
        while (current != null) {
            if (current is T) return true
            current = current.cause
        }
        return false
    }

    private companion object {
        const val PARTITION_HANDOFF_TIMEOUT_NANOS = 30_000_000_000L
        const val RECOVERY_RETRY_BACKOFF_MILLIS = 1_000L
        const val RECOVERY_TIMEOUT_NANOS = 120_000_000_000L
    }
}
