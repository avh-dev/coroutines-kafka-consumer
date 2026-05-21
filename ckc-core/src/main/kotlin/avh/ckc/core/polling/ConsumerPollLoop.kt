package avh.ckc.core.polling

import avh.ckc.core.config.ConsumerConfigAdapter
import avh.ckc.core.ProcessingMode
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.offset.OffsetTrackerMetadata
import avh.ckc.core.partition.PartitionRegistry
import avh.ckc.core.partition.PartitionState
import avh.ckc.core.processing.PolledRecordSink
import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors
import java.util.regex.Pattern
import kotlin.concurrent.Volatile
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.toKotlinDuration

/**
 * Single-thread Kafka poll loop + dispatch into [recordSink].
 *
 * Design notes:
 * - KafkaConsumer is confined to a dedicated single thread (not thread-safe).
 * - Keys and values are deserialized as raw ByteArray (ByteArrayDeserializer).
 *   Actual business deserialization is performed in worker coroutines for better parallelism
 *   and to keep the poll loop lightweight.
 *
 * Processing modes:
 * - AT_LEAST_ONCE_UNORDERED: non-blocking dispatch via `trySend` + `pause/resume`
 *   with bounded local stash and explicit contiguous commits.
 * - FRESHNESS_FIRST: suspending `send`, intended for setups relying on
 *   channel-level dropping and auto-commit.
 *
 * Invariants (AT_LEAST_ONCE_UNORDERED):
 * - Poll loop never suspends on dispatch.
 * - Pause/resume applies to all assigned partitions.
 * - Offsets are committed only when contiguous-ready (via [PartitionState]).
 */
internal class ConsumerPollLoop<K, V>(
    val id: Int,
    parentContext: CoroutineContext,
    private val processingMode: ProcessingMode,
    private val commitIntervalMs: Long,
    private val metrics: ConsumerMetrics<K, V>,
    private val consumerProperties: Map<String, Any?>,
    private val consumerConfigAdapter: ConsumerConfigAdapter,
    private val topics: List<String>?,
    private val topicsPattern: Pattern?,
    private val recordSink: PolledRecordSink,
    private val partitionStateRegistry: PartitionRegistry,
    private val kafkaConsumerFactory:
        (consumerProperties: Map<String, Any?>) -> KafkaConsumer<ByteArray, ByteArray> = {
            KafkaConsumer(it, ByteArrayDeserializer(), ByteArrayDeserializer())
        },
) : ConsumerPollLoopControl {
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
    private val scope = CoroutineScope(
        SupervisorJob(parentContext[Job]) +
                parentContext.minusKey(Job) +
                dispatcher +
                CoroutineName("ConsumerPollLoop-$id")
    )

    private var job: Job? = null

    /** Assigned partition states (mutated only on poll thread via rebalance callbacks). */
    private val assignedPartitions = mutableSetOf<PartitionState>()

    private val log = LoggerFactory.getLogger(this::class.java)

    /** Set from outside poll thread to initiate graceful shutdown. */
    @Volatile
    private var shutdownRequested = false

    /** Used to call wakeup() from outside poll thread. */
    @Volatile
    private var consumerRef: Consumer<ByteArray, ByteArray>? = null

    /** Completed when tracked at-least-once shutdown tail is drained (caller should cancel job afterwards). */
    private val readyForShutdownSignal = CompletableDeferred<Unit>()

    override fun start(): Job {
        check(job == null)
        job = scope.launch { runLoop() }
        return job!!
    }

    /**
     * Phase 1 shutdown: request draining and pause poll().
     * Phase 2: loop completes [readyForShutdownSignal] once stash drained and poll returns empty.
     */
    override fun prepareForShutdown(): Deferred<Unit> {
        shutdownRequested = true
        consumerRef?.wakeup()
        return readyForShutdownSignal
    }

    private suspend fun runLoop() {
        val consumer = kafkaConsumerFactory(consumerProperties)
        consumerRef = consumer
        try {
            subscribe(consumer)
            consumerLoop(consumer)
        } finally {
            withContext(NonCancellable) {
                try {
                    consumer.close()
                } catch (_: Exception) {
                }
            }
        }
    }

    private fun subscribe(consumer: KafkaConsumer<ByteArray, ByteArray>) {
        if (topicsPattern != null) {
            consumer.subscribe(topicsPattern, createRebalanceListener(consumer, processingMode))
        } else {
            consumer.subscribe(topics, createRebalanceListener(consumer, processingMode))
        }
    }

    /**
     * Rebalance listener selection.
     *
     * - AT_LEAST_ONCE_UNORDERED: requires rebalance hooks for partition state tracking + commit on revoke.
     * - FRESHNESS_FIRST: does not maintain commit tracking; no-op listener is sufficient.
     */
    private fun createRebalanceListener(
        consumer: KafkaConsumer<ByteArray, ByteArray>,
        processingMode: ProcessingMode
    ): ConsumerRebalanceListener =
        if (processingMode == ProcessingMode.AT_LEAST_ONCE_UNORDERED)
            atLeastOnceRebalanceListener(consumer)
        else
            noOpRebalanceListener()

    private fun noOpRebalanceListener(): ConsumerRebalanceListener =
        object : ConsumerRebalanceListener {
            override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) = Unit

            override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) = Unit
        }

    private fun atLeastOnceRebalanceListener(consumer: KafkaConsumer<ByteArray, ByteArray>): ConsumerRebalanceListener =
        object : ConsumerRebalanceListener {
            override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) =
                handlePartitionsRevoked(consumer, partitions)

            override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) =
                handlePartitionsAssigned(consumer, partitions)
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
        consumer: KafkaConsumer<ByteArray, ByteArray>,
        partitions: Collection<TopicPartition>
    ) {
        val revokedPartitionStates = mutableSetOf<PartitionState>()
        for (tp in partitions) {
            val partitionState = partitionStateRegistry.partitionStateFor(tp)
            if (partitionState == null) {
                log.warn("Kafka consumer #$id: revoking unassigned partition $tp")
            } else {
                revokedPartitionStates += partitionState
                assignedPartitions.remove(partitionState)
            }
        }
        commitReadyOffsets(consumer, revokedPartitionStates)
        revokedPartitionStates.forEach {
            metrics.unbindPartitionMetrics(it.topic, it.partition)
        }
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
        consumer: KafkaConsumer<ByteArray, ByteArray>,
        partitions: Collection<TopicPartition>
    ) {
        val assignedPartitionStates = partitionStateRegistry.onPartitionsAssigned(partitions)
        val committedOffsets = consumer.committed(partitions.toSet())
        assignedPartitionStates.forEach {
            val committed = committedOffsets[it.topicPartition]
            if (committed == null) {
                it.init(consumer.position(it.topicPartition))
            } else {
                initPartitionState(it, committed)
            }
            metrics.bindPartitionMetrics(it)
        }
        assignedPartitions += assignedPartitionStates
    }

    private fun initPartitionState(
        partitionState: PartitionState,
        committed: OffsetAndMetadata
    ) {
        val metadata = committed.metadata()
        if (metadata.isNullOrEmpty()) {
            partitionState.init(committed.offset())
            return
        }
        try {
            partitionState.init(
                committedOffset = committed.offset(),
                snapshot = OffsetTrackerMetadata.decode(metadata)
            )
        } catch (e: Exception) {
            log.warn(
                "Kafka consumer #$id: failed to restore offset metadata for ${partitionState.topicPartition}",
                e
            )
            partitionState.init(committed.offset())
        }
    }

    private suspend fun consumerLoop(consumer: KafkaConsumer<ByteArray, ByteArray>) = try {
        when (processingMode) {
            ProcessingMode.AT_LEAST_ONCE_UNORDERED -> consumerLoopAtLeastOnceUnordered(consumer)
            ProcessingMode.FRESHNESS_FIRST -> consumerLoopFreshnessFirst(consumer)
        }
    } catch (_: CancellationException) {
        log.info("Kafka consumer loop #$id cancelled")
    } catch (ex: Throwable) {
        log.error("Kafka consumer loop #$id failed", ex)
        throw ex
    } finally {
        if (processingMode == ProcessingMode.AT_LEAST_ONCE_UNORDERED) {
            // Final best-effort commit on shutdown.
            commitReadyOffsets(consumer, assignedPartitions)
            assignedPartitions.forEach {
                metrics.unbindPartitionMetrics(it.topic, it.partition)
            }
            assignedPartitions.clear()
        }
    }

    /** Commit contiguous-ready offsets only (PartitionState encapsulates readiness). */
    private fun commitReadyOffsets(
        consumer: KafkaConsumer<ByteArray, ByteArray>,
        partitionStates: Set<PartitionState>
    ) {
        val offsets = mutableMapOf<TopicPartition, OffsetAndMetadata>()
        var offsetsCount = 0L
        for (partitionState in partitionStates) {
            val commitData = partitionState.advanceAndGetCommitData()
            if (commitData != null) {
                val metadata = OffsetTrackerMetadata.encode(commitData.offsetTrackerSnapshot)
                val kafkaOffset = commitData.offset + 1
                offsets[partitionState.topicPartition] = if (metadata == null) {
                    OffsetAndMetadata(kafkaOffset)
                } else {
                    OffsetAndMetadata(kafkaOffset, metadata)
                }
                offsetsCount += commitData.advancedOffsetsCount
            }
        }
        if (!offsets.isEmpty()) {
            val startedAt = System.nanoTime()
            try {
                consumer.commitSync(offsets)
                metrics.onCommit(offsets.size, offsetsCount, System.nanoTime() - startedAt, true)
            } catch (e: Exception) {
                metrics.onCommit(offsets.size, offsetsCount, System.nanoTime() - startedAt, false)
                log.warn("Error committing offsets in manager #$id", e)
            }
        }
    }

    /**
     * AT_LEAST_ONCE_UNORDERED mode loop.
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
    private suspend fun consumerLoopAtLeastOnceUnordered(consumer: KafkaConsumer<ByteArray, ByteArray>) {

        val maxPollRecords = consumerConfigAdapter.getInt(MAX_POLL_RECORDS_CONFIG)!!

        // Bounded spill queue for records already fetched when channel is saturated.
        val stash = ArrayDeque<ConsumerRecord<ByteArray, ByteArray>>(maxPollRecords)

        // Do not commit on every loop iteration.
        var lastCommitAt = System.currentTimeMillis()

        var state = State.ACTIVE

        while (currentCoroutineContext().isActive) {

            /**
             * Shutdown gate:
             * - On first observation of shutdownRequested, stop intake and pause partitions.
             * - Continue looping to drain local tail.
             */
            if (shutdownRequested && !state.shuttingDown) {
                state = State.DRAINING_TAIL
                pause(consumer)
            }

            /**
             * Periodic commit of ready offsets (best-effort).
             */
            val now = System.currentTimeMillis()
            if (now - lastCommitAt >= commitIntervalMs) {
                commitReadyOffsets(consumer, assignedPartitions)
                lastCommitAt = now
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
    }

    /**
     * poll wrapper:
     * - In PAUSED/draining states poll timeout is zero; add a delay on empty to avoid busy spin.
     * - WakeupException is expected during shutdown; translate to an empty batch.
     */
    private suspend fun pollRecords(
        consumer: KafkaConsumer<ByteArray, ByteArray>,
        state: State
    ): ConsumerRecords<ByteArray, ByteArray> {
        return try {
            val startedAt = System.nanoTime()
            val records = consumer.poll(state.pollTimeout)
            metrics.onPoll(records.count(), System.nanoTime() - startedAt)
            if (state.pollTimeout == Duration.ZERO && records.isEmpty) {
                delay(State.ACTIVE.pollTimeout.toKotlinDuration())
            }
            records
        } catch (_: WakeupException) {
            ConsumerRecords<ByteArray, ByteArray>(emptyMap())
        }
    }

    /** Drain stash to processing runtime; returns true if fully drained. */
    private fun drainStash(stash: ArrayDeque<ConsumerRecord<ByteArray, ByteArray>>): Boolean {
        while (stash.isNotEmpty()) {
            val record = stash.first()
            if (!recordSink.tryEmit(record)) {
                return false
            }
            stash.removeFirst()
        }
        return true
    }

    /** Pause all assigned partitions (best-effort). */
    private fun pause(consumer: KafkaConsumer<ByteArray, ByteArray>) {
        try {
            val assigned = consumer.assignment()
            if (assigned.isNotEmpty()) {
                consumer.pause(assigned)
            }
        } catch (e: Exception) {
            log.warn("Error pausing consumer in manager #$id", e)
        }
    }

    /** Resume all assigned partitions (best-effort). */
    private fun resume(consumer: KafkaConsumer<ByteArray, ByteArray>) {
        try {
            val assigned = consumer.assignment()
            if (assigned.isNotEmpty()) {
                consumer.resume(assigned)
            }
        } catch (e: Exception) {
            log.warn("Error resuming consumer in manager #$id", e)
        }
    }

    /**
     * FRESHNESS_FIRST mode:
     * - Minimal mode; uses best-effort dispatch into a dropping queue.
     * - Intended to be paired with a channel that drops and with client internal auto-commit.
     */
    private suspend fun consumerLoopFreshnessFirst(consumer: KafkaConsumer<ByteArray, ByteArray>) {
        while (currentCoroutineContext().isActive) {
            if (shutdownRequested) {
                readyForShutdownSignal.complete(Unit)
                break
            }

            val records = pollRecords(consumer, State.ACTIVE)

            for (record in records) {
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
}
