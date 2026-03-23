package avh.ckc.core

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.SendChannel
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
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
 * Single-thread Kafka poll loop + dispatch into [workChannel].
 *
 * Design notes:
 * - KafkaConsumer is confined to a dedicated single thread (not thread-safe).
 * - Keys and values are deserialized as raw ByteArray (ByteArrayDeserializer).
 *   Actual business deserialization is performed in worker coroutines for better parallelism
 *   and to keep the poll loop lightweight.
 *
 * Overflow strategies:
 * - BACKPRESSURE: non-blocking dispatch via `trySend` + `pause/resume`
 *   with bounded local stash and explicit contiguous commits.
 * - LOSSY: suspending `send`, intended for setups relying on
 *   channel-level dropping and auto-commit.
 *
 * Invariants (BACKPRESSURE):
 * - Poll loop never suspends on dispatch.
 * - Pause/resume applies to all assigned partitions.
 * - Offsets are committed only when contiguous-ready (via [PartitionState]).
 */
internal class ConsumerPollLoop(
    val id: Int,
    parentContext: CoroutineContext,
    private val deliveryStrategy: DeliveryStrategy,
    private val commitIntervalMs: Long,
    private val consumerProperties: Map<String, Any?>,
    private val consumerConfigAdapter: ConsumerConfigAdapter,
    private val topics: List<String>?,
    private val topicsPattern: Pattern?,
    private val workChannel: SendChannel<ConsumerRecord<ByteArray, ByteArray>>,
    private val partitionStateRegistry: PartitionRegistry,
    private val kafkaConsumerFactory:
        (consumerProperties: Map<String, Any?>) -> KafkaConsumer<ByteArray, ByteArray> = {
            KafkaConsumer(it, ByteArrayDeserializer(), ByteArrayDeserializer())
        },
) {
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

    /** Completed when BACKPRESSURE shutdown tail is drained (caller should cancel job afterwards). */
    private val readyForShutdownSignal = CompletableDeferred<Unit>()

    fun start(): Job {
        check(job == null)
        job = scope.launch { runLoop() }
        return job!!
    }

    /**
     * Phase 1 shutdown: request draining and pause poll().
     * Phase 2: loop completes [readyForShutdownSignal] once stash drained and poll returns empty.
     */
    fun prepareForShutdown(): Deferred<Unit> {
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
            consumer.subscribe(topicsPattern, createRebalanceListener(consumer, deliveryStrategy))
        } else {
            consumer.subscribe(topics, createRebalanceListener(consumer, deliveryStrategy))
        }
    }

    /**
     * Rebalance listener selection.
     *
     * - BACKPRESSURE: requires rebalance hooks for partition state tracking + commit on revoke.
     * - LOSSY: does not maintain commit tracking; no-op listener is sufficient.
     */
    private fun createRebalanceListener(
        consumer: KafkaConsumer<ByteArray, ByteArray>,
        deliveryStrategy: DeliveryStrategy
    ): ConsumerRebalanceListener =
        if (deliveryStrategy == DeliveryStrategy.BACKPRESSURE)
            backpressureRebalanceListener(consumer)
        else
            NoOpConsumerRebalanceListener()

    private fun backpressureRebalanceListener(consumer: KafkaConsumer<ByteArray, ByteArray>): ConsumerRebalanceListener =
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
    }

    /**
     * Rebalance callback: partitions were assigned to this consumer.
     *
     * Responsibilities:
     * - Register assigned partitions in [PartitionRegistry].
     * - Initialize each partition's offset tracker based on `consumer.position(tp)`.
     */
    private fun handlePartitionsAssigned(
        consumer: KafkaConsumer<ByteArray, ByteArray>,
        partitions: Collection<TopicPartition>
    ) {
        val assignedPartitionStates = partitionStateRegistry.onPartitionsAssigned(partitions)
        assignedPartitionStates.forEach {
            it.init(consumer.position(it.topicPartition))
        }
        assignedPartitions += assignedPartitionStates
    }

    private suspend fun consumerLoop(consumer: KafkaConsumer<ByteArray, ByteArray>) = try {
        when (deliveryStrategy) {
            DeliveryStrategy.BACKPRESSURE -> consumerLoopBackpressure(consumer)
            DeliveryStrategy.LOSSY -> consumerLoopLOSSY(consumer)
        }
    } catch (_: CancellationException) {
        log.info("Kafka consumer loop #$id cancelled")
    } catch (ex: Throwable) {
        log.error("Kafka consumer loop #$id failed", ex)
        throw ex
    } finally {
        if (deliveryStrategy == DeliveryStrategy.BACKPRESSURE) {
            // Final best-effort commit on shutdown.
            commitReadyOffsets(consumer, assignedPartitions)
        }
    }

    /** Commit contiguous-ready offsets only (PartitionState encapsulates readiness). */
    private fun commitReadyOffsets(
        consumer: KafkaConsumer<ByteArray, ByteArray>,
        partitionStates: Set<PartitionState>
    ) {
        val offsets = mutableMapOf<TopicPartition, OffsetAndMetadata>()
        for (partitionState in partitionStates) {
            val offset = partitionState.advanceCommitOffset()
            if (offset != null) {
                offsets[partitionState.topicPartition] = OffsetAndMetadata(offset)
            }
        }
        if (!offsets.isEmpty()) {
            try {
                consumer.commitSync(offsets)
            } catch (e: Exception) {
                log.warn("Error committing offsets in manager #$id", e)
            }
        }
    }

    /**
     * BACKPRESSURE mode loop.
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
    private suspend fun consumerLoopBackpressure(consumer: KafkaConsumer<ByteArray, ByteArray>) {

        val maxPollRecords = consumerConfigAdapter.getInt(MAX_POLL_RECORDS_CONFIG)!!

        // Bounded spill queue for records already fetched when channel is saturated.
        val stash = ArrayDeque<ConsumerRecord<ByteArray, ByteArray>>(maxPollRecords)

        // Do not commit on every loop iteration.
        var lastCommitAt = System.currentTimeMillis()

        val channel = workChannel

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

                    // IMPORTANT: must not suspend poll thread.
                    val result = channel.trySend(record)

                    if (!result.isSuccess) {
                        // Channel full -> pause Kafka and stash the rest of this poll batch.
                        state = State.PAUSED
                        stash.addLast(record)
                        pause(consumer)
                        break
                    }
                }
            }

            // Always stash the remainder of the current poll batch.
            while (iterator.hasNext()) {
                stash.addLast(iterator.next())
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
            val records = consumer.poll(state.pollTimeout)
            if (state.pollTimeout == Duration.ZERO && records.isEmpty) {
                delay(State.ACTIVE.pollTimeout.toKotlinDuration())
            }
            records
        } catch (_: WakeupException) {
            ConsumerRecords<ByteArray, ByteArray>(emptyMap())
        }
    }

    /** Drain stash to channel; returns true if fully drained. */
    private fun drainStash(stash: ArrayDeque<ConsumerRecord<ByteArray, ByteArray>>): Boolean {
        val channel = workChannel
        while (stash.isNotEmpty()) {
            val record = stash.first()
            val result = channel.trySend(record)
            if (!result.isSuccess) {
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
     * LOSSY strategy:
     * - Minimal mode; uses suspending send().
     * - Intended to be paired with a channel that drops and with client internal auto-commit.
     */
    private suspend fun consumerLoopLOSSY(consumer: KafkaConsumer<ByteArray, ByteArray>) {
        val pollTimeout = State.ACTIVE.pollTimeout
        val channel = workChannel
        while (currentCoroutineContext().isActive) {
            if (shutdownRequested) {
                readyForShutdownSignal.complete(Unit)
                break
            }

            val records = try {
                consumer.poll(pollTimeout)
            } catch (_: WakeupException) {
                if (shutdownRequested) {
                    readyForShutdownSignal.complete(Unit)
                    break
                }
                continue
            }

            for (record in records) {
                channel.send(record)
            }
        }
    }

    /**
     * Poll loop states for BACKPRESSURE strategy.
     *
     * - ACTIVE: normal poll and dispatch.
     * - PAUSED: downstream saturated -> [workChannel] is full; Kafka paused; draining stash.
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
