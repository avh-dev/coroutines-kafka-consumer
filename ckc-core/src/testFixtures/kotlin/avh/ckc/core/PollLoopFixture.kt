package avh.ckc.core

import avh.ckc.core.extension.RecordAdmission
import avh.ckc.core.kafka.KafkaConsumerConfigAdapter
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.polling.ConsumerPollLoop
import avh.ckc.core.polling.partition.PartitionRegistry
import avh.ckc.core.polling.partition.PartitionState
import avh.ckc.core.processing.AdmissionControlledRecordSink
import avh.ckc.core.processing.PartitionProcessedRecordTracker
import avh.ckc.core.processing.PolledRecordSink
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.trySendBlocking
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.mockito.Mockito.timeout
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever

class PollLoopFixture(
    processingMode: ProcessingMode,
    @Suppress("UNCHECKED_CAST")
    metrics: ConsumerMetrics<ByteArray, ByteArray> = ConsumerMetrics.NOOP as ConsumerMetrics<ByteArray, ByteArray>,
    workChannelCapacity: Int,
    assignmentPosition: Long = 0L,
    private val committedOffsets: Map<TopicPartition, OffsetAndMetadata> = emptyMap(),
    commitIntervalMs: Long = 5_000L,
    commitRecordsThreshold: Int = 1_000,
    maxTrackedOffsetsPerPartition: Int = 1_048_576,
    private val additionalAssignmentPositions: Map<TopicPartition, Long> = emptyMap(),
    initialChannelRecords: List<ConsumerRecord<ByteArray, ByteArray>> = emptyList(),
    private val listenerRef: AtomicReference<ConsumerRebalanceListener?> = AtomicReference(),
    admission: RecordAdmission<ByteArray, ByteArray>? = null,
    private val filterPausedPollRecords: Boolean = true,
    private val pollAnswer: PollLoopFixture.() -> ConsumerRecords<ByteArray, ByteArray>
) {
    val consumer: KafkaConsumer<ByteArray, ByteArray> = mock()
    val workChannel = Channel<ConsumerRecord<ByteArray, ByteArray>>(
        capacity = workChannelCapacity,
        onBufferOverflow = when (processingMode) {
            ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
            ProcessingMode.AT_LEAST_ONCE_KEY_ORDERING,
            ProcessingMode.AT_LEAST_ONCE_PARTITION_ORDERING -> BufferOverflow.SUSPEND
            ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST,
            ProcessingMode.FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY -> BufferOverflow.DROP_OLDEST
        }
    )
    internal val registry = PartitionRegistry(maxTrackedOffsetsPerPartition, processingMode.tracksProcessedOffsets())
    private val baseRecordSink = object : PolledRecordSink<ByteArray, ByteArray> {
        override fun tryEmit(record: ConsumerRecord<ByteArray, ByteArray>): Boolean {
            if (processingMode.tracksProcessedOffsets() &&
                registry.partitionStateFor(record)?.isProcessed(record.offset()) == true
            ) {
                return true
            }
            return workChannel.trySend(record).isSuccess
        }
    }
    private val recordSink = admission?.let {
        AdmissionControlledRecordSink(
            baseRecordSink,
            PartitionProcessedRecordTracker(registry, processingMode.tracksProcessedOffsets()),
            it
        )
    } ?: baseRecordSink
    val topicPartition = TopicPartition("topic-a", 0)
    private val assignmentPositions = ConcurrentHashMap<TopicPartition, Long>().apply {
        put(topicPartition, assignmentPosition)
        putAll(additionalAssignmentPositions)
    }
    private val consumerProperties = testConsumerProperties()
    internal val loop = ConsumerPollLoop<ByteArray, ByteArray>(
        id = 1,
        parentContext = Dispatchers.Default,
        processingMode = processingMode,
        commitIntervalMs = commitIntervalMs,
        commitRecordsThreshold = commitRecordsThreshold,
        metrics = metrics,
        consumerProperties = consumerProperties,
        consumerConfigAdapter = KafkaConsumerConfigAdapter(consumerProperties),
        topics = listOf(topicPartition.topic()),
        topicsPattern = null,
        recordSink = recordSink,
        partitionStateRegistry = registry,
        kafkaConsumerFactory = { consumer }
    )

    private val assignedOnce = AtomicBoolean(false)
    private val pausedPartitions = ConcurrentHashMap.newKeySet<TopicPartition>()

    init {
        initialChannelRecords.forEach { workChannel.trySendBlocking(it) }

        whenever(consumer.subscribe(any<List<String>>(), any<ConsumerRebalanceListener>()))
            .thenAnswer { invocation ->
                listenerRef.set(invocation.getArgument(1))
            }

        whenever(consumer.position(any<TopicPartition>())).thenAnswer { invocation ->
            assignmentPositions.getValue(invocation.getArgument(0))
        }
        whenever(consumer.position(any<TopicPartition>(), any<Duration>())).thenAnswer { invocation ->
            assignmentPositions.getValue(invocation.getArgument(0))
        }
        whenever(consumer.committed(any<Set<TopicPartition>>())).thenReturn(committedOffsets)
        whenever(consumer.committed(any<Set<TopicPartition>>(), any<Duration>())).thenReturn(committedOffsets)
        whenever(consumer.assignment()).thenAnswer { assignmentPositions.keys.toSet() }
        doAnswer { invocation ->
            pausedPartitions.addAll(invocation.getArgument<Collection<TopicPartition>>(0))
            null
        }.whenever(consumer).pause(any())
        doAnswer { invocation ->
            pausedPartitions.removeAll(invocation.getArgument<Collection<TopicPartition>>(0).toSet())
            null
        }.whenever(consumer).resume(any())
        whenever(consumer.close()).then { }

        whenever(consumer.poll(any<Duration>()))
            .thenAnswer {
                if (assignedOnce.compareAndSet(false, true)) {
                    listenerRef.get()?.onPartitionsAssigned(assignmentPositions.keys)
                }
                val fetched = pollAnswer()
                ConsumerRecords(
                    fetched.partitions()
                        .filterNot { filterPausedPollRecords && it in pausedPartitions }
                        .associateWith { fetched.records(it) }
                )
            }
    }

    fun start(): Job = loop.start()

    fun assignAdditional(topicPartition: TopicPartition, position: Long) {
        assignmentPositions[topicPartition] = position
        checkNotNull(listenerRef.get()).onPartitionsAssigned(listOf(topicPartition))
    }

    fun prepareForShutdown(): Deferred<Unit> = loop.prepareForShutdown()

    fun finalizeShutdown(): Deferred<Unit> = loop.finalizeShutdown()

    internal suspend fun awaitAssignedState(lastCommittedOffset: Long? = null): PartitionState =
        awaitFor(2_000L, 10L) {
            val state = registry.partitionStateFor(topicPartition) ?: return@awaitFor null
            if (lastCommittedOffset == null || state.lastCommittedOffset == lastCommittedOffset) {
                state
            } else {
                null
            }
        }

    fun awaitPause() {
        verify(consumer, timeout(2_000)).pause(eq(setOf(topicPartition)))
    }

    fun awaitResume() {
        verify(consumer, timeout(2_000)).resume(eq(setOf(topicPartition)))
    }

    fun awaitCommit(offset: Long) {
        verify(consumer, timeout(2_000)).commitSync(
            argThat<Map<TopicPartition, OffsetAndMetadata>> {
                val committed = get(topicPartition)
                committed?.offset() == offset + 1 && !committed.metadata().isNullOrEmpty()
            }
        )
    }

    fun awaitFreshnessCommit(offset: Long) {
        verify(consumer, timeout(2_000)).commitSync(
            argThat<Map<TopicPartition, OffsetAndMetadata>> {
                val committed = get(topicPartition)
                committed?.offset() == offset + 1 && committed.metadata().isEmpty()
            }
        )
    }

    fun awaitDeadlineBoundedCommit(offset: Long) {
        verify(consumer, timeout(2_000)).commitSync(
            argThat<Map<TopicPartition, OffsetAndMetadata>> {
                val committed = get(topicPartition)
                committed?.offset() == offset + 1 && !committed.metadata().isNullOrEmpty()
            },
            any<Duration>()
        )
    }

    fun awaitDeadlineBoundedFreshnessCommit(offset: Long) {
        verify(consumer, timeout(2_000)).commitSync(
            argThat<Map<TopicPartition, OffsetAndMetadata>> {
                val committed = get(topicPartition)
                committed?.offset() == offset + 1 && committed.metadata().isEmpty()
            },
            any<Duration>()
        )
    }

    fun awaitCommitAttempts(attempts: Int) {
        verify(consumer, timeout(2_000).times(attempts)).commitSync(any<Map<TopicPartition, OffsetAndMetadata>>())
    }
}
