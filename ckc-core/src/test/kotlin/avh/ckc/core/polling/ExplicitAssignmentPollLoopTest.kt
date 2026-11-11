package avh.ckc.core.polling

import avh.ckc.core.ProcessingMode
import avh.ckc.core.kafka.KafkaConsumerConfigAdapter
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.polling.partition.PartitionRegistry
import avh.ckc.core.processing.PolledRecordSink
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Duration

class ExplicitAssignmentPollLoopTest {
    @Test
    @Suppress("UNCHECKED_CAST")
    fun `explicit recipient assignment seeks to current end before readiness`() = runBlocking {
        val partition = TopicPartition("warmup", 2)
        val consumer = mock<KafkaConsumer<ByteArray, ByteArray>>()
        whenever(consumer.endOffsets(setOf(partition))).thenReturn(mapOf(partition to 42L))
        whenever(consumer.assignment()).thenReturn(setOf(partition))
        whenever(consumer.poll(any<Duration>())).thenAnswer {
            Thread.sleep(1)
            ConsumerRecords.empty<ByteArray, ByteArray>()
        }
        val properties = mapOf<String, Any?>(
            ConsumerConfig.GROUP_ID_CONFIG to "warmup-group",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java
        )
        val loop = ConsumerPollLoop(
            id = 0,
            parentContext = Dispatchers.Default,
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
            commitIntervalMs = 5_000,
            commitRecordsThreshold = 1_000,
            metrics = ConsumerMetrics.NOOP as ConsumerMetrics<ByteArray, ByteArray>,
            consumerProperties = properties,
            consumerConfigAdapter = KafkaConsumerConfigAdapter(properties),
            topics = null,
            topicsPattern = null,
            recordSink = mock<PolledRecordSink<ByteArray, ByteArray>>(),
            partitionStateRegistry = PartitionRegistry(128),
            assignedPartitionsAtEnd = setOf(partition),
            kafkaConsumerFactory = { consumer }
        )
        val job = loop.start()
        try {
            withTimeout(1_000) { loop.readiness().await() }
            verify(consumer).assign(setOf(partition))
            verify(consumer).seek(partition, 42L)
        } finally {
            job.cancelAndJoin()
            loop.releaseResources()
        }
    }
}
