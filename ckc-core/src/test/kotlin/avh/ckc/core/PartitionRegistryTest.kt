package avh.ckc.core

import avh.ckc.core.partition.PartitionRegistry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class PartitionRegistryTest {

    @Test
    fun `when partitions assigned then states are returned and accessible`() {
        val reg = PartitionRegistry()

        val tps = listOf(
            TopicPartition("A", 0),
            TopicPartition("A", 1),
            TopicPartition("B", 3)
        )

        val states = reg.onPartitionsAssigned(tps)

        assertEquals(3, states.size)

        for (tp in tps) {
            val state = reg.partitionStateFor(tp)
            assertNotNull(state)
            assertEquals(tp, state!!.topicPartition)
        }
    }

    @Test
    fun `when lookup performed by consumer record then correct state returned`() {
        val reg = PartitionRegistry()

        reg.onPartitionsAssigned(listOf(TopicPartition("A", 2)))

        val record = ConsumerRecord<ByteArray, ByteArray>("A", 2, 10L, null, null)

        val state = reg.partitionStateFor(record)

        assertNotNull(state)
        assertEquals("A", state!!.topicPartition.topic())
        assertEquals(2, state.topicPartition.partition())
    }

    @Test
    fun `when topic or partition missing then lookup returns null`() {
        val reg = PartitionRegistry()

        reg.onPartitionsAssigned(listOf(TopicPartition("A", 0)))

        assertNull(reg.partitionStateFor(TopicPartition("Missing", 0)))
        assertNull(reg.partitionStateFor(TopicPartition("A", 999)))
    }

    @Test
    fun `when large partition assigned then capacity grows`() {
        val reg = PartitionRegistry()

        reg.onPartitionsAssigned(listOf(TopicPartition("A", 10)))

        assertNotNull(reg.partitionStateFor(TopicPartition("A", 10)))
        assertNull(reg.partitionStateFor(TopicPartition("A", 9)))
    }

    @Test
    fun `when partition already assigned then same state instance reused`() {
        val reg = PartitionRegistry()

        reg.onPartitionsAssigned(listOf(TopicPartition("A", 1)))

        val s1 = reg.partitionStateFor(TopicPartition("A", 1))
        val s2 = reg.partitionStateFor(TopicPartition("A", 1))

        assertSame(s1, s2)
    }

    @Test
    fun `when partitions assigned again then new snapshot is published`() {
        val reg = PartitionRegistry()

        reg.onPartitionsAssigned(
            listOf(
                TopicPartition("A", 10),
                TopicPartition("A", 0)
            )
        )

        val oldSnapshot = reg.snapshotForTest()
        val oldArray = oldSnapshot["A"]!!

        reg.onPartitionsAssigned(listOf(TopicPartition("A", 5)))

        val newSnapshot = reg.snapshotForTest()
        val newArray = newSnapshot["A"]!!

        assertNotSame(oldArray, newArray)
        assertNull(oldArray[5])
        assertNotNull(newArray[5])
    }
}
