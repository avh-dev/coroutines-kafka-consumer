package avh.ckc.core

import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ChannelOverflowTest {

    @Test
    fun `when drop oldest channel overflows then undelivered callback receives evicted record`() {
        val droppedOffsets = mutableListOf<Long>()
        val channel = Channel<ConsumerRecord<ByteArray, ByteArray>>(
            capacity = 1,
            onBufferOverflow = BufferOverflow.DROP_OLDEST,
            onUndeliveredElement = { record -> droppedOffsets += record.offset() }
        )

        assertTrue(channel.trySend(testRecord(offset = 1L)).isSuccess)
        assertTrue(channel.trySend(testRecord(offset = 2L)).isSuccess)

        assertEquals(listOf(1L), droppedOffsets)
    }

    @Test
    fun `when suspend channel is full then try send fails without undelivered callback`() {
        val droppedOffsets = mutableListOf<Long>()
        val channel = Channel<ConsumerRecord<ByteArray, ByteArray>>(
            capacity = 1,
            onBufferOverflow = BufferOverflow.SUSPEND,
            onUndeliveredElement = { record -> droppedOffsets += record.offset() }
        )

        assertTrue(channel.trySend(testRecord(offset = 1L)).isSuccess)
        assertFalse(channel.trySend(testRecord(offset = 2L)).isSuccess)

        assertEquals(emptyList<Long>(), droppedOffsets)
    }
}
