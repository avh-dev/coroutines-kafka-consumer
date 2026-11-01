package avh.ckc.core.warmup

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.mockito.kotlin.mock
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals

class TrafficWarmupContextTest {
    @Test
    fun `consumer channels share transport and stopping one does not stop its peers`() = runBlocking {
        val transports = CopyOnWriteArrayList<TestTransport>()
        val hub = SharedWarmupControlHub(CoroutineScope(Dispatchers.Default + SupervisorJob())) { handler ->
            TestTransport(handler).also(transports::add)
        }
        val firstRecords = Channel<WarmupControlRecord>(Channel.UNLIMITED)
        val secondRecords = Channel<WarmupControlRecord>(Channel.UNLIMITED)
        val first = hub.channel(WarmupControlMessageHandler(firstRecords::send))
        val second = hub.channel(WarmupControlMessageHandler(secondRecords::send))

        first.start()
        first.awaitReady()
        second.start()
        second.awaitReady()
        assertEquals(1, transports.size)

        transports.single().emit(InstanceReady(WarmupSessionId("one"), WarmupInstanceId("pod")))
        withTimeout(1_000) { firstRecords.receive() }
        withTimeout(1_000) { secondRecords.receive() }

        first.stop()
        assertEquals(0, transports.single().stopCalls.get())
        transports.single().emit(InstanceReady(WarmupSessionId("two"), WarmupInstanceId("pod")))
        assertEquals("two", withTimeout(1_000) { secondRecords.receive() }.message.sessionId.value)

        second.stop()
        assertEquals(1, transports.single().stopCalls.get())

        val third = hub.channel(WarmupControlMessageHandler { })
        third.start()
        third.awaitReady()
        assertEquals(2, transports.size)
        third.stop()
    }

    private class TestTransport(
        private val handler: WarmupControlMessageHandler
    ) : WarmupControlChannel {
        private val termination = CompletableDeferred<Unit>()
        private var offset = 0L
        val stopCalls = AtomicInteger()

        override fun start() = Unit
        override suspend fun awaitReady() = Unit
        override fun logPosition() = WarmupControlLogPosition(TopicPartition("control", 0), 0, offset)
        override suspend fun send(message: WarmupControlMessage): RecordMetadata = mock()
        override suspend fun awaitTermination() = termination.await()
        override suspend fun stop() {
            stopCalls.incrementAndGet()
            termination.complete(Unit)
        }

        suspend fun emit(message: WarmupControlMessage) {
            handler.onMessage(WarmupControlRecord(offset++, message))
        }
    }
}
