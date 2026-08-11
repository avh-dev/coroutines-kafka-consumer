package avh.ckc.loadtest.kafka

import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import kotlin.test.Test
import kotlin.test.assertEquals

class TopicProducerPoolTest {
    @Test
    fun `routes the same key to the same producer`() {
        val producers = List(4) { MockProducer(true, StringSerializer(), StringSerializer()) }
        val pool = TopicProducerPool(producers)

        val first = pool.indexFor("order-42")

        repeat(10) {
            assertEquals(first, pool.indexFor("order-42"))
        }
    }

    @Test
    fun `handles negative key hashes`() {
        val pool = TopicProducerPool(List(3) { MockProducer<String, String>() })
        val negativeHashKey = object {
            override fun hashCode(): Int = Int.MIN_VALUE
        }
        val genericPool = TopicProducerPool(List(3) { MockProducer<Any, String>() })

        assertEquals(0, pool.indexFor(null))
        assertEquals(1, genericPool.indexFor(negativeHashKey))
    }
}
