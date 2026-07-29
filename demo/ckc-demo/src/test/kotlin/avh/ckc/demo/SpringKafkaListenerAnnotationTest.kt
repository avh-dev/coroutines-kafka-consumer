package avh.ckc.demo

import avh.ckc.demo.consumer.springkafka.SpringKafkaTrackingListeners
import avh.ckc.demo.consumer.springkafkacoroutinesnaive.SpringKafkaCoroutinesNaiveListeners
import avh.ckc.demo.consumer.springkafkathreadpool.SpringKafkaThreadPoolListeners
import org.junit.jupiter.api.Test
import org.springframework.kafka.annotation.KafkaListener
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SpringKafkaListenerAnnotationTest {
    @Test
    fun `spring kafka listener ids do not override the shared consumer group`() {
        val listeners = listOf(
            SpringKafkaTrackingListeners::class.java,
            SpringKafkaCoroutinesNaiveListeners::class.java,
            SpringKafkaThreadPoolListeners::class.java
        ).flatMap { type ->
            type.declaredMethods.mapNotNull { method -> method.getAnnotation(KafkaListener::class.java) }
        }

        assertEquals(9, listeners.size)
        listeners.forEach { listener ->
            assertTrue(listener.id.startsWith("spring-kafka-consumer-"))
            assertFalse(listener.idIsGroup)
            assertEquals("", listener.groupId)
        }
    }
}
