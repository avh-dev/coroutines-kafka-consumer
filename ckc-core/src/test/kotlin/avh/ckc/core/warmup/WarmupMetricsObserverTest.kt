package avh.ckc.core.warmup

import avh.ckc.core.RecordingMetrics
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.WarmupFallbackReason
import avh.ckc.core.metrics.WarmupResultMetricOutcome
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

class WarmupMetricsObserverTest {
    @Test
    fun `observer routes results and aggregates pending across donor sessions`() {
        val metrics = RecordingMetrics<String, String>()
        val observer = WarmupMetricsObserver(metrics)
        val record = ConsumerRecord("orders", 2, 9, "key", "value")
        val forwarded = WarmupForwardedEvent(
            WarmupSessionId("first"),
            TopicPartition("orders", 2),
            9,
            TopicPartition("warmup-orders", 0),
            15
        )

        observer.onForwarded(record, forwarded, 11)
        observer.onResult(record, WarmupRemoteOutcome.Succeeded, 12)
        observer.onResult(record, WarmupRemoteOutcome.TerminalFailure, 13)
        observer.onResult(
            record,
            WarmupRemoteOutcome.Discarded(avh.ckc.core.metrics.RecordDropReason.QUEUE_OVERFLOW),
            14
        )
        observer.onFallback(record, WarmupFallbackReason.RESULT_TIMEOUT)
        observer.onPendingChanged(WarmupSessionId("first"), 2)
        observer.onPendingChanged(WarmupSessionId("second"), 3)
        observer.onPendingChanged(WarmupSessionId("first"), 0)

        assertEquals(listOf(record to 11L), metrics.warmupDelegated)
        assertEquals(
            listOf(
                WarmupResultMetricOutcome.SUCCEEDED,
                WarmupResultMetricOutcome.TERMINAL_FAILURE,
                WarmupResultMetricOutcome.DISCARDED
            ),
            metrics.warmupResults.map { it.outcome }
        )
        assertEquals(listOf(record to WarmupFallbackReason.RESULT_TIMEOUT), metrics.warmupFallbacks)
        assertEquals(listOf(2, 5, 3), metrics.warmupPending)
    }

    @Test
    fun `concurrent pending updates cannot publish an older aggregate last`() {
        val firstCallbackEntered = CountDownLatch(1)
        val releaseFirstCallback = CountDownLatch(1)
        val zeroPublished = CountDownLatch(1)
        val published = CopyOnWriteArrayList<Int>()
        val metrics = object : ConsumerMetrics<String, String> {
            override fun onWarmupPendingChanged(pending: Int) {
                if (pending == 1) {
                    firstCallbackEntered.countDown()
                    releaseFirstCallback.await()
                }
                published += pending
                if (pending == 0) zeroPublished.countDown()
            }
        }
        val observer = WarmupMetricsObserver(metrics)
        val sessionId = WarmupSessionId("session")
        val first = thread(name = "first-pending-update", isDaemon = true) {
            observer.onPendingChanged(sessionId, 1)
        }
        assertTrue(firstCallbackEntered.await(1, TimeUnit.SECONDS))
        val secondInvocationStarted = CountDownLatch(1)
        val second = thread(name = "second-pending-update", isDaemon = true) {
            secondInvocationStarted.countDown()
            observer.onPendingChanged(sessionId, 0)
        }
        assertTrue(secondInvocationStarted.await(1, TimeUnit.SECONDS))

        val publishedBeforeRelease = try {
            zeroPublished.await(250, TimeUnit.MILLISECONDS)
        } finally {
            releaseFirstCallback.countDown()
            first.join(1_000)
            second.join(1_000)
        }

        assertFalse(publishedBeforeRelease)
        assertFalse(first.isAlive)
        assertFalse(second.isAlive)
        assertEquals(listOf(1, 0), published)
    }
}
