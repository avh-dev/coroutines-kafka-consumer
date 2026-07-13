package avh.ckc.demo.consumer

import avh.ckc.core.ProcessingMode
import avh.ckc.demo.config.DemoApplicationProperties
import io.confluent.parallelconsumer.ParallelConsumerOptions
import org.springframework.stereotype.Component

internal fun ProcessingMode.toConfluentProcessingOrder(): ParallelConsumerOptions.ProcessingOrder =
    when (this) {
        ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
        ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST -> ParallelConsumerOptions.ProcessingOrder.UNORDERED
        ProcessingMode.AT_LEAST_ONCE_KEY_ORDERING -> ParallelConsumerOptions.ProcessingOrder.KEY
        ProcessingMode.AT_LEAST_ONCE_PARTITION_ORDERING -> ParallelConsumerOptions.ProcessingOrder.PARTITION
        ProcessingMode.FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY ->
            throw IllegalArgumentException("Processing mode $this is not supported by the confluent demo profile")
    }

internal fun ProcessingMode.requireSupportedBySpringKafka(): ProcessingMode =
    when (this) {
        ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST,
        ProcessingMode.AT_LEAST_ONCE_PARTITION_ORDERING -> this
        ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
        ProcessingMode.AT_LEAST_ONCE_KEY_ORDERING,
        ProcessingMode.FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY ->
            throw IllegalArgumentException("Processing mode $this is not supported by the spring-kafka demo profile")
    }

@Component
class FreshnessFirstRecordFilter(
    private val properties: DemoApplicationProperties
) {
    fun shouldDiscard(
        runtime: DemoApplicationProperties.ConsumerRuntime,
        recordTimestamp: Long,
        nowMillis: Long = System.currentTimeMillis()
    ): Boolean {
        val freshnessFirstMode = runtime.processingMode == ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST ||
                runtime.processingMode == ProcessingMode.FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY
        if (!freshnessFirstMode || recordTimestamp <= 0L) {
            return false
        }
        val maxRecordAgeSeconds = properties.consumers.freshnessFirstMaxRecordAgeSeconds
        require(maxRecordAgeSeconds >= 0) {
            "demo.consumers.freshness-first-max-record-age-seconds must be >= 0"
        }
        return nowMillis - recordTimestamp > maxRecordAgeSeconds * 1_000L
    }
}
