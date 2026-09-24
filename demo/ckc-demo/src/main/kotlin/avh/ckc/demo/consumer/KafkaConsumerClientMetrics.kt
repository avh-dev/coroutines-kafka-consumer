package avh.ckc.demo.consumer

import avh.ckc.demo.config.DemoApplicationProperties
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics
import org.apache.kafka.clients.consumer.Consumer
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.MicrometerConsumerListener

internal fun <K, V> kafkaConsumerFactoryWithClientMetrics(
    consumerProperties: Map<String, Any>,
    meterRegistry: MeterRegistry,
    consumerId: String,
    enabled: Boolean
): ConsumerFactory<K, V> = DefaultKafkaConsumerFactory<K, V>(consumerProperties).apply {
    if (enabled) {
        addListener(
            MicrometerConsumerListener(
                meterRegistry,
                listOf(Tag.of(CONSUMER_ID_TAG, consumerId))
            )
        )
    }
}

internal fun bindKafkaClientMetrics(
    consumer: Consumer<*, *>,
    meterRegistry: MeterRegistry,
    consumerId: String,
    pollLoopId: Int,
    enabled: Boolean
): KafkaClientMetrics? =
    if (enabled) {
        KafkaClientMetrics(
            consumer,
            Tags.of(
                CONSUMER_ID_TAG, consumerId,
                POLL_LOOP_TAG, pollLoopId.toString()
            )
        ).apply {
            bindTo(meterRegistry)
        }
    } else {
        null
    }

internal val DemoApplicationProperties.kafkaClientMetricsEnabled: Boolean
    get() = micrometerMetricsEnabled && consumers.kafkaClientMetricsEnabled

internal val DemoApplicationProperties.micrometerMetricsEnabled: Boolean
    get() = consumers.metricsImplementation == DemoApplicationProperties.MetricsImplementation.MICROMETER

private const val CONSUMER_ID_TAG = "consumer_id"
private const val POLL_LOOP_TAG = "poll_loop"
