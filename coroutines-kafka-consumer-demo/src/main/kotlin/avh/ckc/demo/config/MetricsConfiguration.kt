package avh.ckc.demo.config

import avh.ckc.core.ConsumerTelemetry
import avh.ckc.micrometer.MicrometerConsumerTelemetry
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration(proxyBeanMethods = false)
class MetricsConfiguration {
    @Bean
    fun consumerTelemetry(meterRegistry: MeterRegistry): ConsumerTelemetry =
        MicrometerConsumerTelemetry(meterRegistry)
}
