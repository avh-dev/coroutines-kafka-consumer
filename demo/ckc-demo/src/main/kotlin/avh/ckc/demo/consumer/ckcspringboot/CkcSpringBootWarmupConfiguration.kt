package avh.ckc.demo.consumer.ckcspringboot

import avh.ckc.demo.consumer.logWarmupForwarded
import avh.ckc.demo.consumer.recordWarmupLifecycle
import avh.ckc.spring.CkcConsumerProperties
import avh.ckc.spring.CkcWarmupForwardedEvent
import avh.ckc.spring.CkcWarmupLifecycleEvent
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.common.config.SslConfigs
import org.springframework.beans.factory.config.BeanPostProcessor
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.context.event.EventListener

@Configuration(proxyBeanMethods = false)
@Profile("ckc-spring-boot")
class CkcSpringBootWarmupConfiguration(private val meterRegistry: MeterRegistry) {
    @EventListener
    fun onWarmupLifecycle(event: CkcWarmupLifecycleEvent) {
        recordWarmupLifecycle(event.event, meterRegistry)
    }

    @EventListener
    fun onWarmupForwarded(event: CkcWarmupForwardedEvent) {
        logWarmupForwarded(event.event)
    }

    companion object {
        @Bean
        @JvmStatic
        fun warmupKafkaTruststoreProperties(): BeanPostProcessor = object : BeanPostProcessor {
            override fun postProcessAfterInitialization(bean: Any, beanName: String): Any {
                if (bean is CkcConsumerProperties && bean.warmup.enabled) {
                    listOf("warmup-control", "warmup-destination").forEach { cluster ->
                        bean.clusters[cluster]?.kafkaProperties?.let { properties ->
                            // Kafka interprets an empty certificate string as a configured PEM store.
                            if (properties[SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG]?.isBlank() == true) {
                                properties.remove(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG)
                                if (properties[SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG] == "PEM" &&
                                    SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG !in properties
                                ) {
                                    properties.remove(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG)
                                }
                            }
                        }
                    }
                }
                return bean
            }
        }
    }
}
