package avh.ckc.spring

import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean

/**
 * Spring Boot auto-configuration entrypoint for the CKC starter.
 *
 * The heavy lifting lives in internal collaborators; this class only exposes
 * the starter lifecycle bean when CKC is enabled.
 */
@AutoConfiguration
@ConditionalOnClass(CkcConsumer::class)
@ConditionalOnProperty(prefix = "ckc", name = ["enabled"], havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(CkcConsumerProperties::class)
class CkcSpringBootAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    fun ckcConsumersLifecycle(applicationContext: ApplicationContext): CkcConsumersLifecycle =
        CkcConsumersLifecycle(applicationContext)
}
