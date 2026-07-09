package avh.ckc.spring

import org.springframework.stereotype.Component

/**
 * Marks a [CkcConsumer] bean and binds it to `ckc.consumers.<name>` properties.
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@MustBeDocumented
@Component
annotation class CkcKafkaConsumer(
    val name: String
)
