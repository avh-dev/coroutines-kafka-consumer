package avh.ckc.spring

import org.springframework.beans.factory.annotation.Qualifier

/**
 * Marks a custom CKC [avh.ckc.core.metrics.ConsumerMetrics] bean.
 *
 * Leave [consumer] blank to declare a default custom metrics bean. Set it to a
 * `@CkcKafkaConsumer` name to bind the bean to one consumer.
 */
@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
@Qualifier
annotation class CkcConsumerMetrics(
    val consumer: String = ""
)

/**
 * Marks a Micrometer record-driven tag extractor bean.
 *
 * Leave [consumer] blank to declare a default extractor. Set it to a
 * `@CkcKafkaConsumer` name to bind the extractor to one consumer.
 */
@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
@Qualifier
annotation class CkcMicrometerRecordTags(
    val consumer: String = ""
)
