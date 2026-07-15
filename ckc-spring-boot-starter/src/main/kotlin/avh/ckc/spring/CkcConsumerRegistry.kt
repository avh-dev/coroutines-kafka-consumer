package avh.ckc.spring

/**
 * Runtime registry for named CKC consumers managed by the Spring Boot starter.
 *
 * This is primarily useful for consumers configured with `auto-startup: false`
 * and for administrative controls that need explicit start or stop operations.
 */
interface CkcConsumerRegistry {
    /**
     * Names of all consumers resolved from `@CkcKafkaConsumer` beans and `ckc.consumers`.
     */
    val consumerNames: Set<String>

    /**
     * Returns whether the named consumer runtime has been explicitly started and not yet stopped.
     */
    fun isRunning(name: String): Boolean

    /**
     * Starts a named consumer runtime if it is not already running.
     */
    fun start(name: String)

    /**
     * Stops a named consumer runtime if it is currently running.
     */
    fun stop(name: String)
}
