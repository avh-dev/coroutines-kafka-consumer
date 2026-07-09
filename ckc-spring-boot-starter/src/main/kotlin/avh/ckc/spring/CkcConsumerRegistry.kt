package avh.ckc.spring

/**
 * Runtime registry for named CKC consumers managed by the Spring Boot starter.
 */
interface CkcConsumerRegistry {
    val consumerNames: Set<String>

    fun isRunning(name: String): Boolean

    fun start(name: String)

    fun stop(name: String)
}
