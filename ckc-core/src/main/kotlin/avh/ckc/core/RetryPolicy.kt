package avh.ckc.core

import kotlin.jvm.JvmName
import kotlin.reflect.KClass
import kotlin.time.Duration
import kotlin.time.Duration.Companion.ZERO

data class RetryRule(
    val exceptionTypes: List<KClass<out Throwable>>,
    val maxRetries: Int,
    val delay: Duration = ZERO
) {
    init {
        require(exceptionTypes.isNotEmpty()) { "exceptionTypes must not be empty" }
        require(maxRetries >= 0) { "maxRetries must be >= 0" }
        require(!delay.isNegative()) { "delay must be >= 0" }
    }

    fun matches(error: Throwable): Boolean = exceptionTypes.any { it.isInstance(error) }

    companion object {
        inline fun <reified T : Throwable> of(
            maxRetries: Int,
            delay: Duration = ZERO
        ): RetryRule = RetryRule(listOf(T::class), maxRetries, delay)

        fun of(
            exceptionTypes: List<KClass<out Throwable>>,
            maxRetries: Int,
            delay: Duration = ZERO
        ): RetryRule = RetryRule(exceptionTypes, maxRetries, delay)
    }
}

class RetryPolicy(
    private val rules: List<RetryRule> = emptyList()
) {
    fun ruleFor(error: Throwable): RetryRule? = rules.firstOrNull { it.matches(error) }

    companion object {
        fun none(): RetryPolicy = RetryPolicy()
        fun of(vararg rules: RetryRule): RetryPolicy = RetryPolicy(rules.toList())
    }
}

fun retryPolicy(block: RetryPolicyBuilder.() -> Unit): RetryPolicy =
    RetryPolicyBuilder().apply(block).build()

class RetryPolicyBuilder {
    private val rules = mutableListOf<RetryRule>()

    @JvmName("retryOne")
    inline fun <reified T : Throwable> retry(
        noinline block: RetryRuleBuilder.() -> Unit
    ) {
        addRule(listOf(T::class), block)
    }

    @JvmName("retryTwo")
    inline fun <reified T1 : Throwable, reified T2 : Throwable> retry(
        noinline block: RetryRuleBuilder.() -> Unit
    ) {
        addRule(listOf(T1::class, T2::class), block)
    }

    @JvmName("retryThree")
    inline fun <reified T1 : Throwable, reified T2 : Throwable, reified T3 : Throwable> retry(
        noinline block: RetryRuleBuilder.() -> Unit
    ) {
        addRule(listOf(T1::class, T2::class, T3::class), block)
    }

    fun retry(
        exceptionTypes: List<KClass<out Throwable>>,
        block: RetryRuleBuilder.() -> Unit
    ) {
        addRule(exceptionTypes, block)
    }

    fun build(): RetryPolicy = RetryPolicy(rules.toList())

    @PublishedApi
    internal fun addRule(
        exceptionTypes: List<KClass<out Throwable>>,
        block: RetryRuleBuilder.() -> Unit
    ) {
        val builder = RetryRuleBuilder().apply(block)
        rules += RetryRule(
            exceptionTypes = exceptionTypes,
            maxRetries = builder.maxRetries,
            delay = builder.delay
        )
    }
}

class RetryRuleBuilder {
    var maxRetries: Int = 0
    var delay: Duration = ZERO
}
