package avh.ckc.spring

import avh.ckc.core.RetryPolicy
import avh.ckc.core.RetryRule
import kotlin.reflect.KClass
import kotlin.time.toKotlinDuration

internal fun resolvedRetrySchemaName(
    properties: CkcConsumerProperties,
    consumerRetrySchema: String?
): String? =
    consumerRetrySchema?.takeIf { it.isNotBlank() }
        ?: properties.defaultRetrySchema?.takeIf { it.isNotBlank() }

internal fun retryPolicy(
    properties: CkcConsumerProperties,
    consumerName: String,
    consumerRetrySchema: String?
): RetryPolicy {
    val schemaName = consumerRetrySchema?.takeIf { it.isNotBlank() }
        ?: properties.defaultRetrySchema?.takeIf { it.isNotBlank() }
        ?: return RetryPolicy.none()
    val schema = properties.retrySchemas[schemaName]
        ?: error("CKC consumer '$consumerName' references unknown retry schema '$schemaName'")
    return retryPolicyFromSchema(consumerName, schemaName, schema)
}

internal fun retryPolicyFromSchema(
    consumerName: String,
    schemaName: String,
    schema: CkcConsumerProperties.RetrySchema
): RetryPolicy {
    require(schema.rules.isNotEmpty()) { "ckc.retry-schemas.$schemaName.rules must not be empty" }
    return RetryPolicy.of(
        *schema.rules.mapIndexed { index, rule ->
            val path = "ckc.retry-schemas.$schemaName.rules[$index]"
            require(rule.maxRetries >= 0) { "$path.max-retries must be >= 0" }
            require(!rule.delay.isNegative) { "$path.delay must be >= 0" }
            RetryRule.of(
                exceptionTypes = resolveExceptionTypes(consumerName, path, rule.exceptions),
                maxRetries = rule.maxRetries,
                delay = rule.delay.toKotlinDuration()
            )
        }.toTypedArray()
    )
}

private fun resolveExceptionTypes(
    consumerName: String,
    path: String,
    exceptionClassNames: List<String>
): List<KClass<out Throwable>> {
    require(exceptionClassNames.isNotEmpty()) {
        "$path.exceptions must not be empty for CKC consumer '$consumerName'"
    }
    return exceptionClassNames.map { className ->
        val trimmedClassName = className.trim()
        require(trimmedClassName.isNotEmpty()) {
            "$path.exceptions must not contain blank class names for CKC consumer '$consumerName'"
        }
        val exceptionClass = runCatching {
            Class.forName(trimmedClassName)
        }.getOrElse { reason ->
            error(
                "$path references exception class '$trimmedClassName' for CKC consumer '$consumerName', " +
                    "but the class could not be loaded: ${reason.message}"
            )
        }
        require(Throwable::class.java.isAssignableFrom(exceptionClass)) {
            "$path references '$trimmedClassName' for CKC consumer '$consumerName', but it is not a Throwable"
        }
        @Suppress("UNCHECKED_CAST")
        (exceptionClass as Class<out Throwable>).kotlin
    }
}
