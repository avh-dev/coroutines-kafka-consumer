package avh.ckc.spring

import avh.ckc.core.RecordProcessingContext
import kotlinx.coroutines.slf4j.MDCContext
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.MDC

internal fun recordProcessingContext(
    properties: CkcConsumerProperties,
    consumerName: String,
    consumerProperties: CkcConsumerProperties.Consumer
): RecordProcessingContext<Any?, Any?>? {
    val mdc = properties.observability.mdc
    if (!mdc.enabled) {
        return null
    }
    require(mdc.maxKeyLength > 0) {
        "ckc.observability.mdc.max-key-length must be > 0"
    }
    return MdcRecordProcessingContext(
        consumerName = consumerName,
        processingMode = consumerProperties.processingMode.name,
        includeKey = mdc.includeKey,
        maxKeyLength = mdc.maxKeyLength
    )
}

internal class MdcRecordProcessingContext(
    private val consumerName: String,
    private val processingMode: String,
    private val includeKey: Boolean,
    private val maxKeyLength: Int
) : RecordProcessingContext<Any?, Any?> {
    override suspend fun withRecordContext(record: ConsumerRecord<Any?, Any?>, block: suspend () -> Unit) {
        val contextMap = MDC.getCopyOfContextMap()?.toMutableMap() ?: linkedMapOf()
        contextMap["ckc.consumer"] = consumerName
        contextMap["ckc.processing.mode"] = processingMode
        contextMap["kafka.topic"] = record.topic()
        contextMap["kafka.partition"] = record.partition().toString()
        contextMap["kafka.offset"] = record.offset().toString()
        if (includeKey) {
            record.key()?.let { contextMap["kafka.key"] = it.toString().truncate(maxKeyLength) }
        }
        withContext(MDCContext(contextMap)) {
            block()
        }
    }

    private fun String.truncate(maxLength: Int): String =
        if (length <= maxLength) this else take(maxLength)
}
