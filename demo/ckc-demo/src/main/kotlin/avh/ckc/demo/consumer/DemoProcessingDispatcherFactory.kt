package avh.ckc.demo.consumer

import avh.ckc.demo.config.DemoApplicationProperties
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import java.io.Closeable
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext

class DemoProcessingDispatcher(
    private val delegate: CoroutineDispatcher,
    private val closeAction: () -> Unit = {}
) : CoroutineDispatcher(), Closeable {
    override fun dispatch(context: CoroutineContext, block: Runnable) {
        delegate.dispatch(context, block)
    }

    override fun close() {
        closeAction()
    }
}

object DemoProcessingDispatcherFactory {
    fun create(
        properties: DemoApplicationProperties,
        dispatcherName: String,
        defaultType: DemoApplicationProperties.ProcessingDispatcherType,
        allowedTypes: Set<DemoApplicationProperties.ProcessingDispatcherType>
    ): DemoProcessingDispatcher {
        val requested = properties.consumers.processingDispatcherType
        val actual = if (requested == DemoApplicationProperties.ProcessingDispatcherType.AUTO) {
            defaultType
        } else {
            requested
        }
        require(actual in allowedTypes) {
            "demo.consumers.processing-dispatcher-type=$actual is not supported for $dispatcherName"
        }
        return when (actual) {
            DemoApplicationProperties.ProcessingDispatcherType.AUTO ->
                error("AUTO must be resolved before dispatcher creation")
            DemoApplicationProperties.ProcessingDispatcherType.DEFAULT ->
                DemoProcessingDispatcher(Dispatchers.Default)
            DemoApplicationProperties.ProcessingDispatcherType.IO ->
                DemoProcessingDispatcher(Dispatchers.IO)
            DemoApplicationProperties.ProcessingDispatcherType.FIXED ->
                fixedDispatcher(properties, dispatcherName)
            DemoApplicationProperties.ProcessingDispatcherType.VIRTUAL ->
                virtualDispatcher(properties, dispatcherName)
        }
    }

    private fun fixedDispatcher(
        properties: DemoApplicationProperties,
        dispatcherName: String
    ): DemoProcessingDispatcher {
        val threads = properties.consumers.workerDispatcherThreads
        require(threads > 0) {
            "demo.consumers.worker-dispatcher-threads must be > 0 for $dispatcherName"
        }
        val threadNumber = AtomicInteger()
        val executor = Executors.newFixedThreadPool(threads) { runnable ->
            Thread(runnable, "$dispatcherName-${threadNumber.incrementAndGet()}").apply {
                isDaemon = true
            }
        }
        val dispatcher = executor.asCoroutineDispatcher()
        return DemoProcessingDispatcher(dispatcher) { dispatcher.close() }
    }

    private fun virtualDispatcher(
        properties: DemoApplicationProperties,
        dispatcherName: String
    ): DemoProcessingDispatcher {
        val threadNumber = AtomicInteger()
        val prefix = properties.consumers.virtualThreadNamePrefix.ifBlank { "$dispatcherName-" }
        val executor = Executors.newThreadPerTaskExecutor { runnable ->
            Thread.ofVirtual()
                .name(prefix, threadNumber.incrementAndGet().toLong())
                .factory()
                .newThread(runnable)
        }
        val dispatcher = executor.asCoroutineDispatcher()
        return DemoProcessingDispatcher(dispatcher) { dispatcher.close() }
    }
}
