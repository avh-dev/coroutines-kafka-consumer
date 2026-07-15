package avh.ckc.spring

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import org.springframework.context.ApplicationContext
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

internal class CkcDispatcherRegistry(
    private val applicationContext: ApplicationContext,
    private val properties: CkcConsumerProperties
) : AutoCloseable {
    private val resolvedDispatchers = linkedMapOf<String, CoroutineDispatcher>()
    private val ownedDispatchers = mutableListOf<ExecutorCoroutineDispatcher>()

    fun processingDispatcher(
        consumerName: String,
        consumerProperties: CkcConsumerProperties.Consumer
    ): CoroutineDispatcher {
        val dispatcherName = resolvedProcessingDispatcherName(properties, consumerProperties)
        return dispatcher(dispatcherName, consumerName)
    }

    private fun dispatcher(name: String, consumerName: String): CoroutineDispatcher =
        resolvedDispatchers.getOrPut(name) {
            when (name) {
                DISPATCHERS_DEFAULT_NAME -> Dispatchers.Default
                DISPATCHERS_IO_NAME -> Dispatchers.IO
                else -> configuredDispatcher(name, consumerName)
            }
        }

    private fun configuredDispatcher(name: String, consumerName: String): CoroutineDispatcher {
        val dispatcher = properties.dispatchers[name]
            ?: error("CKC consumer '$consumerName' references unknown processing dispatcher '$name'")
        return when (dispatcher.type) {
            CkcConsumerProperties.DispatcherType.DISPATCHERS_DEFAULT -> Dispatchers.Default
            CkcConsumerProperties.DispatcherType.DISPATCHERS_IO -> Dispatchers.IO
            CkcConsumerProperties.DispatcherType.FIXED_THREAD_POOL -> fixedThreadPoolDispatcher(name, dispatcher)
            CkcConsumerProperties.DispatcherType.VIRTUAL_THREAD_PER_TASK -> virtualThreadPerTaskDispatcher(name, dispatcher)
            CkcConsumerProperties.DispatcherType.BEAN -> beanDispatcher(name, consumerName, dispatcher)
        }
    }

    private fun fixedThreadPoolDispatcher(
        name: String,
        dispatcher: CkcConsumerProperties.Dispatcher
    ): ExecutorCoroutineDispatcher {
        require(dispatcher.threads > 0) { "ckc.dispatchers.$name.threads must be > 0" }
        val threadNumber = AtomicInteger()
        val prefix = dispatcher.threadNamePrefix ?: "ckc-$name-worker-"
        return Executors.newFixedThreadPool(dispatcher.threads) { runnable ->
            Thread(runnable, "$prefix${threadNumber.incrementAndGet()}").apply {
                isDaemon = true
            }
        }.asCoroutineDispatcher().also {
            ownedDispatchers += it
            logger.info(
                "Resolved CKC dispatcher '$name': type=${dispatcher.type}, threads=${dispatcher.threads}, " +
                    "threadNamePrefix=$prefix"
            )
        }
    }

    private fun virtualThreadPerTaskDispatcher(
        name: String,
        dispatcher: CkcConsumerProperties.Dispatcher
    ): ExecutorCoroutineDispatcher {
        val threadNumber = AtomicInteger()
        val prefix = dispatcher.threadNamePrefix ?: "ckc-$name-virtual-"
        return Executors.newThreadPerTaskExecutor { runnable ->
            Thread.ofVirtual()
                .name(prefix, threadNumber.incrementAndGet().toLong())
                .factory()
                .newThread(runnable)
        }.asCoroutineDispatcher().also {
            ownedDispatchers += it
            logger.info(
                "Resolved CKC dispatcher '$name': type=${dispatcher.type}, threadNamePrefix=$prefix"
            )
        }
    }

    private fun beanDispatcher(
        name: String,
        consumerName: String,
        dispatcher: CkcConsumerProperties.Dispatcher
    ): CoroutineDispatcher {
        val beanName = requireNotNull(dispatcher.beanName?.takeIf { it.isNotBlank() }) {
            "ckc.dispatchers.$name.bean-name must be specified for BEAN dispatchers"
        }
        val bean = runCatching {
            applicationContext.getBean(beanName, CoroutineDispatcher::class.java)
        }.getOrElse { reason ->
            error(
                "CKC consumer '$consumerName' references dispatcher '$name' backed by bean '$beanName', " +
                    "but no CoroutineDispatcher bean with that name is available: ${reason.message}"
            )
        }
        logger.info("Resolved CKC dispatcher '$name': type=${dispatcher.type}, beanName=$beanName")
        return bean
    }

    override fun close() {
        ownedDispatchers.asReversed().forEach { dispatcher ->
            runCatching { dispatcher.close() }
        }
        ownedDispatchers.clear()
        resolvedDispatchers.clear()
    }

    companion object {
        const val DISPATCHERS_DEFAULT_NAME = "dispatchers-default"
        const val DISPATCHERS_IO_NAME = "dispatchers-io"
        val BUILT_IN_DISPATCHER_NAMES = setOf(DISPATCHERS_DEFAULT_NAME, DISPATCHERS_IO_NAME)
    }
}
