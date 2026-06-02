package avh.ckc.demostubs

import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.pubsub.RedisPubSubAdapter
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import kotlinx.serialization.json.Json
import java.io.Closeable
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

internal class DemoStubsSettingsState(
    private val store: DemoStubsSettingsStore,
    fallback: DemoStubsSettings = DemoStubsSettings.baseline()
) : Closeable {
    private val current = AtomicReference(fallback)

    init {
        subscribeToUpdates()
        current.set(restoreOrFallback(fallback))
    }

    fun get(): DemoStubsSettings =
        current.get()

    fun update(settings: DemoStubsSettings) {
        store.save(settings)
        current.set(settings)
    }

    override fun close() {
        store.close()
    }

    private fun restoreOrFallback(fallback: DemoStubsSettings): DemoStubsSettings =
        try {
            store.load() ?: fallback
        } catch (error: Exception) {
            System.err.println("Failed to restore demo-stubs settings from Redis, using baseline: ${error.message}")
            fallback
        }

    private fun subscribeToUpdates() {
        try {
            store.subscribe { current.set(it) }
        } catch (error: Exception) {
            System.err.println("Failed to subscribe to demo-stubs settings updates: ${error.message}")
        }
    }
}

internal interface DemoStubsSettingsStore : Closeable {
    fun load(): DemoStubsSettings?

    fun save(settings: DemoStubsSettings)

    fun subscribe(listener: (DemoStubsSettings) -> Unit)
}

internal class RedisDemoStubsSettingsStore(
    redisUri: String,
    private val json: Json
) : DemoStubsSettingsStore {
    private val client = RedisClient.create(redisUri)
    private val connectionDelegate = lazy { client.connect() }
    private val connection: StatefulRedisConnection<String, String> by connectionDelegate
    private val listener = AtomicReference<(DemoStubsSettings) -> Unit>()
    private val subscribed = AtomicBoolean()
    private val closed = AtomicBoolean()
    private val retryExecutorDelegate = lazy {
        Executors.newSingleThreadScheduledExecutor { runnable ->
            Thread(runnable, "demo-stubs-settings-subscription").apply {
                isDaemon = true
            }
        }
    }
    private val retryExecutor: ScheduledExecutorService by retryExecutorDelegate
    private val pubSubConnectionDelegate = lazy {
        client.connectPubSub().also {
            it.addListener(object : RedisPubSubAdapter<String, String>() {
                override fun message(channel: String, message: String) {
                    if (channel != SETTINGS_CHANNEL) {
                        return
                    }
                    try {
                        listener.get()?.invoke(json.decodeFromString(DemoStubsSettings.serializer(), message))
                    } catch (error: Exception) {
                        System.err.println("Failed to apply demo-stubs settings update from Redis: ${error.message}")
                    }
                }
            })
        }
    }
    private val pubSubConnection: StatefulRedisPubSubConnection<String, String> by pubSubConnectionDelegate

    override fun load(): DemoStubsSettings? =
        connection.sync().get(SETTINGS_KEY)?.let {
            json.decodeFromString(DemoStubsSettings.serializer(), it)
        }

    override fun save(settings: DemoStubsSettings) {
        val encoded = json.encodeToString(DemoStubsSettings.serializer(), settings)
        connection.sync().set(
            SETTINGS_KEY,
            encoded
        )
        connection.sync().publish(SETTINGS_CHANNEL, encoded)
    }

    override fun subscribe(listener: (DemoStubsSettings) -> Unit) {
        this.listener.set(listener)
        subscribeOrRetry()
    }

    override fun close() {
        closed.set(true)
        if (retryExecutorDelegate.isInitialized()) {
            retryExecutor.shutdownNow()
        }
        if (pubSubConnectionDelegate.isInitialized()) {
            pubSubConnection.close()
        }
        if (connectionDelegate.isInitialized()) {
            connection.close()
        }
        client.shutdown()
    }

    private fun subscribeOrRetry() {
        if (closed.get() || subscribed.get()) {
            return
        }
        try {
            pubSubConnection.sync().subscribe(SETTINGS_CHANNEL)
            subscribed.set(true)
            listener.get()?.let { apply ->
                load()?.let(apply)
            }
        } catch (error: Exception) {
            System.err.println("Failed to subscribe to demo-stubs settings updates, retrying: ${error.message}")
            if (!closed.get()) {
                try {
                    retryExecutor.schedule(::subscribeOrRetry, SUBSCRIPTION_RETRY_DELAY_SECONDS, TimeUnit.SECONDS)
                } catch (_: RejectedExecutionException) {
                    // The store was closed while the retry was being scheduled.
                }
            }
        }
    }

    companion object {
        internal const val SETTINGS_KEY = "demo-stubs:settings"
        internal const val SETTINGS_CHANNEL = "demo-stubs:settings-updates"
        private const val SUBSCRIPTION_RETRY_DELAY_SECONDS = 1L
    }
}
