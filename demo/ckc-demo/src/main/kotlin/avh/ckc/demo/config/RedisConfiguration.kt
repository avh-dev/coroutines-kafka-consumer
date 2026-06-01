package avh.ckc.demo.config

import avh.ckc.demo.repository.RedisBrewingStateStore
import avh.ckc.demo.repository.RedisSuspendBrewingStateRepository
import avh.ckc.demo.repository.RedisSyncBrewingStateRepository
import avh.ckc.demo.repository.SuspendBrewingStateRepository
import avh.ckc.demo.repository.SyncBrewingStateRepository
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import io.lettuce.core.api.coroutines.RedisCoroutinesCommandsImpl
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.codec.StringCodec
import kotlinx.serialization.json.Json
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.io.Closeable

@OptIn(ExperimentalLettuceCoroutinesApi::class)
@Configuration(proxyBeanMethods = false)
class RedisConfiguration {
    @Bean
    fun redisJson(): Json = Json { ignoreUnknownKeys = true }

    @Bean(destroyMethod = "close")
    fun demoRedisCommands(
        @Value("\${spring.data.redis.host:localhost}") host: String,
        @Value("\${spring.data.redis.port:6379}") port: Int
    ): DemoRedisCommands = LettuceDemoRedisCommands("redis://$host:$port")

    @Bean
    fun redisBrewingStateStore(
        demoRedisCommands: DemoRedisCommands,
        redisJson: Json
    ): RedisBrewingStateStore = RedisBrewingStateStore(demoRedisCommands, redisJson)

    @Bean
    fun syncBrewingStateRepository(store: RedisBrewingStateStore): SyncBrewingStateRepository =
        RedisSyncBrewingStateRepository(store)

    @Bean
    fun suspendBrewingStateRepository(store: RedisBrewingStateStore): SuspendBrewingStateRepository =
        RedisSuspendBrewingStateRepository(store)
}

@OptIn(ExperimentalLettuceCoroutinesApi::class)
interface DemoRedisCommands {
    fun sync(): RedisCommands<String, ByteArray>

    fun coroutines(): RedisCoroutinesCommands<String, ByteArray>
}

@OptIn(ExperimentalLettuceCoroutinesApi::class)
private class LettuceDemoRedisCommands(redisUri: String) : DemoRedisCommands, Closeable {
    private val client = RedisClient.create(redisUri)
    private val connectionDelegate = lazy {
        client.connect(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE))
    }
    private val connection: StatefulRedisConnection<String, ByteArray> by connectionDelegate
    private val coroutineCommandsDelegate = lazy {
        RedisCoroutinesCommandsImpl(connection.reactive())
    }

    override fun sync(): RedisCommands<String, ByteArray> =
        connection.sync()

    override fun coroutines(): RedisCoroutinesCommands<String, ByteArray> =
        coroutineCommandsDelegate.value

    override fun close() {
        if (connectionDelegate.isInitialized()) {
            connection.close()
        }
        client.shutdown()
    }
}
