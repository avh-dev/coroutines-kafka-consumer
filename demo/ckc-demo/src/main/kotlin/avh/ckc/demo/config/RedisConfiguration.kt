package avh.ckc.demo.config

import avh.ckc.demo.repository.RedisBrewingStateStore
import avh.ckc.demo.repository.RedisSuspendBrewingStateRepository
import avh.ckc.demo.repository.RedisSyncBrewingStateRepository
import avh.ckc.demo.repository.SuspendBrewingStateRepository
import avh.ckc.demo.repository.SyncBrewingStateRepository
import kotlinx.serialization.json.Json
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.serializer.RedisSerializer
import org.springframework.data.redis.serializer.RedisSerializationContext
import org.springframework.data.redis.serializer.StringRedisSerializer

@Configuration(proxyBeanMethods = false)
class RedisConfiguration {
    @Bean
    fun redisJson(): Json = Json { ignoreUnknownKeys = true }

    @Bean
    fun orderRedisTemplate(connectionFactory: ReactiveRedisConnectionFactory): ReactiveRedisTemplate<String, ByteArray> {
        val serializationContext = RedisSerializationContext
            .newSerializationContext<String, ByteArray>(StringRedisSerializer())
            .value(RedisSerializer.byteArray())
            .build()

        return ReactiveRedisTemplate(connectionFactory, serializationContext)
    }

    @Bean
    fun redisBrewingStateStore(
        orderRedisTemplate: ReactiveRedisTemplate<String, ByteArray>,
        redisJson: Json
    ): RedisBrewingStateStore = RedisBrewingStateStore(orderRedisTemplate, redisJson)

    @Bean
    fun syncBrewingStateRepository(store: RedisBrewingStateStore): SyncBrewingStateRepository =
        RedisSyncBrewingStateRepository(store)

    @Bean
    fun suspendBrewingStateRepository(store: RedisBrewingStateStore): SuspendBrewingStateRepository =
        RedisSuspendBrewingStateRepository(store)
}
