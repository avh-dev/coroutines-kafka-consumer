package avh.ckc.core.warmup

import io.lettuce.core.RedisClient
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.SetArgs
import io.lettuce.core.api.StatefulRedisConnection
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Settings for the MVP-wide exclusive warm-up capacity reservation.
 *
 * The lock deliberately has no expiry. Core deletes it only after a successful SessionCompleted;
 * an interrupted or ambiguous session remains quarantined until an operator verifies the warm-up
 * partitions and deletes [lockKey]. This favors isolation over automatic recovery in the MVP.
 */
class WarmupAssignmentConfiguration(
    val redisUri: String,
    val lockKey: String = "ckc:traffic-warmup:v1:global-assignment",
    val warmupPartition: Int = 0
) {
    init {
        require(redisUri.isNotBlank()) { "redisUri must not be blank" }
        require(lockKey.isNotBlank()) { "lockKey must not be blank" }
        require(lockKey.none(Char::isISOControl)) { "lockKey must not contain control characters" }
        require(warmupPartition >= 0) { "warmupPartition must not be negative" }
    }
}

internal interface WarmupAssignmentLease {
    val assignments: List<WarmupPartitionAssignment>
    suspend fun releaseCompleted()
    fun quarantine()
}

internal fun interface WarmupAssignmentLeaseProvider {
    suspend fun tryAcquire(
        sessionId: WarmupSessionId,
        donorInstanceId: WarmupInstanceId,
        recipientInstanceId: WarmupInstanceId,
        participants: List<WarmupParticipantConfiguration>
    ): WarmupAssignmentLease?
}

internal class RedisWarmupAssignmentLeaseProvider(
    private val configuration: WarmupAssignmentConfiguration,
    private val dispatcher: CoroutineDispatcher = Dispatchers.IO,
    private val client: RedisClient = RedisClient.create(configuration.redisUri)
) : WarmupAssignmentLeaseProvider, AutoCloseable {
    private val connectionDelegate = lazy(LazyThreadSafetyMode.SYNCHRONIZED) { client.connect() }
    private val connection: StatefulRedisConnection<String, String> by connectionDelegate

    override suspend fun tryAcquire(
        sessionId: WarmupSessionId,
        donorInstanceId: WarmupInstanceId,
        recipientInstanceId: WarmupInstanceId,
        participants: List<WarmupParticipantConfiguration>
    ): WarmupAssignmentLease? = withContext(dispatcher) {
        val token = listOf(sessionId.value, donorInstanceId.value, recipientInstanceId.value)
            .joinToString(":")
        if (connection.sync().set(configuration.lockKey, token, SetArgs.Builder.nx()) != "OK") {
            return@withContext null
        }
        RedisLease(
            configuration,
            token,
            participants.flatMap { participant ->
                participant.topicMappings.map { mapping ->
                    WarmupPartitionAssignment(
                        participant.consumerId,
                        mapping.sourceTopic,
                        mapping.warmupTopic,
                        configuration.warmupPartition
                    )
                }
            },
            connection,
            dispatcher
        )
    }

    override fun close() {
        if (connectionDelegate.isInitialized()) connection.close()
        client.shutdown()
    }

    private class RedisLease(
        private val configuration: WarmupAssignmentConfiguration,
        private val token: String,
        override val assignments: List<WarmupPartitionAssignment>,
        private val connection: StatefulRedisConnection<String, String>,
        private val dispatcher: CoroutineDispatcher
    ) : WarmupAssignmentLease {
        private val finished = AtomicBoolean(false)

        override suspend fun releaseCompleted() = withContext(dispatcher) {
            check(finished.compareAndSet(false, true)) { "Warm-up assignment lease already finished" }
            val removed = connection.sync().eval<Long>(
                RELEASE_SCRIPT,
                ScriptOutputType.INTEGER,
                arrayOf(configuration.lockKey),
                token
            )
            check(removed == 1L) { "Warm-up assignment reservation ownership was lost" }
        }

        override fun quarantine() {
            finished.compareAndSet(false, true)
        }
    }

    private companion object {
        const val RELEASE_SCRIPT =
            "if redis.call('get', KEYS[1]) == ARGV[1] then " +
                "return redis.call('del', KEYS[1]) else return 0 end"
    }
}
