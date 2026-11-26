package avh.ckc.warmup

import com.github.dockerjava.api.model.Bind
import com.github.dockerjava.api.model.Volume
import java.net.ServerSocket
import org.testcontainers.DockerClientFactory
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

internal class ReplaceableWarmupKafka {
    private val port = ServerSocket(0).use { it.localPort }
    private var kafka = newContainer()

    fun start() = kafka.start()

    fun connection(): Map<String, Any?> = mapOf("bootstrap.servers" to kafka.bootstrapServers)

    fun destroyData() {
        kafka.stop()
        kafka = newContainer()
    }

    fun startEmptyReplacement() {
        kafka.start()
    }

    fun stop() = kafka.stop()

    private fun newContainer(): KafkaContainer = FixedPortKafkaContainer(
        DockerImageName.parse("apache/kafka-native:3.8.0")
    ).withFixedKafkaPort(port)
}

private class FixedPortKafkaContainer(imageName: DockerImageName) : KafkaContainer(imageName) {
    fun withFixedKafkaPort(hostPort: Int): KafkaContainer =
        apply { addFixedExposedPort(hostPort, 9092) }
}

internal class PersistedWarmupKafka : AutoCloseable {
    private val docker = DockerClientFactory.instance().client()
    private val volume = docker.createVolumeCmd().exec().name
    private val port = ServerSocket(0).use { it.localPort }
    private var kafka = newContainer()

    fun start() = kafka.start()

    fun bootstrapServers(): String = kafka.bootstrapServers

    fun kill() {
        docker.killContainerCmd(kafka.containerId).exec()
    }

    fun restart() {
        kafka.stop()
        kafka = newContainer()
        kafka.start()
    }

    override fun close() {
        try {
            kafka.stop()
        } finally {
            docker.removeVolumeCmd(volume).exec()
        }
    }

    private fun newContainer(): KafkaContainer = FixedPortKafkaContainer(
        DockerImageName.parse("apache/kafka:3.8.0")
    ).withFixedKafkaPort(port)
        .withEnv("CLUSTER_ID", "M8gW-iVjQkGqC1rH0vM4xA")
        .withEnv("KAFKA_LOG_DIRS", "/var/lib/kafka/data")
        .withCreateContainerCmdModifier { command ->
            val config = requireNotNull(command.hostConfig)
            config.withBinds(config.binds.orEmpty().toList() + Bind(volume, Volume("/var/lib/kafka/data")))
        }
}
