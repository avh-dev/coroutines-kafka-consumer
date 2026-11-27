package avh.ckc.warmup.broker

import avh.ckc.warmup.broker.BrokerLifecycleEvent.ContextDrained
import avh.ckc.warmup.broker.BrokerLifecycleEvent.DrainAccepted
import avh.ckc.warmup.broker.BrokerLifecycleEvent.DrainRequested
import avh.ckc.warmup.broker.BrokerLifecycleEvent.Joined
import avh.ckc.warmup.broker.BrokerLifecycleEvent.Left
import avh.ckc.warmup.broker.BrokerLifecycleEvent.Started
import avh.ckc.warmup.TrafficWarmupBrokerLifecycleConfiguration
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.admin.RecordsToDelete
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class BrokerLifecycleCliIntegrationTest {
    private val connection get() = mapOf("bootstrap.servers" to kafka.bootstrapServers)
    private val timeout = Duration.ofSeconds(5)

    @Test
    fun `CLI waits for the exact boundary membership from a minimal runtime classpath`() {
        val topic = createTopic()
        val epoch = UUID.randomUUID().toString()
        KafkaBrokerLifecycleLog(topic, connection, 3_000).use { log ->
            val start = log.submit(Started(epoch)).get(5, TimeUnit.SECONDS)
            log.seek(start)
            log.submit(Joined(epoch, "left-before", "one")).get(5, TimeUnit.SECONDS)
            log.submit(Joined(epoch, "left-after", "two")).get(5, TimeUnit.SECONDS)
            log.submit(Left(epoch, "left-before")).get(5, TimeUnit.SECONDS)
            val cli = launchCli(topic, epoch, 10_000)
            try {
                val boundary = awaitRequest(log)
                val request = boundary.event as DrainRequested
                log.submit(Joined(epoch, "joined-after", "three")).get(5, TimeUnit.SECONDS)
                log.submit(Left(epoch, "left-after")).get(5, TimeUnit.SECONDS)
                log.submit(DrainAccepted(epoch, request.drainId, "left-after")).get(5, TimeUnit.SECONDS)
                log.submit(ContextDrained(epoch, request.drainId, "joined-after", emptySet())).get(5, TimeUnit.SECONDS)
                log.submit(ContextDrained(epoch, request.drainId, "joined-after", emptySet())).get(5, TimeUnit.SECONDS)
                assertTrue(cli.isAlive)
                log.submit(ContextDrained(epoch, request.drainId, "left-after", setOf("session"))).get(5, TimeUnit.SECONDS)

                val output = cli.finish(0)
                assertContains(output, "status=DRAINED")
                assertContains(output, "brokerEpoch=$epoch drainId=${request.drainId} boundaryOffset=${boundary.offset}")
                assertContains(output, "expected=[left-after] accepted=[left-after] missing=[]")
            } finally {
                cli.destroyForcibly()
            }
        }
    }

    @Test
    fun `missing final acknowledgement times out and repeated drain keeps the same offset and deadline`() {
        val topic = createTopic()
        KafkaBrokerLifecycleLog(topic, connection, 3_000).use { log ->
            log.seek(log.submit(Started("epoch")).get(5, TimeUnit.SECONDS))
            val peer = ProcessBuilder(
                brokerJava(),
                "-cp", brokerClasspath() + File.pathSeparator +
                    CrashingLifecyclePeer::class.java.protectionDomain.codeSource.location.toURI().let(Path::of),
                CrashingLifecyclePeer::class.java.name, kafka.bootstrapServers, topic
            ).redirectErrorStream(true).start()
            var cli: Process? = null
            try {
                awaitRecord(log) { it is Joined }
                val draining = launchCli(topic, "epoch", 6_000)
                cli = draining
                val boundary = awaitRequest(log)
                val request = boundary.event as DrainRequested
                awaitRecord(log) { it is DrainAccepted }
                assertTrue(peer.destroyForcibly().waitFor(5, TimeUnit.SECONDS))
                log.submit(DrainRequested("epoch", "duplicate", request.deadlineMillis + 60_000)).get(5, TimeUnit.SECONDS)
                val first = draining.finish(1)
                val repeat = launchCli(topic, "epoch", 4_000).finish(1)
                val coordinates = "drainId=${request.drainId} boundaryOffset=${boundary.offset} deadlineMillis=${request.deadlineMillis}"
                for (output in listOf(first, repeat)) {
                    assertContains(output, "status=TIMED_OUT")
                    assertContains(output, coordinates)
                    assertContains(output, "expected=[disappeared] accepted=[disappeared] missing=[disappeared]")
                }
            } finally {
                cli?.destroyForcibly()
                peer.destroyForcibly()
            }
        }
    }

    @Test
    fun `a context finishing normal exit still acknowledges a request that precedes its Left`() = runBlocking {
        val topic = createTopic()
        val epoch = UUID.randomUUID().toString()
        val ready = Files.createTempFile("broker-witness", ".ready")
        val witness = launchBrokerLifecycleCli("serve", kafka.bootstrapServers, topic, epoch, "5000", ready.toString())
        val configuration = TrafficWarmupBrokerLifecycleConfiguration().apply {
            this.topic = topic
            kafkaProperties = connection
        }
        val left = CompletableDeferred<Left>()
        val leftAck = CompletableFuture<Long>()
        val acceptedAck = CompletableFuture<Long>()
        val cleanupStarted = CompletableDeferred<Unit>()
        val cleanup = CompletableDeferred<Set<String>>()
        val before = WarmupBrokerLifecycle(
            configuration, this, Dispatchers.IO, fence = { _, _ -> CompletableDeferred(emptySet()) }, reopen = { true }
        )
        val after = WarmupBrokerLifecycle(
            configuration, this, Dispatchers.IO, fence = { _, _ ->
                cleanupStarted.complete(Unit)
                cleanup
            }, reopen = { true }, submit = { sender, event ->
                when (event) {
                    is Left -> {
                        left.complete(event)
                        leftAck
                    }
                    is DrainAccepted -> {
                        sender.submit(event)
                        acceptedAck
                    }
                    else -> sender.submit(event)
                }
            }
        )
        var cli: Process? = null
        try {
            withTimeout(5.seconds) {
                while (Files.readString(ready) != epoch) {
                    check(witness.isAlive) { witness.inputStream.bufferedReader().readText() }
                    delay(20)
                }
            }
            before.start()
            after.start()
            assertTrue(before.awaitInitialAdmission(5.seconds))
            assertTrue(after.awaitInitialAdmission(5.seconds))
            before.leave(5.seconds)
            val exiting = async { after.leave(10.seconds) }
            val pendingLeft = withTimeout(5.seconds) { left.await() }
            val process = launchCli(topic, epoch, 10_000)
            cli = process
            KafkaBrokerLifecycleLog(topic, connection, 3_000).use { observer ->
                observer.seek(0)
                val request = withContext(Dispatchers.IO) { awaitRequest(observer) }
                val leftOffset = observer.submit(pendingLeft).get(5, TimeUnit.SECONDS)
                assertTrue(leftOffset > request.offset)
                leftAck.complete(leftOffset)
            }
            withTimeout(5.seconds) { cleanupStarted.await() }
            assertFalse(acceptedAck.isDone)
            cleanup.complete(setOf("finished-session"))
            exiting.await()
            val output = withContext(Dispatchers.IO) { process.finish(0) }
            assertContains(output, "expected=[${pendingLeft.context}]")
            assertContains(output, "finished-session")
        } finally {
            cleanup.complete(emptySet())
            leftAck.completeExceptionally(IllegalStateException("test finished"))
            cli?.destroyForcibly()
            witness.destroyForcibly().waitFor(5, TimeUnit.SECONDS)
            Files.deleteIfExists(ready)
        }
    }

    @Test
    fun `a truncated log cannot prove a visibly empty roster`() {
        val topic = createTopic()
        val partition = TopicPartition(topic, 0)
        KafkaBrokerLifecycleLog(topic, connection, 3_000).use { log ->
            log.submit(Started("lost")).get(5, TimeUnit.SECONDS)
            log.submit(Joined("lost", "missing-from-log", "registration")).get(5, TimeUnit.SECONDS)
            Admin.create(connection).use { admin ->
                admin.deleteRecords(mapOf(partition to RecordsToDelete.beforeOffset(log.end(timeout))))
                    .all().get(5, TimeUnit.SECONDS)
            }
            val output = launchCli(topic, "lost", 4_000).finish(2)
            assertContains(output, "status=CONTROL_UNAVAILABLE")
            assertContains(output, "expected=null")
        }
    }

    private fun awaitRequest(log: KafkaBrokerLifecycleLog): BrokerLifecycleRecord =
        awaitRecord(log) { it is DrainRequested }

    private fun awaitRecord(log: KafkaBrokerLifecycleLog, predicate: (BrokerLifecycleEvent) -> Boolean): BrokerLifecycleRecord {
        val budget = BrokerBudget(8_000)
        while (budget.remaining() > 0) {
            log.poll(budget.timeout(100)).firstOrNull { predicate(it.event) }?.let {
                log.seek(it.offset + 1)
                return it
            }
        }
        error("Lifecycle event did not reach Kafka")
    }

    private fun launchCli(topic: String, epoch: String, budgetMillis: Int): Process {
        return launchBrokerLifecycleCli("drain", kafka.bootstrapServers, topic, epoch, budgetMillis.toString())
    }

    private fun Process.finish(expectedStatus: Int): String {
        try {
            assertTrue(waitFor(15, TimeUnit.SECONDS), "CLI did not exit")
            val output = inputStream.bufferedReader().readText()
            assertEquals(expectedStatus, exitValue(), output)
            return output
        } finally {
            destroyForcibly()
        }
    }

    private fun createTopic(): String = "broker-lifecycle-${UUID.randomUUID()}".also { topic ->
        Admin.create(connection).use { admin ->
            admin.createTopics(listOf(NewTopic(topic, 1, 1).configs(mapOf(
                "retention.ms" to "-1", "retention.bytes" to "-1", "message.timestamp.type" to "LogAppendTime"
            )))).all().get(5, TimeUnit.SECONDS)
        }
    }

    companion object {
        @Container
        @JvmStatic
        val kafka = KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"))
    }
}

private object CrashingLifecyclePeer {
    @JvmStatic
    fun main(args: Array<String>) {
        KafkaBrokerLifecycleLog(args[1], mapOf("bootstrap.servers" to args[0]), 3_000).use { log ->
            val offset = log.submit(Joined("epoch", "disappeared", "registration")).get(5, TimeUnit.SECONDS)
            log.seek(offset)
            while (true) {
                val request = log.poll(Duration.ofMillis(100)).map { it.event }
                    .filterIsInstance<DrainRequested>().firstOrNull() ?: continue
                log.submit(DrainAccepted("epoch", request.drainId, "disappeared")).get(5, TimeUnit.SECONDS)
                Thread.sleep(60_000)
                error("The test must kill this participant before it sends ContextDrained")
            }
        }
    }
}

private fun brokerClasspath(): String {
    val directory = Path.of(requireNotNull(System.getProperty("ckc.warmup.brokerCliDirectory")))
    val jars = Files.list(directory).use { it.toList() }
    assertTrue(jars.any { it.fileName.toString().startsWith("coroutines-kafka-consumer-warmup-") })
    assertTrue(jars.none { it.fileName.toString().contains(Regex("consumer-core|spring|micrometer")) })
    return jars.joinToString(File.pathSeparator)
}

private fun brokerJava(): String = Path.of(System.getProperty("java.home"), "bin", "java").toString()

internal fun launchBrokerLifecycleCli(vararg args: String): Process = ProcessBuilder(
    brokerJava(), "-cp", brokerClasspath(), BrokerLifecycleCli::class.java.name, *args
).redirectErrorStream(true).start()
