package avh.ckc.demo

import avh.ckc.demo.audit.auditTopicId
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.serialization.OrderLifecycleEventSerializer
import avh.ckc.spring.CkcWarmupForwardedEvent
import avh.ckc.spring.CkcWarmupLifecycleEvent
import avh.ckc.warmup.TrafficWarmupLifecyclePhase.MAIN_READY
import avh.ckc.warmup.TrafficWarmupLifecyclePhase.RECIPIENT_FINISHED
import avh.ckc.warmup.TrafficWarmupLifecyclePhase.WARMING
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import io.micrometer.core.instrument.MeterRegistry
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit.SECONDS
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.boot.WebApplicationType
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.ApplicationListener
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.context.PayloadApplicationEvent
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

@Testcontainers(disabledWithoutDocker = true)
class CkcSpringBootWarmupAuditIntegrationTest {
    @Test
    fun `starter audits the original offset after warmup delegation with CKC metrics disabled`() {
        val connection = mapOf("bootstrap.servers" to kafka.bootstrapServers)
        Admin.create(connection).use { admin ->
            val topics = listOf(
                DemoTopics.ORDER_EVENTS, DemoTopics.BATCH_EVENTS, DemoTopics.CAULDRON_EVENTS,
                DemoTopics.WARMUP_ORDER_EVENTS, DemoTopics.WARMUP_BATCH_EVENTS, DemoTopics.WARMUP_CAULDRON_EVENTS,
                DemoTopics.WARMUP_CONTROL
            )
            admin.createTopics(topics.map { NewTopic(it, 1, 1.toShort()) }).all().get(10, SECONDS)
        }

        val audit = CopyOnWriteArrayList<List<String>>()
        val appender = object : AppenderBase<ILoggingEvent>() {
            override fun append(event: ILoggingEvent) {
                audit += event.formattedMessage.split('|')
            }
        }
        val logger = LoggerFactory.getLogger("AUDIT_TCP") as Logger

        val donor = WarmupEvents()
        val recipient = WarmupEvents()
        try {
            startApplication("donor", donor).use { donorContext ->
                await { donor.mainReady() }
                startApplication("recipient", recipient).use { recipientContext ->
                    appender.start()
                    logger.addAppender(appender)
                    await { recipient.lifecycle.any { it.event.phase == WARMING } }
                    KafkaProducer(connection, StringSerializer(), OrderLifecycleEventSerializer()).use { producer ->
                        runBlocking {
                            withTimeout(8_000) {
                                var key = 0
                                while (donor.forwarded.isEmpty()) {
                                    val record = ProducerRecord(
                                        DemoTopics.ORDER_EVENTS, "order-${key++}", OrderLifecycleEvent.getDefaultInstance()
                                    )
                                    producer.send(record).get(5, SECONDS)
                                    delay(150)
                                }
                            }
                        }
                    }

                    val forwarded = donor.forwarded.first().event
                    val source = listOf(
                        "C", auditTopicId(forwarded.sourceTopic).toString(),
                        forwarded.sourcePartition.toString(), forwarded.sourceOffset.toString()
                    )
                    val destination = listOf(
                        "C", auditTopicId(forwarded.destinationTopic).toString(),
                        forwarded.destinationPartition.toString(), forwarded.destinationOffset.toString()
                    )
                    await { recipient.mainReady() }
                    assertNull(recipient.lifecycle.single { it.event.phase == RECIPIENT_FINISHED }.event.reasonCode)
                    val destinationAudits = audit.filter { it.take(4) == destination }
                    val sourceAudits = audit.filter { it.take(4) == source }
                    assertEquals(1, destinationAudits.size, "recipient audit for $destination")
                    assertEquals(1, sourceAudits.size, "donor audit for $source")
                    assertEquals(destinationAudits.single()[5], sourceAudits.single()[5])
                    for (context in listOf(donorContext, recipientContext)) {
                        val registry = context.getBean(MeterRegistry::class.java)
                        assertTrue(registry.meters.none { it.id.name.startsWith("demo.ckc.") })
                    }
                }
            }
        } finally {
            logger.detachAppender(appender)
            appender.stop()
        }
    }

    private fun startApplication(instance: String, events: WarmupEvents): ConfigurableApplicationContext =
        SpringApplicationBuilder(DemoApplication::class.java)
            .profiles("ckc-spring-boot")
            .web(WebApplicationType.NONE)
            .listeners(events)
            .run(
                "--KAFKA_ENABLED=true",
                "--KAFKA_BOOTSTRAP_SERVERS=${kafka.bootstrapServers}",
                "--TRAFFIC_WARMUP_ENABLED=true",
                "--TRAFFIC_WARMUP_INSTANCE_ID=$instance",
                "--TRAFFIC_WARMUP_CONTROL_KAFKA_BOOTSTRAP_SERVERS=${kafka.bootstrapServers}",
                "--TRAFFIC_WARMUP_DESTINATION_KAFKA_BOOTSTRAP_SERVERS=${kafka.bootstrapServers}",
                "--TRAFFIC_WARMUP_OFFER_WINDOW_MS=1000",
                "--TRAFFIC_WARMUP_DURATION_SECONDS=12",
                "--CKC_METRICS_ENABLED=false",
                "--AUDIT_LOG_ENABLED=true",
                "--demo.audit.enabled=\${AUDIT_LOG_ENABLED}",
                "--demo.consumers.processing-enabled=false",
                "--ckc.lifecycle.terminal-failure-action=REPORT_ONLY",
                "--spring.autoconfigure.exclude=com.linecorp.armeria.spring.ArmeriaAutoConfiguration," +
                    "com.linecorp.armeria.spring.actuate.ArmeriaSpringActuatorAutoConfiguration"
            )

    private fun await(condition: () -> Boolean) = runBlocking {
        withTimeout(25_000) {
            while (!condition()) delay(25)
        }
    }

    private class WarmupEvents : ApplicationListener<PayloadApplicationEvent<*>> {
        val lifecycle = CopyOnWriteArrayList<CkcWarmupLifecycleEvent>()
        val forwarded = CopyOnWriteArrayList<CkcWarmupForwardedEvent>()

        override fun onApplicationEvent(event: PayloadApplicationEvent<*>) {
            when (val payload = event.payload) {
                is CkcWarmupLifecycleEvent -> lifecycle += payload
                is CkcWarmupForwardedEvent -> forwarded += payload
            }
        }

        fun mainReady(): Boolean = lifecycle.count { it.event.phase == MAIN_READY } == 3
    }

    companion object {
        @Container
        @JvmStatic
        val kafka = KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"))
    }
}
