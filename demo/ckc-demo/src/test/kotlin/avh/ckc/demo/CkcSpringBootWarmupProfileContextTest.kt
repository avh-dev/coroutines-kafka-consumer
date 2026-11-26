package avh.ckc.demo

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.serialization.BatchLifecycleEventSerializer
import avh.ckc.demo.serialization.CauldronTelemetryEventSerializer
import avh.ckc.demo.serialization.OrderLifecycleEventSerializer
import avh.ckc.spring.CkcConsumerProperties
import avh.ckc.spring.CkcConsumerRegistry
import avh.ckc.spring.CkcWarmupForwardedEvent
import avh.ckc.spring.CkcWarmupLifecycleEvent
import avh.ckc.warmup.TrafficWarmupForwardedEvent
import avh.ckc.warmup.TrafficWarmupLifecycleEvent
import avh.ckc.warmup.TrafficWarmupLifecyclePhase
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.micrometer.core.instrument.MeterRegistry
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(
    properties = [
        "KAFKA_ENABLED=false",
        "SERVER_PORT=0",
        "demo.audit.enabled=true",
        "TRAFFIC_WARMUP_ENABLED=true",
        "TRAFFIC_WARMUP_INSTANCE_ID=pod-test",
        "TRAFFIC_WARMUP_NAMESPACE=profile-test",
        "TRAFFIC_WARMUP_MAX_INFLIGHT_DELEGATIONS=17",
        "TRAFFIC_WARMUP_CONTROL_KAFKA_BOOTSTRAP_SERVERS=control:9092",
        "TRAFFIC_WARMUP_CONTROL_KAFKA_SECURITY_PROTOCOL=SSL",
        "TRAFFIC_WARMUP_CONTROL_KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM=https",
        "TRAFFIC_WARMUP_DESTINATION_KAFKA_BOOTSTRAP_SERVERS=destination:9092",
        "TRAFFIC_WARMUP_DESTINATION_KAFKA_SECURITY_PROTOCOL=SSL",
        "TRAFFIC_WARMUP_DESTINATION_KAFKA_SSL_TRUSTSTORE_CERTIFICATES=test-inline-certificate",
        "TRAFFIC_WARMUP_BROKER_LIFECYCLE_TOPIC=warmup-broker",
        "TRAFFIC_WARMUP_ORDER_QUOTA_PROFILE=0 -> (3s, ramp) -> 23",
        "TRAFFIC_WARMUP_ORDER_QUOTA_INTERVAL_MS=2000",
        "TRAFFIC_WARMUP_BATCH_QUOTA_PROFILE=11",
        "TRAFFIC_WARMUP_BATCH_QUOTA_INTERVAL_MS=3000",
        "TRAFFIC_WARMUP_CAULDRON_QUOTA_PROFILE=7",
        "TRAFFIC_WARMUP_CAULDRON_QUOTA_INTERVAL_MS=4000",
        "TRAFFIC_WARMUP_PRODUCER_LINGER_MS=8",
        "TRAFFIC_WARMUP_PRODUCER_BATCH_SIZE=32768",
        "spring.autoconfigure.exclude=com.linecorp.armeria.spring.ArmeriaAutoConfiguration," +
            "com.linecorp.armeria.spring.actuate.ArmeriaSpringActuatorAutoConfiguration"
    ]
)
@ActiveProfiles("ckc-spring-boot")
class CkcSpringBootWarmupProfileContextTest(
    @Autowired private val applicationContext: ApplicationContext,
    @Autowired private val properties: CkcConsumerProperties,
    @Autowired private val meterRegistry: MeterRegistry,
    @Autowired private val registry: CkcConsumerRegistry
) {
    @Test
    fun `all three registered participants bind mappings and quotas in one context`() {
        assertTrue(properties.warmup.enabled)
        val shared = properties.warmup.contexts.getValue("demo")
        assertEquals("pod-test", shared.instanceId)
        assertEquals("profile-test", shared.namespace)
        assertEquals(DemoTopics.WARMUP_CONTROL, shared.controlTopic)
        assertEquals("warmup-control", shared.controlCluster)
        assertEquals(17, shared.maxInflightDelegations)
        val broker = assertNotNull(shared.brokerLifecycle)
        assertEquals("warmup-broker", broker.topic)
        assertEquals("warmup-destination", broker.cluster)

        val control = properties.clusters.getValue(shared.controlCluster).kafkaProperties
        assertEquals("control:9092", control["bootstrap.servers"])
        assertEquals("SSL", control["security.protocol"])
        assertEquals("https", control["ssl.endpoint.identification.algorithm"])
        assertFalse("ssl.truststore.certificates" in control)
        assertFalse("ssl.truststore.type" in control)
        val destination = properties.clusters.getValue("warmup-destination").kafkaProperties
        assertEquals("destination:9092", destination["bootstrap.servers"])
        assertEquals("SSL", destination["security.protocol"])
        assertEquals("", destination["ssl.endpoint.identification.algorithm"])
        assertEquals("test-inline-certificate", destination["ssl.truststore.certificates"])
        assertEquals("PEM", destination["ssl.truststore.type"])

        assertMapping(
            "order-events", DemoTopics.ORDER_EVENTS, DemoTopics.WARMUP_ORDER_EVENTS,
            OrderLifecycleEventSerializer::class.java, "0 -> (3s, ramp) -> 23", 2
        )
        assertMapping(
            "batch-events", DemoTopics.BATCH_EVENTS, DemoTopics.WARMUP_BATCH_EVENTS,
            BatchLifecycleEventSerializer::class.java, "11", 3
        )
        assertMapping(
            "cauldron-events", DemoTopics.CAULDRON_EVENTS, DemoTopics.WARMUP_CAULDRON_EVENTS,
            CauldronTelemetryEventSerializer::class.java, "7", 4
        )

        val consumers = setOf("order-events", "batch-events", "cauldron-events")
        assertEquals(consumers, registry.consumerNames)
        assertTrue(consumers.none(registry::isRunning))
        val health = applicationContext.getBean("ckcHealthIndicator", HealthIndicator::class.java).health()
        val participants = health.details["consumers"] as Map<*, *>
        for (name in consumers) {
            assertEquals(
                mapOf("context" to "demo", "phase" to "REGISTERED", "mainReady" to false),
                (participants[name] as Map<*, *>)["warmup"]
            )
        }
        assertEquals(CkcConsumerProperties.MetricsImplementation.CUSTOM, properties.metrics.implementation)
        assertEquals(consumers, meterRegistry.find("demo.ckc.warmup.pending").gauges().map {
            assertNotNull(it.id.getTag("consumer_id"))
        }.toSet())
    }

    @Test
    fun `spring warmup events reach the shared log and recipient metrics handlers`() {
        val logger = LoggerFactory.getLogger("avh.ckc.demo.consumer.DemoWarmupObservations") as Logger
        val appender = ListAppender<ILoggingEvent>().also { it.start() }
        logger.addAppender(appender)
        try {
            applicationContext.publishEvent(CkcWarmupLifecycleEvent(
                "demo",
                TrafficWarmupLifecycleEvent(
                    TrafficWarmupLifecyclePhase.RECIPIENT_FINISHED,
                    sessionId = "demo-test",
                    durationMillis = 1250L,
                    processedRecords = 7L
                )
            ))
            applicationContext.publishEvent(CkcWarmupForwardedEvent(
                "demo",
                "order-events",
                TrafficWarmupForwardedEvent("demo-test", DemoTopics.ORDER_EVENTS, 1, 5L, DemoTopics.WARMUP_ORDER_EVENTS, 0, 8L)
            ))

            assertEquals(listOf(
                "Warm-up lifecycle: phase=RECIPIENT_FINISHED, participantId=null, sessionId=demo-test, " +
                    "reasonCode=null, recoveryDurationMillis=null",
                "Warm-up record forwarded: sessionId=demo-test, source=${DemoTopics.ORDER_EVENTS}-1@5, " +
                    "destination=${DemoTopics.WARMUP_ORDER_EVENTS}-0@8"
            ), appender.list.map(ILoggingEvent::getFormattedMessage))
            assertEquals(7.0, meterRegistry.get("demo.warmup.recipient.records")
                .tags("outcome", "success", "attempt", "demo-test").gauge().value())
            assertEquals(1.25, meterRegistry.get("demo.warmup.recipient.duration.seconds")
                .tags("outcome", "success", "attempt", "demo-test").gauge().value())
        } finally {
            logger.detachAppender(appender)
            appender.stop()
        }
    }

    @Test
    fun `custom audit binding records remote source success once and preserves recipient discard logging`() {
        @Suppress("UNCHECKED_CAST")
        val metrics = applicationContext.getBean("orderConsumerMetrics") as ConsumerMetrics<String, OrderLifecycleEvent>
        val value = OrderLifecycleEvent.getDefaultInstance()
        val source = ConsumerRecord(DemoTopics.ORDER_EVENTS, 1, 5L, "order", value)
        val forwarded = ConsumerRecord(DemoTopics.WARMUP_ORDER_EVENTS, 0, 8L, "order", value)
        val logger = LoggerFactory.getLogger("AUDIT_TCP") as Logger
        val appender = ListAppender<ILoggingEvent>().also { it.start() }
        logger.addAppender(appender)
        try {
            metrics.onRecordProcessed("order", value, source, 5, 5000, processedRemotely = false)
            metrics.onRecordProcessed("order", value, source, 5, 5000, processedRemotely = true)
            metrics.runtimeBinding("demo-audit-test").onRecordDropped(forwarded, RecordDropReason.STALE_AGE)

            assertEquals(listOf("C|1|1|5", "D|101|0|8"), appender.list.map {
                it.formattedMessage.split('|').take(4).joinToString("|")
            })
        } finally {
            logger.detachAppender(appender)
            appender.stop()
        }
    }

    private fun assertMapping(
        name: String,
        source: String,
        destination: String,
        serializer: Class<*>,
        quota: String,
        intervalSeconds: Long
    ) {
        val consumer = properties.consumers.getValue(name)
        val warmup = assertNotNull(consumer.warmup)
        assertEquals(listOf(source), consumer.topics)
        assertEquals("demo", warmup.context)
        assertEquals("warmup-destination", warmup.destinationCluster)
        assertEquals(StringSerializer::class.java.name, warmup.keySerializer)
        assertEquals(serializer.name, warmup.valueSerializer)
        assertEquals(mapOf("acks" to "all", "linger.ms" to "8", "batch.size" to "32768"), warmup.producerProperties)
        val mapping = warmup.mappings.single()
        assertEquals(source, mapping.sourceTopic)
        assertEquals(destination, mapping.warmupTopic)
        assertEquals(quota, mapping.quotaProfile)
        assertEquals(Duration.ofSeconds(intervalSeconds), mapping.quotaInterval)
    }
}
