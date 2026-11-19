package ckc.warmup.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** One-shot broker primer. It never writes to protocol topics. */
public final class PrewarmBroker {
    private static final String TOPIC_PREFIX = "__ckc_warmup_broker_prewarm_";

    private PrewarmBroker() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException("bootstrap server, client config, and evidence file are required");
        }
        Configuration configuration = Configuration.fromEnvironment();
        run(args[0], Path.of(args[1]), Path.of(args[2]), configuration);
    }

    private static void run(
        String bootstrapServer,
        Path clientConfig,
        Path evidenceFile,
        Configuration configuration
    ) throws Exception {
        Instant startedAt = Instant.now();
        long startedNanos = System.nanoTime();
        long processDeadlineNanos = saturatedAdd(
            startedNanos,
            Duration.ofSeconds(configuration.prewarmTimeoutSeconds).toNanos()
        );
        String topic = TOPIC_PREFIX + UUID.randomUUID().toString().replace("-", "");
        AtomicBoolean stopping = new AtomicBoolean();
        AtomicLong accepted = new AtomicLong();
        AtomicLong acknowledged = new AtomicLong();
        AtomicLong consumed = new AtomicLong();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch shutdownComplete = new CountDownLatch(1);
        Thread shutdownHook = new Thread(() -> {
            stopping.set(true);
            try {
                shutdownComplete.await(
                    configuration.operationTimeoutSeconds + configuration.deliveryTimeoutSeconds + 5L,
                    TimeUnit.SECONDS
                );
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }, "prewarm-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        Properties common = loadProperties(clientConfig);
        common.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        common.put(AdminClientConfig.CLIENT_ID_CONFIG, "ckc-warmup-broker-prewarm-admin");
        boolean topicCreated = false;
        String outcome = "failed";
        String reason = "unknown";

        Admin admin = Admin.create(common);
        try {
            deleteStaleTopics(admin, configuration.operationTimeoutSeconds);
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                .all().get(configuration.operationTimeoutSeconds, TimeUnit.SECONDS);
            topicCreated = true;

            Properties consumerProperties = new Properties();
            consumerProperties.putAll(common);
            consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "ckc-warmup-broker-prewarm-consumer");
            consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, topic);
            consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString().toLowerCase());
            consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

            Properties producerProperties = new Properties();
            producerProperties.putAll(common);
            producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "ckc-warmup-broker-prewarm-producer");
            producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
            producerProperties.put(ProducerConfig.LINGER_MS_CONFIG, "5");
            producerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, "32768");
            producerProperties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
            producerProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, Long.toString(configuration.maxBufferedBytes));
            producerProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.toString(configuration.operationTimeoutSeconds * 1000L));
            producerProperties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Long.toString(configuration.deliveryTimeoutSeconds * 1000L));

            KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties);
            try {
                KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProperties);
                boolean processingCompleted = false;
                try {
                    AtomicBoolean consumerAssigned = new AtomicBoolean();
                    Thread consumerThread = new Thread(() -> consume(
                        consumer, topic, stopping, consumerAssigned, consumed, failure
                    ), "prewarm-consumer");
                    consumerThread.start();
                    try {
                        waitFor(
                            () -> consumerAssigned.get(), failure, stopping,
                            configuration.operationTimeoutSeconds, "consumer assignment"
                        );

                        Semaphore inflight = new Semaphore(configuration.maxInflight);
                        byte[] payload = new byte[configuration.payloadBytes];
                        long profileStarted = System.nanoTime();
                        long lastSend = profileStarted;
                        while (!stopping.get()) {
                            long elapsedNanos = System.nanoTime() - profileStarted;
                            if (elapsedNanos >= configuration.durationNanos ||
                                accepted.get() >= configuration.recordBudget()) {
                                break;
                            }
                            Throwable deliveryFailure = failure.get();
                            if (deliveryFailure != null) {
                                throw new IllegalStateException("prewarm delivery failed", deliveryFailure);
                            }

                            double rate = configuration.rateAt(elapsedNanos);
                            long intervalNanos = Math.max(1L, (long) (1_000_000_000d / rate));
                            long now = System.nanoTime();
                            long waitNanos = lastSend + intervalNanos - now;
                            if (waitNanos > 0) {
                                LockSupport.parkNanos(waitNanos);
                            }
                            if (Thread.interrupted() || stopping.get()) {
                                break;
                            }
                            // Scheduling or producer backpressure never creates a catch-up burst.
                            lastSend = System.nanoTime();
                            if (!inflight.tryAcquire(configuration.operationTimeoutSeconds, TimeUnit.SECONDS)) {
                                throw new IllegalStateException("prewarm producer inflight budget remained full");
                            }
                            long sequence = accepted.incrementAndGet();
                            ByteBuffer.wrap(payload).putLong(0, sequence);
                            try {
                                producer.send(new ProducerRecord<>(topic, payload.clone()), (metadata, exception) -> {
                                    try {
                                        if (exception == null) {
                                            acknowledged.incrementAndGet();
                                        } else {
                                            failure.compareAndSet(null, exception);
                                        }
                                    } finally {
                                        inflight.release();
                                    }
                                });
                            } catch (Throwable sendFailure) {
                                accepted.decrementAndGet();
                                inflight.release();
                                throw sendFailure;
                            }
                        }

                        waitFor(
                            () -> acknowledged.get() == accepted.get(), failure, stopping,
                            configuration.deliveryTimeoutSeconds, "producer acknowledgements"
                        );
                        producer.flush();
                        waitFor(
                            () -> consumed.get() >= acknowledged.get(), failure, stopping,
                            configuration.deliveryTimeoutSeconds, "consumer drain"
                        );
                        processingCompleted = true;
                    } catch (Throwable processingFailure) {
                        writeEvidence(
                            evidenceFile,
                            "failed",
                            describe(processingFailure),
                            startedAt,
                            elapsedMillis(startedNanos),
                            accepted.get(), acknowledged.get(), consumed.get(), configuration.payloadBytes
                        );
                        throw processingFailure;
                    } finally {
                        stopping.set(true);
                        consumer.wakeup();
                        consumerThread.join(Math.max(1L, remaining(processDeadlineNanos).toMillis()));
                        if (consumerThread.isAlive()) {
                            throw new IllegalStateException("prewarm consumer did not stop");
                        }
                    }
                } finally {
                    if (processingCompleted) {
                        writeEvidence(
                            evidenceFile,
                            "pending_cleanup",
                            "client_cleanup_started",
                            startedAt,
                            elapsedMillis(startedNanos),
                            accepted.get(), acknowledged.get(), consumed.get(), configuration.payloadBytes
                        );
                    }
                    producer.close(remaining(processDeadlineNanos));
                }
            } finally {
                consumer.close(remaining(processDeadlineNanos));
            }

            deleteTopic(admin, topic, configuration.operationTimeoutSeconds);
            topicCreated = false;
            outcome = "success";
            reason = "completed";
        } catch (Throwable thrown) {
            reason = describe(thrown);
            throw thrown;
        } finally {
            try {
                admin.close(remaining(processDeadlineNanos));
            } catch (Throwable closeFailure) {
                outcome = "failed";
                reason += "; admin_close=" + describe(closeFailure);
            }
            stopping.set(true);
            if (topicCreated) {
                Admin cleanup = null;
                try {
                    cleanup = Admin.create(common);
                    deleteTopic(cleanup, topic, configuration.operationTimeoutSeconds);
                } catch (Throwable cleanupFailure) {
                    reason += "; cleanup=" + cleanupFailure.getClass().getSimpleName();
                } finally {
                    if (cleanup != null) {
                        try {
                            cleanup.close(remaining(processDeadlineNanos));
                        } catch (Throwable closeFailure) {
                            reason += "; cleanup_close=" + closeFailure.getClass().getSimpleName();
                        }
                    }
                }
            }
            writeEvidence(
                evidenceFile, outcome, reason, startedAt, elapsedMillis(startedNanos),
                accepted.get(), acknowledged.get(), consumed.get(), configuration.payloadBytes
            );
            shutdownComplete.countDown();
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException ignored) {
                // JVM shutdown is already in progress.
            }
        }
    }

    private static String describe(Throwable thrown) {
        return thrown.getClass().getSimpleName() + ": " + String.valueOf(thrown.getMessage());
    }

    private static long elapsedMillis(long startedNanos) {
        return Duration.ofNanos(System.nanoTime() - startedNanos).toMillis();
    }

    private static Duration remaining(long deadlineNanos) {
        return Duration.ofNanos(Math.max(0L, deadlineNanos - System.nanoTime()));
    }

    private static long saturatedAdd(long left, long right) {
        if (Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private static void consume(
        KafkaConsumer<byte[], byte[]> consumer,
        String topic,
        AtomicBoolean stopping,
        AtomicBoolean assigned,
        AtomicLong consumed,
        AtomicReference<Throwable> failure
    ) {
        try {
            TopicPartition partition = new TopicPartition(topic, 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            // Resolve the initial position before permitting the producer to run.
            consumer.position(partition);
            assigned.set(true);
            while (!stopping.get()) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
                consumed.addAndGet(records.count());
            }
        } catch (WakeupException expectedOnStop) {
            if (!stopping.get()) {
                failure.compareAndSet(null, expectedOnStop);
            }
        } catch (Throwable thrown) {
            failure.compareAndSet(null, thrown);
        }
    }

    private static void waitFor(
        Condition condition,
        AtomicReference<Throwable> failure,
        AtomicBoolean stopping,
        int timeoutSeconds,
        String description
    ) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(timeoutSeconds).toNanos();
        while (!condition.test()) {
            Throwable asynchronousFailure = failure.get();
            if (asynchronousFailure != null) {
                throw new IllegalStateException(description + " failed", asynchronousFailure);
            }
            if (stopping.get()) {
                throw new InterruptedException(description + " interrupted");
            }
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException(description + " timed out");
            }
            LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
        }
    }

    private static void deleteTopic(Admin admin, String topic, int timeoutSeconds) throws Exception {
        admin.deleteTopics(List.of(topic)).all().get(timeoutSeconds, TimeUnit.SECONDS);
        long deadline = System.nanoTime() + Duration.ofSeconds(timeoutSeconds).toNanos();
        while (admin.listTopics().names().get(timeoutSeconds, TimeUnit.SECONDS).contains(topic)) {
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("temporary prewarm topic deletion timed out");
            }
            LockSupport.parkNanos(Duration.ofMillis(100).toNanos());
        }
    }

    private static void deleteStaleTopics(Admin admin, int timeoutSeconds) throws Exception {
        List<String> staleTopics = admin.listTopics().names()
            .get(timeoutSeconds, TimeUnit.SECONDS)
            .stream()
            .filter(name -> name.startsWith(TOPIC_PREFIX))
            .toList();
        if (staleTopics.isEmpty()) {
            return;
        }
        admin.deleteTopics(staleTopics).all().get(timeoutSeconds, TimeUnit.SECONDS);
        long deadline = System.nanoTime() + Duration.ofSeconds(timeoutSeconds).toNanos();
        while (admin.listTopics().names().get(timeoutSeconds, TimeUnit.SECONDS)
            .stream().anyMatch(name -> name.startsWith(TOPIC_PREFIX))) {
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("stale temporary prewarm topic deletion timed out");
            }
            LockSupport.parkNanos(Duration.ofMillis(100).toNanos());
        }
    }

    private static Properties loadProperties(Path path) throws IOException {
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(path)) {
            properties.load(input);
        }
        return properties;
    }

    private static void writeEvidence(
        Path path,
        String outcome,
        String reason,
        Instant startedAt,
        long elapsedMillis,
        long accepted,
        long acknowledged,
        long consumed,
        int payloadBytes
    ) {
        String json = String.format(
            "{\"outcome\":\"%s\",\"reason\":\"%s\",\"started_at\":\"%s\",\"duration_ms\":%d," +
                "\"accepted_records\":%d,\"acknowledged_records\":%d,\"consumed_records\":%d," +
                "\"accepted_bytes\":%d,\"achieved_records_per_second\":%.3f}%n",
            escape(outcome), escape(reason), startedAt, elapsedMillis, accepted, acknowledged, consumed,
            accepted * payloadBytes, elapsedMillis == 0 ? 0d : accepted * 1000d / elapsedMillis
        );
        try {
            Files.writeString(path, json, StandardCharsets.UTF_8);
            System.out.print("warmup-kafka-prewarm evidence=" + json);
            System.out.flush();
        } catch (IOException writeFailure) {
            System.err.println("warmup-kafka-prewarm evidence_write_failed=" + writeFailure);
        }
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }

    @FunctionalInterface
    private interface Condition {
        boolean test();
    }

    private record Configuration(
        int firstPhaseSeconds,
        int secondPhaseSeconds,
        double startRate,
        double middleRate,
        double endRate,
        int payloadBytes,
        long maxRecords,
        long maxBytes,
        int maxInflight,
        long maxBufferedBytes,
        int operationTimeoutSeconds,
        int deliveryTimeoutSeconds,
        int prewarmTimeoutSeconds,
        long durationNanos
    ) {
        static Configuration fromEnvironment() {
            int first = positiveInt("KAFKA_PREWARM_FIRST_PHASE_SECONDS", 30);
            int second = positiveInt("KAFKA_PREWARM_SECOND_PHASE_SECONDS", 90);
            double start = positiveDouble("KAFKA_PREWARM_START_RATE", 1);
            double middle = positiveDouble("KAFKA_PREWARM_MIDDLE_RATE", 100);
            double end = positiveDouble("KAFKA_PREWARM_END_RATE", 2000);
            int payload = positiveInt("KAFKA_PREWARM_PAYLOAD_BYTES", 512);
            long maxRecords = positiveLong("KAFKA_PREWARM_MAX_RECORDS", 100_000);
            long maxBytes = positiveLong("KAFKA_PREWARM_MAX_BYTES", 64L * 1024 * 1024);
            int inflight = positiveInt("KAFKA_PREWARM_MAX_INFLIGHT", 256);
            long buffered = positiveLong("KAFKA_PREWARM_MAX_BUFFERED_BYTES", 16L * 1024 * 1024);
            int operationTimeout = positiveInt("KAFKA_OPERATION_TIMEOUT_SECONDS", 10);
            int deliveryTimeout = positiveInt("KAFKA_PREWARM_DELIVERY_TIMEOUT_SECONDS", 30);
            int prewarmTimeout = positiveInt("KAFKA_PREWARM_TIMEOUT_SECONDS", 180);
            if (payload < Long.BYTES) {
                throw new IllegalArgumentException(
                    "KAFKA_PREWARM_PAYLOAD_BYTES must be at least " + Long.BYTES + ": " + payload
                );
            }
            if (maxBytes < payload) {
                throw new IllegalArgumentException(
                    "KAFKA_PREWARM_MAX_BYTES must fit at least one payload: " + maxBytes
                );
            }
            return new Configuration(
                first, second, start, middle, end, payload, maxRecords, maxBytes, inflight,
                buffered, operationTimeout, deliveryTimeout, prewarmTimeout,
                Duration.ofSeconds(first + second).toNanos()
            );
        }

        long recordBudget() {
            return Math.min(maxRecords, maxBytes / payloadBytes);
        }

        double rateAt(long elapsedNanos) {
            double elapsedSeconds = elapsedNanos / 1_000_000_000d;
            if (elapsedSeconds <= firstPhaseSeconds) {
                return startRate + (middleRate - startRate) * elapsedSeconds / firstPhaseSeconds;
            }
            double secondElapsed = Math.min(secondPhaseSeconds, elapsedSeconds - firstPhaseSeconds);
            return middleRate + (endRate - middleRate) * secondElapsed / secondPhaseSeconds;
        }

        private static int positiveInt(String name, int defaultValue) {
            long value = positiveLong(name, defaultValue);
            if (value > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(name + " is too large: " + value);
            }
            return (int) value;
        }

        private static long positiveLong(String name, long defaultValue) {
            String raw = System.getenv().getOrDefault(name, Long.toString(defaultValue));
            long value = Long.parseLong(raw);
            if (value <= 0) {
                throw new IllegalArgumentException(name + " must be positive: " + raw);
            }
            return value;
        }

        private static double positiveDouble(String name, double defaultValue) {
            String raw = System.getenv().getOrDefault(name, Double.toString(defaultValue));
            double value = Double.parseDouble(raw);
            if (!Double.isFinite(value) || value <= 0) {
                throw new IllegalArgumentException(name + " must be positive: " + raw);
            }
            return value;
        }
    }
}
