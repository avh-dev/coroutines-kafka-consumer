package avh.ckc.warmup

import avh.ckc.warmup.broker.BrokerLifecycleEvent
import avh.ckc.warmup.broker.BrokerLifecycleJournal
import avh.ckc.warmup.broker.BrokerLifecycleRecord
import avh.ckc.warmup.broker.KafkaBrokerLifecycleLog
import java.time.Duration as JavaDuration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory

/** Follows the broker journal and coordinates admission and drain for this context. */
internal class WarmupBrokerLifecycle(
    private val configuration: TrafficWarmupBrokerLifecycleConfiguration,
    private val scope: CoroutineScope,
    private val blocking: CoroutineDispatcher,
    private val fence: (String, Long?) -> Deferred<Set<String>>,
    private val reopen: () -> Boolean,
    private val submit: (KafkaBrokerLifecycleLog, BrokerLifecycleEvent) -> CompletableFuture<Long> =
        KafkaBrokerLifecycleLog::submit
) {
    private val logger = LoggerFactory.getLogger(javaClass)
    // Distinct from the user-configurable v6 routing identity.
    private val instance = UUID.randomUUID().toString()
    private val initialAdmission = CompletableDeferred<Boolean>()
    private val leaving = CompletableDeferred<Unit>()
    private var admissionOpen = false
    private lateinit var watcher: Job

    fun start() {
        watcher = scope.launch {
            while (isActive) {
                val log = KafkaBrokerLifecycleLog(
                    configuration.topic, configuration.kafkaProperties, configuration.operationTimeout.inWholeMilliseconds
                )
                try {
                    follow(log)
                    return@launch
                } catch (error: CancellationException) {
                    throw error
                } catch (error: Exception) {
                    fenceUnavailable()
                    logger.warn("Warm-up broker lifecycle unavailable for instance {}", instance, error)
                    delay(250.milliseconds)
                } finally {
                    withContext(NonCancellable + blocking) { log.close() }
                }
            }
        }
    }

    suspend fun awaitInitialAdmission(timeout: Duration): Boolean =
        withTimeoutOrNull(timeout) { initialAdmission.await() } ?: false

    suspend fun leave(timeout: Duration) {
        if (!::watcher.isInitialized) return
        leaving.complete(Unit)
        val finished = withTimeoutOrNull(timeout) {
            watcher.join()
            !watcher.isCancelled
        }
        if (finished != true) {
            logger.warn("Warm-up lifecycle exit could not be confirmed for instance {}", instance)
            watcher.cancelAndJoin()
        }
    }

    private fun fenceUnavailable() {
        if (admissionOpen) {
            admissionOpen = false
            fence("control_unavailable", null)
        }
    }

    private suspend fun follow(log: KafkaBrokerLifecycleLog) = coroutineScope {
        val timeout = JavaDuration.ofMillis(configuration.operationTimeout.inWholeMilliseconds)
        val metadata = runInterruptible(blocking) { log.metadata(timeout) }
        val floor = runInterruptible(blocking) { log.beginning(timeout).also(log::seek) }
        val replayEnd = runInterruptible(blocking) { log.end(timeout) }
        BrokerConnection(log, timeout, metadata, floor, replayEnd, this).follow()
    }

    /**
     * Holds the journal cursor and observed epoch for one watcher connection. Drain replies use
     * independent connections, but their jobs belong to the scope of this watcher connection.
     */
    private inner class BrokerConnection(
        private val log: KafkaBrokerLifecycleLog,
        private val timeout: JavaDuration,
        private val metadata: KafkaBrokerLifecycleLog.Metadata,
        private val floor: Long,
        private var replayEnd: Long,
        private val connectionScope: CoroutineScope
    ) {
        private var verified = TimeSource.Monotonic.markNow()
        private val journal = BrokerLifecycleJournal()
        private var observedEpoch: String? = null
        private var registration: BrokerLifecycleEvent.Joined? = null
        private var registrationOffset: Long? = null
        private var confirmed = false
        private var registeredAt = TimeSource.Monotonic.markNow()
        private var replyTo: BrokerLifecycleJournal.Drain? = null
        private var response: Job? = null
        private var acceptance: Job? = null
        private var leaveOffset: Long? = null

        suspend fun follow() {
            while (connectionScope.isActive) {
                val records = runInterruptible(blocking) { log.poll(JavaDuration.ofMillis(150)) }
                readRecords(records)
                val nextOffset = journal.nextOffset ?: floor
                verifyJournal(nextOffset)
                if (nextOffset < replayEnd) continue

                val epoch = journal.epoch
                observeEpoch(epoch)
                val drain = journal.drain
                if (drain != null) {
                    handleDrain(drain)
                } else if (epoch != null && !leaving.isCompleted) {
                    if (registerOrReopen(epoch)) continue
                }
                if (leaving.isCompleted && handleExit(epoch, nextOffset)) return
            }
        }

        private fun readRecords(records: List<BrokerLifecycleRecord>) {
            for (record in records) {
                journal.accept(record)
                val joined = registration ?: continue
                val event = record.event
                if (event is BrokerLifecycleEvent.JoinConfirmed && event.epoch == joined.epoch &&
                    event.context == instance && event.registration == joined.registration &&
                    record.offset > (registrationOffset ?: Long.MAX_VALUE)
                ) confirmed = true
            }
        }

        private suspend fun verifyJournal(nextOffset: Long) {
            if (verified.elapsedNow() < 1.seconds) return
            runInterruptible(blocking) {
                check(log.topicId(timeout) == metadata.topicId) { "Warm-up lifecycle topic was recreated" }
                replayEnd = log.end(timeout)
                check(nextOffset <= replayEnd) { "Warm-up lifecycle journal was truncated" }
                check(log.position(timeout) <= nextOffset) { "Warm-up lifecycle journal has an unreadable tail" }
            }
            verified = TimeSource.Monotonic.markNow()
        }

        private fun observeEpoch(epoch: String?) {
            if (epoch == observedEpoch) return
            if (observedEpoch != null) fenceUnavailable()
            observedEpoch = epoch
            registration = null
            registrationOffset = null
            confirmed = false
            replyTo = null
            response?.cancel()
            acceptance?.cancel()
            leaveOffset = null
        }

        /** Returns true when the loop should poll for confirmation of a submitted registration. */
        private suspend fun registerOrReopen(epoch: String): Boolean {
            if (confirmed) {
                if (!admissionOpen && reopen()) {
                    admissionOpen = true
                    initialAdmission.complete(true)
                }
                return false
            }
            val previous = registration
            if (previous != null && registeredAt.elapsedNow() < 2.seconds) return false
            val joined = previous ?: BrokerLifecycleEvent.Joined(epoch, instance, UUID.randomUUID().toString())
            registration = joined
            // Keep the original offset when resending the same registration nonce.
            val appended = publish(log, joined)
            if (registrationOffset == null) registrationOffset = appended
            registeredAt = TimeSource.Monotonic.markNow()
            return true
        }

        private fun handleDrain(drain: BrokerLifecycleJournal.Drain) {
            if (drain === replyTo) return
            admissionOpen = false
            val cleanup = fence(BROKER_DRAINING, drain.request.deadlineMillis)
            initialAdmission.complete(false)
            replyTo = drain
            if (instance !in drain.expected) return

            val request = drain.request
            val acceptedSubmitted = CompletableDeferred<Unit>()
            val acknowledgementConnection = configuration.kafkaProperties +
                ("bootstrap.servers" to metadata.advertisedBroker)
            val acceptanceJob = connectionScope.launch {
                withReportLog(acknowledgementConnection) { sender ->
                    try {
                        val accepted = BrokerLifecycleEvent.DrainAccepted(request.epoch, request.drainId, instance)
                        val confirmation = runInterruptible(blocking) { submit(sender, accepted) }
                        acceptedSubmitted.complete(Unit)
                        confirmation.await()
                    } catch (error: CancellationException) {
                        throw error
                    } catch (error: Exception) {
                        logger.warn("Could not publish warm-up broker drain acceptance", error)
                    } finally {
                        acceptedSubmitted.complete(Unit)
                    }
                }
            }
            acceptance = acceptanceJob
            response = connectionScope.launch {
                val sessions = try {
                    cleanup.await()
                } catch (error: CancellationException) {
                    throw error
                } catch (error: Exception) {
                    logger.warn("Warm-up broker drain cleanup incomplete for instance {}", instance, error)
                    return@launch
                }
                acceptedSubmitted.await()
                // The final confirmation has its own connection to this broker.
                withTimeoutOrNull(250.milliseconds) { acceptanceJob.join() }
                acceptanceJob.cancelAndJoin()
                withReportLog(acknowledgementConnection) { sender ->
                    report(sender, request, sessions, metadata.advertisedBroker)
                }
            }
        }

        private suspend fun handleExit(epoch: String?, nextOffset: Long): Boolean {
            checkNotNull(epoch) { "Warm-up lifecycle epoch unavailable during context exit" }
            val exitingAt = leaveOffset
            if (exitingAt == null) {
                leaveOffset = publish(log, BrokerLifecycleEvent.Left(epoch, instance))
                return false
            }
            if (nextOffset <= exitingAt) return false
            response?.join()
            acceptance?.cancel()
            return true
        }
    }

    private suspend fun publish(log: KafkaBrokerLifecycleLog, event: BrokerLifecycleEvent): Long =
        runInterruptible(blocking) { submit(log, event) }.await()

    private suspend fun report(
        log: KafkaBrokerLifecycleLog, request: BrokerLifecycleEvent.DrainRequested, sessions: Set<String>, broker: String
    ) {
        val event = BrokerLifecycleEvent.ContextDrained(request.epoch, request.drainId, instance, sessions)
        logger.info(
            "Warm-up broker lifecycle opening final confirmation connection brokerEpoch={} drainId={} context={} advertisedBroker={}",
            request.epoch, request.drainId, instance, broker
        )
        while (System.currentTimeMillis() < request.deadlineMillis) {
            try {
                val remaining = request.deadlineMillis - System.currentTimeMillis()
                val offset = withTimeoutOrNull(remaining) { publish(log, event) } ?: return
                logger.info(
                    "Warm-up broker lifecycle ContextDrained brokerEpoch={} drainId={} context={} advertisedBroker={} offset={}",
                    request.epoch, request.drainId, instance, broker, offset
                )
                return
            } catch (error: CancellationException) {
                throw error
            } catch (error: Exception) {
                logger.warn("Could not publish warm-up broker lifecycle event {}", event, error)
                delay(100.milliseconds)
            }
        }
    }

    private suspend fun withReportLog(
        connection: Map<String, Any?>,
        block: suspend (KafkaBrokerLifecycleLog) -> Unit
    ) {
        val sender = KafkaBrokerLifecycleLog(
            configuration.topic, connection, configuration.operationTimeout.inWholeMilliseconds
        )
        try {
            block(sender)
        } finally {
            withContext(NonCancellable + blocking) { sender.close() }
        }
    }
}
