package avh.ckc.core

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.launch
import java.util.Collections
import java.util.IdentityHashMap
import java.util.concurrent.atomic.AtomicInteger

/** Lifecycle phase in which one [ConsumerContainer] participant failed. */
enum class ConsumerContainerPhase {
    START,
    READINESS,
    STOP
}

/** Identifies one participant failure by its zero-based position in the fixed container composition. */
data class ConsumerContainerFailure(
    val participantIndex: Int,
    val phase: ConsumerContainerPhase,
    val error: Throwable
)

/** Reports every participant failure observed by one container operation. */
class ConsumerContainerException internal constructor(
    message: String,
    failures: List<ConsumerContainerFailure>
) : IllegalStateException(message, failures.firstOrNull()?.error) {
    val failures: List<ConsumerContainerFailure> = failures.toList()

    init {
        this.failures.drop(1).forEach { addSuppressed(it.error) }
    }
}

internal interface ConsumerContainerParticipant {
    fun start()
    suspend fun awaitReady()
    suspend fun stop()
    fun isStopComplete(): Boolean
}

/**
 * Starts and stops a fixed set of heterogeneous consumers as one application lifecycle unit.
 *
 * The container owns no processing or Kafka configuration. A stop attempt runs in a container-owned
 * scope, so cancellation of one waiting caller cannot abandon cleanup. Concurrent callers share the
 * same attempt; a later call retries only participants whose resources are still incomplete.
 *
 * [start] is single-use and still invokes every participant when an earlier participant reports an
 * ordinary failure. Fatal JVM errors stop the launch sequence immediately and are rethrown without
 * container wrapping. Calling [stop] before [start] permanently stops the container; restarting a
 * stopped or failed container is deliberately unsupported. Consumers retain their individual APIs.
 */
class ConsumerContainer internal constructor(
    participantLifecycles: List<ConsumerContainerParticipant>
) {
    private enum class State {
        NEW,
        STARTED,
        START_FAILED,
        STOPPING,
        STOPPED
    }

    private class ParticipantState(
        val index: Int,
        val lifecycle: ConsumerContainerParticipant
    ) {
        // Set only after the underlying lifecycle confirms that no participant resources remain owned.
        var stopComplete = false
    }

    private data class StopOutcome(
        val participant: ParticipantState,
        val error: Throwable?,
        val complete: Boolean
    )

    private val lock = Any()
    private val participants = participantLifecycles.mapIndexed(::ParticipantState)
    private val lifecycleJob = SupervisorJob()
    private val scope = CoroutineScope(
        lifecycleJob + Dispatchers.Default + CoroutineName("ConsumerContainer")
    )
    private val readiness = CompletableDeferred<Unit>()
    private val readyCount = AtomicInteger()
    private var readinessJobs: List<Job> = emptyList()
    private var state = State.NEW
    private var stopAttempt: Deferred<Unit>? = null

    init {
        require(participants.isNotEmpty()) { "Consumer container must not be empty" }
    }

    /** Starts every participant without waiting for readiness between calls. */
    fun start() {
        val failures = mutableListOf<ConsumerContainerFailure>()
        var fatalFailure: Throwable? = null
        synchronized(lock) {
            check(state == State.NEW) { "Consumer container cannot be started from state $state" }
            for (participant in participants) {
                try {
                    participant.lifecycle.start()
                } catch (error: Throwable) {
                    if (error.isFatalJvmError()) {
                        fatalFailure = error
                        break
                    }
                    failures += participant.failure(ConsumerContainerPhase.START, error)
                }
            }
            if (fatalFailure != null) {
                state = State.START_FAILED
                readiness.completeExceptionally(fatalFailure!!)
            } else if (failures.isEmpty()) {
                state = State.STARTED
                observeReadinessLocked()
            } else {
                state = State.START_FAILED
                readiness.completeExceptionally(
                    ConsumerContainerException("Consumer container failed to start", failures)
                )
            }
        }
        fatalFailure?.let { throw it }
        if (failures.isNotEmpty()) {
            throw ConsumerContainerException("Consumer container failed to start", failures)
        }
    }

    /**
     * Waits for collective readiness and fails as soon as any participant reports a readiness error.
     * Cancelling this wait does not stop consumers or cancel the shared readiness observation.
     */
    suspend fun awaitReady() {
        synchronized(lock) {
            check(state != State.NEW) { "Consumer container must be started before awaiting readiness" }
        }
        readiness.await()
    }

    /**
     * Stops all participants concurrently and reports all failures from this attempt.
     *
     * If some resources remain incomplete, the next invocation creates another attempt for only
     * those participants. Once every participant is complete, repeated calls observe the terminal
     * result of the final attempt without running cleanup again.
     */
    suspend fun stop() {
        val attempt: Deferred<Unit>
        var startAttempt = false
        synchronized(lock) {
            stopAttempt?.let {
                attempt = it
                return@synchronized
            }
            if (state == State.NEW) {
                readiness.completeExceptionally(
                    IllegalStateException("Consumer container stopped before start")
                )
            }
            state = State.STOPPING
            attempt = scope.async(start = CoroutineStart.LAZY) { runStopAttempt() }
            stopAttempt = attempt
            startAttempt = true
        }
        if (startAttempt) attempt.start()
        attempt.await()
    }

    private fun observeReadinessLocked() {
        readinessJobs = participants.map { participant ->
            scope.launch {
                try {
                    participant.lifecycle.awaitReady()
                    if (readyCount.incrementAndGet() == participants.size) readiness.complete(Unit)
                } catch (error: Throwable) {
                    if (error.isFatalJvmError()) {
                        readiness.completeExceptionally(error)
                    } else {
                        readiness.completeExceptionally(
                            ConsumerContainerException(
                                "Consumer container readiness failed",
                                listOf(participant.failure(ConsumerContainerPhase.READINESS, error))
                            )
                        )
                    }
                }
            }
        }
    }

    private suspend fun runStopAttempt() {
        val pending = synchronized(lock) { participants.filterNot(ParticipantState::stopComplete) }
        val outcomes = pending.map { participant ->
            scope.async {
                val error = try {
                    participant.lifecycle.stop()
                    null
                } catch (error: Throwable) {
                    error
                }
                StopOutcome(participant, error, participant.lifecycle.isStopComplete())
            }
        }.awaitAll()

        val failures = mutableListOf<ConsumerContainerFailure>()
        val allComplete = synchronized(lock) {
            outcomes.forEach { outcome ->
                if (outcome.complete) outcome.participant.stopComplete = true
                val error = outcome.error ?: if (!outcome.complete) {
                    IllegalStateException(
                        "Consumer container participant ${outcome.participant.index} cleanup is incomplete"
                    )
                } else {
                    null
                }
                if (error != null) {
                    failures += outcome.participant.failure(ConsumerContainerPhase.STOP, error)
                }
            }
            val complete = participants.all(ParticipantState::stopComplete)
            if (complete) {
                state = State.STOPPED
                readinessJobs.forEach(Job::cancel)
            } else {
                if (stopAttempt?.isActive == true) stopAttempt = null
                state = State.STOPPING
            }
            complete
        }
        if (allComplete) lifecycleJob.complete()

        outcomes.firstNotNullOfOrNull { it.error?.takeIf(Throwable::isFatalJvmError) }?.let { throw it }
        if (failures.isNotEmpty()) {
            throw ConsumerContainerException(
                if (allComplete) "Consumer container stopped with participant failures"
                else "Consumer container cleanup is incomplete",
                failures
            )
        }
    }

    private fun ParticipantState.failure(
        phase: ConsumerContainerPhase,
        error: Throwable
    ): ConsumerContainerFailure = ConsumerContainerFailure(index, phase, error)
}

/** Creates a fixed lifecycle container without changing ownership of the supplied consumers. */
fun consumerContainer(
    vararg consumers: CoroutinesKafkaConsumer<*, *>
): ConsumerContainer {
    require(consumers.isNotEmpty()) { "Consumer container must not be empty" }
    val identities = Collections.newSetFromMap(
        IdentityHashMap<CoroutinesKafkaConsumer<*, *>, Boolean>()
    )
    consumers.forEach { consumer ->
        require(identities.add(consumer)) { "The same consumer instance cannot be included more than once" }
    }
    return ConsumerContainer(
        consumers.map { consumer ->
            object : ConsumerContainerParticipant {
                override fun start() = consumer.start()
                override suspend fun awaitReady() = consumer.awaitReady()
                override suspend fun stop() = consumer.stop()
                override fun isStopComplete(): Boolean = consumer.isLifecycleStopComplete()
            }
        }
    )
}
