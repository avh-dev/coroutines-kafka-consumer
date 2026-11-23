package avh.ckc.warmup.broker

import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import org.apache.kafka.common.errors.TimeoutException

internal enum class BrokerDrainStatus { DRAINED, TIMED_OUT, CONTROL_UNAVAILABLE }

internal data class BrokerDrainResult(
    val status: BrokerDrainStatus,
    val epoch: String,
    val drain: BrokerLifecycleJournal.Drain?,
    val detail: String? = null
)

internal class BrokerDrainCoordinator(
    private val log: KafkaBrokerLifecycleLog,
    private val epoch: String,
    private val observe: (BrokerLifecycleRecord) -> Unit = {}
) {
    /**
     * The caller supplies the epoch of the broker process it owns. A restarted CLI resumes the
     * first request in the journal even if its own append was not acknowledged.
     */
    fun drain(budgetMillis: Long): BrokerDrainResult {
        val budget = BrokerBudget(budgetMillis)
        val journal = BrokerLifecycleJournal()
        var boundary: BrokerLifecycleJournal.Drain? = null
        fun result(status: BrokerDrainStatus, detail: String? = null) =
            BrokerDrainResult(status, epoch, boundary, detail)

        return try {
            val topicId = log.topicId(budget.timeout())
            log.seek(log.beginning(budget.timeout()))
            val snapshotEnd = log.end(budget.timeout())
            replayThrough(snapshotEnd, budget, journal)
            check(journal.epoch == epoch) { "Current broker epoch cannot be recovered from the lifecycle journal" }
            boundary = journal.drain
            if (boundary == null) {
                val submitted = log.submit(
                    BrokerLifecycleEvent.DrainRequested(epoch, UUID.randomUUID().toString(), budget.deadlineMillis)
                ).get(budget.remaining(), TimeUnit.MILLISECONDS)
                replayThrough(submitted + 1, budget, journal)
                check(journal.epoch == epoch) { "Lifecycle journal changed while requesting drain" }
                boundary = checkNotNull(journal.drain) { "Drain boundary could not be read from the lifecycle journal" }
            }
            val active = checkNotNull(boundary)
            check(log.topicId(budget.timeout()) == topicId) { "Lifecycle topic was recreated" }
            budget.endNoLaterThan(active.request.deadlineMillis)
            var lastLive = System.nanoTime()
            while (active.missing.isNotEmpty() && budget.remaining() > 0) {
                val remaining = budget.remaining()
                if (remaining > 100) {
                    try {
                        val end = log.end(budget.timeout(250))
                        check((journal.nextOffset ?: 0) <= end) { "Lifecycle journal was truncated" }
                        lastLive = System.nanoTime()
                    } catch (_: TimeoutException) {
                        // Retry temporary loss within the same shutdown budget.
                    }
                }
                val pollMillis = minOf(100, budget.remaining())
                if (pollMillis == 0L) break
                log.poll(Duration.ofMillis(pollMillis)).forEach { applyRecord(journal, it) }
                check(journal.drain === active) { "Lifecycle history changed after the drain boundary" }
            }
            result(if (active.missing.isEmpty()) {
                BrokerDrainStatus.DRAINED
            } else if (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastLive) > 500) {
                BrokerDrainStatus.CONTROL_UNAVAILABLE
            } else {
                BrokerDrainStatus.TIMED_OUT
            })
        } catch (error: Exception) {
            result(BrokerDrainStatus.CONTROL_UNAVAILABLE, error.message ?: error.javaClass.simpleName)
        }
    }

    private fun replayThrough(end: Long, budget: BrokerBudget, journal: BrokerLifecycleJournal) {
        while (log.position(budget.timeout()) < end) {
            log.poll(budget.timeout(100)).forEach { applyRecord(journal, it) }
        }
        if (journal.epoch != null) {
            check((journal.nextOffset ?: 0) >= end) { "Lifecycle journal has an unreadable tail" }
        }
    }

    private fun applyRecord(journal: BrokerLifecycleJournal, record: BrokerLifecycleRecord) {
        val previous = journal.drain
        val accepted = previous?.accepted?.size
        val drained = previous?.drained?.size
        journal.accept(record)
        val boundary = journal.drain ?: return
        if (boundary.request.epoch != epoch) return
        if (boundary !== previous || boundary.accepted.size != accepted || boundary.drained.size != drained) {
            observe(record)
        }
    }
}

/** One process-local budget, additionally shortened by the canonical request's wall-clock deadline. */
internal class BrokerBudget(millis: Long) {
    init {
        require(millis in 1..Int.MAX_VALUE.toLong()) { "Invalid broker lifecycle budget" }
    }

    private val startedNanos = System.nanoTime()
    private var limitMillis = millis
    val deadlineMillis: Long = System.currentTimeMillis() + millis

    fun endNoLaterThan(deadline: Long) {
        val canonicalRemaining = (deadline - System.currentTimeMillis()).coerceAtLeast(0)
        limitMillis = minOf(limitMillis, elapsedMillis() + canonicalRemaining)
    }

    fun remaining(): Long = (limitMillis - elapsedMillis()).coerceAtLeast(0)

    fun timeout(maxMillis: Long = remaining()): Duration =
        Duration.ofMillis(minOf(remaining(), maxMillis).also { check(it > 0) { "Lifecycle budget expired" } })

    private fun elapsedMillis(): Long = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos)
}
