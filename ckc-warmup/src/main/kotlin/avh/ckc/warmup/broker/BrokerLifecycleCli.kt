package avh.ckc.warmup.broker

import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.system.exitProcess

/**
 * Standalone JVM entry point for a temporary warm-up broker supervisor. Uses the dedicated Kafka
 * journal to advertise a broker epoch and request coordinated drain.
 */
object BrokerLifecycleCli {
    /**
     * Accepts `serve <bootstrap> <topic> <epoch> <budget-ms> <ready-file>` or
     * `drain <bootstrap> <topic> <epoch> <budget-ms>`. Serve writes the readiness file
     * after publishing the epoch. Drain exits with 0 on confirmation, 1 on timeout, or 2
     * when the journal is unavailable. Invalid arguments also exit with 2.
     */
    @JvmStatic
    fun main(args: Array<String>) {
        if (args.size !in 5..6 || args[0] !in setOf("serve", "drain") ||
            (args[0] == "serve") != (args.size == 6)
        ) {
            System.err.println("Usage: BrokerLifecycleCli serve|drain <bootstrap> <topic> <epoch> <budget-ms> [ready-file]")
            exitProcess(2)
        }
        val (command, bootstrap, topic, epoch, budgetValue) = args
        val budgetMillis = budgetValue.toLongOrNull()?.takeIf { it in 1..Int.MAX_VALUE.toLong() }
        if (budgetMillis == null || epoch.isBlank()) {
            System.err.println("Invalid broker lifecycle epoch or budget")
            exitProcess(2)
        }
        val connection = mapOf("bootstrap.servers" to bootstrap)
        val code = KafkaBrokerLifecycleLog(topic, connection, minOf(3_000, budgetMillis)).use { log ->
            if (command == "serve") {
                try {
                    serve(log, epoch, budgetMillis) {
                        Files.writeString(Path.of(args[5]), epoch)
                    }
                    0
                } catch (error: Exception) {
                    System.err.println("broker-lifecycle event=epoch-unavailable brokerEpoch=$epoch error=${error.message}")
                    2
                }
            } else {
                val result = BrokerDrainCoordinator(log, epoch) { record ->
                    println("broker-lifecycle offset=${record.offset} timestamp=${record.timestamp} event=${record.event}")
                }.drain(budgetMillis)
                val drain = result.drain
                println(
                    "broker-lifecycle status=${result.status} brokerEpoch=$epoch drainId=${drain?.request?.drainId} " +
                        "boundaryOffset=${drain?.offset} deadlineMillis=${drain?.request?.deadlineMillis} " +
                        "expected=${drain?.expected?.sorted()} accepted=${drain?.accepted?.sorted()} " +
                        "missing=${drain?.missing?.sorted()} detail=${result.detail}"
                )
                when (result.status) {
                    BrokerDrainStatus.DRAINED -> 0
                    BrokerDrainStatus.TIMED_OUT -> 1
                    BrokerDrainStatus.CONTROL_UNAVAILABLE -> 2
                }
            }
        }
        exitProcess(code)
    }

    /**
     * The supervisor owns this process alongside Kafka. A confirmation is proof that a fresh
     * registration nonce was seen by the owner of the current broker process.
     */
    internal fun serve(
        log: KafkaBrokerLifecycleLog,
        epoch: String,
        startupMillis: Long,
        ready: () -> Unit
    ) {
        val budget = BrokerBudget(startupMillis)
        log.topicId(budget.timeout())
        val start = log.submit(BrokerLifecycleEvent.Started(epoch)).get(budget.remaining(), TimeUnit.MILLISECONDS)
        log.seek(start)
        val journal = BrokerLifecycleJournal()
        println("broker-lifecycle event=BrokerStarted brokerEpoch=$epoch offset=$start")
        ready()
        while (!Thread.currentThread().isInterrupted) {
            log.poll(Duration.ofMillis(200)).forEach { record ->
                journal.accept(record)
                check(journal.epoch == epoch) { "Broker lifecycle epoch changed" }
                val event = record.event
                if (event is BrokerLifecycleEvent.Joined && event.epoch == epoch && journal.drain == null) {
                    log.submit(BrokerLifecycleEvent.JoinConfirmed(epoch, event.context, event.registration))
                }
            }
        }
    }
}
