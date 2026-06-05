package avh.ckc.loadtest.generator

import avh.ckc.loadtest.domain.SimulationSnapshot
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

class TrafficStats {
    private val counters = ConcurrentHashMap<String, GeneratorCounters>()

    fun record(generatorName: String, result: EmitResult) {
        val counter = counters.computeIfAbsent(generatorName) { GeneratorCounters() }
        counter.total.incrementAndGet()
        if (result.emitted) {
            counter.emitted.addAndGet(result.emittedCount.toLong())
        }
        if (result.blocked) {
            counter.blocked.incrementAndGet()
        }
        if (result.delegated > 0) {
            counter.delegated.addAndGet(result.delegated.toLong())
        }
    }

    fun format(snapshot: SimulationSnapshot): String {
        val generatorStats = counters.entries
            .sortedBy { it.key }
            .joinToString(" ") { (name, counter) ->
                "$name(total=${counter.total.get()},emitted=${counter.emitted.get()}," +
                    "delegated=${counter.delegated.get()},blocked=${counter.blocked.get()})"
            }
        return "stats $generatorStats queues(" +
            "orders.created=${snapshot.createdOrders},orders.assigned=${snapshot.assignedOrders}," +
            "orders.waiting=${snapshot.waitingForBottlingOrders},orders.completable=${snapshot.readyForCompletionOrders}," +
            "batches.created=${snapshot.createdBatches},batches.preparing=${snapshot.preparingBatches}," +
            "batches.prepared=${snapshot.preparedBatches},batches.waiting_cauldron=${snapshot.waitingForCauldronBatches}," +
            "batches.cauldron_assigned=${snapshot.cauldronAssignedBatches},batches.brewing=${snapshot.brewingBatches}," +
            "batches.brewing_completed=${snapshot.brewingCompletedBatches},batches.waiting_bottling=${snapshot.waitingForBottlingBatches}," +
            "batches.bottling=${snapshot.bottlingBatches},cauldrons.active=${snapshot.activeCauldrons})"
    }

    private class GeneratorCounters {
        val total = AtomicLong()
        val emitted = AtomicLong()
        val delegated = AtomicLong()
        val blocked = AtomicLong()
    }
}
