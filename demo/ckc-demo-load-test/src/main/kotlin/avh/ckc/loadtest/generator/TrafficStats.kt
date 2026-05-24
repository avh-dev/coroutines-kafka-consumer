package avh.ckc.loadtest.generator

import avh.ckc.loadtest.domain.SimulationSnapshot
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

class TrafficStats {
    private val counters = ConcurrentHashMap<String, GeneratorCounters>()

    fun record(generatorName: String, real: Boolean) {
        val counter = counters.computeIfAbsent(generatorName) { GeneratorCounters() }
        counter.total.incrementAndGet()
        if (real) {
            counter.real.incrementAndGet()
        } else {
            counter.fake.incrementAndGet()
        }
    }

    fun format(snapshot: SimulationSnapshot): String {
        val generatorStats = counters.entries
            .sortedBy { it.key }
            .joinToString(" ") { (name, counter) ->
                "$name(total=${counter.total.get()},real=${counter.real.get()},fake=${counter.fake.get()})"
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
        val real = AtomicLong()
        val fake = AtomicLong()
    }
}
