package avh.ckc.core.warmup

import org.apache.kafka.common.TopicPartition
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class WarmupControlPlaneTest {
    private val newcomer = WarmupInstanceId("new")
    private val oldActive = WarmupInstanceId("old-active")
    private val newActive = WarmupInstanceId("new-active")
    private val sync = WarmupSessionId("sync")
    private val position = WarmupControlLogPosition(TopicPartition("control", 0), 0, 10)

    @Test
    fun `snapshot from old active is corrected by election buffered after its boundary`() {
        val replica = WarmupControlPlaneReplica(newcomer, position, sync)
        replica.apply(WarmupControlRecord(11, InstanceReady(WarmupSessionId("ready"), newActive)))
        replica.apply(WarmupControlRecord(12, CoordinatorClaim(
            WarmupSessionId("claim"), newActive, 4, oldActive, 9
        )))

        val installed = replica.apply(WarmupControlRecord(13, StateSnapshot(
            sync, oldActive, newcomer, 4, oldActive, 10, 9, setOf(oldActive, newActive), emptyList()
        )))

        assertTrue(installed.synchronizedNow)
        assertEquals(5, replica.view().generation)
        assertEquals(newActive, replica.view().activeCoordinator)
        assertEquals(13, replica.view().lastAppliedOffset)
    }

    @Test
    fun `snapshot range validation allows Kafka offset gaps but rejects an uncovered range`() {
        val replica = WarmupControlPlaneReplica(newcomer, position, sync)
        replica.apply(WarmupControlRecord(15, InstanceReady(WarmupSessionId("ready"), newActive)))
        replica.apply(WarmupControlRecord(20, StateSnapshot(
            sync, oldActive, newcomer, 1, oldActive, 10, 9, setOf(oldActive), emptyList()
        )))
        assertEquals(20, replica.view().lastAppliedOffset)

        val uncovered = WarmupControlPlaneReplica(newcomer, position.copy(endOffset = 20), sync)
        assertFailsWith<IllegalStateException> {
            uncovered.apply(WarmupControlRecord(25, StateSnapshot(
                sync, oldActive, newcomer, 1, oldActive, 10, 9, setOf(oldActive), emptyList()
            )))
        }
    }

    @Test
    fun `assignment exists only after journal application and duplicate decision is idempotent`() {
        val replica = activeReplica()
        val assignment = assignment("session")
        val decision = AssignmentProposed(assignment.sessionId, oldActive, 1, assignment)

        assertNull(replica.view().sessions[assignment.sessionId])
        assertTrue(replica.apply(WarmupControlRecord(2, decision)).accepted)
        assertTrue(replica.apply(WarmupControlRecord(3, decision)).accepted)
        assertEquals(assignment, replica.view().sessions.getValue(assignment.sessionId).assignment)
    }

    @Test
    fun `only first assignment owns partition and completion itself releases it`() {
        val replica = activeReplica()
        val first = assignment("one")
        val second = assignment("two")
        assertTrue(replica.apply(WarmupControlRecord(2, AssignmentProposed(first.sessionId, oldActive, 1, first))).accepted)
        assertFalse(replica.apply(WarmupControlRecord(3, AssignmentProposed(second.sessionId, oldActive, 1, second))).accepted)

        assertTrue(replica.apply(WarmupControlRecord(4, SessionCompleted(
            first.sessionId, first.recipientInstanceId, setOf(WarmupConsumerId("orders"))
        ))).accepted)
        assertTrue(replica.view().sessions.isEmpty())
        assertTrue(replica.apply(WarmupControlRecord(5, AssignmentProposed(second.sessionId, oldActive, 1, second))).accepted)
    }

    @Test
    fun `leader change fences new commands but keeps participant completion valid`() {
        val replica = activeReplica()
        val assignment = assignment("session")
        replica.apply(WarmupControlRecord(2, AssignmentProposed(assignment.sessionId, oldActive, 1, assignment)))
        replica.apply(WarmupControlRecord(3, InstanceReady(WarmupSessionId("new-ready"), newActive)))
        replica.apply(WarmupControlRecord(4, CoordinatorClaim(
            WarmupSessionId("claim"), newActive, 1, oldActive, 1
        )))

        assertFalse(replica.apply(WarmupControlRecord(5, StopForwarding(
            assignment.sessionId, oldActive, assignment.donorInstanceId, 1
        ))).accepted)
        assertTrue(replica.apply(WarmupControlRecord(6, SessionCompleted(
            assignment.sessionId, assignment.recipientInstanceId, setOf(WarmupConsumerId("orders"))
        ))).accepted)
        assertTrue(replica.view().sessions.isEmpty())
    }

    @Test
    fun `bounded state rejects excess live sessions and sync buffering`() {
        val limits = WarmupControlPlaneLimits(maximumBufferedRecords = 1, maximumSessions = 1)
        val unsynced = WarmupControlPlaneReplica(newcomer, position, sync, limits)
        unsynced.apply(WarmupControlRecord(10, InstanceReady(WarmupSessionId("one"), oldActive)))
        assertFailsWith<IllegalStateException> {
            unsynced.apply(WarmupControlRecord(11, InstanceReady(WarmupSessionId("two"), newActive)))
        }

        val active = activeReplica(limits)
        val first = assignment("one", 0)
        val second = assignment("two", 1)
        assertTrue(active.apply(WarmupControlRecord(2, AssignmentProposed(first.sessionId, oldActive, 1, first))).accepted)
        assertFalse(active.apply(WarmupControlRecord(3, AssignmentProposed(second.sessionId, oldActive, 1, second))).accepted)
    }

    private fun activeReplica(limits: WarmupControlPlaneLimits = WarmupControlPlaneLimits()): WarmupControlPlaneReplica {
        val replica = WarmupControlPlaneReplica(oldActive,
            WarmupControlLogPosition(TopicPartition("control", 0), 0, 0), sync, limits)
        assertTrue(replica.bootstrapEmptyJournal())
        replica.apply(WarmupControlRecord(0, InstanceReady(WarmupSessionId("ready"), oldActive)))
        replica.apply(WarmupControlRecord(1, CoordinatorClaim(
            WarmupSessionId("claim"), oldActive, 0, null, -1
        )))
        return replica
    }

    private fun assignment(id: String, partition: Int = 0) = WarmupSessionAssignment(
        WarmupSessionId(id), WarmupInstanceId("donor"), WarmupInstanceId("recipient-$id"),
        listOf(WarmupPartitionAssignment(WarmupConsumerId("orders"), "orders", "warmup-orders", partition))
    )
}
