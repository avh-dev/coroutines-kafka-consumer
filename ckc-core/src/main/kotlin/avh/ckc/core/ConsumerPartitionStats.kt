package avh.ckc.core

interface ConsumerPartitionStats {
    val topic: String
    val partition: Int
    val offsetTrackerBitCapacity: Int
}
