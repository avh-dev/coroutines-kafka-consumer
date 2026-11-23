package avh.ckc.warmup.broker

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream

internal sealed interface BrokerLifecycleEvent {
    val epoch: String

    data class Started(override val epoch: String) : BrokerLifecycleEvent
    data class Joined(override val epoch: String, val context: String, val registration: String) : BrokerLifecycleEvent
    data class JoinConfirmed(
        override val epoch: String,
        val context: String,
        val registration: String
    ) : BrokerLifecycleEvent
    data class Left(override val epoch: String, val context: String) : BrokerLifecycleEvent
    data class DrainRequested(
        override val epoch: String,
        val drainId: String,
        val deadlineMillis: Long
    ) : BrokerLifecycleEvent
    data class DrainAccepted(
        override val epoch: String,
        val drainId: String,
        val context: String
    ) : BrokerLifecycleEvent
    data class ContextDrained(
        override val epoch: String,
        val drainId: String,
        val context: String,
        val sessions: Set<String>
    ) : BrokerLifecycleEvent
}

/** Independent of the warm-up session protocol and its wire format. */
internal object BrokerLifecycleCodec {
    private const val VERSION = 1

    fun encode(event: BrokerLifecycleEvent): ByteArray {
        val bytes = ByteArrayOutputStream()
        DataOutputStream(bytes).use { out ->
            out.writeByte(VERSION)
            out.writeUTF(event.epoch)
            when (event) {
                is BrokerLifecycleEvent.Started -> out.writeByte(1)
                is BrokerLifecycleEvent.Joined -> {
                    out.writeByte(2)
                    out.writeUTF(event.context)
                    out.writeUTF(event.registration)
                }
                is BrokerLifecycleEvent.JoinConfirmed -> {
                    out.writeByte(3)
                    out.writeUTF(event.context)
                    out.writeUTF(event.registration)
                }
                is BrokerLifecycleEvent.Left -> {
                    out.writeByte(4)
                    out.writeUTF(event.context)
                }
                is BrokerLifecycleEvent.DrainRequested -> {
                    out.writeByte(5)
                    out.writeUTF(event.drainId)
                    out.writeLong(event.deadlineMillis)
                }
                is BrokerLifecycleEvent.DrainAccepted -> {
                    out.writeByte(6)
                    out.writeUTF(event.drainId)
                    out.writeUTF(event.context)
                }
                is BrokerLifecycleEvent.ContextDrained -> {
                    out.writeByte(7)
                    out.writeUTF(event.drainId)
                    out.writeUTF(event.context)
                    out.writeInt(event.sessions.size)
                    event.sessions.sorted().forEach(out::writeUTF)
                }
            }
        }
        return bytes.toByteArray()
    }

    fun decode(bytes: ByteArray): BrokerLifecycleEvent = DataInputStream(ByteArrayInputStream(bytes)).use { input ->
        require(input.readUnsignedByte() == VERSION) { "Unsupported broker lifecycle protocol version" }
        val epoch = input.readUTF()
        val event = when (input.readUnsignedByte()) {
            1 -> BrokerLifecycleEvent.Started(epoch)
            2 -> BrokerLifecycleEvent.Joined(epoch, input.readUTF(), input.readUTF())
            3 -> BrokerLifecycleEvent.JoinConfirmed(epoch, input.readUTF(), input.readUTF())
            4 -> BrokerLifecycleEvent.Left(epoch, input.readUTF())
            5 -> BrokerLifecycleEvent.DrainRequested(epoch, input.readUTF(), input.readLong())
            6 -> BrokerLifecycleEvent.DrainAccepted(epoch, input.readUTF(), input.readUTF())
            7 -> {
                val drainId = input.readUTF()
                val context = input.readUTF()
                val count = input.readInt()
                require(count >= 0 && count <= input.available() / 2) { "Invalid broker lifecycle session count" }
                val sessions = buildSet { repeat(count) { add(input.readUTF()) } }
                BrokerLifecycleEvent.ContextDrained(epoch, drainId, context, sessions)
            }
            else -> error("Unknown broker lifecycle event")
        }
        require(input.available() == 0) { "Trailing bytes in broker lifecycle event" }
        event
    }
}
