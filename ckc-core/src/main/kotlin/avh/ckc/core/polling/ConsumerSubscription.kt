package avh.ckc.core.polling

import org.apache.kafka.common.TopicPartition
import java.util.Collections
import java.util.regex.Pattern

internal sealed interface ConsumerSubscription {
    class Topics(names: Collection<String>) : ConsumerSubscription {
        val names: List<String> = Collections.unmodifiableList(ArrayList(names))
    }

    class PatternSubscription(val pattern: Pattern) : ConsumerSubscription

    class AssignedAtEnd(partitions: Collection<TopicPartition>) : ConsumerSubscription {
        val partitions: Set<TopicPartition> = Collections.unmodifiableSet(LinkedHashSet(partitions))
    }
}

internal fun consumerSubscription(
    topics: List<String>?,
    topicsPattern: Pattern?,
    assignedPartitionsAtEnd: Collection<TopicPartition>?
): ConsumerSubscription {
    val configuredModes = listOf(topics, topicsPattern, assignedPartitionsAtEnd).count { it != null }
    require(configuredModes == 1) {
        "Exactly one of topics, topicsPattern, or assignedPartitionsAtEnd must be specified"
    }
    return when {
        topics != null -> ConsumerSubscription.Topics(topics)
        topicsPattern != null -> ConsumerSubscription.PatternSubscription(topicsPattern)
        else -> ConsumerSubscription.AssignedAtEnd(requireNotNull(assignedPartitionsAtEnd))
    }
}
