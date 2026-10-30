package avh.ckc.core.warmup

import java.util.Collections

internal fun <T> immutableListSnapshot(values: Collection<T>): List<T> =
    Collections.unmodifiableList(ArrayList(values))

internal fun <T> immutableSetSnapshot(values: Collection<T>): Set<T> =
    Collections.unmodifiableSet(LinkedHashSet(values))
