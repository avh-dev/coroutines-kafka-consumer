package avh.ckc.core.processing

import avh.ckc.core.ProcessingRuntimeStateSnapshot

internal interface RecordProcessingRuntime<K, V> : PolledRecordSink<K, V>, RecordProcessingLifecycle {
    fun stateSnapshot(): ProcessingRuntimeStateSnapshot
}
