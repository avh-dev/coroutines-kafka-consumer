package avh.ckc.core.processing

internal interface RecordProcessingRuntime<K, V> : PolledRecordSink<K, V>, RecordProcessingLifecycle
