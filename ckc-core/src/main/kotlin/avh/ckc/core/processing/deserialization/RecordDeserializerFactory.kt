package avh.ckc.core.processing.deserialization

internal typealias RecordDeserializerFactory<K, V> = (workerIndex: Int) -> RecordDeserializer<K, V>
