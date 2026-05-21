package avh.ckc.core.deserialization

internal typealias RecordDeserializerFactory<K, V> = (workerIndex: Int) -> RecordDeserializer<K, V>
