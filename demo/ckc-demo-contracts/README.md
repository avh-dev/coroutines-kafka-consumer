# Demo Contracts

This module contains shared protobuf contracts and Kafka serde used by the demo application and related tooling.

It defines the event model for the potion workshop demo domain:

- `OrderLifecycleEvent`
- `CauldronTelemetryEvent`

The module is intentionally small and reusable:

- protobuf schemas live in `src/main/proto`
- generated classes are shared across demo and future load-test modules
- Kafka serializer/deserializer implementations are provided for the generated messages

Use this module when you need a stable wire contract for:

- the Spring Boot demo application
- traffic generators
- local or cloud load-test tools
