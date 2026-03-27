# Demo Stubs

This module contains a lightweight HTTP stub service for the demo domain.

Module: `coroutines-kafka-consumer-demo-stubs`

Current scope:

- `POST /eta` endpoint compatible with the demo application's model client
- configurable latency buckets for local load and resiliency testing
- optional error injection for resiliency testing

Key environment variables:

- `PORT`
- `STUB_WORKERS`
- `DELAY_P90_MS`
- `DELAY_P95_MS`
- `DELAY_P99_MS`
- `DELAY_P100_MS`
- `ERROR_RATE_PERCENT`
