# Demo Stubs

This module contains a lightweight Armeria HTTP stub service for the demo domain.

Module: `ckc-demo-stubs`

Current scope:

- `POST /eta` endpoint compatible with the demo application's ETA model client
- `POST /flavour` endpoint compatible with the demo application's order flavour model client
- `GET /latency` and `POST /latency` runtime latency settings for both model endpoints
- configurable latency buckets for local load and resiliency testing
- optional error injection for resiliency testing

Key environment variables:

- `PORT`
- `STUB_WORKERS` (defaults to `4`)
- `DELAY_P90_MS`
- `DELAY_P95_MS`
- `DELAY_P99_MS`
- `DELAY_P100_MS`
- `ETA_DELAY_P90_MS`, `ETA_DELAY_P95_MS`, `ETA_DELAY_P99_MS`, `ETA_DELAY_P100_MS`
- `FLAVOUR_DELAY_P90_MS`, `FLAVOUR_DELAY_P95_MS`, `FLAVOUR_DELAY_P99_MS`, `FLAVOUR_DELAY_P100_MS`
- `ERROR_RATE_PERCENT`
