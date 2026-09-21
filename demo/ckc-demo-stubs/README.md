# Demo Stubs

This module contains a lightweight Armeria HTTP stub service for the demo domain.

Module: `ckc-demo-stubs`

Current scope:

- `POST /eta` endpoint compatible with the demo application's ETA model client
- `POST /flavour` endpoint compatible with the demo application's order flavour model client
- `GET /settings` and `POST /settings` runtime latency and error settings for both model endpoints
- configurable latency buckets for local load and resiliency testing
- optional error injection for resiliency testing

Key environment variables:

- `PORT`
- `STUB_WORKERS` (defaults to `4`)

Runtime settings are intentionally configured after startup:

```sh
curl -fsS -X POST http://localhost:8080/settings \
  -H 'Content-Type: application/json' \
  --data '{"eta":{"percentiles":{"p50":20,"p90":40,"p999":160,"p100":300}},"flavour":{"percentiles":{"p50":20,"p90":40,"p999":160,"p100":300}},"errorRatePercent":0}'
```

Percentile keys encode their quantile as decimal digits: `p50` is `0.50`,
`p999` is `0.999`, and the required terminal `p100` is `1.0`. Delays are
milliseconds and must be non-negative and non-decreasing by quantile.
