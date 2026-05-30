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
  --data '{"eta":{"delayP90Ms":40,"delayP95Ms":80,"delayP99Ms":160,"delayP100Ms":300},"flavour":{"delayP90Ms":40,"delayP95Ms":80,"delayP99Ms":160,"delayP100Ms":300},"errorRatePercent":0}'
```
