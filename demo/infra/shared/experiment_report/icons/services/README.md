# Experiment report service icons

Service artwork for the self-contained experiment timeline SVGs lives in this
directory. The renderer accepts SVG, PNG, or WebP and embeds the selected file
as a base64 data URI; generated reports never depend on external paths.

The bundled catalog includes Kubernetes, Docker, Apache Kafka, Redis,
Prometheus, Fluent Bit, and Grafana artwork copied from the local documentation
icon catalog. Official Alloy and Loki artwork comes from their Grafana projects.
`demo-stubs.svg` is a
project-owned web-service glyph that combines a gear with a `</>` endpoint mark.
The CKC demo flask, load-generator speedometer, and exporter badges are
project-owned diagram glyphs.

Recognized asset names:

- `kubernetes.svg` for `ckc-demo` pod scenarios;
- `demo-stubs.svg` for stubs degradation;
- `redis.svg`;
- `kafka.svg`;
- `audit.svg` for the Fluent Bit audit pipeline.

Environment-topology assets use explicit names such as `kubernetes-brand.svg`,
`docker.svg`, `apache-kafka.svg`, `redis-brand.svg`, `prometheus.svg`,
`alloy.svg`, `fluent-bit.svg`, `loki.svg`, `grafana.svg`,
`ckc-demo-app.svg`, `load-generator.svg`, `kafka-exporter.svg`, and
`process-exporter.svg`.

The same basename with `.png` or `.webp` is also accepted. SVG is preferred.
When an asset is absent, the renderer uses a deterministic colored letter badge.
