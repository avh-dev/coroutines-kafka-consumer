# Experiment report service icons

Service artwork for the self-contained experiment timeline SVGs lives in this
directory. The renderer accepts SVG, PNG, or WebP and embeds the selected file
as a base64 data URI; generated reports never depend on external paths.

The bundled catalog includes Kubernetes, Apache Kafka, Redis, and Fluent Bit
artwork copied from the local documentation icon catalog. `demo-stubs.svg` is a
project-owned web-service glyph that combines a gear with a `</>` endpoint mark.

Recognized asset names:

- `kubernetes.svg` for `ckc-demo` pod scenarios;
- `demo-stubs.svg` for stubs degradation;
- `redis.svg`;
- `kafka.svg`;
- `audit.svg` for the Fluent Bit audit pipeline.

The same basename with `.png` or `.webp` is also accepted. SVG is preferred.
When an asset is absent, the renderer uses a deterministic colored letter badge.
