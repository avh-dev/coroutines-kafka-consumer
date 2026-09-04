# Offline restore

Extract `evidence.tar.gz`, enter the `evidence` directory, and run:

```bash
./restore/open-result.sh ./result
```

The command accepts an optional Grafana port as its second argument. It uses the
same pinned Grafana, Loki, and VictoriaMetrics images for internal-lab and AWS
evidence. Stop the stack with `./restore/close-result.sh ./result`.

The restore runtime is created under `restore/.runtime` and is not evidence.
