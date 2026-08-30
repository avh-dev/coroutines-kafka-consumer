# Restore an AWS smoke result

`open-result.sh` starts the same VictoriaMetrics and Grafana versions used by
the ephemeral runner against a downloaded result directory. The AWS session
must already be destroyed; restore uses only local files and Docker.

Every final result bundle contains this restore kit, including Grafana
provisioning. After extracting a bundle, run it without a repository checkout:

```bash
cd <run-id>
./restore/open-result.sh
```

Stop the containers with `./restore/close-result.sh`. The extracted metrics and
Grafana state remain available for the next start.

From a repository checkout, an explicit result path remains supported:

```bash
./demo/infra/aws/restore/open-result.sh \
  .demo-infra/aws/sessions/<session-id>/result
```

Grafana listens on `0.0.0.0:3002` by default so the report can be viewed from
another machine. Pass a second argument to select another port and a third
argument to select another bind address, for example:

```bash
./restore/open-result.sh . 3002 127.0.0.1
```

The same address can be set with
`CKC_AWS_RESTORE_GRAFANA_BIND_ADDRESS`. Grafana initially uses `admin/admin`;
do not expose this listener to an untrusted network without changing the
password or adding network-level access controls.

The extracted database and Grafana state remain in the result-local `.restore`
directory; it can be deleted and recreated from the archived metrics.
