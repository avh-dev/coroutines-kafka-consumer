# Internal Lab Notifications

`run-experiment.sh` can call an optional notification hook during long experiment runs.
The hook must live in this directory on the lab host and must be executable:

```text
/opt/ckc-lab/notify/notify.py
/opt/ckc-lab/notify/notify.sh
```

The interface is:

```sh
notify-hook event-name payload.json
```

`payload.json` is a temporary JSON file written by the experiment runner. The hook
may read it, send a message, and exit. A missing hook disables notifications.

Common event names:

- `experiment_started`
- `test_started`
- `test_finished`
- `measurements_finished`
- `audit_analysis_started`
- `audit_analysis_finished`
- `audit_run_analysis_started`
- `audit_run_analysis_finished`
- `kafka_warmup_started`
- `report_ready` — emitted after `report.md` has been written
- `bundle_ready` — emitted after the evidence and audit archives are finalized
- `experiment_completed` — the last successful lifecycle event
- `experiment_failed`

For quick tuning iterations, retain the generated report while skipping evidence
collection and both final archives:

```sh
/opt/ckc-lab/bin/run-experiment.sh --skip-archives EXPERIMENT
```

Without `--skip-archives`, evidence collection and archive generation retain their
existing behavior.

## Telegram Example

This directory includes `notify-telegram.py`. It is a ready-to-use example that
uses the Telegram Bot API directly from Python's standard library.

### 1. Create a bot

1. Open Telegram and search for `@BotFather`.
2. Send `/start`.
3. Send `/newbot`.
4. Choose a display name, for example `CKC Lab`.
5. Choose a username that ends with `bot`, for example `ckc_lab_notify_bot`.
6. BotFather returns a token that looks like `123456:ABC-...`.

Keep that token private. Do not commit it.

### 2. Find your chat id

For a direct message:

1. Open your new bot in Telegram.
2. Send it any message, for example `hello`.
3. On the lab host, run:

```sh
curl -fsS "https://api.telegram.org/bot<token>/getUpdates"
```

4. Find `message.chat.id` in the JSON response. That number is
   `TELEGRAM_CHAT_ID`.

For a group:

1. Add the bot to the group.
2. Send a message in the group.
3. Run the same `getUpdates` command.
4. Use the group `chat.id`. Group ids are often negative numbers.

For a forum topic, also set `TELEGRAM_THREAD_ID` to the topic id.

### 3. Enable the hook

Store the variables on the operator machine in
`~/.config/ckc-lab/telegram.env`:

```sh
install -d -m 0700 ~/.config/ckc-lab
cat > ~/.config/ckc-lab/telegram.env <<'EOF'
export TELEGRAM_BOT_TOKEN='replace-me'
export TELEGRAM_CHAT_ID='replace-me'
# export TELEGRAM_THREAD_ID='replace-me'
EOF
chmod 0600 ~/.config/ckc-lab/telegram.env
demo/infra/internal-lab/scripts/lab.sh up
```

`lab.sh up` transfers the file with mode `0600`. The managed `notify.sh` wrapper
sources it at event time; the secret is never stored in the repository.

By default, the Telegram example sends only high-signal experiment-level events.
The first message identifies the environment, Kafka shape, targets, and expected
workload duration. Report and bundle readiness are emitted in that order, and the
terminal completion event is sent only after final artifacts exist. Warm-up is
reported only when it actually runs.
Override this with a comma-separated allowlist:

```sh
export TELEGRAM_EVENTS='experiment_started,bundle_ready,experiment_completed,experiment_failed'
```

### 4. Test it

```sh
cat > /tmp/ckc-notify-test.json <<'EOF'
{"experiment":"manual-test","environment":{"name":"internal-lab","detail":"optilab"},"kafka":{"implementation":"apache-kafka","topology":"single","brokers":1},"targets":[{"name":"ckc","profile":"ckc","replicas":2,"base_tps":1000,"duration_seconds":120}],"expected_duration_seconds":120}
EOF
/opt/ckc-lab/notify/notify.sh experiment_started /tmp/ckc-notify-test.json
```

If the message does not arrive, check:

- the bot token is correct;
- the bot has received at least one message from you or was added to the group;
- `TELEGRAM_CHAT_ID` matches the chat from `getUpdates`;
- the wrapper is executable.
