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
- `experiment_runs_finished`
- `audit_analysis_started`
- `audit_analysis_finished`
- `audit_run_analysis_started`
- `audit_run_analysis_finished`
- `experiment_finished`
- `experiment_failed`

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

Create a local wrapper named `/opt/ckc-lab/notify/notify.sh`. This wrapper is
not managed by `update-lab`, so your token stays on the server:

```sh
cat > /opt/ckc-lab/notify/notify.sh <<'EOF'
#!/usr/bin/env sh
export TELEGRAM_BOT_TOKEN='replace-me'
export TELEGRAM_CHAT_ID='replace-me'
# export TELEGRAM_THREAD_ID='replace-me'
exec /opt/ckc-lab/notify/notify-telegram.py "$@"
EOF
chmod 0750 /opt/ckc-lab/notify/notify.sh
```

By default, the Telegram example sends only high-signal experiment-level events.
Override this with a comma-separated allowlist:

```sh
export TELEGRAM_EVENTS='experiment_started,experiment_finished,experiment_failed'
```

### 4. Test it

```sh
cat > /tmp/ckc-notify-test.json <<'EOF'
{"experiment":"manual-test","targets":1,"exit_code":0}
EOF
/opt/ckc-lab/notify/notify.sh experiment_started /tmp/ckc-notify-test.json
```

If the message does not arrive, check:

- the bot token is correct;
- the bot has received at least one message from you or was added to the group;
- `TELEGRAM_CHAT_ID` matches the chat from `getUpdates`;
- the wrapper is executable.
