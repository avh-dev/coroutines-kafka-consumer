#!/usr/bin/env sh

set -eu

if [ "$(id -u)" -ne 0 ]; then
  echo "Run as root." >&2
  exit 1
fi

swapoff -a || true

if command -v cpupower >/dev/null 2>&1; then
  cpupower frequency-set -g performance || true
fi

systemctl stop apt-daily.service apt-daily-upgrade.service 2>/dev/null || true
systemctl stop apt-daily.timer apt-daily-upgrade.timer 2>/dev/null || true

echo "Perf tuning applied for this boot."
cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || true
