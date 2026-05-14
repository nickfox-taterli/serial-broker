#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOCK="${ROOT}/broker.sock"
LOGS="${ROOT}/logs-test"
FAKE_OUT="$(mktemp)"
BROKER_OUT="$(mktemp)"
rm -f "${SOCK}"

python3 "${ROOT}/scripts/fake_target.py" >"${FAKE_OUT}" &
FAKE_PID=$!
for _ in $(seq 1 50); do
  if [ -s "${FAKE_OUT}" ]; then break; fi
  sleep 0.1
done
SERIAL="$(head -n1 "${FAKE_OUT}")"

"${ROOT}/serial-broker" --serial "${SERIAL}" --socket "${SOCK}" --log-dir "${LOGS}" >"${BROKER_OUT}" 2>&1 &
BROKER_PID=$!
cleanup() {
  kill "${BROKER_PID}" "${FAKE_PID}" 2>/dev/null || true
  rm -f "${FAKE_OUT}" "${BROKER_OUT}"
}
trap cleanup EXIT

for _ in $(seq 1 50); do
  [ -S "${SOCK}" ] && break
  if ! kill -0 "${BROKER_PID}" 2>/dev/null; then
    cat "${BROKER_OUT}" >&2
    exit 1
  fi
  sleep 0.1
done

"${ROOT}/sbctl" --socket "${SOCK}" status --json
"${ROOT}/sbctl" --socket "${SOCK}" wait "login:" --timeout 3 --json
"${ROOT}/sbctl" --socket "${SOCK}" run "echo hello" --timeout 5 --json
printf 'small file\n' >"${ROOT}/small.txt"
"${ROOT}/sbctl" --socket "${SOCK}" upload --method base64 "${ROOT}/small.txt" /tmp/small.txt --timeout 10 --json
"${ROOT}/sbctl" --socket "${SOCK}" tail 20
