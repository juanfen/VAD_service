#!/usr/bin/env bash

set -euo pipefail

START_CONCURRENCY=50
MAX_CONCURRENCY=2000
RAMP_FACTOR=2
DURATION="60s"
REALTIME="true"
TRANSPORT="ws"
TARGET_URL="ws://127.0.0.1:8866/ws/v1/vad"
WAV_PATH=""
PROGRESS="5s"
SESSION_PREFIX="max-concurrency"
ALLOW_SESSION_FAILURES=0
ALLOW_FRAME_FAILURES=0
OUTPUT_PATH=""
EXTRA_ARGS=()

usage() {
  cat <<'EOF'
Usage:
  ./find-max-concurrency.sh --wav /path/to/test.wav [options]

Options:
  --wav PATH                     Required. Path to WAV file.
  --url URL                      Target URL.
  --transport ws|http            Transport mode. Default: ws
  --realtime true|false          Whether to simulate realtime sending. Default: true
  --duration 60s                 Duration for each load test run. Default: 60s
  --start 50                     Starting concurrency. Default: 50
  --max 2000                     Upper bound for search. Default: 2000
  --ramp-factor 2                Multiplication factor during ramp-up. Default: 2
  --progress 5s                  Progress print interval. Default: 5s
  --session-prefix PREFIX        Session prefix for load tests.
  --allow-session-failures 0     Allowed failed sessions before marking a run unstable.
  --allow-frame-failures 0       Allowed failed frames before marking a run unstable.
  --out FILE.md                  Optional markdown output path.
  --extra-arg ARG                Extra argument passed to go run; repeatable.

Example:
  ./find-max-concurrency.sh \
    --wav /data/test.wav \
    --transport ws \
    --url ws://127.0.0.1:8866/ws/v1/vad \
    --realtime true \
    --duration 60s \
    --start 50 \
    --max 1000 \
    --out max-concurrency-report.md
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --wav)
      WAV_PATH="$2"
      shift 2
      ;;
    --url)
      TARGET_URL="$2"
      shift 2
      ;;
    --transport)
      TRANSPORT="$2"
      shift 2
      ;;
    --realtime)
      REALTIME="$2"
      shift 2
      ;;
    --duration)
      DURATION="$2"
      shift 2
      ;;
    --start)
      START_CONCURRENCY="$2"
      shift 2
      ;;
    --max)
      MAX_CONCURRENCY="$2"
      shift 2
      ;;
    --ramp-factor)
      RAMP_FACTOR="$2"
      shift 2
      ;;
    --progress)
      PROGRESS="$2"
      shift 2
      ;;
    --session-prefix)
      SESSION_PREFIX="$2"
      shift 2
      ;;
    --allow-session-failures)
      ALLOW_SESSION_FAILURES="$2"
      shift 2
      ;;
    --allow-frame-failures)
      ALLOW_FRAME_FAILURES="$2"
      shift 2
      ;;
    --out)
      OUTPUT_PATH="$2"
      shift 2
      ;;
    --extra-arg)
      EXTRA_ARGS+=("$2")
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$WAV_PATH" ]]; then
  echo "missing required argument: --wav" >&2
  usage >&2
  exit 1
fi

if [[ ! -f "$WAV_PATH" ]]; then
  echo "wav file does not exist: $WAV_PATH" >&2
  exit 1
fi

if [[ "$TRANSPORT" != "ws" && "$TRANSPORT" != "http" ]]; then
  echo "--transport must be ws or http" >&2
  exit 1
fi

if [[ "$REALTIME" != "true" && "$REALTIME" != "false" ]]; then
  echo "--realtime must be true or false" >&2
  exit 1
fi

if ! command -v go >/dev/null 2>&1; then
  echo "go is not installed or not in PATH" >&2
  exit 1
fi

declare -a RESULT_ROWS=()
BEST_STABLE=0
BEST_STABLE_SUMMARY=""
FIRST_UNSTABLE=0

extract_value() {
  local pattern="$1"
  local file="$2"
  sed -n "s/${pattern}/\\1/p" "$file" | tail -n 1
}

append_row() {
  local concurrency="$1"
  local status="$2"
  local sessions="$3"
  local frames="$4"
  local session_rate="$5"
  local frame_rate="$6"
  local avg_latency="$7"
  local p95_latency="$8"
  local p99_latency="$9"
  RESULT_ROWS+=("| ${concurrency} | ${status} | ${sessions} | ${frames} | ${session_rate} | ${frame_rate} | ${avg_latency} | ${p95_latency} | ${p99_latency} |")
}

run_once() {
  local concurrency="$1"
  local log_file
  log_file="$(mktemp)"

  echo
  echo "==> running concurrency=${concurrency}"

  set +e
  env CGO_ENABLED=0 go run . \
    -mode load \
    -transport "$TRANSPORT" \
    -url "$TARGET_URL" \
    -wav "$WAV_PATH" \
    -concurrency "$concurrency" \
    -duration "$DURATION" \
    -realtime="$REALTIME" \
    -progress "$PROGRESS" \
    -session-prefix "${SESSION_PREFIX}-${concurrency}" \
    "${EXTRA_ARGS[@]}" | tee "$log_file"
  local exit_code=${PIPESTATUS[0]}
  set -e

  local sessions_started sessions_ok sessions_failed
  local frames_started frames_ok frames_failed
  local session_rate frame_rate
  local avg_latency p95_latency p99_latency

  sessions_started="$(extract_value '^sessions started=\([0-9]\+\) ok=[0-9]\+ failed=[0-9]\+$' "$log_file")"
  sessions_ok="$(extract_value '^sessions started=[0-9]\+ ok=\([0-9]\+\) failed=[0-9]\+$' "$log_file")"
  sessions_failed="$(extract_value '^sessions started=[0-9]\+ ok=[0-9]\+ failed=\([0-9]\+\)$' "$log_file")"
  frames_started="$(extract_value '^frames started=\([0-9]\+\) ok=[0-9]\+ failed=[0-9]\+$' "$log_file")"
  frames_ok="$(extract_value '^frames started=[0-9]\+ ok=\([0-9]\+\) failed=[0-9]\+$' "$log_file")"
  frames_failed="$(extract_value '^frames started=[0-9]\+ ok=[0-9]\+ failed=\([0-9]\+\)$' "$log_file")"
  session_rate="$(extract_value '^throughput sessions_per_sec=\([0-9.]\+\) frames_per_sec=[0-9.]\+$' "$log_file")"
  frame_rate="$(extract_value '^throughput sessions_per_sec=[0-9.]\+ frames_per_sec=\([0-9.]\+\)$' "$log_file")"
  avg_latency="$(extract_value '^latency avg=\([^ ]\+\) p50=[^ ]\+ p95=[^ ]\+ p99=[^ ]\+ max=[^ ]\+$' "$log_file")"
  p95_latency="$(extract_value '^latency avg=[^ ]\+ p50=[^ ]\+ p95=\([^ ]\+\) p99=[^ ]\+ max=[^ ]\+$' "$log_file")"
  p99_latency="$(extract_value '^latency avg=[^ ]\+ p50=[^ ]\+ p95=[^ ]\+ p99=\([^ ]\+\) max=[^ ]\+$' "$log_file")"

  sessions_started="${sessions_started:-0}"
  sessions_ok="${sessions_ok:-0}"
  sessions_failed="${sessions_failed:-999999}"
  frames_started="${frames_started:-0}"
  frames_ok="${frames_ok:-0}"
  frames_failed="${frames_failed:-999999}"
  session_rate="${session_rate:-N/A}"
  frame_rate="${frame_rate:-N/A}"
  avg_latency="${avg_latency:-N/A}"
  p95_latency="${p95_latency:-N/A}"
  p99_latency="${p99_latency:-N/A}"

  local status="unstable"
  if [[ "$exit_code" -eq 0 && "$sessions_failed" -le "$ALLOW_SESSION_FAILURES" && "$frames_failed" -le "$ALLOW_FRAME_FAILURES" ]]; then
    status="stable"
  fi

  append_row \
    "$concurrency" \
    "$status" \
    "${sessions_ok}/${sessions_started} failed=${sessions_failed}" \
    "${frames_ok}/${frames_started} failed=${frames_failed}" \
    "$session_rate" \
    "$frame_rate" \
    "$avg_latency" \
    "$p95_latency" \
    "$p99_latency"

  local summary
  summary="sessions=${sessions_ok}/${sessions_started} failed=${sessions_failed}, frames=${frames_ok}/${frames_started} failed=${frames_failed}, session_rate=${session_rate}, frame_rate=${frame_rate}, avg=${avg_latency}, p95=${p95_latency}, p99=${p99_latency}"

  rm -f "$log_file"

  if [[ "$status" == "stable" ]]; then
    BEST_STABLE="$concurrency"
    BEST_STABLE_SUMMARY="$summary"
    return 0
  fi

  return 1
}

current="$START_CONCURRENCY"
last_stable=0
first_unstable=0

while (( current <= MAX_CONCURRENCY )); do
  if run_once "$current"; then
    last_stable="$current"
    if (( current == MAX_CONCURRENCY )); then
      break
    fi

    next=$(( current * RAMP_FACTOR ))
    if (( next <= current )); then
      next=$(( current + 1 ))
    fi
    if (( next > MAX_CONCURRENCY )); then
      next="$MAX_CONCURRENCY"
    fi
    current="$next"
  else
    first_unstable="$current"
    break
  fi
done

if (( last_stable == 0 )); then
  FIRST_UNSTABLE="$first_unstable"
elif (( first_unstable == 0 )); then
  FIRST_UNSTABLE=0
else
  low="$last_stable"
  high="$first_unstable"
  while (( high - low > 1 )); do
    mid=$(( (low + high) / 2 ))
    if run_once "$mid"; then
      low="$mid"
    else
      high="$mid"
    fi
  done
  BEST_STABLE="$low"
  FIRST_UNSTABLE="$high"
fi

report_timestamp="$(date '+%Y-%m-%d %H:%M:%S %Z')"
report_content="$(cat <<EOF
# Max Concurrency Search Report

Generated at: ${report_timestamp}

## Test Profile

- transport: \`${TRANSPORT}\`
- url: \`${TARGET_URL}\`
- wav: \`${WAV_PATH}\`
- realtime: \`${REALTIME}\`
- duration per run: \`${DURATION}\`
- start concurrency: \`${START_CONCURRENCY}\`
- max concurrency: \`${MAX_CONCURRENCY}\`
- ramp factor: \`${RAMP_FACTOR}\`
- allowed session failures: \`${ALLOW_SESSION_FAILURES}\`
- allowed frame failures: \`${ALLOW_FRAME_FAILURES}\`

## Result

- max stable concurrency: \`${BEST_STABLE}\`
- first unstable concurrency: \`${FIRST_UNSTABLE}\`
- best stable summary: ${BEST_STABLE_SUMMARY:-N/A}

## Run Details

| Concurrency | Status | Sessions | Frames | Sessions/s | Frames/s | Avg Latency | P95 | P99 |
|---:|---|---|---|---:|---:|---:|---:|---:|
$(printf '%s\n' "${RESULT_ROWS[@]}")
EOF
)"

echo
echo "$report_content"

if [[ -n "$OUTPUT_PATH" ]]; then
  printf '%s\n' "$report_content" > "$OUTPUT_PATH"
  echo
  echo "markdown report written to: $OUTPUT_PATH"
fi
