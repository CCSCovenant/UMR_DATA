#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$ROOT_DIR/logs"
mkdir -p "$LOG_DIR"
TS="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$LOG_DIR/gateway_${TS}.log"
PYTHON_BIN="${PYTHON_BIN:-python3}"
nohup "$PYTHON_BIN" "$ROOT_DIR/annotator_gateway.py" >> "$LOG_FILE" 2>&1 &
PID=$!
echo "$PID" > "$LOG_DIR/gateway.pid"
echo "gateway_started pid=$PID log=$LOG_FILE"
