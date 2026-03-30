#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$ROOT_DIR/logs"
mkdir -p "$LOG_DIR"
TS="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$LOG_DIR/inference_server_${TS}.log"
PYTHON_BIN="${PYTHON_BIN:-python3}"
nohup "$PYTHON_BIN" "$ROOT_DIR/inference_server.py" >> "$LOG_FILE" 2>&1 &
PID=$!
echo "$PID" > "$LOG_DIR/inference_server.pid"
echo "inference_server_started pid=$PID log=$LOG_FILE"
