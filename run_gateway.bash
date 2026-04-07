#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$ROOT_DIR/logs"
mkdir -p "$LOG_DIR"
TS="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$LOG_DIR/gateway_${TS}.log"
PYTHON_BIN="${PYTHON_BIN:-python3}"
UMRM_VIDEO_ROOT="${UMRM_VIDEO_ROOT:-/data/UMRM/data/EGO/videos/full_scale}"
UMRM_INFER_BASE_URL="${UMRM_INFER_BASE_URL:-http://127.0.0.1:18877}"
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY
unset all_proxy ALL_PROXY no_proxy NO_PROXY
unset ftp_proxy FTP_PROXY rsync_proxy RSYNC_PROXY socks_proxy SOCKS_PROXY
nohup env UMRM_VIDEO_ROOT="$UMRM_VIDEO_ROOT" UMRM_INFER_BASE_URL="$UMRM_INFER_BASE_URL" "$PYTHON_BIN" "$ROOT_DIR/annotator_gateway.py" >> "$LOG_FILE" 2>&1 &
PID=$!
echo "$PID" > "$LOG_DIR/gateway.pid"
echo "gateway_started pid=$PID log=$LOG_FILE"
