#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="${SCRIPT_DIR}/.torbox-media-center.pid"
CONTAINER_NAME="${TORBOX_CONTAINER_NAME:-torbox-media-center}"

if [[ -f "${PID_FILE}" ]]; then
    PID="$(tr -d '[:space:]' < "${PID_FILE}")"
    PROCESS_CMD="$(ps -p "${PID}" -o args= 2>/dev/null || true)"

    if [[ "${PID}" =~ ^[0-9]+$ ]] && kill -0 "${PID}" 2>/dev/null && [[ "${PROCESS_CMD}" == *"main.py"* ]]; then
        kill -USR1 "${PID}"
        echo "Manual refresh requested for local process ${PID}."
        exit 0
    fi

    echo "PID file found but process is not running. Falling back to Docker check..."
fi

if command -v docker >/dev/null 2>&1; then
    if docker ps --format '{{.Names}}' | grep -Fxq "${CONTAINER_NAME}"; then
        docker kill --signal=USR1 "${CONTAINER_NAME}" >/dev/null
        echo "Manual refresh requested for Docker container ${CONTAINER_NAME}."
        exit 0
    fi
fi

echo "Could not find a running TorBox Media Center process."
echo "Start the app first, then run this script again."
exit 1
