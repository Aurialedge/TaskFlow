#!/bin/sh
set -e

# If specific command passed (like /app/bin/worker), execute it directly
if [ "$#" -gt 0 ] && [ "$1" != "/app/bin/api" ]; then
    exec "$@"
fi

# On single-container platforms (e.g. Render free web service),
# start background worker process alongside API unless explicitly disabled.
if [ "$RUN_WORKER" != "false" ]; then
    echo "Starting TaskFlow background worker daemon..."
    /app/bin/worker &
fi

echo "Starting TaskFlow HTTP API server..."
exec /app/bin/api
