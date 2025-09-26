#!/bin/bash
# Health check script for Cloudberry sandbox
set -euo pipefail

CONTAINER_NAME="${1:-cbdb-cdw}"

echo "Performing health check on container: $CONTAINER_NAME"

# Check container status
if ! docker ps --filter "name=$CONTAINER_NAME" --filter "status=running" | grep -q "$CONTAINER_NAME"; then
    echo "Error: Container $CONTAINER_NAME is not running"
    exit 1
fi

# Check SSH service
if ! docker exec "$CONTAINER_NAME" systemctl is-active sshd >/dev/null 2>&1; then
    echo "Warning: SSH service is not active"
fi

# Check database connectivity
if ! docker exec "$CONTAINER_NAME" su - gpadmin -c "psql -d template1 -c 'SELECT 1'" >/dev/null 2>&1; then
    echo "Error: Database is not accessible"
    exit 1
fi

# Check segment status
SEGMENTS_DOWN=$(docker exec "$CONTAINER_NAME" su - gpadmin -c "psql -d template1 -t -c \"SELECT count(*) FROM gp_segment_configuration WHERE status != 'u'\"" 2>/dev/null | tr -d ' ')

if [ "$SEGMENTS_DOWN" != "0" ]; then
    echo "Warning: $SEGMENTS_DOWN segments are down"
    exit 1
fi

echo "Health check passed"