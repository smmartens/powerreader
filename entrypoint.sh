#!/bin/sh
# Fix ownership of /data for volumes created by earlier (root) runs,
# then drop privileges to appuser.
chown -R appuser:appuser /data
exec su -s /bin/sh appuser -c 'exec uv run --no-sync uvicorn powerreader.main:app --host 0.0.0.0 --port 8080'
