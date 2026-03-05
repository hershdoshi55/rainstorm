#!/bin/sh
set -e

# Create runtime directories required by prod mode
mkdir -p \
    /home/mp4/hydfs_file_store \
    /home/mp4/local_file_store \
    /home/mp4/rainstorm_local_logs \
    /home/mp4/rainstorm_file_store \
    /home/mp4/rainstorm-c7/rainstorm_dataset \
    /home/mp4/rainstorm-c7/dataset

# Ensure operator binaries are executable (volume mounts may strip permissions)
chmod +x /home/mp4/binaries/* 2>/dev/null || true

exec "$@"
