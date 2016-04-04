#!/bin/bash

## Fix this check when we arrive at XL.
if [ -z "$1" ]; then
    echo "Invalid arguments"
    echo "Usage: <export_dir>"
    exit 1
else
    /bin/mkdir -p "$1" && /minio server "$1"
fi
