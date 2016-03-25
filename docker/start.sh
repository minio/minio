#!/bin/bash

if [ "$1" = 'fs' ]; then
    if [ -z "$2" ]; then
        echo "Invalid arguments"
        echo "Usage: fs <export_dir>"
        exit 1
    else
        /bin/mkdir -p "$2" && /minio init fs "$2" && /minio server
    fi
else
    echo "Usage: fs <export_dir>"
    exit 0
fi
