#!/bin/bash
set -e

REPO_URL=$(git remote get-url origin)
COMMIT_ID=$(git rev-parse HEAD)
SHORT_COMMIT_ID=$(git rev-parse --short=12 HEAD)
#RELEASE_TAG=$(git describe --tags --exact-match 2>/dev/null || echo "DEVELOPMENT.$(date -u +%Y-%m-%dT%H-%M-%SZ)")
RELEASE_TAG=$(git describe --tags --exact-match 2>/dev/null || echo "DEVELOPMENT.NOW")

# if podman installed use it instead of docker
DOCKER_CMD="docker"
if command -v podman &>/dev/null; then
  DOCKER_CMD="podman"
fi

DOCKER_CMD build \
  --build-arg REPO_URL="${REPO_URL}" \
  --build-arg COMMIT_ID="${COMMIT_ID}" \
  --build-arg SHORT_COMMIT_ID="${SHORT_COMMIT_ID}" \
  --build-arg RELEASE_TAG="${RELEASE_TAG}" \
  -t minio:latest \
  .