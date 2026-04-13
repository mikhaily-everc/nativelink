#!/usr/bin/env bash
# Build the NativeLink worker-init image from the fork and push to ECR.
#
# The worker-init init container copies the NativeLink binary into a shared
# volume so the worker OS image (tools/rbe/worker-image) can run it without
# embedding a specific NativeLink version.
#
# Usage:
#   ./build-worker-init.sh
#
# After pushing, update DEFAULT_WORKER_INIT_IMAGE in
# tools/rbe/k8s/manifests/worker-provisioner-deployment.yaml
# with the @sha256 digest printed at the end.
#
# Prerequisites:
#   - Docker with linux/amd64 platform support
#   - "nix-store" Docker volume (created on first run, persists across builds)
#   - Docker ECR credentials helper configured in ~/.docker/config.json
#
# Note: Run from the repo root OR from tools/rbe/nativelink — the script
# locates itself and mounts the entire repo so Nix can resolve the git
# submodule's parent .git directory.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
ECR_REPO="692503192357.dkr.ecr.us-east-1.amazonaws.com/mlrc-gradle-cache/worker-init"
OUTPUT_TAR="${SCRIPT_DIR}/nativelink-worker-init.tar"

echo "==> Building nativelink-worker-init"
echo "    Repo root : ${REPO_ROOT}"
echo "    Output tar: ${OUTPUT_TAR}"

docker run --rm --platform linux/amd64 \
  --security-opt seccomp=unconfined \
  -v nix-store:/nix \
  -v "${REPO_ROOT}:/repo" \
  -w /repo/tools/rbe/nativelink \
  nixos/nix:2.34.4 sh -c "
    echo 'sandbox = false' >> /etc/nix/nix.conf &&
    echo 'filter-syscalls = false' >> /etc/nix/nix.conf &&
    nix build .#nativelink-worker-init \
      --extra-experimental-features 'nix-command flakes' --no-link &&
    nix run .#nativelink-worker-init.copyTo \
      --extra-experimental-features 'nix-command flakes' \
      -- docker-archive:/repo/tools/rbe/nativelink/nativelink-worker-init.tar"

echo "==> Loading image into Docker"
LOADED=$(docker load < "${OUTPUT_TAR}")
IMAGE_ID=$(echo "${LOADED}" | grep 'Loaded image ID:' | awk '{print $NF}')

echo "==> Tagging as ${ECR_REPO}:latest"
docker tag "${IMAGE_ID}" "${ECR_REPO}:latest"

echo "==> Pushing ${ECR_REPO}:latest"
DIGEST=$(docker push "${ECR_REPO}:latest" | grep '^latest: digest:' | awk '{print $3}')

echo ""
echo "==> Update DEFAULT_WORKER_INIT_IMAGE in"
echo "    tools/rbe/k8s/manifests/worker-provisioner-deployment.yaml"
echo "    to: ${ECR_REPO}@${DIGEST}"
