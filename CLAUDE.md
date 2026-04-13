# NativeLink Build & Deploy

Fork of [TraceMachina/nativelink](https://github.com/TraceMachina/nativelink) with custom patches.

## Current Patches

- **ByteStream Write resume fix**: Accept data from offset 0 on UUID collision
- **Directory cache fix**: Skip work dir pre-creation when directory cache is enabled to prevent hardlink failures

## Branch

`fix/bytestream-write-resume-v3` — based on upstream `main`, rebased periodically.

## Building the CAS Image

Uses Nix flake with a persistent Docker volume for caching. Builds target `linux/amd64`.

### Prerequisites

- Docker with `linux/amd64` platform support
- `nix-store` Docker volume (created on first run, persists deps across builds)

### Build Command

```bash
cd tools/rbe/nativelink

docker run --rm --platform linux/amd64 \
  --security-opt seccomp=unconfined \
  -v nix-store:/nix \
  -v $(pwd):/src \
  -w /src \
  nixos/nix:2.34.4 sh -c "
    echo 'sandbox = false' >> /etc/nix/nix.conf &&
    echo 'filter-syscalls = false' >> /etc/nix/nix.conf &&
    nix build .#nativelink-image \
      --extra-experimental-features 'nix-command flakes' --no-link &&
    nix run .#nativelink-image.copyTo \
      --extra-experimental-features 'nix-command flakes' \
      -- docker-archive:/src/nativelink-image.tar"
```

First build: ~28 min (downloads all deps). Subsequent builds with only Rust changes: ~2-5 min (nix-store volume caches deps).

**Important**: Nix uses the git commit hash in the image metadata. Always commit changes before building.

### Load and Push

```bash
# Load into Docker
docker load < nativelink-image.tar

# Tag for ECR
docker tag nativelink:latest \
  692503192357.dkr.ecr.us-east-1.amazonaws.com/mlrc-gradle-cache/cas:latest

# Login to ECR
aws ecr get-login-password --region us-east-1 --profile dev | \
  docker login --username AWS --password-stdin \
  692503192357.dkr.ecr.us-east-1.amazonaws.com

# Push
docker push \
  692503192357.dkr.ecr.us-east-1.amazonaws.com/mlrc-gradle-cache/cas:latest
```

### Update Helm Values

Update `values.yaml` with the new image tag/SHA:

```yaml
nativelink:
  image:
    repository: 692503192357.dkr.ecr.us-east-1.amazonaws.com/mlrc-gradle-cache/cas
    tag: "latest"
```

## Worker Init Image

Init container that copies the NativeLink binary into the worker pod's shared volume. Built from the fork (same binary as the CAS image) so the worker runs the patched NativeLink version.

```bash
# From the repo root or tools/rbe/nativelink — script locates itself
./tools/rbe/nativelink/build-worker-init.sh [TAG]
# TAG defaults to "v3"
```

See `build-worker-init.sh` for full details. After pushing, update `DEFAULT_WORKER_INIT_IMAGE` in `tools/rbe/k8s/manifests/worker-provisioner-deployment.yaml`.

**Note**: The script mounts the full repo root (not just the submodule dir) so Nix can resolve the git submodule's parent `.git` directory.

## Worker OS Image

Separate image at `tools/rbe/worker-image/Dockerfile`. AL2023-based with system libs for Bazel toolchains. Does NOT contain the NativeLink binary — that comes from the worker-init container above.

```bash
cd tools/rbe/worker-image
docker build --platform linux/amd64 -t \
  692503192357.dkr.ecr.us-east-1.amazonaws.com/mlrc-gradle-cache/worker:latest .
docker push \
  692503192357.dkr.ecr.us-east-1.amazonaws.com/mlrc-gradle-cache/worker:latest
```

Update `worker-specs.json` `podImage` with the push digest (`@sha256:...`).

## ECR Repositories

| Repo | Content |
|------|---------|
| `mlrc-gradle-cache/cas` | NativeLink CAS/scheduler binary (Nix-built) |
| `mlrc-gradle-cache/worker-init` | Worker init container — copies NativeLink binary into shared volume |
| `mlrc-gradle-cache/worker` | Worker OS image (AL2023 + dev headers) |

## Updating from Upstream

```bash
cd tools/rbe/nativelink
git fetch origin main
git merge origin/main --no-edit
# Resolve conflicts if any
# Rebuild and push
```
