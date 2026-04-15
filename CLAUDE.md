# NativeLink Build & Deploy

Fork of [TraceMachina/nativelink](https://github.com/TraceMachina/nativelink) with custom patches.

## Current Patches

- **ByteStream Write resume fix**: Accept data from offset 0 on UUID collision
- **Directory cache fix**: Skip work dir pre-creation when directory cache is enabled to prevent hardlink failures

## Branch

`fix/bytestream-write-resume-v3` — based on upstream `main`, rebased periodically.

## Building and Pushing Images

All three images are built via Bazel for `linux/amd64`. The nativelink binary is compiled by
`rules_rust` on RBE linux/amd64 workers from `@nativelink//:nativelink`. No Nix required.

### Images

| Repo | Content | Build target |
|------|---------|--------------|
| `mlrc-gradle-cache/cas` | `al2023_minimal` + nativelink binary at `/bin/nativelink` | `//tools/rbe/cas-image:image` |
| `mlrc-gradle-cache/worker-init` | `busybox:1.37` + nativelink binary, entrypoint `cp /bin/nativelink` | `//tools/rbe/worker-init-image:image` |
| `mlrc-gradle-cache/worker` | `al2023_minimal` running as uid 1000 (no nativelink — injected by worker-init) | `//tools/rbe/worker-image:image` |

### Build & Push

```bash
# Build all images (Rust compilation runs on RBE linux/amd64 workers)
bazel build \
  //tools/rbe/cas-image:image \
  //tools/rbe/worker-init-image:image \
  //tools/rbe/worker-image:image

# Push to ECR from macOS (--config=oci-push sets host_platform=macos_arm64 so
# crane/jq in push script runfiles resolve to darwin arm64 rather than linux)
bazel run --config=oci-push \
  //tools/rbe/cas-image:push \
  //tools/rbe/worker-init-image:push \
  //tools/rbe/worker-image:push
```

### After Pushing

Update image digests in k8s configs and apply:

```bash
# Get new digests
docker inspect --format='{{index .RepoDigests 0}}' \
  692503192357.dkr.ecr.us-east-1.amazonaws.com/mlrc-gradle-cache/cas:latest \
  692503192357.dkr.ecr.us-east-1.amazonaws.com/mlrc-gradle-cache/worker-init:latest \
  692503192357.dkr.ecr.us-east-1.amazonaws.com/mlrc-gradle-cache/worker:latest

# Apply to cluster
bazel run //tools/rbe/k8s:nativelink.apply --config=local
```

## Updating from Upstream

```bash
cd tools/rbe/nativelink
git fetch origin main
git merge origin/main --no-edit
# Resolve conflicts if any, then rebuild and push
```
