# NativeLink Build & Deploy

Fork of [TraceMachina/nativelink](https://github.com/TraceMachina/nativelink) with custom patches.

## Current Patches

- **ByteStream Write resume fix**: Accept data from offset 0 on UUID collision
- **Directory cache fix**: Skip work dir pre-creation when directory cache is enabled to prevent hardlink failures

## Branch

`fix/bytestream-write-resume-v3` — based on upstream `main`, rebased periodically.

## Building and Pushing Images

All three images are built via Bazel for `linux_amd64`. Pushing uses `--config=oci-push` which sets exec platform to macOS so crane/jq resolve to darwin binaries. ECR credentials are handled automatically via the Docker credential helper.

### Full E2E workflow (Bazel-native)

```bash
# 1. Build all images (heavy Rust compilation runs on RBE linux workers)
bazel build \
  //tools/rbe/cas-image:image \
  //tools/rbe/worker-init-image:image \
  //tools/rbe/worker-image:image \
  --platforms=//tools/bazel/oci:linux_amd64

# 2. Push to ECR (--config=oci-push runs the push script locally with darwin crane)
bazel run //tools/rbe/cas-image:push --config=oci-push
bazel run //tools/rbe/worker-init-image:push --config=oci-push
bazel run //tools/rbe/worker-image:push --config=oci-push

# 3. Generate manifests stamped with the built image digests
bazel build //tools/rbe/k8s:cas_deployment_stamped \
            //tools/rbe/k8s:worker_provisioner_deployment_stamped \
            //tools/rbe/k8s:worker_specs_stamped

# Outputs:
#   bazel-bin/tools/rbe/k8s/cas-deployment-stamped.yaml
#   bazel-bin/tools/rbe/k8s/worker-provisioner-deployment-stamped.yaml
#   bazel-bin/tools/rbe/k8s/worker-specs-stamped.json
```

> **Digest stamping**: OCI digests are content-addressable (sha256 of image content), so they're known before pushing. The stamped manifests are correct before step 2 completes — whatever digest appears in the manifest is exactly what ECR stores after push.

### `--config=oci-push` explained

The generated `oci_push` script bundles crane/jq via exec-platform toolchain resolution. With the default config (exec=linux, RBE), it bundles linux binaries that can't run on macOS.

`--config=oci-push` sets `--host_platform=macos_arm64` → exec platform becomes macOS → crane/jq resolve to darwin arm64 binaries. The NativeLink binary (target config, linux/amd64) is reused from disk cache — its cache key excludes exec platform.

Requires `oci.toolchains()` in MODULE.bazel (already added) which registers the darwin crane binary for lazy fetching.

### Images

| Repo | Content | Build target |
|------|---------|--------------|
| `mlrc-gradle-cache/cas` | NativeLink binary on `al2023_minimal` | `//tools/rbe/cas-image:image` |
| `mlrc-gradle-cache/worker-init` | CAS image + `copy_nativelink` Go binary | `//tools/rbe/worker-init-image:image` |
| `mlrc-gradle-cache/worker` | `al2023_minimal` + worker user (uid 1000) | `//tools/rbe/worker-image:image` |

### Fallback: system crane

If `--config=oci-push` has issues (first run fetching darwin crane), use system crane directly:

```bash
IMAGE_DIR=bazel-bin/tools/rbe/cas-image/image
REPO=692503192357.dkr.ecr.us-east-1.amazonaws.com/mlrc-gradle-cache/cas
DIGEST=$(jq -r '.manifests[0].digest' "$IMAGE_DIR/index.json")
crane push "$IMAGE_DIR" "$REPO@$DIGEST" --image-refs /tmp/cas-refs.txt
crane tag $(cat /tmp/cas-refs.txt) latest
# Repeat for worker-init and worker images
```

## Updating from Upstream

```bash
cd tools/rbe/nativelink
git fetch origin main
git merge origin/main --no-edit
# Resolve conflicts if any
# Rebuild and push
```
