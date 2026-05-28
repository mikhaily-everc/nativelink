# NativeLink Build & Deploy

Fork of [TraceMachina/nativelink](https://github.com/TraceMachina/nativelink) with custom patches.

## Current Patches

- **ByteStream Write resume fix**: Accept data from offset 0 on UUID collision
- **Directory cache fix**: Skip work dir pre-creation when directory cache is enabled to prevent hardlink failures
- **REAPI SplitBlob/SpliceBlob**: Implements the `ContentAddressableStorage.SplitBlob`/`SpliceBlob`
  RPCs (and the matching `CacheCapabilities.split_blob_support`/`splice_blob_support` flags) so
  Bazel 9.1.0 clients can use `--experimental_remote_cache_chunking`. Opt-in per-instance via
  `CasStoreConfig.splice_manifest_store`; when unset the RPCs return `UNIMPLEMENTED` and
  capabilities advertise `false`. Manifests are bincode `DedupIndex`es stored in the manifest
  backend; `SpliceBlob` verifies chunk presence + reassembled digest before persisting.

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

### Build, Push & Deploy

One-shot deploy — `//tools/rbe/k8s:nativelink.apply` (rules_gitops) depends on
the three OCI push targets, so building, pushing to ECR, injecting fresh
digests, and `kubectl apply` all happen in a single invocation:

```bash
aspect rbe
```

Do NOT run `aspect run //tools/rbe/oci:push_all` or `aspect build` on the
images first — that's redundant work the apply target already performs.

The underlying targets (for reference / debugging only):

| Repo | Build target |
|------|--------------|
| `mlrc-gradle-cache/cas` | `//tools/rbe/cas-image:image` |
| `mlrc-gradle-cache/worker-init` | `//tools/rbe/worker-init-image:image` |
| `mlrc-gradle-cache/worker` | `//tools/rbe/worker-image:image` |

After editing `tools/rbe/k8s/configs/worker-specs/worker-specs.json` or `configs/default-pod-spec-template/pod-spec.yaml`, the apply updates the ConfigMap but does not restart the worker-provisioner — `kubectl -n mlrc-gradle-cache rollout restart deployment/worker-provisioner` to pick up the new spec.

## Updating from Upstream

```bash
cd tools/rbe/nativelink
git fetch origin main
git merge origin/main --no-edit
# Resolve conflicts if any, then rebuild and push
```
