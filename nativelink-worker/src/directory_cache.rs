// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use core::pin::Pin;
use core::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::stream::{self, FuturesUnordered, StreamExt, TryStreamExt};
use nativelink_error::{Error, ResultExt};
use nativelink_proto::build::bazel::remote::execution::v2::Directory as ProtoDirectory;
use nativelink_store::ac_utils::get_and_decode_digest;
use nativelink_store::cas_utils::is_zero_digest;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::{FileEntry, FilesystemStore};
use nativelink_util::common::DigestInfo;
use nativelink_util::fs_util::{calculate_directory_size, hardlink_directory_tree};
#[cfg(test)]
use nativelink_util::store_trait::StoreKey;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, trace, warn};

/// Monotonic logical counter for LRU ordering. Avoids `SystemTime::now()`
/// (cheaper, no clock-skew edge cases) and resets to 1 on process start —
/// fine because inherited on-disk entries are reindexed at startup and freshly
/// inserted entries are always newer than inherited ones in steady state.
static NEXT_ACCESS: AtomicU64 = AtomicU64::new(1);

fn next_access() -> u64 {
    NEXT_ACCESS.fetch_add(1, Ordering::Relaxed)
}

/// Configuration for the directory cache
#[derive(Debug, Clone)]
pub struct DirectoryCacheConfig {
    /// Maximum total size in bytes (0 = unlimited).
    pub max_size_bytes: u64,
    /// Base directory for cache storage
    pub cache_root: PathBuf,
}

impl Default for DirectoryCacheConfig {
    fn default() -> Self {
        Self {
            max_size_bytes: 10 * 1024 * 1024 * 1024, // 10 GB
            cache_root: std::env::temp_dir().join("nativelink_directory_cache"),
        }
    }
}

/// Metadata for a cached directory. Stored as `Arc<CachedDirectoryMetadata>`
/// inside the cache HashMap so concurrent readers can clone out the Arc under
/// a brief read lock and then perform their hardlink walk lock-free.
#[derive(Debug)]
struct CachedDirectoryMetadata {
    /// Canonical on-disk location of the cached tree. Immutable after insert.
    path: PathBuf,
    /// Sum of `FileNode.digest.size_bytes` accumulated during construction.
    /// Immutable after insert.
    size: u64,
    /// Logical LRU clock — bumped on every cache hit.
    last_access: AtomicU64,
    /// Currently-running hardlink operations against this entry. Eviction
    /// skips entries with `ref_count > 0`.
    ref_count: AtomicUsize,
}

impl CachedDirectoryMetadata {
    fn new(path: PathBuf, size: u64) -> Self {
        Self {
            path,
            size,
            last_access: AtomicU64::new(next_access()),
            ref_count: AtomicUsize::new(0),
        }
    }

    fn touch(&self) {
        self.last_access.store(next_access(), Ordering::Release);
    }
}

/// High-performance directory cache that uses hardlinks to avoid repeated
/// directory reconstruction from the CAS.
///
/// When actions need input directories, instead of fetching and reconstructing
/// files from the CAS each time, we:
/// 1. Check if we've already constructed this exact directory (by digest)
/// 2. If yes, hardlink the entire tree to the action's workspace
/// 3. If no, construct it once (hardlinking from `content_path-cas`) and
///    cache for future use.
///
/// Construction never byte-copies: every file lands as a hardlink to the
/// blob already on disk in the filesystem-store fast tier. The cache
/// directory and the filesystem store therefore MUST share a filesystem;
/// the operator wires this up via the pod spec (`/tmp/nativelink` PVC).
/// Cross-filesystem hardlinks return `EXDEV`; the caller
/// (`prepare_action_inputs`) catches the error and falls back to
/// `download_to_directory`, incrementing `directory_cache_fallback`.
#[derive(Debug)]
pub struct DirectoryCache {
    /// Configuration
    config: DirectoryCacheConfig,
    /// Cache mapping digest -> metadata Arc.
    cache: Arc<RwLock<HashMap<DigestInfo, Arc<CachedDirectoryMetadata>>>>,
    /// Per-digest mutex to deduplicate concurrent cold misses.
    construction_locks: Arc<Mutex<HashMap<DigestInfo, Arc<Mutex<()>>>>>,
    /// FastSlowStore — used for `populate_fast_store` before each hardlink.
    cas_store: Arc<FastSlowStore>,
    /// FilesystemStore — backs the fast tier of `cas_store`. We need the
    /// concrete type because `FileEntry::get_file_path_locked` (the only
    /// API that yields a hardlink-able path) lives on the concrete store.
    filesystem_store: Arc<FilesystemStore>,
}

impl DirectoryCache {
    /// Creates a new `DirectoryCache`.
    ///
    /// On startup, scans `cache_root` for entries left by prior processes
    /// (the directory persists across restarts and, in K8s deployments, across
    /// pod lifetimes via pool PVCs). Each entry whose name parses as a valid
    /// `DigestInfo` is sized and inserted into the in-memory LRU so inherited
    /// content participates in the size/entry caps. Entries with foreign names
    /// or non-directory file types are removed best-effort. After indexing,
    /// `evict_if_needed` is invoked with `incoming_size = 0` to bring the
    /// merged set under cap if the inherited footprint exceeds it.
    pub async fn new(
        config: DirectoryCacheConfig,
        cas_store: Arc<FastSlowStore>,
        filesystem_store: Arc<FilesystemStore>,
    ) -> Result<Self, Error> {
        fs::create_dir_all(&config.cache_root).await.err_tip(|| {
            format!(
                "Failed to create cache root: {}",
                config.cache_root.display()
            )
        })?;

        let cache_map = Self::index_existing_entries(&config.cache_root).await?;

        let this = Self {
            config,
            cache: Arc::new(RwLock::new(cache_map)),
            construction_locks: Arc::new(Mutex::new(HashMap::new())),
            cas_store,
            filesystem_store,
        };

        // Enforce the cap on the merged inherited set. `incoming_size = 0`
        // means "make room for nothing new" — `evict_if_needed` simply evicts
        // until `current_size <= max_size_bytes`.
        {
            let mut cache = this.cache.write().await;
            this.evict_if_needed(0, &mut cache).await?;
        }

        let inherited = this.cache.read().await.len();
        if inherited > 0 {
            debug!(
                entries = inherited,
                cache_root = ?this.config.cache_root,
                "DirectoryCache indexed inherited on-disk entries"
            );
        }

        Ok(this)
    }

    /// Walks `cache_root` once and returns an LRU-ready map of well-formed
    /// entries. Foreign entries (unparseable names, non-directories) are
    /// removed so they cannot leak across restarts. `last_access` is set to
    /// `next_access()` for every inherited entry — startup eviction order
    /// between them is unspecified, but freshly-inserted entries will always
    /// look newer once the cache is in steady state.
    ///
    /// Sizing runs concurrently across entries via `buffer_unordered` — full
    /// caches can hold ~1000 entries with ~500 files each, and the sequential
    /// recursive stat walk is the dominant startup cost. Top-level
    /// concurrency keeps in-entry recursion sequential (cheap to reason about)
    /// while saturating the I/O bandwidth that matters.
    async fn index_existing_entries(
        cache_root: &Path,
    ) -> Result<HashMap<DigestInfo, Arc<CachedDirectoryMetadata>>, Error> {
        // Phase 1: enumerate cache_root. Inline-validate each entry; foreign
        // entries are removed immediately and not surfaced to phase 2.
        let mut read_dir = fs::read_dir(cache_root).await.err_tip(|| {
            format!("Failed to read cache root: {}", cache_root.display())
        })?;
        let mut to_size: Vec<(DigestInfo, PathBuf)> = Vec::new();
        loop {
            let entry = match read_dir.next_entry().await {
                Ok(Some(e)) => e,
                Ok(None) => break,
                Err(e) => {
                    warn!(error = ?e, root = ?cache_root, "Failed to enumerate cache root; aborting scan");
                    break;
                }
            };
            let path = entry.path();
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                warn!(?path, "Removing non-UTF8 entry from cache_root");
                remove_foreign_entry(&path).await;
                continue;
            };
            let Some(digest) = parse_digest_dirname(&name) else {
                warn!(?name, "Removing foreign entry from cache_root");
                remove_foreign_entry(&path).await;
                continue;
            };
            match entry.file_type().await {
                Ok(ft) if ft.is_dir() => {}
                Ok(_) => {
                    warn!(?path, "Removing non-directory entry from cache_root");
                    remove_foreign_entry(&path).await;
                    continue;
                }
                Err(e) => {
                    warn!(error = ?e, ?path, "Failed to stat inherited cache entry; removing");
                    remove_foreign_entry(&path).await;
                    continue;
                }
            }
            to_size.push((digest, path));
        }

        // Phase 2: size each valid entry concurrently. Per-entry sizing is a
        // recursive stat walk; top-level parallelism amortizes the fixed
        // syscall latency without overwhelming the filesystem.
        const SIZE_CONCURRENCY: usize = 32;
        let sized: Vec<Option<(DigestInfo, Arc<CachedDirectoryMetadata>)>> = stream::iter(to_size)
            .map(|(digest, path)| async move {
                match calculate_directory_size(&path).await {
                    Ok(size) => Some((digest, Arc::new(CachedDirectoryMetadata::new(path, size)))),
                    Err(e) => {
                        warn!(error = ?e, ?path, "Failed to size inherited cache entry; removing");
                        remove_foreign_entry(&path).await;
                        None
                    }
                }
            })
            .buffer_unordered(SIZE_CONCURRENCY)
            .collect()
            .await;

        Ok(sized.into_iter().flatten().collect())
    }

    /// Gets or creates a directory in the cache, then hardlinks it to the destination
    ///
    /// # Arguments
    /// * `digest` - Digest of the root Directory proto
    /// * `dest_path` - Where to hardlink/create the directory
    ///
    /// # Returns
    /// * `Ok(true)` - Cache hit (directory was hardlinked)
    /// * `Ok(false)` - Cache miss (directory was constructed)
    /// * `Err` - Error during construction or hardlinking
    pub async fn get_or_create(&self, digest: DigestInfo, dest_path: &Path) -> Result<bool, Error> {
        // Fast path: lock-free cache lookup. Bind the Arc-clone to a local
        // BEFORE the if-let so the RwLockReadGuard temporary drops at the
        // semicolon; otherwise `if let Some(_) = self.cache.read().await...`
        // would extend the guard across the entire body and serialize all
        // cache hits on the read lock (the whole point of this layout).
        let cached = self.cache.read().await.get(&digest).cloned();
        if let Some(metadata) = cached {
            debug!(?digest, path = ?metadata.path, "Directory cache HIT");
            metadata.touch();
            metadata.ref_count.fetch_add(1, Ordering::AcqRel);
            let result = hardlink_directory_tree(&metadata.path, dest_path).await;
            metadata.ref_count.fetch_sub(1, Ordering::AcqRel);
            match result {
                Ok(_clone_method) => return Ok(true),
                Err(e) => {
                    warn!(
                        ?digest,
                        error = ?e,
                        "Failed to hardlink from cache, will reconstruct"
                    );
                    // Fall through to reconstruction.
                }
            }
        }

        debug!(?digest, "Directory cache MISS");

        // Stampede protection: only one task constructs a given digest at a time.
        let construction_lock = {
            let mut locks = self.construction_locks.lock().await;
            locks
                .entry(digest)
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _guard = construction_lock.lock().await;

        // Re-check after acquiring the construction lock — another task may
        // have populated the cache while we waited. Same lock-lifetime
        // pattern as above (separate `let` so the read guard drops).
        let cached = self.cache.read().await.get(&digest).cloned();
        if let Some(metadata) = cached {
            metadata.touch();
            metadata.ref_count.fetch_add(1, Ordering::AcqRel);
            let result = hardlink_directory_tree(&metadata.path, dest_path).await;
            metadata.ref_count.fetch_sub(1, Ordering::AcqRel);
            return result.map(|_clone_method| true);
        }

        // Construct the canonical cache entry. `construct_directory` returns
        // the accumulated size — replaces a separate `calculate_directory_size`
        // walk.
        let cache_path = self.get_cache_path(&digest);
        let size = self.construct_directory(digest, &cache_path).await?;

        // Insert + evict under a brief write lock.
        {
            let mut cache = self.cache.write().await;
            self.evict_if_needed(size, &mut cache).await?;
            cache.insert(
                digest,
                Arc::new(CachedDirectoryMetadata::new(cache_path.clone(), size)),
            );
        }

        // Bound the `construction_locks` map. Any task currently waiting on
        // this digest already holds an `Arc<Mutex<()>>` clone, so removing the
        // HashMap entry is safe; new arrivals will see the cache hit on
        // re-check.
        {
            let mut locks = self.construction_locks.lock().await;
            locks.remove(&digest);
        }

        // Hardlink from the canonical entry to the action's dest path —
        // outside any lock.
        hardlink_directory_tree(&cache_path, dest_path)
            .await
            .err_tip(|| "Failed to hardlink newly cached directory")?;

        Ok(false)
    }

    /// Recursively materializes the input tree into `dest_path` by hardlinking
    /// from the filesystem store's `content_path-cas`. Returns the accumulated
    /// logical size (sum of `FileNode.digest.size_bytes`). All entries at a
    /// given level run concurrently via `FuturesUnordered`, matching the
    /// no-cache `download_to_directory` path in `running_actions_manager.rs`.
    fn construct_directory<'a>(
        &'a self,
        digest: DigestInfo,
        dest_path: &'a Path,
    ) -> BoxFuture<'a, Result<u64, Error>> {
        Box::pin(async move {
            trace!(?digest, ?dest_path, "Constructing directory");

            let directory: ProtoDirectory =
                get_and_decode_digest(self.cas_store.as_ref(), digest.into())
                    .await
                    .err_tip(|| format!("Failed to fetch Directory: {digest:?}"))?;

            fs::create_dir_all(dest_path)
                .await
                .err_tip(|| format!("Failed to create directory: {}", dest_path.display()))?;

            let filesystem_store = Pin::new(self.filesystem_store.as_ref());
            let mut futures: FuturesUnordered<BoxFuture<'a, Result<u64, Error>>> =
                FuturesUnordered::new();

            for file in directory.files {
                let dest = dest_path.join(&file.name);
                let file_digest: DigestInfo = file
                    .digest
                    .err_tip(|| "Expected Digest in FileNode")?
                    .try_into()
                    .err_tip(|| "Invalid digest in FileNode")?;
                let size = file_digest.size_bytes();
                let is_executable = file.is_executable;
                futures.push(Box::pin(async move {
                    self.cas_store
                        .populate_fast_store(file_digest.into())
                        .await
                        .err_tip(|| format!("populate_fast_store for {file_digest}"))?;
                    if is_zero_digest(file_digest) {
                        let mut f = fs::File::create(&dest).await.err_tip(|| {
                            format!("Failed to create empty file: {}", dest.display())
                        })?;
                        f.write_all(&[]).await.err_tip(|| {
                            format!("Failed to write empty file: {}", dest.display())
                        })?;
                    } else {
                        let file_entry = filesystem_store
                            .get_file_entry_for_digest(&file_digest)
                            .await
                            .err_tip(|| format!("get_file_entry_for_digest({file_digest})"))?;
                        let src_path = file_entry
                            .get_file_path_locked(|s| async move { Ok(PathBuf::from(s)) })
                            .await?;
                        fs::hard_link(&src_path, &dest).await.err_tip(|| {
                            format!(
                                "hard_link {} -> {}",
                                src_path.display(),
                                dest.display()
                            )
                        })?;
                    }
                    #[cfg(unix)]
                    if is_executable {
                        use std::os::unix::fs::PermissionsExt;
                        let mut perms = fs::metadata(&dest)
                            .await
                            .err_tip(|| "stat for chmod")?
                            .permissions();
                        perms.set_mode(0o755);
                        fs::set_permissions(&dest, perms)
                            .await
                            .err_tip(|| "chmod 0o755")?;
                    }
                    #[cfg(not(unix))]
                    let _ = is_executable;
                    Ok(size)
                }));
            }

            for dir_node in directory.directories {
                let subdir_dest = dest_path.join(&dir_node.name);
                let subdir_digest: DigestInfo = dir_node
                    .digest
                    .err_tip(|| "Expected Digest in DirectoryNode")?
                    .try_into()
                    .err_tip(|| "Invalid digest in DirectoryNode")?;
                futures.push(Box::pin(async move {
                    self.construct_directory(subdir_digest, &subdir_dest).await
                }));
            }

            for symlink in directory.symlinks {
                let link_path = dest_path.join(&symlink.name);
                let target = symlink.target;
                futures.push(Box::pin(async move {
                    #[cfg(unix)]
                    fs::symlink(&target, &link_path).await.err_tip(|| {
                        format!("Failed to create symlink: {}", link_path.display())
                    })?;
                    #[cfg(windows)]
                    fs::symlink_file(&target, &link_path).await.err_tip(|| {
                        format!("Failed to create symlink: {}", link_path.display())
                    })?;
                    Ok(0u64)
                }));
            }

            let mut total = 0u64;
            while let Some(sz) = futures.try_next().await? {
                total = total.saturating_add(sz);
            }
            Ok(total)
        })
    }

    /// Evicts LRU entries until the cache fits under `max_size_bytes`
    /// (`0` = unlimited).
    async fn evict_if_needed(
        &self,
        incoming_size: u64,
        cache: &mut HashMap<DigestInfo, Arc<CachedDirectoryMetadata>>,
    ) -> Result<(), Error> {
        if self.config.max_size_bytes == 0 {
            return Ok(());
        }
        let current_size: u64 = cache.values().map(|m| m.size).sum();
        let mut size_after = current_size.saturating_add(incoming_size);
        while size_after > self.config.max_size_bytes {
            let evicted = self.evict_lru(cache).await?;
            if evicted == 0 {
                // Nothing further is evictable (all entries are in-use, or
                // map is empty). Stop instead of looping forever — the
                // incoming entry will simply land above the cap.
                break;
            }
            size_after = size_after.saturating_sub(evicted);
        }
        Ok(())
    }

    /// Evicts the least recently used entry that is not currently in use.
    async fn evict_lru(
        &self,
        cache: &mut HashMap<DigestInfo, Arc<CachedDirectoryMetadata>>,
    ) -> Result<u64, Error> {
        let to_evict = cache
            .iter()
            .filter(|(_, m)| m.ref_count.load(Ordering::Acquire) == 0)
            .min_by_key(|(_, m)| m.last_access.load(Ordering::Acquire))
            .map(|(digest, _)| *digest);

        if let Some(digest) = to_evict
            && let Some(metadata) = cache.remove(&digest)
        {
            debug!(?digest, size = metadata.size, "Evicting cached directory");

            if let Err(e) = fs::remove_dir_all(&metadata.path).await {
                warn!(
                    ?digest,
                    path = ?metadata.path,
                    error = ?e,
                    "Failed to remove evicted directory from disk"
                );
            }

            return Ok(metadata.size);
        }

        Ok(0)
    }

    /// Gets the cache path for a digest
    fn get_cache_path(&self, digest: &DigestInfo) -> PathBuf {
        self.config.cache_root.join(format!("{digest}"))
    }

    /// Returns cache statistics
    pub async fn stats(&self) -> CacheStats {
        let cache = self.cache.read().await;
        let total_size: u64 = cache.values().map(|m| m.size).sum();
        let in_use = cache
            .values()
            .filter(|m| m.ref_count.load(Ordering::Acquire) > 0)
            .count();

        CacheStats {
            entries: cache.len(),
            total_size_bytes: total_size,
            in_use_entries: in_use,
        }
    }
}

/// Statistics about the directory cache
#[derive(Debug, Clone, Copy)]
pub struct CacheStats {
    pub entries: usize,
    pub total_size_bytes: u64,
    pub in_use_entries: usize,
}

/// Parses a `cache_root` subdirectory name back into a `DigestInfo`. The
/// on-disk layout is fixed by `DirectoryCache::get_cache_path`: each entry is
/// named `{hash}-{size}` where the hash is the hex SHA-256 of the Directory
/// proto and the size is the proto's encoded byte length.
fn parse_digest_dirname(name: &str) -> Option<DigestInfo> {
    let (hash, size) = name.split_once('-')?;
    let size_bytes: u64 = size.parse().ok()?;
    DigestInfo::try_new(hash, size_bytes).ok()
}

/// Best-effort removal of a foreign or corrupt entry under `cache_root`. Tries
/// `remove_dir_all` first, falls back to `remove_file` for non-directory
/// stragglers. Logs and continues on failure — a stray entry is preferable to
/// aborting cache initialization.
async fn remove_foreign_entry(path: &Path) {
    if let Err(dir_err) = fs::remove_dir_all(path).await
        && let Err(file_err) = fs::remove_file(path).await
    {
        warn!(
            ?path,
            ?dir_err,
            ?file_err,
            "Failed to remove foreign cache_root entry"
        );
    }
}

#[cfg(test)]
mod tests {
    use nativelink_config::stores::{
        FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec,
    };
    use nativelink_macro::nativelink_test;
    use nativelink_proto::build::bazel::remote::execution::v2::{
        Directory as ProtoDirectory, FileNode,
    };
    use nativelink_store::fast_slow_store::FastSlowStore;
    use nativelink_store::filesystem_store::FilesystemStore;
    use nativelink_store::memory_store::MemoryStore;
    use nativelink_util::common::DigestInfo;
    use nativelink_util::store_trait::{Store, StoreLike};
    use prost::Message;
    use tempfile::TempDir;

    use super::*;

    /// Wires a production-shape store stack: `FastSlowStore(FilesystemStore,
    /// MemoryStore)`. The filesystem store backs the fast tier so that
    /// `populate_fast_store + get_file_entry_for_digest + fs::hard_link`
    /// behaves the same as it does on the worker. Returns a populated CAS
    /// (file blob + Directory proto) plus the Directory's digest.
    async fn setup_test_store(
        content_path: PathBuf,
        temp_path: PathBuf,
    ) -> Result<(Arc<FastSlowStore>, Arc<FilesystemStore>, DigestInfo), Error> {
        let fast_config = FilesystemSpec {
            content_path: content_path.to_string_lossy().into_owned(),
            temp_path: temp_path.to_string_lossy().into_owned(),
            eviction_policy: None,
            ..Default::default()
        };
        let slow_config = MemorySpec::default();
        let filesystem_store = FilesystemStore::new(&fast_config).await?;
        let memory_store = MemoryStore::new(&slow_config);
        let cas_store = FastSlowStore::new(
            &FastSlowSpec {
                fast: StoreSpec::Filesystem(fast_config),
                slow: StoreSpec::Memory(slow_config),
                fast_direction: StoreDirection::default(),
                slow_direction: StoreDirection::default(),
            },
            Store::new(filesystem_store.clone()),
            Store::new(memory_store.clone()),
        );

        // Upload the file blob.
        let file_content = b"Hello, World!";
        let file_digest = DigestInfo::try_new(
            "dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f",
            13,
        )
        .unwrap();
        cas_store
            .as_pin()
            .update_oneshot(StoreKey::from(file_digest), file_content.to_vec().into())
            .await?;

        // Build + upload the Directory proto.
        let directory = ProtoDirectory {
            files: vec![FileNode {
                name: "test.txt".to_string(),
                digest: Some(file_digest.into()),
                is_executable: false,
                ..Default::default()
            }],
            directories: vec![],
            symlinks: vec![],
            ..Default::default()
        };
        let mut dir_data = Vec::new();
        directory.encode(&mut dir_data).unwrap();
        let dir_digest = DigestInfo::try_new(
            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            dir_data.len() as u64,
        )
        .unwrap();
        cas_store
            .as_pin()
            .update_oneshot(StoreKey::from(dir_digest), dir_data.into())
            .await?;

        Ok((cas_store, filesystem_store, dir_digest))
    }

    /// Minimal dummy stack for tests that exercise startup behavior only
    /// (inherited-entry indexing, eviction, foreign-entry cleanup). The
    /// stores are constructed but never read during the test body, so an
    /// empty MemoryStore-backed fast/slow pair is sufficient.
    async fn setup_dummy_stack(
        content_path: PathBuf,
        temp_path: PathBuf,
    ) -> Result<(Arc<FastSlowStore>, Arc<FilesystemStore>), Error> {
        let fast_config = FilesystemSpec {
            content_path: content_path.to_string_lossy().into_owned(),
            temp_path: temp_path.to_string_lossy().into_owned(),
            eviction_policy: None,
            ..Default::default()
        };
        let slow_config = MemorySpec::default();
        let filesystem_store = FilesystemStore::new(&fast_config).await?;
        let memory_store = MemoryStore::new(&slow_config);
        let cas_store = FastSlowStore::new(
            &FastSlowSpec {
                fast: StoreSpec::Filesystem(fast_config),
                slow: StoreSpec::Memory(slow_config),
                fast_direction: StoreDirection::default(),
                slow_direction: StoreDirection::default(),
            },
            Store::new(filesystem_store.clone()),
            Store::new(memory_store.clone()),
        );
        Ok((cas_store, filesystem_store))
    }

    #[nativelink_test]
    async fn test_directory_cache_basic() -> Result<(), Error> {
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("cache");
        // content_path-cas + cache_root must share a filesystem (TempDir
        // satisfies this since both live under /tmp/<tempdir>).
        let (cas_store, filesystem_store, dir_digest) = setup_test_store(
            temp_dir.path().join("content_path"),
            temp_dir.path().join("temp_path"),
        )
        .await?;

        let config = DirectoryCacheConfig {
            max_size_bytes: 1024 * 1024,
            cache_root,
        };
        let cache = DirectoryCache::new(config, cas_store, filesystem_store).await?;

        // First access — cache miss; entry is constructed via hardlink from
        // content_path-cas.
        let dest1 = temp_dir.path().join("dest1");
        let hit = cache.get_or_create(dir_digest, &dest1).await?;
        assert!(!hit, "First access should be cache miss");
        assert!(dest1.join("test.txt").exists());
        assert_eq!(
            fs::read(dest1.join("test.txt")).await?,
            b"Hello, World!",
            "first-run file content must match the original blob",
        );

        // Second access — cache hit; entry hardlinked from the cached tree.
        let dest2 = temp_dir.path().join("dest2");
        let hit = cache.get_or_create(dir_digest, &dest2).await?;
        assert!(hit, "Second access should be cache hit");
        assert!(dest2.join("test.txt").exists());
        assert_eq!(
            fs::read(dest2.join("test.txt")).await?,
            b"Hello, World!",
            "second-run file content must match the original blob",
        );

        // Verify stats.
        let stats = cache.stats().await;
        assert_eq!(stats.entries, 1);

        Ok(())
    }

    /// Inherited on-disk entries (left by a prior process) are indexed into
    /// the in-memory LRU on construction so they participate in eviction.
    /// Regression for the disk-leak where prior pods' caches survived recycled
    /// PVCs without being tracked by the new process.
    #[nativelink_test]
    async fn indexes_inherited_entries_on_startup() -> Result<(), Error> {
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("cache");
        let (cas_store, filesystem_store) = setup_dummy_stack(
            temp_dir.path().join("content_path"),
            temp_dir.path().join("temp_path"),
        )
        .await?;

        // Simulate a prior process: write a well-formed entry directly on
        // disk under cache_root with the digest-as-dirname layout.
        let inherited_digest = DigestInfo::try_new(
            "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
            42,
        )
        .unwrap();
        let inherited_path = cache_root.join(format!("{inherited_digest}"));
        fs::create_dir_all(&inherited_path).await?;
        fs::write(inherited_path.join("inherited.txt"), b"prior process payload").await?;

        let config = DirectoryCacheConfig {
            max_size_bytes: 1024 * 1024,
            cache_root: cache_root.clone(),
        };
        let cache = DirectoryCache::new(config, cas_store, filesystem_store).await?;

        let stats = cache.stats().await;
        assert_eq!(stats.entries, 1, "inherited entry should be indexed");
        assert!(
            stats.total_size_bytes > 0,
            "inherited size should be accounted for"
        );
        assert!(inherited_path.exists(), "inherited dir should not be deleted");

        Ok(())
    }

    /// When the on-disk footprint exceeds `max_size_bytes`, startup eviction
    /// brings the cache under the cap. Without this, inherited entries would
    /// leak forever.
    #[nativelink_test]
    async fn enforces_size_cap_on_inherited_entries() -> Result<(), Error> {
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("cache");
        let (cas_store, filesystem_store) = setup_dummy_stack(
            temp_dir.path().join("content_path"),
            temp_dir.path().join("temp_path"),
        )
        .await?;

        // Three inherited entries, each 1 KiB. Cap is 1500 bytes so two
        // must be evicted during startup.
        for i in 0..3u8 {
            let hash = format!(
                "{:02x}cdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
                i
            );
            let digest = DigestInfo::try_new(&hash, 1).unwrap();
            let p = cache_root.join(format!("{digest}"));
            fs::create_dir_all(&p).await?;
            fs::write(p.join("blob"), vec![0u8; 1024]).await?;
        }

        let config = DirectoryCacheConfig {
            max_size_bytes: 1500,
            cache_root: cache_root.clone(),
        };
        let cache = DirectoryCache::new(config, cas_store, filesystem_store).await?;

        let stats = cache.stats().await;
        assert_eq!(stats.entries, 1, "cap should evict down to fit 1500 bytes");
        assert!(
            stats.total_size_bytes <= 1500,
            "post-eviction size {} should be <= cap",
            stats.total_size_bytes
        );
        // Verify the evicted entries were actually removed from disk —
        // otherwise the leak isn't fixed.
        let remaining = std::fs::read_dir(&cache_root)?.count();
        assert_eq!(remaining, 1, "evicted dirs should be removed from disk");

        Ok(())
    }

    /// Entries with unparseable names (not `{hash}-{size}`) under `cache_root`
    /// are removed at startup so they cannot accumulate indefinitely.
    #[nativelink_test]
    async fn removes_foreign_entries_on_startup() -> Result<(), Error> {
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("cache");
        fs::create_dir_all(&cache_root).await?;
        // A directory with a non-digest name (e.g. left by an operator).
        let foreign_dir = cache_root.join("not-a-digest");
        fs::create_dir_all(&foreign_dir).await?;
        fs::write(foreign_dir.join("junk"), b"x").await?;
        // A loose file directly under cache_root.
        fs::write(cache_root.join("stray-file"), b"y").await?;

        let (cas_store, filesystem_store) = setup_dummy_stack(
            temp_dir.path().join("content_path"),
            temp_dir.path().join("temp_path"),
        )
        .await?;
        let config = DirectoryCacheConfig {
            max_size_bytes: 1024 * 1024,
            cache_root: cache_root.clone(),
        };
        let _cache = DirectoryCache::new(config, cas_store, filesystem_store).await?;

        assert!(!foreign_dir.exists(), "foreign directory should be removed");
        assert!(
            !cache_root.join("stray-file").exists(),
            "stray file should be removed"
        );

        Ok(())
    }
}
