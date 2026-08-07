// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use core::convert::Into;
use core::pin::Pin;
use core::time::Duration;
use std::collections::{HashMap, VecDeque};

use bytes::{Bytes, BytesMut};
use futures::stream::{FuturesUnordered, Stream};
use futures::{StreamExt, TryStreamExt};
use nativelink_config::cas_server::{CasStoreConfig, WithInstanceName};
use nativelink_error::{Code, Error, ResultExt, error_if, make_err, make_input_err};
use nativelink_proto::build::bazel::remote::execution::v2::content_addressable_storage_server::{
    ContentAddressableStorage, ContentAddressableStorageServer as Server,
};
use nativelink_proto::build::bazel::remote::execution::v2::{
    BatchReadBlobsRequest, BatchReadBlobsResponse, BatchUpdateBlobsRequest,
    BatchUpdateBlobsResponse, Digest, Directory, FindMissingBlobsRequest, FindMissingBlobsResponse,
    GetTreeRequest, GetTreeResponse, SpliceBlobRequest, SpliceBlobResponse, SplitBlobRequest,
    SplitBlobResponse, batch_read_blobs_response, batch_update_blobs_response, chunking_function,
    compressor,
};
use nativelink_proto::google::rpc::Status as GrpcStatus;
use nativelink_store::ac_utils::get_and_decode_digest;
use nativelink_store::compression_store::WincodeConfig;
use nativelink_store::dedup_store::DedupIndex;
use nativelink_store::grpc_store::GrpcStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{
    DigestHasher, DigestHasherFunc, default_digest_hasher_func, make_ctx_for_hash_func,
};
use nativelink_util::store_trait::{Store, StoreLike};
use opentelemetry::context::Context;
use opentelemetry::context::FutureExt;
use tonic::{Request, Response, Status};
use tracing::{Instrument, Level, debug, error_span, instrument};

/// Per-instance handle bundle consumed by SplitBlob/SpliceBlob.
#[derive(Debug, Clone)]
struct CasInstance {
    cas_store: Store,
    /// Optional side-table store for REAPI SplitBlob/SpliceBlob manifests
    /// (see `CasStoreConfig::splice_manifest_store`).
    manifest_store: Option<Store>,
}

#[derive(Debug)]
pub struct CasServer {
    instances: HashMap<String, CasInstance>,
}

type GetTreeStream = Pin<Box<dyn Stream<Item = Result<GetTreeResponse, Status>> + Send + 'static>>;

/// Per-blob deadline applied inside `BatchReadBlobs` / `BatchUpdateBlobs`.
const BATCH_PER_BLOB_TIMEOUT: Duration = Duration::from_secs(30);

impl CasServer {
    pub fn new(
        configs: &[WithInstanceName<CasStoreConfig>],
        store_manager: &StoreManager,
    ) -> Result<Self, Error> {
        let mut instances = HashMap::with_capacity(configs.len());
        for config in configs {
            let cas_store = store_manager.get_store(&config.cas_store).ok_or_else(|| {
                make_input_err!("'cas_store': '{}' does not exist", config.cas_store)
            })?;
            let manifest_store = match config.splice_manifest_store.as_ref() {
                Some(name) => Some(store_manager.get_store(name).ok_or_else(|| {
                    make_input_err!("'splice_manifest_store': '{name}' does not exist")
                })?),
                None => None,
            };
            instances.insert(
                config.instance_name.clone(),
                CasInstance {
                    cas_store,
                    manifest_store,
                },
            );
        }
        Ok(Self { instances })
    }

    pub fn into_service(self) -> Server<Self> {
        Server::new(self)
    }

    /// Returns the set of instance names that have a `splice_manifest_store`
    /// configured and therefore support SplitBlob/SpliceBlob.
    pub fn chunking_instances(&self) -> HashMap<String, bool> {
        self.instances
            .iter()
            .map(|(name, instance)| (name.clone(), instance.manifest_store.is_some()))
            .collect()
    }

    async fn inner_find_missing_blobs(
        &self,
        request: FindMissingBlobsRequest,
    ) -> Result<Response<FindMissingBlobsResponse>, Error> {
        let instance_name = &request.instance_name;
        let store = self
            .instances
            .get(instance_name)
            .err_tip(|| format!("'instance_name' not configured for '{instance_name}'"))?
            .cas_store
            .clone();

        let mut requested_blobs = Vec::with_capacity(request.blob_digests.len());
        for digest in &request.blob_digests {
            requested_blobs.push(DigestInfo::try_from(digest.clone())?.into());
        }
        let sizes = store
            .has_many(&requested_blobs)
            .await
            .err_tip(|| "In find_missing_blobs")?;
        let missing_blob_digests = sizes
            .into_iter()
            .zip(request.blob_digests)
            .filter_map(|(maybe_size, digest)| maybe_size.map_or_else(|| Some(digest), |_| None))
            .collect();

        Ok(Response::new(FindMissingBlobsResponse {
            missing_blob_digests,
        }))
    }

    async fn inner_batch_update_blobs(
        &self,
        request: BatchUpdateBlobsRequest,
    ) -> Result<Response<BatchUpdateBlobsResponse>, Error> {
        let instance_name = &request.instance_name;

        let store = self
            .instances
            .get(instance_name)
            .err_tip(|| format!("'instance_name' not configured for '{instance_name}'"))?
            .cas_store
            .clone();

        // If we are a GrpcStore we shortcut here, as this is a special store.
        // Note: We don't know the digests here, so we try perform a very shallow
        // check to see if it's a grpc store.
        if let Some(grpc_store) = store.downcast_ref::<GrpcStore>(None) {
            return grpc_store.batch_update_blobs(Request::new(request)).await;
        }

        let store_ref = &store;
        let update_futures: FuturesUnordered<_> = request
            .requests
            .into_iter()
            .map(|request| async move {
                let digest = request
                    .digest
                    .clone()
                    .err_tip(|| "Digest not found in request")?;
                let request_data = request.data;
                let digest_info = DigestInfo::try_from(digest.clone())?;
                let size_bytes = usize::try_from(digest_info.size_bytes())
                    .err_tip(|| "Digest size_bytes was not convertible to usize")?;
                error_if!(
                    size_bytes != request_data.len(),
                    "Digest for upload had mismatching sizes, digest said {} data  said {}",
                    size_bytes,
                    request_data.len()
                );
                // Apply a per-blob deadline so one slow upload does not
                // make the whole batch hit the client's overall deadline.
                let result = match tokio::time::timeout(
                    BATCH_PER_BLOB_TIMEOUT,
                    store_ref.update_oneshot(digest_info, request_data),
                )
                .await
                {
                    Ok(r) => r.err_tip(|| "Error writing to store"),
                    Err(_elapsed) => Err(make_err!(
                        Code::DeadlineExceeded,
                        "BatchUpdateBlobs per-blob timeout ({} s) elapsed for digest {}",
                        BATCH_PER_BLOB_TIMEOUT.as_secs(),
                        digest_info,
                    )),
                };
                Ok::<_, Error>(batch_update_blobs_response::Response {
                    digest: Some(digest),
                    status: Some(result.map_or_else(Into::into, |()| GrpcStatus::default())),
                })
            })
            .collect();
        let responses = update_futures
            .try_collect::<Vec<batch_update_blobs_response::Response>>()
            .await?;

        Ok(Response::new(BatchUpdateBlobsResponse { responses }))
    }

    async fn inner_batch_read_blobs(
        &self,
        request: BatchReadBlobsRequest,
    ) -> Result<Response<BatchReadBlobsResponse>, Error> {
        let instance_name = &request.instance_name;

        let store = self
            .instances
            .get(instance_name)
            .err_tip(|| format!("'instance_name' not configured for '{instance_name}'"))?
            .cas_store
            .clone();

        // If we are a GrpcStore we shortcut here, as this is a special store.
        // Note: We don't know the digests here, so we try perform a very shallow
        // check to see if it's a grpc store.
        if let Some(grpc_store) = store.downcast_ref::<GrpcStore>(None) {
            return grpc_store.batch_read_blobs(Request::new(request)).await;
        }

        let store_ref = &store;
        let read_futures: FuturesUnordered<_> = request
            .digests
            .into_iter()
            .map(|digest| async move {
                let digest_copy = DigestInfo::try_from(digest.clone())?;
                // TODO(palfrey) There is a security risk here of someone taking all the memory on the instance.
                // Apply a per-blob deadline so one slow read does not
                // make the whole batch hit the client's overall deadline.
                let result = match tokio::time::timeout(
                    BATCH_PER_BLOB_TIMEOUT,
                    store_ref.get_part_unchunked(digest_copy, 0, None),
                )
                .await
                {
                    Ok(r) => r.err_tip(|| "Error reading from store"),
                    Err(_elapsed) => Err(make_err!(
                        Code::DeadlineExceeded,
                        "BatchReadBlobs per-blob timeout ({} s) elapsed for digest {}",
                        BATCH_PER_BLOB_TIMEOUT.as_secs(),
                        digest_copy,
                    )),
                };
                let (status, data) = result.map_or_else(
                    |mut e| {
                        if e.code == Code::NotFound {
                            // Trim the error code. Not Found is quite common and we don't want to send a large
                            // error (debug) message for something that is common. We resize to just the last
                            // message as it will be the most relevant.
                            e.messages.resize_with(1, String::new);
                        }
                        (e.into(), Bytes::new())
                    },
                    |v| (GrpcStatus::default(), v),
                );
                Ok::<_, Error>(batch_read_blobs_response::Response {
                    status: Some(status),
                    digest: Some(digest),
                    compressor: compressor::Value::Identity.into(),
                    data,
                })
            })
            .collect();
        let responses = read_futures
            .try_collect::<Vec<batch_read_blobs_response::Response>>()
            .await?;

        Ok(Response::new(BatchReadBlobsResponse { responses }))
    }

    async fn inner_get_tree(
        &self,
        request: GetTreeRequest,
    ) -> Result<impl Stream<Item = Result<GetTreeResponse, Status>> + Send + use<>, Error> {
        let instance_name = &request.instance_name;

        let store = self
            .instances
            .get(instance_name)
            .err_tip(|| format!("'instance_name' not configured for '{instance_name}'"))?
            .cas_store
            .clone();

        // If we are a GrpcStore we shortcut here, as this is a special store.
        // Note: We don't know the digests here, so we try perform a very shallow
        // check to see if it's a grpc store.
        if let Some(grpc_store) = store.downcast_ref::<GrpcStore>(None) {
            let stream = grpc_store
                .get_tree(Request::new(request))
                .await?
                .into_inner();
            return Ok(stream.left_stream());
        }
        let root_digest: DigestInfo = request
            .root_digest
            .err_tip(|| "Expected root_digest to exist in GetTreeRequest")?
            .try_into()
            .err_tip(|| "In GetTreeRequest::root_digest")?;

        let mut deque: VecDeque<DigestInfo> = VecDeque::new();
        let mut directories: Vec<Directory> = Vec::new();
        // `page_token` will return the `{hash_str}-{size_bytes}` of the current request's first directory digest.
        let page_token_digest = if request.page_token.is_empty() {
            root_digest
        } else {
            let mut page_token_parts = request.page_token.split('-');
            DigestInfo::try_new(
                page_token_parts
                    .next()
                    .err_tip(|| "Failed to parse `hash_str` in `page_token`")?,
                page_token_parts
                    .next()
                    .err_tip(|| "Failed to parse `size_bytes` in `page_token`")?
                    .parse::<i64>()
                    .err_tip(|| "Failed to parse `size_bytes` as i64")?,
            )
            .err_tip(|| "Failed to parse `page_token` as `Digest` in `GetTreeRequest`")?
        };
        let page_size = request.page_size;
        // If `page_size` is 0, paging is not necessary.
        let mut page_token_matched = page_size == 0;
        deque.push_back(root_digest);

        while !deque.is_empty() {
            let digest: DigestInfo = deque.pop_front().err_tip(|| "In VecDeque::pop_front")?;
            let directory = get_and_decode_digest::<Directory>(&store, digest.into())
                .await
                .err_tip(|| "Converting digest to Directory")?;
            if digest == page_token_digest {
                page_token_matched = true;
            }
            for directory in &directory.directories {
                let digest: DigestInfo = directory
                    .digest
                    .clone()
                    .err_tip(|| "Expected Digest to exist in Directory::directories::digest")?
                    .try_into()
                    .err_tip(|| "In Directory::file::digest")?;
                deque.push_back(digest);
            }

            let page_size_usize = usize::try_from(page_size).unwrap_or(usize::MAX);

            if page_token_matched {
                directories.push(directory);
                if directories.len() == page_size_usize {
                    break;
                }
            }
        }
        // `next_page_token` will return the `{hash_str}:{size_bytes}` of the next request's first directory digest.
        // It will be an empty string when it reached the end of the directory tree.
        let next_page_token: String = deque
            .front()
            .map_or_else(String::new, |value| format!("{value}"));

        Ok(futures::stream::once(async {
            Ok(GetTreeResponse {
                directories,
                next_page_token,
            })
        })
        .right_stream())
    }

    async fn inner_split_blob(
        &self,
        request: SplitBlobRequest,
    ) -> Result<Response<SplitBlobResponse>, Error> {
        let instance_name = &request.instance_name;
        let instance = self
            .instances
            .get(instance_name)
            .err_tip(|| format!("'instance_name' not configured for '{instance_name}'"))?;
        let manifest_store = instance.manifest_store.as_ref().ok_or_else(|| {
            make_err!(
                Code::Unimplemented,
                "SplitBlob is not enabled for instance '{instance_name}'"
            )
        })?;
        let cas_store = &instance.cas_store;

        let blob_digest_proto = request
            .blob_digest
            .err_tip(|| "Expected blob_digest in SplitBlobRequest")?;
        let blob_digest = DigestInfo::try_from(blob_digest_proto.clone())
            .err_tip(|| "Invalid blob_digest in SplitBlobRequest")?;

        let index_bytes = manifest_store
            .get_part_unchunked(blob_digest, 0, None)
            .await
            .map_err(|mut err| {
                if err.code == Code::NotFound {
                    err.messages.resize_with(1, String::new);
                }
                err
            })
            .err_tip(|| "Reading splice manifest")?;
        let manifest: DedupIndex = wincode::config::deserialize::<DedupIndex, WincodeConfig>(
            &index_bytes,
            WincodeConfig::new(),
        )
        .map_err(|e| make_err!(Code::Internal, "Corrupt splice manifest: {e}"))?;

        let chunk_keys: Vec<_> = manifest.entries.iter().map(|d| (*d).into()).collect();
        let presence = cas_store
            .has_many(&chunk_keys)
            .await
            .err_tip(|| "In SplitBlob chunk presence check")?;
        if let Some(missing_idx) = presence.iter().position(Option::is_none) {
            return Err(make_err!(
                Code::NotFound,
                "Chunk {} referenced by blob {} is missing from the CAS",
                manifest.entries[missing_idx],
                blob_digest
            ));
        }

        let chunk_digests: Vec<Digest> = manifest.entries.into_iter().map(Into::into).collect();
        Ok(Response::new(SplitBlobResponse {
            chunk_digests,
            chunking_function: chunking_function::Value::Unknown as i32,
        }))
    }

    async fn inner_splice_blob(
        &self,
        request: SpliceBlobRequest,
    ) -> Result<Response<SpliceBlobResponse>, Error> {
        let instance_name = &request.instance_name;
        let instance = self
            .instances
            .get(instance_name)
            .err_tip(|| format!("'instance_name' not configured for '{instance_name}'"))?;
        let manifest_store = instance.manifest_store.as_ref().ok_or_else(|| {
            make_err!(
                Code::Unimplemented,
                "SpliceBlob is not enabled for instance '{instance_name}'"
            )
        })?;
        let cas_store = &instance.cas_store;

        let blob_digest_proto = request
            .blob_digest
            .err_tip(|| "Expected blob_digest in SpliceBlobRequest")?;
        let expected_blob_digest = DigestInfo::try_from(blob_digest_proto.clone())
            .err_tip(|| "Invalid blob_digest in SpliceBlobRequest")?;

        let mut chunk_digests = Vec::with_capacity(request.chunk_digests.len());
        for chunk in request.chunk_digests {
            chunk_digests.push(
                DigestInfo::try_from(chunk)
                    .err_tip(|| "Invalid chunk digest in SpliceBlobRequest")?,
            );
        }

        let expected_size: u64 = chunk_digests.iter().map(DigestInfo::size_bytes).sum();
        if expected_size != expected_blob_digest.size_bytes() {
            return Err(make_err!(
                Code::InvalidArgument,
                "Sum of chunk sizes ({expected_size}) does not match blob size ({})",
                expected_blob_digest.size_bytes()
            ));
        }

        let chunk_keys: Vec<_> = chunk_digests.iter().map(|d| (*d).into()).collect();
        let chunk_presence = cas_store
            .has_many(&chunk_keys)
            .await
            .err_tip(|| "In SpliceBlob chunk presence check")?;
        let missing: Vec<DigestInfo> = chunk_presence
            .iter()
            .zip(&chunk_digests)
            .filter_map(|(present, digest)| present.is_none().then_some(*digest))
            .collect();
        if !missing.is_empty() {
            return Err(make_err!(
                Code::FailedPrecondition,
                "SpliceBlob missing chunks from CAS: {}",
                missing
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }

        // Manifest already persisted → short-circuit (spec-legal no-op).
        if manifest_store
            .has(expected_blob_digest)
            .await
            .err_tip(|| "Checking existing splice manifest")?
            .is_some()
        {
            return Ok(Response::new(SpliceBlobResponse {
                blob_digest: Some(blob_digest_proto),
            }));
        }

        // Always re-fetch and verify the chunks before persisting either the blob
        // or the manifest. The REAPI spec forbids trusting client-supplied
        // digests, and a CAS hit on `expected_blob_digest` from a non-splice
        // upload path tells us nothing about whether *these chunks* reassemble
        // to that blob — the manifest must only attest a relationship we
        // re-verified on this request.
        let blob_already_cached = cas_store
            .has(expected_blob_digest)
            .await
            .err_tip(|| "Checking existing spliced blob in CAS")?
            .is_some();

        let expected_size_usize = usize::try_from(expected_blob_digest.size_bytes())
            .err_tip(|| "Blob size does not fit into usize")?;
        let mut buffer = BytesMut::with_capacity(expected_size_usize.min(64 * 1024 * 1024));
        // Must come from the OTel ctx that `splice_blob` installed from the
        // request's `digest_function`, NOT `default_digest_hasher_func()` — that
        // is only the fallback when the client requested none, and it defaults to
        // sha256. Hashing BLAKE3 chunks with sha256 yields a same-length digest,
        // so the mismatch surfaces as bogus DataLoss "corrupted in CAS" rather
        // than as a hash-function error. Same failure as the VerifyStore one
        // fixed in grpc_store.rs.
        let request_hasher = Context::current()
            .get::<DigestHasherFunc>()
            .map_or_else(default_digest_hasher_func, |v| *v);
        let mut hasher = request_hasher.hasher();
        for (idx, chunk_digest) in chunk_digests.iter().enumerate() {
            let data = cas_store
                .get_part_unchunked(*chunk_digest, 0, None)
                .await
                .err_tip(|| format!("Fetching chunk {chunk_digest} during SpliceBlob"))?;
            if data.len() as u64 != chunk_digest.size_bytes() {
                return Err(make_err!(
                    Code::DataLoss,
                    "Chunk {chunk_digest} returned {} bytes, expected {}",
                    data.len(),
                    chunk_digest.size_bytes()
                ));
            }
            // Per-chunk content verification: a same-length-different-content
            // corruption (e.g. from a concurrent ByteStream.Write race on the
            // same UUID) would otherwise only surface at the whole-blob hash
            // step below, with no signal which chunk is poisoned.
            let mut chunk_hasher = request_hasher.hasher();
            chunk_hasher.update(&data);
            let computed_chunk = chunk_hasher.finalize_digest();
            if computed_chunk != *chunk_digest {
                return Err(make_err!(
                    Code::DataLoss,
                    "Chunk {idx} of blob {expected_blob_digest} corrupted in CAS: \
                     stored bytes hash to {computed_chunk}, claimed digest is \
                     {chunk_digest}. Evict and re-upload to repair."
                ));
            }
            hasher.update(&data);
            buffer.extend_from_slice(&data);
        }
        let computed = hasher.finalize_digest();
        if computed != expected_blob_digest {
            return Err(make_err!(
                Code::InvalidArgument,
                "Reassembled digest {computed} does not match expected {expected_blob_digest}"
            ));
        }

        if !blob_already_cached {
            // Persist the reassembled blob so non-chunking clients can still
            // fetch it via BatchReadBlobs / ByteStream.Read.
            cas_store
                .update_oneshot(expected_blob_digest, buffer.freeze())
                .await
                .err_tip(|| "Persisting spliced blob")?;
        }

        let manifest = DedupIndex {
            entries: chunk_digests,
        };
        let encoded = wincode::config::serialize(&manifest, WincodeConfig::new())
            .map_err(|e| make_err!(Code::Internal, "Encoding splice manifest: {e}"))?;
        manifest_store
            .update_oneshot(expected_blob_digest, encoded.into())
            .await
            .err_tip(|| "Persisting splice manifest")?;

        Ok(Response::new(SpliceBlobResponse {
            blob_digest: Some(blob_digest_proto),
        }))
    }
}

#[tonic::async_trait]
impl ContentAddressableStorage for CasServer {
    type GetTreeStream = GetTreeStream;

    #[instrument(
        err,
        ret(level = Level::DEBUG),
        level = Level::ERROR,
        skip_all,
        fields(
            // Mostly to skip request.blob_digests which is sometimes enormous
            request.instance_name = ?grpc_request.get_ref().instance_name,
            request.digest_function = ?grpc_request.get_ref().digest_function
        )
    )]
    async fn find_missing_blobs(
        &self,
        grpc_request: Request<FindMissingBlobsRequest>,
    ) -> Result<Response<FindMissingBlobsResponse>, Status> {
        let request = grpc_request.into_inner();
        let digest_function = request.digest_function;
        self.inner_find_missing_blobs(request)
            .instrument(error_span!("cas_server_find_missing_blobs"))
            .with_context(
                make_ctx_for_hash_func(digest_function)
                    .err_tip(|| "In CasServer::find_missing_blobs")?,
            )
            .await
            .err_tip(|| "Failed on find_missing_blobs() command")
            .map_err(Into::into)
    }

    #[instrument(
        err,
        ret(level = Level::DEBUG),
        level = Level::ERROR,
        skip_all,
        fields(request = ?grpc_request.get_ref())
    )]
    async fn batch_update_blobs(
        &self,
        grpc_request: Request<BatchUpdateBlobsRequest>,
    ) -> Result<Response<BatchUpdateBlobsResponse>, Status> {
        let request = grpc_request.into_inner();
        let digest_function = request.digest_function;

        self.inner_batch_update_blobs(request)
            .instrument(error_span!("cas_server_batch_update_blobs"))
            .with_context(
                make_ctx_for_hash_func(digest_function)
                    .err_tip(|| "In CasServer::batch_update_blobs")?,
            )
            .await
            .err_tip(|| "Failed on batch_update_blobs() command")
            .map_err(Into::into)
    }

    #[instrument(
        err,
        ret(level = Level::INFO),
        level = Level::ERROR,
        skip_all,
        fields(request = ?grpc_request.get_ref())
    )]
    async fn batch_read_blobs(
        &self,
        grpc_request: Request<BatchReadBlobsRequest>,
    ) -> Result<Response<BatchReadBlobsResponse>, Status> {
        let request = grpc_request.into_inner();
        let digest_function = request.digest_function;

        self.inner_batch_read_blobs(request)
            .instrument(error_span!("cas_server_batch_read_blobs"))
            .with_context(
                make_ctx_for_hash_func(digest_function)
                    .err_tip(|| "In CasServer::batch_read_blobs")?,
            )
            .await
            .err_tip(|| "Failed on batch_read_blobs() command")
            .map_err(Into::into)
    }

    #[instrument(
        err,
        level = Level::ERROR,
        skip_all,
        fields(request = ?grpc_request.get_ref())
    )]
    async fn get_tree(
        &self,
        grpc_request: Request<GetTreeRequest>,
    ) -> Result<Response<Self::GetTreeStream>, Status> {
        let request = grpc_request.into_inner();
        let digest_function = request.digest_function;

        let resp = self
            .inner_get_tree(request)
            .instrument(error_span!("cas_server_get_tree"))
            .with_context(
                make_ctx_for_hash_func(digest_function).err_tip(|| "In CasServer::get_tree")?,
            )
            .await
            .err_tip(|| "Failed on get_tree() command")
            .map(|stream| -> Response<Self::GetTreeStream> { Response::new(Box::pin(stream)) })
            .map_err(Into::into);

        if resp.is_ok() {
            debug!(return = "Ok(<stream>)");
        }
        resp
    }

    #[instrument(
        err,
        ret(level = Level::DEBUG),
        level = Level::ERROR,
        skip_all,
        fields(request = ?grpc_request.get_ref())
    )]
    async fn split_blob(
        &self,
        grpc_request: Request<SplitBlobRequest>,
    ) -> Result<Response<SplitBlobResponse>, Status> {
        let request = grpc_request.into_inner();
        let digest_function = request.digest_function;
        self.inner_split_blob(request)
            .instrument(error_span!("cas_server_split_blob"))
            .with_context(
                make_ctx_for_hash_func(digest_function).err_tip(|| "In CasServer::split_blob")?,
            )
            .await
            .err_tip(|| "Failed on split_blob() command")
            .map_err(Into::into)
    }

    #[instrument(
        err,
        ret(level = Level::DEBUG),
        level = Level::ERROR,
        skip_all,
        fields(request = ?grpc_request.get_ref())
    )]
    async fn splice_blob(
        &self,
        grpc_request: Request<SpliceBlobRequest>,
    ) -> Result<Response<SpliceBlobResponse>, Status> {
        let request = grpc_request.into_inner();
        let digest_function = request.digest_function;
        self.inner_splice_blob(request)
            .instrument(error_span!("cas_server_splice_blob"))
            .with_context(
                make_ctx_for_hash_func(digest_function).err_tip(|| "In CasServer::splice_blob")?,
            )
            .await
            .err_tip(|| "Failed on splice_blob() command")
            .map_err(Into::into)
    }
}
