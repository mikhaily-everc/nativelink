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

use core::pin::Pin;
use std::sync::Arc;

use futures::StreamExt;
use nativelink_config::cas_server::WithInstanceName;
use nativelink_config::stores::{MemorySpec, StoreSpec};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::content_addressable_storage_server::ContentAddressableStorage;
use nativelink_proto::build::bazel::remote::execution::v2::{
    BatchReadBlobsRequest, BatchReadBlobsResponse, BatchUpdateBlobsRequest,
    BatchUpdateBlobsResponse, Digest, Directory, DirectoryNode, FindMissingBlobsRequest,
    GetTreeRequest, GetTreeResponse, NodeProperties, SpliceBlobRequest, SplitBlobRequest,
    batch_read_blobs_response, batch_update_blobs_request, batch_update_blobs_response, compressor,
    digest_function,
};
use nativelink_proto::google::rpc::Status as GrpcStatus;
use nativelink_service::cas_server::CasServer;
use nativelink_store::ac_utils::serialize_and_upload_message;
use nativelink_store::default_store_factory::store_factory;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use nativelink_util::store_trait::{StoreKey, StoreLike};
use pretty_assertions::assert_eq;
use prost_types::Timestamp;
use tonic::{Code, Request};

const INSTANCE_NAME: &str = "foo_instance_name";
const HASH1: &str = "0123456789abcdef000000000000000000000000000000000123456789abcdef";
const HASH2: &str = "9993456789abcdef000000000000000000000000000000000123456789abc999";
const HASH3: &str = "7773456789abcdef000000000000000000000000000000000123456789abc777";
const BAD_HASH: &str = "BAD_HASH";

async fn make_store_manager() -> Result<Arc<StoreManager>, Error> {
    let store_manager = Arc::new(StoreManager::new());
    store_manager.add_store(
        "main_cas",
        store_factory(
            &StoreSpec::Memory(MemorySpec::default()),
            &store_manager,
            None,
        )
        .await?,
    );
    Ok(store_manager)
}

fn make_cas_server(store_manager: &StoreManager) -> Result<CasServer, Error> {
    CasServer::new(
        &[WithInstanceName {
            instance_name: "foo_instance_name".to_string(),
            config: nativelink_config::cas_server::CasStoreConfig {
                cas_store: "main_cas".to_string(),
                splice_manifest_store: None,
            },
        }],
        store_manager,
    )
}

async fn make_store_manager_with_manifest() -> Result<Arc<StoreManager>, Error> {
    let store_manager = make_store_manager().await?;
    store_manager.add_store(
        "splice_manifest",
        store_factory(
            &StoreSpec::Memory(MemorySpec::default()),
            &store_manager,
            None,
        )
        .await?,
    );
    Ok(store_manager)
}

fn make_cas_server_with_manifest(store_manager: &StoreManager) -> Result<CasServer, Error> {
    CasServer::new(
        &[WithInstanceName {
            instance_name: INSTANCE_NAME.to_string(),
            config: nativelink_config::cas_server::CasStoreConfig {
                cas_store: "main_cas".to_string(),
                splice_manifest_store: Some("splice_manifest".to_string()),
            },
        }],
        store_manager,
    )
}

#[nativelink_test]
async fn empty_store() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;

    let raw_response = cas_server
        .find_missing_blobs(Request::new(FindMissingBlobsRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digests: vec![Digest {
                hash: HASH1.to_string(),
                size_bytes: 0,
            }],
            digest_function: digest_function::Value::Sha256.into(),
        }))
        .await;
    assert!(raw_response.is_ok());
    let response = raw_response.unwrap().into_inner();
    assert_eq!(response.missing_blob_digests.len(), 1);
    Ok(())
}

#[nativelink_test]
async fn store_one_item_existence() -> Result<(), Box<dyn core::error::Error>> {
    const VALUE: &str = "1";

    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    store
        .update_oneshot(DigestInfo::try_new(HASH1, VALUE.len())?, VALUE.into())
        .await?;
    let raw_response = cas_server
        .find_missing_blobs(Request::new(FindMissingBlobsRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digests: vec![Digest {
                hash: HASH1.to_string(),
                size_bytes: VALUE.len() as i64,
            }],
            digest_function: digest_function::Value::Sha256.into(),
        }))
        .await;
    assert!(raw_response.is_ok());
    let response = raw_response.unwrap().into_inner();
    assert_eq!(response.missing_blob_digests.len(), 0); // All items should have been found.
    Ok(())
}

#[nativelink_test]
async fn has_three_requests_one_bad_hash() -> Result<(), Box<dyn core::error::Error>> {
    const VALUE: &str = "1";

    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    store
        .update_oneshot(DigestInfo::try_new(HASH1, VALUE.len())?, VALUE.into())
        .await?;
    let raw_response = cas_server
        .find_missing_blobs(Request::new(FindMissingBlobsRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digests: vec![
                Digest {
                    hash: HASH1.to_string(),
                    size_bytes: VALUE.len() as i64,
                },
                Digest {
                    hash: BAD_HASH.to_string(),
                    size_bytes: VALUE.len() as i64,
                },
                Digest {
                    hash: HASH1.to_string(),
                    size_bytes: VALUE.len() as i64,
                },
            ],
            digest_function: digest_function::Value::Sha256.into(),
        }))
        .await;
    let error = raw_response.unwrap_err();
    assert!(
        error.to_string().contains("Invalid sha256 hash: BAD_HASH"),
        "'Invalid sha256 hash: BAD_HASH' not found in: {error:?}"
    );
    Ok(())
}

#[nativelink_test]
async fn update_existing_item() -> Result<(), Box<dyn core::error::Error>> {
    const VALUE1: &str = "1";
    const VALUE2: &str = "2";

    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    let digest = Digest {
        hash: HASH1.to_string(),
        size_bytes: VALUE2.len() as i64,
    };

    store
        .update_oneshot(DigestInfo::try_new(HASH1, VALUE1.len())?, VALUE1.into())
        .await
        .expect("Update should have succeeded");

    let raw_response = cas_server
        .batch_update_blobs(Request::new(BatchUpdateBlobsRequest {
            instance_name: INSTANCE_NAME.to_string(),
            requests: vec![batch_update_blobs_request::Request {
                digest: Some(digest.clone()),
                data: VALUE2.into(),
                compressor: compressor::Value::Identity.into(),
            }],
            digest_function: digest_function::Value::Sha256.into(),
        }))
        .await;
    assert!(raw_response.is_ok());
    assert_eq!(
        raw_response.unwrap().into_inner(),
        BatchUpdateBlobsResponse {
            responses: vec![batch_update_blobs_response::Response {
                digest: Some(digest),
                status: Some(GrpcStatus {
                    code: 0, // Status Ok.
                    message: String::new(),
                    details: vec![],
                }),
            },],
        }
    );
    let new_data = store
        .get_part_unchunked(DigestInfo::try_new(HASH1, VALUE1.len())?, 0, None)
        .await
        .expect("Get should have succeeded");
    assert_eq!(
        new_data,
        VALUE2.as_bytes(),
        "Expected store to have been updated to new value"
    );
    Ok(())
}

#[nativelink_test]
async fn batch_read_blobs_read_two_blobs_success_one_fail()
-> Result<(), Box<dyn core::error::Error>> {
    const VALUE1: &str = "1";
    const VALUE2: &str = "23";

    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    let digest1 = Digest {
        hash: HASH1.to_string(),
        size_bytes: VALUE1.len() as i64,
    };
    let digest2 = Digest {
        hash: HASH2.to_string(),
        size_bytes: VALUE2.len() as i64,
    };
    {
        // Insert dummy data.
        store
            .update_oneshot(DigestInfo::try_new(HASH1, VALUE1.len())?, VALUE1.into())
            .await
            .expect("Update should have succeeded");
        store
            .update_oneshot(DigestInfo::try_new(HASH2, VALUE2.len())?, VALUE2.into())
            .await
            .expect("Update should have succeeded");
    }
    {
        // Read two blobs and additional blob should come back not found.
        let digest3 = Digest {
            hash: HASH3.to_string(),
            size_bytes: 3,
        };
        let raw_response = cas_server
            .batch_read_blobs(Request::new(BatchReadBlobsRequest {
                instance_name: INSTANCE_NAME.to_string(),
                digests: vec![digest1.clone(), digest2.clone(), digest3.clone()],
                acceptable_compressors: vec![compressor::Value::Identity.into()],
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert!(raw_response.is_ok());
        assert_eq!(
            raw_response.unwrap().into_inner(),
            BatchReadBlobsResponse {
                responses: vec![
                    batch_read_blobs_response::Response {
                        digest: Some(digest1),
                        data: VALUE1.into(),
                        status: Some(GrpcStatus {
                            code: 0, // Status Ok.
                            message: String::new(),
                            details: vec![],
                        }),
                        compressor: compressor::Value::Identity.into(),
                    },
                    batch_read_blobs_response::Response {
                        digest: Some(digest2),
                        data: VALUE2.into(),
                        status: Some(GrpcStatus {
                            code: 0, // Status Ok.
                            message: String::new(),
                            details: vec![],
                        }),
                        compressor: compressor::Value::Identity.into(),
                    },
                    batch_read_blobs_response::Response {
                        digest: Some(digest3.clone()),
                        data: vec![].into(),
                        status: Some(GrpcStatus {
                            code: Code::NotFound as i32,
                            message: format!(
                                "Key {:?} not found",
                                StoreKey::from(DigestInfo::try_from(digest3)?)
                            ),
                            details: vec![],
                        }),
                        compressor: compressor::Value::Identity.into(),
                    }
                ],
            }
        );
    }
    Ok(())
}

struct SetupDirectoryResult {
    root_directory: Directory,
    root_directory_digest_info: DigestInfo,
    sub_directories: Vec<Directory>,
    sub_directory_digest_infos: Vec<DigestInfo>,
}
async fn setup_directory_structure(
    store_pinned: Pin<&impl StoreLike>,
) -> Result<SetupDirectoryResult, Error> {
    // Set up 5 sub-directories.
    const SUB_DIRECTORIES_LENGTH: i32 = 5;
    let mut sub_directory_nodes: Vec<DirectoryNode> = vec![];
    let mut sub_directories: Vec<Directory> = vec![];
    let mut sub_directory_digest_infos: Vec<DigestInfo> = vec![];

    for i in 0..SUB_DIRECTORIES_LENGTH {
        let sub_directory: Directory = Directory {
            files: vec![],
            directories: vec![],
            symlinks: vec![],
            node_properties: Some(NodeProperties {
                properties: vec![],
                mtime: Some(Timestamp {
                    seconds: i64::from(i),
                    nanos: 0,
                }),
                unix_mode: Some(0o755),
            }),
        };
        let sub_directory_digest_info: DigestInfo = serialize_and_upload_message(
            &sub_directory,
            store_pinned,
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        sub_directory_digest_infos.push(sub_directory_digest_info);
        sub_directory_nodes.push(DirectoryNode {
            name: format!("sub_directory_{i}"),
            digest: Some(sub_directory_digest_info.into()),
        });
        sub_directories.push(sub_directory);
    }

    // Set up a root directory.
    let root_directory: Directory = Directory {
        files: vec![],
        directories: sub_directory_nodes,
        symlinks: vec![],
        node_properties: None,
    };
    let root_directory_digest_info: DigestInfo = serialize_and_upload_message(
        &root_directory,
        store_pinned,
        &mut DigestHasherFunc::Sha256.hasher(),
    )
    .await?;

    Ok(SetupDirectoryResult {
        root_directory,
        root_directory_digest_info,
        sub_directories,
        sub_directory_digest_infos,
    })
}

#[nativelink_test]
async fn get_tree_read_directories_without_paging() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    // Setup directory structure.
    let SetupDirectoryResult {
        root_directory,
        root_directory_digest_info,
        sub_directories,
        sub_directory_digest_infos: _,
    } = setup_directory_structure(store.as_pin()).await?;

    // Must work when paging is disabled ( `page_size` is 0 ).
    // It reads all directories at once.

    // First verify that using an empty page token is treated as if the client had sent the root
    // digest.
    {
        let raw_response = cas_server
            .get_tree(Request::new(GetTreeRequest {
                instance_name: INSTANCE_NAME.to_string(),
                page_size: 0,
                page_token: String::new(),
                root_digest: Some(root_directory_digest_info.into()),
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert_eq!(
            raw_response
                .unwrap()
                .into_inner()
                .filter_map(|x| async move { Some(x.unwrap()) })
                .collect::<Vec<_>>()
                .await,
            vec![GetTreeResponse {
                directories: vec![
                    root_directory.clone(),
                    sub_directories[0].clone(),
                    sub_directories[1].clone(),
                    sub_directories[2].clone(),
                    sub_directories[3].clone(),
                    sub_directories[4].clone()
                ],
                next_page_token: String::new()
            }]
        );
    }

    // Also verify that sending the root digest returns the entire tree as well.
    {
        let raw_response = cas_server
            .get_tree(Request::new(GetTreeRequest {
                instance_name: INSTANCE_NAME.to_string(),
                page_size: 0,
                page_token: format!("{root_directory_digest_info}"),
                root_digest: Some(root_directory_digest_info.into()),
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert_eq!(
            raw_response
                .unwrap()
                .into_inner()
                .filter_map(|x| async move { Some(x.unwrap()) })
                .collect::<Vec<_>>()
                .await,
            vec![GetTreeResponse {
                directories: vec![
                    root_directory.clone(),
                    sub_directories[0].clone(),
                    sub_directories[1].clone(),
                    sub_directories[2].clone(),
                    sub_directories[3].clone(),
                    sub_directories[4].clone()
                ],
                next_page_token: String::new()
            }]
        );
    }

    Ok(())
}

#[nativelink_test]
async fn get_tree_read_directories_with_paging() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    // Setup directory structure.
    let SetupDirectoryResult {
        root_directory,
        root_directory_digest_info,
        sub_directories,
        sub_directory_digest_infos,
    } = setup_directory_structure(store.as_pin()).await?;

    // Must work when paging is enabled ( `page_size` is 2 ).
    // First, it reads `root_directory` and `sub_directory[0]`.
    // Then, it reads `sub_directory[1]` and `sub_directory[2]`.
    // Finally, it reads `sub_directory[3]` and `sub_directory[4]`.

    // First, verify that an empty initial page token is treated as if the client had sent the
    // root digest and respects the page size.
    {
        let raw_response = cas_server
            .get_tree(Request::new(GetTreeRequest {
                instance_name: INSTANCE_NAME.to_string(),
                page_size: 2,
                page_token: String::new(),
                root_digest: Some(root_directory_digest_info.into()),
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert_eq!(
            raw_response
                .unwrap()
                .into_inner()
                .filter_map(|x| async move { Some(x.unwrap()) })
                .collect::<Vec<_>>()
                .await,
            vec![GetTreeResponse {
                directories: vec![root_directory.clone(), sub_directories[0].clone()],
                next_page_token: format!("{}", sub_directory_digest_infos[1]),
            }]
        );
    }

    // Also verify that sending the root digest as the page token is treated as paging from the
    // beginning and respects page size.
    {
        let raw_response = cas_server
            .get_tree(Request::new(GetTreeRequest {
                instance_name: INSTANCE_NAME.to_string(),
                page_size: 2,
                page_token: format!("{root_directory_digest_info}"),
                root_digest: Some(root_directory_digest_info.into()),
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert_eq!(
            raw_response
                .unwrap()
                .into_inner()
                .filter_map(|x| async move { Some(x.unwrap()) })
                .collect::<Vec<_>>()
                .await,
            vec![GetTreeResponse {
                directories: vec![root_directory.clone(), sub_directories[0].clone()],
                next_page_token: format!("{}", sub_directory_digest_infos[1]),
            }]
        );
    }

    // Verify that paging from a non-initial page token will return the expected content.
    {
        let raw_response = cas_server
            .get_tree(Request::new(GetTreeRequest {
                instance_name: INSTANCE_NAME.to_string(),
                page_size: 2,
                page_token: format!("{}", sub_directory_digest_infos[1]),
                root_digest: Some(root_directory_digest_info.into()),
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert_eq!(
            raw_response
                .unwrap()
                .into_inner()
                .filter_map(|x| async move { Some(x.unwrap()) })
                .collect::<Vec<_>>()
                .await,
            vec![GetTreeResponse {
                directories: vec![sub_directories[1].clone(), sub_directories[2].clone()],
                next_page_token: format!("{}", sub_directory_digest_infos[3]),
            }]
        );

        let raw_response = cas_server
            .get_tree(Request::new(GetTreeRequest {
                instance_name: INSTANCE_NAME.to_string(),
                page_size: 2,
                page_token: format!("{}", sub_directory_digest_infos[3]),
                root_digest: Some(root_directory_digest_info.into()),
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert_eq!(
            raw_response
                .unwrap()
                .into_inner()
                .filter_map(|x| async move { Some(x.unwrap()) })
                .collect::<Vec<_>>()
                .await,
            vec![GetTreeResponse {
                directories: vec![sub_directories[3].clone(), sub_directories[4].clone()],
                next_page_token: String::new(),
            }]
        );
    }

    Ok(())
}

#[nativelink_test]
async fn batch_update_blobs_two_items_existence_with_third_missing()
-> Result<(), Box<dyn core::error::Error>> {
    const VALUE1: &str = "1";
    const VALUE2: &str = "23";

    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;

    let digest1 = Digest {
        hash: HASH1.to_string(),
        size_bytes: VALUE1.len() as i64,
    };
    let digest2 = Digest {
        hash: HASH2.to_string(),
        size_bytes: VALUE2.len() as i64,
    };

    {
        // Send update to insert two entries into backend.
        let raw_response = cas_server
            .batch_update_blobs(Request::new(BatchUpdateBlobsRequest {
                instance_name: INSTANCE_NAME.to_string(),
                requests: vec![
                    batch_update_blobs_request::Request {
                        digest: Some(digest1.clone()),
                        data: VALUE1.into(),
                        compressor: compressor::Value::Identity.into(),
                    },
                    batch_update_blobs_request::Request {
                        digest: Some(digest2.clone()),
                        data: VALUE2.into(),
                        compressor: compressor::Value::Identity.into(),
                    },
                ],
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert!(raw_response.is_ok());
        assert_eq!(
            raw_response.unwrap().into_inner(),
            BatchUpdateBlobsResponse {
                responses: vec![
                    batch_update_blobs_response::Response {
                        digest: Some(digest1),
                        status: Some(GrpcStatus {
                            code: 0, // Status Ok.
                            message: String::new(),
                            details: vec![],
                        }),
                    },
                    batch_update_blobs_response::Response {
                        digest: Some(digest2),
                        status: Some(GrpcStatus {
                            code: 0, // Status Ok.
                            message: String::new(),
                            details: vec![],
                        }),
                    }
                ],
            }
        );
    }
    {
        // Query the backend for inserted entries plus one that is not
        // present and ensure it only returns the one that is missing.
        let missing_digest = Digest {
            hash: HASH3.to_string(),
            size_bytes: 1,
        };
        let raw_response = cas_server
            .find_missing_blobs(Request::new(FindMissingBlobsRequest {
                instance_name: INSTANCE_NAME.to_string(),
                blob_digests: vec![
                    Digest {
                        hash: HASH1.to_string(),
                        size_bytes: VALUE1.len() as i64,
                    },
                    missing_digest.clone(),
                    Digest {
                        hash: HASH2.to_string(),
                        size_bytes: VALUE2.len() as i64,
                    },
                ],
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert!(raw_response.is_ok());
        let response = raw_response.unwrap().into_inner();
        assert_eq!(response.missing_blob_digests, vec![missing_digest]);
    }
    Ok(())
}

// --- SplitBlob / SpliceBlob tests (REAPI --experimental_remote_cache_chunking) ---

fn sha256_digest_info(data: &[u8]) -> DigestInfo {
    let mut hasher = DigestHasherFunc::Sha256.hasher();
    DigestHasher::update(&mut hasher, data);
    hasher.finalize_digest()
}

async fn upload_chunk(
    store: &nativelink_util::store_trait::Store,
    bytes: &'static [u8],
) -> DigestInfo {
    let digest = sha256_digest_info(bytes);
    store
        .update_oneshot(digest, bytes.into())
        .await
        .expect("chunk upload");
    digest
}

#[nativelink_test]
async fn splice_blob_not_configured_returns_unimplemented()
-> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;

    let err = cas_server
        .splice_blob(Request::new(SpliceBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(Digest {
                hash: HASH1.to_string(),
                size_bytes: 1,
            }),
            chunk_digests: vec![],
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: 0,
        }))
        .await
        .expect_err("should fail when manifest store unset");
    assert_eq!(err.code(), Code::Unimplemented);
    Ok(())
}

#[nativelink_test]
async fn splice_blob_round_trip() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager_with_manifest().await?;
    let cas_server = make_cas_server_with_manifest(&store_manager)?;
    let cas_store = store_manager.get_store("main_cas").unwrap();

    let chunks: [&[u8]; 3] = [b"alpha-chunk-", b"beta-chunk--", b"gamma-chunk-"];
    let mut chunk_digests = Vec::new();
    let mut reassembled = Vec::new();
    for bytes in &chunks {
        chunk_digests.push(upload_chunk(&cas_store, bytes).await);
        reassembled.extend_from_slice(bytes);
    }
    let blob_digest = sha256_digest_info(&reassembled);

    let splice_resp = cas_server
        .splice_blob(Request::new(SpliceBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(blob_digest.into()),
            chunk_digests: chunk_digests.iter().copied().map(Into::into).collect(),
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: 0,
        }))
        .await?
        .into_inner();
    let resp_digest: DigestInfo = splice_resp.blob_digest.unwrap().try_into()?;
    assert_eq!(resp_digest, blob_digest);

    // Reassembled blob should now be readable directly from the CAS.
    let reassembled_fetch = cas_store.get_part_unchunked(blob_digest, 0, None).await?;
    assert_eq!(&reassembled_fetch[..], reassembled.as_slice());

    let split_resp = cas_server
        .split_blob(Request::new(SplitBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(blob_digest.into()),
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: 0,
        }))
        .await?
        .into_inner();
    let returned: Vec<DigestInfo> = split_resp
        .chunk_digests
        .into_iter()
        .map(|d| DigestInfo::try_from(d).unwrap())
        .collect();
    assert_eq!(returned, chunk_digests);
    Ok(())
}

#[nativelink_test]
async fn splice_blob_missing_chunk_returns_failed_precondition()
-> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager_with_manifest().await?;
    let cas_server = make_cas_server_with_manifest(&store_manager)?;
    let cas_store = store_manager.get_store("main_cas").unwrap();

    let present_digest = upload_chunk(&cas_store, b"present-chunk").await;
    let missing_digest = sha256_digest_info(b"absent-chunk-");
    let blob_digest = sha256_digest_info(b"present-chunkabsent-chunk-");

    let err = cas_server
        .splice_blob(Request::new(SpliceBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(blob_digest.into()),
            chunk_digests: vec![present_digest.into(), missing_digest.into()],
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: 0,
        }))
        .await
        .expect_err("should fail when chunk missing");
    assert_eq!(err.code(), Code::FailedPrecondition);
    Ok(())
}

#[nativelink_test]
async fn splice_blob_digest_mismatch_rejects() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager_with_manifest().await?;
    let cas_server = make_cas_server_with_manifest(&store_manager)?;
    let cas_store = store_manager.get_store("main_cas").unwrap();
    let manifest_store = store_manager.get_store("splice_manifest").unwrap();

    let chunk_digest = upload_chunk(&cas_store, b"real-content").await;
    let claimed_digest = DigestInfo::try_new(HASH1, b"real-content".len()).expect("digest");

    let err = cas_server
        .splice_blob(Request::new(SpliceBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(claimed_digest.into()),
            chunk_digests: vec![chunk_digest.into()],
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: 0,
        }))
        .await
        .expect_err("should reject digest mismatch");
    assert_eq!(err.code(), Code::InvalidArgument);

    // Manifest MUST NOT have been persisted.
    assert!(
        manifest_store.has(claimed_digest).await?.is_none(),
        "manifest should not be persisted on digest mismatch"
    );
    Ok(())
}

#[nativelink_test]
async fn split_blob_not_found_returns_not_found() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager_with_manifest().await?;
    let cas_server = make_cas_server_with_manifest(&store_manager)?;

    let err = cas_server
        .split_blob(Request::new(SplitBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(Digest {
                hash: HASH1.to_string(),
                size_bytes: 42,
            }),
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: 0,
        }))
        .await
        .expect_err("split should fail when no manifest");
    assert_eq!(err.code(), Code::NotFound);
    Ok(())
}

#[nativelink_test]
async fn chunking_instances_map_tracks_manifest_presence() -> Result<(), Box<dyn core::error::Error>>
{
    let with_manifest_mgr = make_store_manager_with_manifest().await?;
    let with_manifest = make_cas_server_with_manifest(&with_manifest_mgr)?;
    assert_eq!(
        with_manifest.chunking_instances().get(INSTANCE_NAME),
        Some(&true)
    );

    let no_manifest_mgr = make_store_manager().await?;
    let no_manifest = make_cas_server(&no_manifest_mgr)?;
    assert_eq!(
        no_manifest.chunking_instances().get(INSTANCE_NAME),
        Some(&false)
    );
    Ok(())
}

#[nativelink_test]
async fn splice_blob_corrupted_chunk_returns_data_loss() -> Result<(), Box<dyn core::error::Error>>
{
    let store_manager = make_store_manager_with_manifest().await?;
    let cas_server = make_cas_server_with_manifest(&store_manager)?;
    let cas_store = store_manager.get_store("main_cas").unwrap();
    let manifest_store = store_manager.get_store("splice_manifest").unwrap();

    // Chunk 0: uploaded normally, content matches its digest.
    let real_chunk = upload_chunk(&cas_store, b"first-chunk-").await;

    // Chunk 1: simulates same-length-different-content corruption at rest.
    // The MemorySpec store has no VerifyStore wrapper, so writing
    // `corrupted` under the digest of `original` succeeds and is
    // indistinguishable from a real upload until the contents are read.
    let original: &[u8] = b"second-chunk";
    let corrupted: &[u8] = b"NOPE!!!!!!!!";
    assert_eq!(original.len(), corrupted.len());
    let claimed_chunk_digest = sha256_digest_info(original);
    cas_store
        .update_oneshot(claimed_chunk_digest, corrupted.into())
        .await?;

    let mut original_blob = Vec::new();
    original_blob.extend_from_slice(b"first-chunk-");
    original_blob.extend_from_slice(original);
    let blob_digest = sha256_digest_info(&original_blob);

    let err = cas_server
        .splice_blob(Request::new(SpliceBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(blob_digest.into()),
            chunk_digests: vec![real_chunk.into(), claimed_chunk_digest.into()],
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: 0,
        }))
        .await
        .expect_err("should reject corrupted chunk");
    // Pre-fix: failure was Code::InvalidArgument from the whole-blob hash
    // step, with no signal which chunk was poisoned. Per-chunk verification
    // upgrades this to Code::DataLoss on the bad chunk.
    assert_eq!(err.code(), Code::DataLoss);
    assert!(
        manifest_store.has(blob_digest).await?.is_none(),
        "manifest should not be persisted on chunk corruption"
    );
    Ok(())
}

#[nativelink_test]
async fn splice_blob_with_cached_blob_rejects_inconsistent_chunks()
-> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager_with_manifest().await?;
    let cas_server = make_cas_server_with_manifest(&store_manager)?;
    let cas_store = store_manager.get_store("main_cas").unwrap();
    let manifest_store = store_manager.get_store("splice_manifest").unwrap();

    // Two valid chunks (each hashes to its claimed digest).
    let chunk_a = upload_chunk(&cas_store, b"chunk-A-").await;
    let chunk_b = upload_chunk(&cas_store, b"chunk-B-").await;

    // The "claimed" blob arrived via a non-splice upload path. Its size
    // matches the chunk-sum but its content does not concatenate from the
    // chunks above.
    let actual_concat: &[u8] = b"chunk-A-chunk-B-";
    let claimed_blob: &[u8] = b"OTHER_BLOB______";
    assert_eq!(actual_concat.len(), claimed_blob.len());
    let claimed_blob_digest = sha256_digest_info(claimed_blob);
    cas_store
        .update_oneshot(claimed_blob_digest, claimed_blob.into())
        .await?;

    let err = cas_server
        .splice_blob(Request::new(SpliceBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(claimed_blob_digest.into()),
            chunk_digests: vec![chunk_a.into(), chunk_b.into()],
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: 0,
        }))
        .await
        .expect_err("cached blob should not bypass reassembly verification");
    // Pre-fix: blob_already_cached short-circuited verification and the
    // server unconditionally wrote a manifest pointing at chunks that don't
    // splice back to the claimed digest. The fix moves verification ahead
    // of the manifest write.
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(
        manifest_store.has(claimed_blob_digest).await?.is_none(),
        "manifest must not be persisted when chunks don't reassemble to cached blob"
    );
    Ok(())
}

#[nativelink_test]
async fn splice_blob_with_cached_blob_persists_manifest_without_overwriting_blob()
-> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager_with_manifest().await?;
    let cas_server = make_cas_server_with_manifest(&store_manager)?;
    let cas_store = store_manager.get_store("main_cas").unwrap();
    let manifest_store = store_manager.get_store("splice_manifest").unwrap();

    let chunk_a = upload_chunk(&cas_store, b"hello-").await;
    let chunk_b = upload_chunk(&cas_store, b"world!").await;

    let blob_bytes: &[u8] = b"hello-world!";
    let blob_digest = sha256_digest_info(blob_bytes);
    cas_store
        .update_oneshot(blob_digest, blob_bytes.into())
        .await?;

    let resp = cas_server
        .splice_blob(Request::new(SpliceBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(blob_digest.into()),
            chunk_digests: vec![chunk_a.into(), chunk_b.into()],
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: 0,
        }))
        .await?
        .into_inner();
    assert_eq!(
        DigestInfo::try_from(resp.blob_digest.unwrap())?,
        blob_digest
    );

    assert!(
        manifest_store.has(blob_digest).await?.is_some(),
        "manifest should be persisted on happy-path splice with cached blob"
    );
    let fetched = cas_store.get_part_unchunked(blob_digest, 0, None).await?;
    assert_eq!(&fetched[..], blob_bytes);

    let split_resp = cas_server
        .split_blob(Request::new(SplitBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(blob_digest.into()),
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: 0,
        }))
        .await?
        .into_inner();
    let returned: Vec<DigestInfo> = split_resp
        .chunk_digests
        .into_iter()
        .map(|d| DigestInfo::try_from(d).unwrap())
        .collect();
    assert_eq!(returned, vec![chunk_a, chunk_b]);
    Ok(())
}
