/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use crate::clients::agent_provider::CouchbaseAgentProvider;
use crate::durability_level::parse_optional_durability_level_to_memdx;
use crate::error;
use crate::error::Error;
use crate::mutation_state::MutationToken;
use crate::options::kv_binary_options::{
    AppendOptions, DecrementOptions, IncrementOptions, PrependOptions,
};
use crate::options::kv_options::*;
use crate::results::kv_binary_results::CounterResult;
use crate::results::kv_results::{
    ExistsResult, GetResult, LookupInResult, LookupInResultEntry, MutateInResult,
    MutateInResultEntry, MutationResult, TouchResult,
};
use crate::results::projection::{build_from_full_doc, build_from_subdoc_entries};
use crate::retry::RetryStrategy;
use crate::subdoc::lookup_in_specs::{GetSpecOptions, LookupInSpec};
use crate::subdoc::mutate_in_specs::MutateInSpec;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use couchbase_core::memdx::subdoc::{reorder_subdoc_ops, MutateInOp, SubdocDocFlag};
use std::sync::Arc;
use std::time::Duration;

const SECS_IN_DAY: u64 = 24 * 60 * 60;

#[derive(Clone)]
pub(crate) struct CouchbaseCoreKvClient {
    agent_provider: CouchbaseAgentProvider,
    bucket_name: Arc<str>,
    scope_name: String,
    collection_name: String,

    default_retry_strategy: Arc<dyn RetryStrategy>,
}

impl CouchbaseCoreKvClient {
    pub fn new(
        agent_provider: CouchbaseAgentProvider,
        bucket_name: String,
        scope_name: String,
        collection_name: String,
        default_retry_strategy: Arc<dyn RetryStrategy>,
    ) -> Self {
        Self {
            agent_provider,
            bucket_name: Arc::from(bucket_name),
            scope_name,
            collection_name,
            default_retry_strategy,
        }
    }

    fn expiry_to_seconds(expiry: Duration) -> error::Result<u32> {
        if expiry.as_millis() < 1000 {
            return Ok(1); // minimum 1 second
        }

        let expiry_secs = expiry.as_secs();
        if expiry_secs < (SECS_IN_DAY * 30) {
            expiry_secs.try_into().map_err(|e| {
                Error::invalid_argument(
                    "expiry",
                    format!("expiry duration is too large for u32: {}", e),
                )
            })
        } else {
            // treat as unix timestamp
            let now = Utc::now().timestamp() as u64;
            let then = now.saturating_add(expiry_secs);
            then.try_into().map_err(|e| {
                Error::invalid_argument(
                    "expiry",
                    format!("expiry as timestamp is too large for u32: {}", e),
                )
            })
        }
    }

    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }

    pub fn scope_name(&self) -> &str {
        &self.scope_name
    }

    pub fn collection_name(&self) -> &str {
        &self.collection_name
    }

    pub async fn upsert(
        &self,
        id: &str,
        value: &[u8],
        flags: u32,
        options: UpsertOptions,
    ) -> error::Result<MutationResult> {
        let agent = self.agent_provider.get_agent().await;
        let retry = options
            .retry_strategy
            .unwrap_or_else(|| self.default_retry_strategy.clone());

        let result = CouchbaseAgentProvider::upgrade_agent(agent)?
            .upsert(
                couchbase_core::options::crud::UpsertOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                    value,
                )
                .flags(flags)
                .expiry(options.expiry.map(Self::expiry_to_seconds).transpose()?)
                .durability_level(parse_optional_durability_level_to_memdx(
                    options.durability_level,
                ))
                .retry_strategy(retry)
                .preserve_expiry(options.preserve_expiry),
            )
            .await?;

        Ok(MutationResult {
            cas: result.cas,
            mutation_token: result
                .mutation_token
                .map(|t| MutationToken::new(t, self.bucket_name.clone())),
        })
    }

    pub async fn insert(
        &self,
        id: &str,
        value: &[u8],
        flags: u32,
        options: InsertOptions,
    ) -> error::Result<MutationResult> {
        let agent = self.agent_provider.get_agent().await;
        let retry = options
            .retry_strategy
            .unwrap_or_else(|| self.default_retry_strategy.clone());
        let result = CouchbaseAgentProvider::upgrade_agent(agent)?
            .add(
                couchbase_core::options::crud::AddOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                    value,
                )
                .flags(flags)
                .expiry(options.expiry.map(Self::expiry_to_seconds).transpose()?)
                .durability_level(parse_optional_durability_level_to_memdx(
                    options.durability_level,
                ))
                .retry_strategy(retry),
            )
            .await?;

        Ok(MutationResult {
            cas: result.cas,
            mutation_token: result
                .mutation_token
                .map(|t| MutationToken::new(t, self.bucket_name.clone())),
        })
    }

    pub async fn replace(
        &self,
        id: &str,
        value: &[u8],
        flags: u32,
        options: ReplaceOptions,
    ) -> error::Result<MutationResult> {
        let agent = self.agent_provider.get_agent().await;
        let retry = options
            .retry_strategy
            .unwrap_or_else(|| self.default_retry_strategy.clone());
        let result = CouchbaseAgentProvider::upgrade_agent(agent)?
            .replace(
                couchbase_core::options::crud::ReplaceOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                    value,
                )
                .flags(flags)
                .expiry(options.expiry.map(Self::expiry_to_seconds).transpose()?)
                .durability_level(parse_optional_durability_level_to_memdx(
                    options.durability_level,
                ))
                .retry_strategy(retry)
                .preserve_expiry(options.preserve_expiry)
                .cas(options.cas),
            )
            .await?;

        Ok(MutationResult {
            cas: result.cas,
            mutation_token: result
                .mutation_token
                .map(|t| MutationToken::new(t, self.bucket_name.clone())),
        })
    }

    pub async fn remove(&self, id: &str, options: RemoveOptions) -> error::Result<MutationResult> {
        let agent = self.agent_provider.get_agent().await;
        let retry = options
            .retry_strategy
            .unwrap_or_else(|| self.default_retry_strategy.clone());
        let result = CouchbaseAgentProvider::upgrade_agent(agent)?
            .delete(
                couchbase_core::options::crud::DeleteOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                )
                .durability_level(parse_optional_durability_level_to_memdx(
                    options.durability_level,
                ))
                .retry_strategy(retry)
                .cas(options.cas),
            )
            .await?;

        Ok(MutationResult {
            cas: result.cas,
            mutation_token: result
                .mutation_token
                .map(|t| MutationToken::new(t, self.bucket_name.clone())),
        })
    }

    pub async fn get(&self, id: &str, options: GetOptions) -> error::Result<GetResult> {
        if options.expiry.is_some() || options.projections.is_some() {
            self.get_with_projections(id, options).await
        } else {
            self.get_direct(id, options).await
        }
    }

    async fn get_direct(&self, id: &str, options: GetOptions) -> error::Result<GetResult> {
        let agent = self.agent_provider.get_agent().await;
        let retry = options
            .retry_strategy
            .unwrap_or_else(|| self.default_retry_strategy.clone());
        let res = CouchbaseAgentProvider::upgrade_agent(agent)?
            .get(
                couchbase_core::options::crud::GetOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                )
                .retry_strategy(retry),
            )
            .await?;

        Ok(res.into())
    }

    async fn get_with_projections(
        &self,
        id: &str,
        options: GetOptions,
    ) -> error::Result<GetResult> {
        let mut num_lookups = options.projections.as_deref().map_or(0, |p| p.len());
        let mut with_flags = false;

        if let Some(true) = options.expiry {
            if num_lookups == 0 {
                // Full doc get with expiry
                with_flags = true;
            }
            num_lookups += 1;
        }

        // Max number of look up ops we can do at once is 16, so do a full look up if there are > 16,
        // or if there are no projections (expiry only)
        let full_lookup = num_lookups > 16 || options.projections.as_deref().is_none();

        let mut specs = vec![];

        if let Some(true) = options.expiry {
            specs.push(LookupInSpec::get(
                "$document.exptime",
                GetSpecOptions::new().xattr(true),
            ));

            if with_flags {
                // We need to fetch the flags for transcoding as they aren't included in a LookupIn
                // response. We only need them when doing a full get with expiry.
                specs.push(LookupInSpec::get(
                    "$document.flags",
                    GetSpecOptions::new().xattr(true),
                ));
            }
        }

        if full_lookup {
            specs.push(LookupInSpec::get("", None));
        } else {
            for projection in options.projections.as_deref().unwrap() {
                specs.push(LookupInSpec::get(projection, None));
                num_lookups += 1;
            }
        }

        let mut res = self.lookup_in(id, &specs, LookupInOptions::new()).await?;
        let mut expires_at = None;
        let mut flags: u32 = 0;

        if let Some(true) = options.expiry {
            let expiry: u64 = res.content_as(0)?;
            expires_at = match DateTime::<Utc>::from_timestamp(expiry as i64, 0) {
                Some(e) => Option::from(e),
                None => {
                    return Err(error::Error::other_failure(
                        "invalid expiry time returned from server".to_string(),
                    ));
                }
            };
            res.entries.remove(0);
            specs.remove(0);

            if with_flags {
                flags = res.content_as(0)?;
                res.entries.remove(0);
                specs.remove(0);
            }
        }

        let content = if full_lookup {
            build_from_full_doc(&res, options.projections.as_deref())?
        } else {
            build_from_subdoc_entries(&specs, &res.entries)?
        };

        Ok(GetResult {
            content: Bytes::from(content),
            flags,
            cas: res.cas,
            expiry_time: expires_at,
        })
    }

    pub async fn exists(&self, id: &str, options: ExistsOptions) -> error::Result<ExistsResult> {
        let agent = self.agent_provider.get_agent().await;
        let retry = options
            .retry_strategy
            .unwrap_or_else(|| self.default_retry_strategy.clone());
        let res = CouchbaseAgentProvider::upgrade_agent(agent)?
            .get_meta(
                couchbase_core::options::crud::GetMetaOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                )
                .retry_strategy(retry),
            )
            .await?;

        Ok(res.into())
    }

    pub async fn get_and_touch(
        &self,
        id: &str,
        expiry: Duration,
        options: GetAndTouchOptions,
    ) -> error::Result<GetResult> {
        let agent = self.agent_provider.get_agent().await;
        let retry = options
            .retry_strategy
            .unwrap_or_else(|| self.default_retry_strategy.clone());
        let res = CouchbaseAgentProvider::upgrade_agent(agent)?
            .get_and_touch(
                couchbase_core::options::crud::GetAndTouchOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                    Self::expiry_to_seconds(expiry)?,
                )
                .retry_strategy(retry),
            )
            .await?;

        Ok(res.into())
    }

    pub async fn get_and_lock(
        &self,
        id: &str,
        lock_time: Duration,
        options: GetAndLockOptions,
    ) -> error::Result<GetResult> {
        let agent = self.agent_provider.get_agent().await;
        let retry = options
            .retry_strategy
            .unwrap_or_else(|| self.default_retry_strategy.clone());
        let res = CouchbaseAgentProvider::upgrade_agent(agent)?
            .get_and_lock(
                couchbase_core::options::crud::GetAndLockOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                    lock_time.as_secs() as u32,
                )
                .retry_strategy(retry),
            )
            .await?;

        Ok(res.into())
    }

    pub async fn unlock(&self, id: &str, cas: u64, options: UnlockOptions) -> error::Result<()> {
        let agent = self.agent_provider.get_agent().await;
        let retry = options
            .retry_strategy
            .unwrap_or_else(|| self.default_retry_strategy.clone());
        CouchbaseAgentProvider::upgrade_agent(agent)?
            .unlock(
                couchbase_core::options::crud::UnlockOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                    cas,
                )
                .retry_strategy(retry),
            )
            .await?;

        Ok(())
    }

    pub async fn touch(
        &self,
        id: &str,
        expiry: Duration,
        options: TouchOptions,
    ) -> error::Result<TouchResult> {
        let agent = self.agent_provider.get_agent().await;
        let retry = options
            .retry_strategy
            .unwrap_or_else(|| self.default_retry_strategy.clone());
        let result = CouchbaseAgentProvider::upgrade_agent(agent)?
            .touch(
                couchbase_core::options::crud::TouchOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                    Self::expiry_to_seconds(expiry)?,
                )
                .retry_strategy(retry),
            )
            .await?;

        Ok(result.into())
    }

    pub async fn append(
        &self,
        id: &str,
        value: &[u8],
        options: AppendOptions,
    ) -> error::Result<MutationResult> {
        let agent = self.agent_provider.get_agent().await;
        let retry = options
            .retry_strategy
            .unwrap_or_else(|| self.default_retry_strategy.clone());
        let result = CouchbaseAgentProvider::upgrade_agent(agent)?
            .append(
                couchbase_core::options::crud::AppendOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                    value,
                )
                .cas(options.cas)
                .durability_level(parse_optional_durability_level_to_memdx(
                    options.durability_level,
                ))
                .retry_strategy(retry),
            )
            .await?;

        Ok(MutationResult {
            cas: result.cas,
            mutation_token: result
                .mutation_token
                .map(|t| MutationToken::new(t, self.bucket_name.clone())),
        })
    }

    pub async fn prepend(
        &self,
        id: &str,
        value: &[u8],
        options: PrependOptions,
    ) -> error::Result<MutationResult> {
        let agent = self.agent_provider.get_agent().await;
        let retry = options
            .retry_strategy
            .unwrap_or_else(|| self.default_retry_strategy.clone());
        let result = CouchbaseAgentProvider::upgrade_agent(agent)?
            .prepend(
                couchbase_core::options::crud::PrependOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                    value,
                )
                .durability_level(parse_optional_durability_level_to_memdx(
                    options.durability_level,
                ))
                .cas(options.cas)
                .retry_strategy(retry),
            )
            .await?;

        Ok(MutationResult {
            cas: result.cas,
            mutation_token: result
                .mutation_token
                .map(|t| MutationToken::new(t, self.bucket_name.clone())),
        })
    }

    pub async fn increment(
        &self,
        id: &str,
        options: IncrementOptions,
    ) -> error::Result<CounterResult> {
        let agent = self.agent_provider.get_agent().await;
        let retry = options
            .retry_strategy
            .unwrap_or_else(|| self.default_retry_strategy.clone());
        let result = CouchbaseAgentProvider::upgrade_agent(agent)?
            .increment(
                couchbase_core::options::crud::IncrementOptions::new(
                    id.as_bytes(),
                    options.delta.unwrap_or(1),
                    &self.scope_name,
                    &self.collection_name,
                )
                .durability_level(parse_optional_durability_level_to_memdx(
                    options.durability_level,
                ))
                .retry_strategy(retry)
                .expiry(options.expiry.map(Self::expiry_to_seconds).transpose()?)
                .initial(options.initial),
            )
            .await?;

        Ok(CounterResult {
            cas: result.cas,
            mutation_token: result
                .mutation_token
                .map(|t| MutationToken::new(t, self.bucket_name.clone())),
            content: result.value,
        })
    }

    pub async fn decrement(
        &self,
        id: &str,
        options: DecrementOptions,
    ) -> error::Result<CounterResult> {
        let agent = self.agent_provider.get_agent().await;
        let retry = options
            .retry_strategy
            .unwrap_or_else(|| self.default_retry_strategy.clone());
        let result = CouchbaseAgentProvider::upgrade_agent(agent)?
            .decrement(
                couchbase_core::options::crud::DecrementOptions::new(
                    id.as_bytes(),
                    options.delta.unwrap_or(1),
                    &self.scope_name,
                    &self.collection_name,
                )
                .durability_level(parse_optional_durability_level_to_memdx(
                    options.durability_level,
                ))
                .retry_strategy(retry)
                .expiry(options.expiry.map(Self::expiry_to_seconds).transpose()?)
                .initial(options.initial),
            )
            .await?;

        Ok(CounterResult {
            cas: result.cas,
            mutation_token: result
                .mutation_token
                .map(|t| MutationToken::new(t, self.bucket_name.clone())),
            content: result.value,
        })
    }

    pub async fn lookup_in(
        &self,
        id: &str,
        specs: &[LookupInSpec],
        options: LookupInOptions,
    ) -> error::Result<LookupInResult> {
        let agent = self.agent_provider.get_agent().await;
        let (ordered_specs, op_indexes) = reorder_subdoc_ops(specs);
        let retry = options
            .retry_strategy
            .unwrap_or_else(|| self.default_retry_strategy.clone());
        let result = CouchbaseAgentProvider::upgrade_agent(agent)?
            .lookup_in(
                couchbase_core::options::crud::LookupInOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                    ordered_specs
                        .iter()
                        .map(|spec| (*spec).into())
                        .collect::<Vec<_>>()
                        .as_slice(),
                )
                .flags({
                    let mut flags = SubdocDocFlag::empty();
                    if options.access_deleted.unwrap_or(false) {
                        flags |= SubdocDocFlag::AccessDeleted;
                    }
                    flags
                })
                .retry_strategy(retry),
            )
            .await?;

        let mut entries = vec![None; specs.len()];

        for (i, x) in result.value.into_iter().enumerate() {
            let original_idx = op_indexes[i];
            entries[original_idx] = Some(LookupInResultEntry {
                value: x.value.as_ref().map(|v| bytes::Bytes::from(v.clone())),
                error: x.err.as_ref().map(|e| e.into()),
                op: ordered_specs[i].op.clone(),
            });
        }

        Ok(LookupInResult {
            cas: result.cas,
            entries: entries.into_iter().map(|x| x.unwrap()).collect(),
            is_deleted: result.doc_is_deleted,
        })
    }

    pub async fn mutate_in(
        &self,
        id: &str,
        specs: &[MutateInSpec],
        options: MutateInOptions,
    ) -> error::Result<MutateInResult> {
        let agent = self.agent_provider.get_agent().await;
        let (ordered_specs, op_indexes) = reorder_subdoc_ops(specs);
        let retry = options
            .retry_strategy
            .unwrap_or_else(|| self.default_retry_strategy.clone());
        let result = CouchbaseAgentProvider::upgrade_agent(agent)?
            .mutate_in(
                couchbase_core::options::crud::MutateInOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                    ordered_specs
                        .iter()
                        .map(|spec| (*spec).try_into())
                        .collect::<error::Result<Vec<MutateInOp>>>()?
                        .as_slice(),
                )
                .flags({
                    let mut flags = SubdocDocFlag::empty();
                    if options.access_deleted.unwrap_or(false) {
                        flags |= SubdocDocFlag::AccessDeleted;
                    }
                    match options.store_semantics {
                        Some(StoreSemantics::Insert) => flags |= SubdocDocFlag::AddDoc,
                        Some(StoreSemantics::Upsert) => flags |= SubdocDocFlag::MkDoc,
                        _ => {}
                    }
                    flags
                })
                .preserve_expiry(options.preserve_expiry)
                .expiry(options.expiry.map(Self::expiry_to_seconds).transpose()?)
                .cas(options.cas)
                .retry_strategy(retry),
            )
            .await?;

        let mut entries = vec![None; specs.len()];

        for (i, x) in result.value.into_iter().enumerate() {
            let original_idx = op_indexes[i];
            entries[original_idx] = Some(MutateInResultEntry {
                value: x.value.as_ref().map(|v| bytes::Bytes::from(v.clone())),
            });
        }

        Ok(MutateInResult {
            cas: result.cas,
            mutation_token: result
                .mutation_token
                .map(|t| MutationToken::new(t, self.bucket_name.clone())),
            entries: entries.into_iter().map(|x| x.unwrap()).collect(),
        })
    }
}
