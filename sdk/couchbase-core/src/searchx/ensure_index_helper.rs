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

use crate::httpx::client::Client;
use crate::httpx::request::OnBehalfOfInfo;
use crate::mgmtx::node_target::NodeTarget;
use crate::searchx::error;
use crate::searchx::mgmt_options::{EnsureIndexPollOptions, GetIndexOptions, RefreshConfigOptions};
use crate::searchx::search::Search;
use std::ops::Add;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Debug, Clone)]
pub struct EnsureIndexHelper<'a> {
    pub user_agent: &'a str,
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,

    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,
    pub index_name: &'a str,

    confirmed_endpoints: Vec<&'a str>,

    first_refresh_poll: Option<Instant>,
    refreshed_endpoints: Vec<String>,
}

#[derive(Copy, Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
#[non_exhaustive]
pub enum DesiredState {
    Created,
    Deleted,
}

impl<'a> EnsureIndexHelper<'a> {
    pub fn new(
        user_agent: &'a str,
        index_name: &'a str,
        bucket_name: Option<&'a str>,
        scope_name: Option<&'a str>,
        on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    ) -> Self {
        Self {
            user_agent,
            on_behalf_of_info,
            bucket_name,
            scope_name,
            index_name,
            confirmed_endpoints: vec![],
            first_refresh_poll: None,
            refreshed_endpoints: vec![],
        }
    }

    async fn poll_one<C: Client>(
        &self,
        client: Arc<C>,
        target: &NodeTarget,
    ) -> error::Result<bool> {
        let resp = Search {
            http_client: client,
            user_agent: self.user_agent.to_string(),
            endpoint: target.endpoint.to_string(),
            canonical_endpoint: target.canonical_endpoint.to_string(),
            auth: target.auth.clone(),
            vector_search_enabled: true,
            score_fusion_enabled: true,
            tracing: Default::default(),
        }
        .get_index(&GetIndexOptions {
            bucket_name: self.bucket_name,
            scope_name: self.scope_name,
            index_name: self.index_name,
            on_behalf_of: self.on_behalf_of_info,
        })
        .await;

        match resp {
            Ok(_r) => Ok(true),
            Err(e) => {
                if let error::ErrorKind::Server(e) = e.kind() {
                    if e.kind() == &error::ServerErrorKind::IndexNotFound {
                        return Ok(false);
                    }
                }

                Err(e)
            }
        }
    }

    async fn maybe_refresh_one<C: Client>(&mut self, client: Arc<C>, target: &NodeTarget) {
        if let Some(first_refresh_poll) = self.first_refresh_poll {
            if first_refresh_poll.add(Duration::from_secs(5)) > Instant::now() {
                return;
            }
        } else {
            self.first_refresh_poll = Some(Instant::now());
            return;
        }

        if self.refreshed_endpoints.contains(&target.endpoint.clone()) {
            return;
        }

        let _ = Search {
            http_client: client,
            user_agent: self.user_agent.to_string(),
            endpoint: target.endpoint.to_string(),
            canonical_endpoint: target.canonical_endpoint.to_string(),
            auth: target.auth.clone(),
            vector_search_enabled: true,
            score_fusion_enabled: true,
            tracing: Default::default(),
        }
        .refresh_config(&RefreshConfigOptions::new())
        .await;

        self.refreshed_endpoints.push(target.endpoint.clone());
    }

    pub async fn poll<C: Client>(
        &mut self,
        opts: &'a EnsureIndexPollOptions<C>,
    ) -> error::Result<bool> {
        let mut filtered_targets = Vec::with_capacity(opts.targets.len());

        for target in &opts.targets {
            if !self.confirmed_endpoints.contains(&target.endpoint.as_str()) {
                filtered_targets.push(target);
            }
        }

        let mut success_endpoints = Vec::new();
        for target in &filtered_targets {
            let mut exists = self.poll_one(opts.client.clone(), target).await?;

            match opts.desired_state {
                DesiredState::Created => {
                    if !exists {
                        self.maybe_refresh_one(opts.client.clone(), target).await;
                        exists = self.poll_one(opts.client.clone(), target).await?;
                    }

                    if exists {
                        success_endpoints.push(target.endpoint.as_str());
                    }
                }
                DesiredState::Deleted => {
                    if !exists {
                        success_endpoints.push(target.endpoint.as_str());
                    }
                }
            }
        }

        self.confirmed_endpoints
            .extend_from_slice(success_endpoints.as_slice());

        Ok(success_endpoints.len() == filtered_targets.len())
    }
}
