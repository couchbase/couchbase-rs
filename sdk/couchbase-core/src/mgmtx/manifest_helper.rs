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
use crate::mgmtx::error;
use crate::mgmtx::mgmt::Management;
use crate::mgmtx::node_target::NodeTarget;
use crate::mgmtx::options::{EnsureManifestPollOptions, GetCollectionManifestOptions};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct EnsureManifestHelper<'a> {
    user_agent: &'a str,
    bucket_name: &'a str,
    manifest_uid: u64,

    on_behalf_of_info: Option<&'a OnBehalfOfInfo>,

    confirmed_endpoints: Vec<&'a str>,
}

impl<'a> EnsureManifestHelper<'a> {
    pub fn new(
        user_agent: &'a str,
        bucket_name: &'a str,
        manifest_uid: u64,
        on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    ) -> Self {
        Self {
            user_agent,
            bucket_name,
            manifest_uid,
            on_behalf_of_info,
            confirmed_endpoints: vec![],
        }
    }

    async fn poll_one<C: Client>(
        &self,
        client: Arc<C>,
        target: &NodeTarget,
    ) -> error::Result<bool> {
        let resp = Management {
            http_client: client,
            user_agent: self.user_agent.to_string(),
            endpoint: target.endpoint.to_string(),
            canonical_endpoint: target.canonical_endpoint.to_string(),
            auth: target.auth.clone(),
            tracing: Default::default(),
        }
        .get_collection_manifest(&GetCollectionManifestOptions {
            on_behalf_of_info: self.on_behalf_of_info,
            bucket_name: self.bucket_name,
        })
        .await?;

        let manifest_uid = u64::from_str_radix(&resp.uid, 16).map_err(|e| {
            error::Error::new_message_error(format!("failed to parse manifest uid: {e}"))
        })?;

        Ok(manifest_uid >= self.manifest_uid)
    }

    pub async fn poll<C: Client>(
        &mut self,
        opts: &'a EnsureManifestPollOptions<C>,
    ) -> error::Result<bool> {
        let mut filtered_targets = vec![];
        for target in &opts.targets {
            if !self.confirmed_endpoints.contains(&target.endpoint.as_str()) {
                filtered_targets.push(target);
            }
        }

        let mut success_endpoints = vec![];
        for target in &opts.targets {
            if self.poll_one(opts.client.clone(), target).await? {
                success_endpoints.push(target.endpoint.as_str());
            }
        }

        self.confirmed_endpoints
            .extend_from_slice(success_endpoints.as_slice());

        Ok(success_endpoints.len() == filtered_targets.len())
    }
}
