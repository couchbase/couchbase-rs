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

use crate::analyticsx::analytics::Query;
use crate::authenticator::Authenticator;
use crate::componentconfigs::NetworkAndCanonicalEndpoint;
use crate::error;
use crate::error::ErrorKind;
use crate::httpcomponent::{HttpComponent, HttpComponentState};
use crate::httpx::client::Client;
use crate::httpx::request::Auth;
use crate::options::analytics::{AnalyticsOptions, GetPendingMutationsOptions};
use crate::results::analytics::AnalyticsResultStream;
use crate::retry::{orchestrate_retries, RetryManager, RetryRequest};
use crate::service_type::ServiceType;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

pub(crate) struct AnalyticsComponent<C: Client> {
    id: String,
    http_component: HttpComponent<C>,

    retry_manager: Arc<RetryManager>,
}

pub(crate) struct AnalyticsComponentConfig {
    pub endpoints: HashMap<String, NetworkAndCanonicalEndpoint>,
    pub authenticator: Authenticator,
}

pub(crate) struct AnalyticsComponentOptions {
    pub id: String,
    pub user_agent: String,
}

impl<C: Client + 'static> AnalyticsComponent<C> {
    pub fn new(
        retry_manager: Arc<RetryManager>,
        http_client: Arc<C>,
        config: AnalyticsComponentConfig,
        opts: AnalyticsComponentOptions,
    ) -> Self {
        Self {
            id: opts.id,
            http_component: HttpComponent::new(
                ServiceType::ANALYTICS,
                opts.user_agent,
                http_client,
                HttpComponentState::new(config.endpoints, config.authenticator),
            ),
            retry_manager,
        }
    }

    pub fn reconfigure(&self, config: AnalyticsComponentConfig) {
        debug!(
            "Analytics component {} updating endpoints to {:?}",
            self.id,
            &config.endpoints.keys().collect::<Vec<_>>()
        );

        self.http_component.reconfigure(HttpComponentState::new(
            config.endpoints,
            config.authenticator,
        ))
    }

    pub async fn query(&self, opts: AnalyticsOptions) -> error::Result<AnalyticsResultStream> {
        let retry_info = RetryRequest::new("analytics", opts.read_only.unwrap_or_default());

        let retry = opts.retry_strategy.clone();
        let endpoint = opts.endpoint.clone();
        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry, retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    endpoint.clone(),
                    async |client: Arc<C>,
                           endpoint_id: String,
                           endpoint: String,
                           canonical_endpoint: String,
                           auth: Auth| {
                        let res = match (Query::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            canonical_endpoint,
                            auth,
                        }
                        .query(&copts)
                        .await)
                        {
                            Ok(r) => r,
                            Err(e) => return Err(ErrorKind::Analytics(e).into()),
                        };

                        Ok(AnalyticsResultStream {
                            inner: res,
                            endpoint,
                        })
                    },
                )
                .await
        })
        .await
    }

    pub async fn get_pending_mutations(
        &self,
        opts: &GetPendingMutationsOptions<'_>,
    ) -> error::Result<HashMap<String, HashMap<String, i64>>> {
        let retry_info = RetryRequest::new("get_pending_mutations", true);

        let retry = opts.retry_strategy.clone();
        let endpoint = opts.endpoint.clone();
        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry, retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    endpoint.clone(),
                    async |client: Arc<C>,
                           endpoint_id: String,
                           endpoint: String,
                           canonical_endpoint: String,
                           auth: Auth| {
                        let res = match (Query::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint,
                            canonical_endpoint,
                            auth,
                        }
                        .get_pending_mutations(&copts)
                        .await)
                        {
                            Ok(r) => r,
                            Err(e) => return Err(ErrorKind::Analytics(e).into()),
                        };

                        Ok(res)
                    },
                )
                .await
        })
        .await
    }
}
