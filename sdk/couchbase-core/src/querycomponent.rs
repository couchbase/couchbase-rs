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

use crate::authenticator::Authenticator;
use crate::componentconfigs::NetworkAndCanonicalEndpoint;
use crate::diagnosticscomponent::PingQueryReportOptions;
use crate::error::ErrorKind;
use crate::httpcomponent::{HttpComponent, HttpComponentState};
use crate::httpx::client::Client;
use crate::httpx::request::Auth;
use crate::mgmtx::node_target::NodeTarget;
use crate::options::query::{
    BuildDeferredIndexesOptions, CreateIndexOptions, CreatePrimaryIndexOptions, DropIndexOptions,
    DropPrimaryIndexOptions, EnsureIndexOptions, GetAllIndexesOptions, QueryOptions,
    WatchIndexesOptions,
};
use crate::queryx::ensure_index_helper::EnsureIndexHelper;
use crate::queryx::index::Index;
use crate::queryx::preparedquery::{PreparedQuery, PreparedStatementCache};
use crate::queryx::query::Query;
use crate::queryx::query_options::{EnsureIndexPollOptions, PingOptions};
use crate::results::pingreport::{EndpointPingReport, PingState};
use crate::results::query::QueryResultStream;
use crate::retry::{orchestrate_retries, RetryManager, RetryRequest, RetryStrategy};
use crate::retrybesteffort::ExponentialBackoffCalculator;
use crate::service_type::ServiceType;
use crate::tracingcomponent::TracingComponent;
use crate::{error, httpx};
use futures::future::join_all;
use futures::StreamExt;
use std::collections::HashMap;
use std::future::Future;
use std::ops::Sub;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::select;
use tracing::debug;

pub(crate) struct QueryComponent<C: Client> {
    id: String,
    http_component: HttpComponent<C>,
    tracing: Arc<TracingComponent>,

    retry_manager: Arc<RetryManager>,
    prepared_cache: Arc<Mutex<PreparedStatementCache>>,
}

pub(crate) struct QueryComponentConfig {
    pub endpoints: HashMap<String, NetworkAndCanonicalEndpoint>,
    pub authenticator: Authenticator,
}

pub(crate) struct QueryComponentOptions {
    pub id: String,
    pub user_agent: String,
}

impl<C: Client + 'static> QueryComponent<C> {
    pub fn new(
        retry_manager: Arc<RetryManager>,
        http_client: Arc<C>,
        tracing: Arc<TracingComponent>,
        config: QueryComponentConfig,
        opts: QueryComponentOptions,
    ) -> Self {
        Self {
            id: opts.id,
            http_component: HttpComponent::new(
                ServiceType::QUERY,
                opts.user_agent,
                http_client,
                HttpComponentState::new(config.endpoints, config.authenticator),
            ),
            tracing,
            retry_manager,
            prepared_cache: Arc::new(Mutex::new(PreparedStatementCache::default())),
        }
    }

    pub fn reconfigure(&self, config: QueryComponentConfig) {
        debug!(
            "Query component {} updating endpoints to {:?}",
            self.id,
            &config.endpoints.keys().collect::<Vec<_>>()
        );

        self.http_component.reconfigure(HttpComponentState::new(
            config.endpoints,
            config.authenticator,
        ))
    }

    pub async fn query(&self, opts: QueryOptions) -> error::Result<QueryResultStream> {
        let retry_info = RetryRequest::new("query", opts.read_only.unwrap_or_default());

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
                            tracing: self.tracing.clone(),
                        }
                        .query(&copts)
                        .await)
                        {
                            Ok(r) => r,
                            Err(e) => return Err(ErrorKind::Query(e).into()),
                        };

                        Ok(QueryResultStream {
                            inner: res,
                            endpoint,
                        })
                    },
                )
                .await
        })
        .await
    }

    pub async fn prepared_query(&self, opts: QueryOptions) -> error::Result<QueryResultStream> {
        let retry_info = RetryRequest::new("prepared_query", opts.read_only.unwrap_or_default());

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
                        let res = match (PreparedQuery {
                            executor: Query::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint: endpoint.clone(),
                                canonical_endpoint,
                                auth,
                                tracing: self.tracing.clone(),
                            },
                            cache: self.prepared_cache.clone(),
                        }
                        .prepared_query(&copts)
                        .await)
                        {
                            Ok(r) => r,
                            Err(e) => return Err(ErrorKind::Query(e).into()),
                        };

                        Ok(QueryResultStream {
                            inner: res,
                            endpoint,
                        })
                    },
                )
                .await
        })
        .await
    }

    pub async fn get_all_indexes(
        &self,
        opts: &GetAllIndexesOptions<'_>,
    ) -> error::Result<Vec<Index>> {
        let retry_info = RetryRequest::new("query_get_all_indexes", true);

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
                            tracing: self.tracing.clone(),
                        }
                        .get_all_indexes(&copts)
                        .await)
                        {
                            Ok(r) => r,
                            Err(e) => {
                                return Err(ErrorKind::Query(e).into());
                            }
                        };

                        Ok(res)
                    },
                )
                .await
        })
        .await
    }

    pub async fn create_primary_index(
        &self,
        opts: &CreatePrimaryIndexOptions<'_>,
    ) -> error::Result<()> {
        let retry_info = RetryRequest::new("query_create_primary_index", false);

        let retry = opts.retry_strategy.clone();
        let endpoint = opts.endpoint.clone();
        let copts = opts.into();

        self.orchestrate_no_res_mgmt_call(
            retry,
            retry_info,
            endpoint.map(|e| e.to_string()),
            async |query| {
                query
                    .create_primary_index(&copts)
                    .await
                    .map_err(|e| ErrorKind::Query(e).into())
            },
        )
        .await
    }

    pub async fn create_index(&self, opts: &CreateIndexOptions<'_>) -> error::Result<()> {
        let retry_info = RetryRequest::new("query_create_index", false);

        let retry = opts.retry_strategy.clone();
        let endpoint = opts.endpoint.clone();
        let copts = opts.into();

        self.orchestrate_no_res_mgmt_call(
            retry,
            retry_info,
            endpoint.map(|e| e.to_string()),
            async |query| {
                query
                    .create_index(&copts)
                    .await
                    .map_err(|e| ErrorKind::Query(e).into())
            },
        )
        .await
    }

    pub async fn drop_primary_index(
        &self,
        opts: &DropPrimaryIndexOptions<'_>,
    ) -> error::Result<()> {
        let retry_info = RetryRequest::new("query_drop_primary_index", false);

        let retry = opts.retry_strategy.clone();
        let endpoint = opts.endpoint.clone();
        let copts = opts.into();

        self.orchestrate_no_res_mgmt_call(
            retry,
            retry_info,
            endpoint.map(|e| e.to_string()),
            async |query| {
                query
                    .drop_primary_index(&copts)
                    .await
                    .map_err(|e| ErrorKind::Query(e).into())
            },
        )
        .await
    }

    pub async fn drop_index(&self, opts: &DropIndexOptions<'_>) -> error::Result<()> {
        let retry_info = RetryRequest::new("query_drop_index", false);

        let retry = opts.retry_strategy.clone();
        let endpoint = opts.endpoint.clone();
        let copts = opts.into();

        self.orchestrate_no_res_mgmt_call(
            retry,
            retry_info,
            endpoint.map(|e| e.to_string()),
            async |query| {
                query
                    .drop_index(&copts)
                    .await
                    .map_err(|e| ErrorKind::Query(e).into())
            },
        )
        .await
    }

    pub async fn build_deferred_indexes(
        &self,
        opts: &BuildDeferredIndexesOptions<'_>,
    ) -> error::Result<()> {
        let retry_info = RetryRequest::new("query_build_deferred_indexes", false);

        let retry = opts.retry_strategy.clone();
        let endpoint = opts.endpoint.clone();
        let copts = opts.into();

        self.orchestrate_no_res_mgmt_call(
            retry,
            retry_info,
            endpoint.map(|e| e.to_string()),
            async |query| {
                query
                    .build_deferred_indexes(&copts)
                    .await
                    .map_err(|e| ErrorKind::Query(e).into())
            },
        )
        .await
    }

    pub async fn watch_indexes(&self, opts: &WatchIndexesOptions<'_>) -> error::Result<()> {
        let retry_info = RetryRequest::new("query_watch_indexes", true);

        let retry = opts.retry_strategy.clone();
        let endpoint = opts.endpoint.clone();
        let copts = opts.into();

        self.orchestrate_no_res_mgmt_call(
            retry,
            retry_info,
            endpoint.map(|e| e.to_string()),
            async |query| {
                query
                    .watch_indexes(&copts)
                    .await
                    .map_err(|e| ErrorKind::Query(e).into())
            },
        )
        .await
    }

    pub async fn ensure_index(&self, opts: &EnsureIndexOptions<'_>) -> error::Result<()> {
        let mut helper = EnsureIndexHelper::new(
            self.http_component.user_agent(),
            opts.index_name,
            opts.bucket_name,
            opts.scope_name,
            opts.collection_name,
            opts.on_behalf_of_info,
        );

        let backoff = ExponentialBackoffCalculator::new(
            Duration::from_millis(100),
            Duration::from_millis(1000),
            1.5,
        );

        self.http_component
            .ensure_resource(backoff, async |client: Arc<C>, targets: Vec<NodeTarget>| {
                helper
                    .clone()
                    .poll(&EnsureIndexPollOptions {
                        client,
                        targets,
                        desired_state: opts.desired_state,
                    })
                    .await
                    .map_err(error::Error::from)
            })
            .await
    }

    pub async fn ping_all_endpoints(
        &self,
        on_behalf_of: Option<&httpx::request::OnBehalfOfInfo>,
    ) -> error::Result<Vec<error::Result<()>>> {
        let (client, targets) = self.http_component.get_all_targets::<NodeTarget>(&[])?;

        let copts = PingOptions { on_behalf_of };

        let mut handles = Vec::with_capacity(targets.len());
        let user_agent = self.http_component.user_agent().to_string();
        for target in targets {
            let user_agent = user_agent.clone();
            let client = Query::<C> {
                http_client: client.clone(),
                user_agent,
                endpoint: target.endpoint,
                canonical_endpoint: target.canonical_endpoint,
                auth: target.auth,
                tracing: self.tracing.clone(),
            };

            let handle = self.ping_one(client, copts.clone());

            handles.push(handle);
        }

        let results = join_all(handles).await;

        Ok(results)
    }

    pub async fn create_ping_report(
        &self,
        opts: PingQueryReportOptions<'_>,
    ) -> error::Result<Vec<EndpointPingReport>> {
        let (client, targets) = self.http_component.get_all_targets::<NodeTarget>(&[])?;

        let copts = PingOptions {
            on_behalf_of: opts.on_behalf_of,
        };
        let timeout = opts.timeout;

        let mut handles = Vec::with_capacity(targets.len());
        let user_agent = self.http_component.user_agent().to_string();
        for target in targets {
            let user_agent = user_agent.clone();
            let client = Query::<C> {
                http_client: client.clone(),
                user_agent,
                endpoint: target.endpoint,
                canonical_endpoint: target.canonical_endpoint,
                auth: target.auth,
                tracing: self.tracing.clone(),
            };

            let handle = self.create_one_report(client, timeout, copts.clone());

            handles.push(handle);
        }

        let reports = join_all(handles).await;

        Ok(reports)
    }

    async fn ping_one(&self, client: Query<C>, opts: PingOptions<'_>) -> error::Result<()> {
        client.ping(&opts).await.map_err(error::Error::from)
    }

    async fn create_one_report(
        &self,
        client: Query<C>,
        timeout: Duration,
        opts: PingOptions<'_>,
    ) -> EndpointPingReport {
        let start = std::time::Instant::now();

        let res = select! {
            e = tokio::time::sleep(timeout) => {
                return EndpointPingReport {
                    remote: client.endpoint,
                    error: None,
                    latency: std::time::Instant::now().sub(start),
                    id: None,
                    namespace: None,
                    state: PingState::Timeout,
                }
            }
            r = client.ping(&opts) => r.map_err(error::Error::from),
        };
        let end = std::time::Instant::now();

        let (error, state) = match res {
            Ok(_) => (None, PingState::Ok),
            Err(e) => (Some(e), PingState::Error),
        };

        EndpointPingReport {
            remote: client.endpoint,
            error,
            latency: end.sub(start),
            id: None,
            namespace: None,
            state,
        }
    }

    async fn orchestrate_no_res_mgmt_call<Fut>(
        &self,
        retry_strategy: Arc<dyn RetryStrategy>,
        retry_info: RetryRequest,
        endpoint: Option<String>,
        operation: impl Fn(Query<C>) -> Fut + Send + Sync,
    ) -> error::Result<()>
    where
        Fut: Future<Output = error::Result<()>> + Send,
        C: Client,
    {
        orchestrate_retries(
            self.retry_manager.clone(),
            retry_strategy,
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        endpoint.clone(),
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            operation(Query::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: self.tracing.clone(),
                            })
                            .await
                        },
                    )
                    .await
            },
        )
        .await
    }
}
