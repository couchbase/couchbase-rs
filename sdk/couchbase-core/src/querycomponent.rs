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
use crate::error::{DispatchOutcome, ErrorKind};
use crate::httpcomponent::{HttpComponent, HttpComponentState};
use crate::httpx::client::Client;
use crate::httpx::request::Auth;
use crate::log_redaction::{not_sensitive, system_data_list};
use crate::mgmtx::node_target::NodeTarget;
use crate::options::conversational_query::{
    AskChatOptions, BeginChatOptions, ChatState, CollectionKeyspace, ConversationalQueryOptions,
    ConversationalQueryOutput, EndChatOptions, PauseChatOptions, ResumeChatOptions,
};
use crate::options::query::{
    BuildDeferredIndexesOptions, CreateIndexOptions, CreatePrimaryIndexOptions, DropIndexOptions,
    DropPrimaryIndexOptions, EnsureIndexOptions, GetAllIndexesOptions, QueryOptions,
    WatchIndexesOptions,
};
use crate::queryx::conversational::{
    begin_chat_statement, classify_error, insert_generation_fields, keyspace_path,
    pause_chat_statement, using_ai_statement, whole_seconds, ConversationalErrorClass,
    END_CHAT_STATEMENT, ERR_CHAT_ALREADY_LIVE, ERR_CHAT_RESUME_FAILED, RESUME_CHAT_STATEMENT,
};
use crate::queryx::ensure_index_helper::EnsureIndexHelper;
use crate::queryx::index::Index;
use crate::queryx::preparedquery::{PreparedQuery, PreparedStatementCache};
use crate::queryx::query::Query;
use crate::queryx::query_options::{EnsureIndexPollOptions, NaturalLanguageOptions, PingOptions};
use crate::queryx::query_result::MetaData;
use crate::results::pingreport::{EndpointPingReport, PingState};
use crate::results::query::QueryResultStream;
use crate::retry::{orchestrate_retries, RetryManager, RetryRequest, RetryStrategy};
use crate::retrybesteffort::ExponentialBackoffCalculator;
use crate::service_type::ServiceType;
use crate::tracingcomponent::TracingComponent;
use crate::{error, httpx};
use futures::future::join_all;
use futures::StreamExt;
use rand::RngExt;
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::ops::Sub;
use std::sync::atomic::{AtomicBool, Ordering};
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

    node_uuids: Mutex<QueryNodeUuids>,
    conversational_query_supported: AtomicBool,
}

pub(crate) struct QueryComponentConfig {
    pub endpoints: HashMap<String, NetworkAndCanonicalEndpoint>,
    pub node_uuids: QueryNodeUuids,
    pub authenticator: Authenticator,
    pub conversational_query_supported: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct QueryNodeUuids {
    /// Node UUID to query endpoint id
    pub endpoint_ids: HashMap<String, String>,
    /// Whether different node UUIDs map to the same query host and port, e.g. when connecting
    /// through Private Links.
    pub shared_endpoint: bool,
}

impl QueryNodeUuids {
    pub fn new(
        endpoint_ids: HashMap<String, String>,
        endpoints: &HashMap<String, NetworkAndCanonicalEndpoint>,
    ) -> Self {
        let mut network_endpoints = HashSet::with_capacity(endpoint_ids.len());
        let shared_endpoint = endpoint_ids
            .values()
            .filter_map(|endpoint_id| endpoints.get(endpoint_id))
            .any(|endpoint| !network_endpoints.insert(endpoint.network_endpoint.as_str()));

        Self {
            endpoint_ids,
            shared_endpoint,
        }
    }
}

fn keyspace_paths(keyspaces: &[CollectionKeyspace]) -> Value {
    Value::from(
        keyspaces
            .iter()
            .map(|k| keyspace_path(&k.bucket_name, &k.scope_name, &k.collection_name))
            .collect::<Vec<_>>(),
    )
}

fn lifecycle_query_options(
    statement: String,
    server_timeout: Option<Duration>,
    retry_strategy: Arc<dyn RetryStrategy>,
) -> QueryOptions {
    QueryOptions::new()
        .statement(statement)
        .client_context_id(uuid::Uuid::new_v4().to_string())
        .metrics(false)
        .timeout(server_timeout)
        .retry_strategy(retry_strategy)
        .idempotent(false)
}

// Reads a chat lifecycle response, which has no rows, and returns its metadata.
async fn lifecycle_metadata(mut stream: QueryResultStream) -> error::Result<MetaData> {
    while let Some(row) = stream.next().await {
        row?;
    }
    Ok(stream.metadata()?.clone())
}

fn is_node_not_known(err: &error::Error) -> bool {
    matches!(err.kind(), ErrorKind::NodeNotKnown { .. })
}

// For BEGIN and RESUME, a target that left the config is the same as finding no node to select.
fn node_not_known_as_unavailable(err: error::Error) -> error::Error {
    if is_node_not_known(&err) {
        return ErrorKind::ServiceNotAvailable {
            service: ServiceType::QUERY,
        }
        .into();
    }
    err
}

fn chat_currently_unavailable(
    chat_id: &str,
    owner: Option<String>,
    operation: &'static str,
    source: Option<error::Error>,
) -> error::Error {
    ErrorKind::ConversationalChatCurrentlyUnavailable {
        chat_id: chat_id.to_string(),
        owner,
        operation,
        source,
    }
    .into()
}

// Maps the errors of an operation routed to the chat's owner.
fn chat_error(
    err: error::Error,
    chat_id: &str,
    owner: String,
    operation: &'static str,
) -> error::Error {
    let not_live = is_node_not_known(&err)
        || classify_error(&err)
            .is_some_and(|e| e.class == Some(ConversationalErrorClass::ChatNotLive));
    if not_live {
        let source = err.with_first_query_error_chosen();
        return chat_currently_unavailable(chat_id, Some(owner), operation, Some(source));
    }
    err
}

// The owner after a RESUME sent to `target` failed. A RESUME that may have been dispatched, or
// that found the chat already live (19255), leaves the chat on `target`. A RESUME to the
// retained owner that could not restore the chat (19243) means it is no longer there.
fn owner_after_failed_resume(
    retained: Option<&str>,
    target: &str,
    outcome: DispatchOutcome,
    first_code: Option<u32>,
) -> Option<String> {
    match (outcome, first_code) {
        (DispatchOutcome::Response, Some(ERR_CHAT_ALREADY_LIVE)) => Some(target.to_string()),
        (DispatchOutcome::Response, Some(ERR_CHAT_RESUME_FAILED)) if retained == Some(target) => {
            None
        }
        (DispatchOutcome::Response, _) | (DispatchOutcome::NotDispatched, _) => {
            retained.map(str::to_string)
        }
        _ => Some(target.to_string()),
    }
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
            node_uuids: Mutex::new(config.node_uuids),
            conversational_query_supported: AtomicBool::new(config.conversational_query_supported),
        }
    }

    pub fn reconfigure(&self, config: QueryComponentConfig) {
        debug!(
            "Query component {} updating endpoints to [{}]",
            not_sensitive(&self.id),
            system_data_list(config.endpoints.keys()).quoted()
        );

        // Held while the endpoints change, so that a node UUID and its endpoint are always
        // resolved from the same config.
        let mut node_uuids = self.node_uuids.lock().unwrap();
        self.http_component.reconfigure(HttpComponentState::new(
            config.endpoints,
            config.authenticator,
        ));
        *node_uuids = config.node_uuids;
        self.conversational_query_supported
            .store(config.conversational_query_supported, Ordering::SeqCst);
    }

    pub async fn conversational_query(
        &self,
        opts: ConversationalQueryOptions,
    ) -> error::Result<QueryResultStream> {
        let mut fields = Map::new();
        fields.insert("keyspaces".into(), keyspace_paths(&opts.keyspaces));
        if let Some(use_knowledge) = opts.use_knowledge {
            fields.insert("knowledge".into(), Value::Bool(use_knowledge));
        }
        insert_generation_fields(
            &mut fields,
            opts.hint,
            opts.execute,
            opts.output.map(|o| o.wire_value()),
        );

        let query_opts = opts
            .query_options
            .statement(using_ai_statement(fields, &opts.prompt))
            .natural_language(opts.natural_language)
            .read_only(true)
            .idempotent(true);

        self.query(query_opts).await
    }

    pub async fn begin_chat(&self, opts: BeginChatOptions) -> error::Result<ChatState> {
        self.ensure_chats_supported()?;

        let mut fields = Map::new();
        fields.insert("keyspaces".into(), keyspace_paths(&opts.keyspaces));
        if let Some(use_knowledge) = opts.use_knowledge {
            fields.insert("knowledge".into(), Value::Bool(use_knowledge));
        }
        if let Some(timeout) = opts.inactivity_timeout {
            fields.insert("timeout".into(), Value::from(whole_seconds(timeout)));
        }
        let query_opts = lifecycle_query_options(
            begin_chat_statement(fields),
            opts.server_timeout,
            opts.retry_strategy,
        );

        let retry_info = RetryRequest::new("query", query_opts.is_idempotent());
        let retry = query_opts.retry_strategy.clone();
        let copts = query_opts.into();

        let (stream, owner) =
            orchestrate_retries(self.retry_manager.clone(), retry, retry_info, async || {
                let target = self.select_chat_node()?;
                let stream = self.query_attempt(&copts, Some(&target)).await?;
                Ok((stream, target))
            })
            .await
            .map_err(node_not_known_as_unavailable)?;

        let meta = lifecycle_metadata(stream).await?;
        let chat_id = meta.chat_id.ok_or_else(|| {
            error::Error::new_message_error("the BEGIN CHAT response did not contain a chat id")
        })?;
        Ok(ChatState::new(chat_id, Some(owner)))
    }

    pub async fn ask_chat(
        &self,
        chat: &ChatState,
        opts: AskChatOptions,
    ) -> error::Result<QueryResultStream> {
        let owner = self.chat_owner(chat, "ask")?;

        let mut fields = Map::new();
        insert_generation_fields(
            &mut fields,
            opts.hint,
            opts.execute,
            opts.output.map(|o| o.wire_value()),
        );

        let query_opts = opts
            .query_options
            .statement(using_ai_statement(fields, &opts.prompt))
            .natural_language(opts.natural_language.chat_id(chat.chat_id().to_string()))
            .node_uuid(owner.clone())
            .idempotent(false);

        self.query(query_opts)
            .await
            .map_err(|e| chat_error(e, chat.chat_id(), owner, "ask"))
    }

    pub async fn pause_chat(
        &self,
        chat: &ChatState,
        opts: PauseChatOptions,
    ) -> error::Result<MetaData> {
        let owner = self.chat_owner(chat, "pause")?;

        // Without summarizing, the model is not invoked, so its parameters are not sent.
        let natural_language = if opts.summarize == Some(false) {
            NaturalLanguageOptions::new()
        } else {
            opts.natural_language
        }
        .chat_id(chat.chat_id().to_string());

        let query_opts = lifecycle_query_options(
            pause_chat_statement(opts.summarize),
            opts.server_timeout,
            opts.retry_strategy,
        )
        .natural_language(natural_language)
        .node_uuid(owner.clone());

        let meta = self
            .execute_chat_lifecycle(query_opts)
            .await
            .map_err(|e| chat_error(e, chat.chat_id(), owner, "pause"))?;
        chat.set_owner(None);
        Ok(meta)
    }

    pub async fn resume_chat(
        &self,
        chat: &ChatState,
        opts: ResumeChatOptions,
    ) -> error::Result<MetaData> {
        let retained = chat.owner()?;
        self.ensure_chats_supported()?;
        // The retained owner if it is in the current config, and otherwise a newly selected node.
        let target = match retained.as_ref().filter(|owner| self.is_known_node(owner)) {
            Some(owner) => owner.clone(),
            None => self.select_chat_node()?,
        };

        // Recorded before dispatch so that the owner is kept if this future is dropped while the
        // outcome is unknown.
        chat.set_owner(Some(target.clone()));

        let query_opts = lifecycle_query_options(
            RESUME_CHAT_STATEMENT.to_string(),
            opts.server_timeout,
            opts.retry_strategy,
        )
        .natural_language(NaturalLanguageOptions::new().chat_id(chat.chat_id().to_string()))
        .node_uuid(target.clone());

        let err = match self.execute_chat_lifecycle(query_opts).await {
            Ok(meta) => return Ok(meta),
            Err(err) => err,
        };

        let first_code = classify_error(&err).map(|e| e.code);
        let owner = owner_after_failed_resume(
            retained.as_deref(),
            &target,
            err.dispatch_outcome(),
            first_code,
        );
        chat.replace_owner(&target, owner);

        Err(node_not_known_as_unavailable(err))
    }

    pub async fn end_chat(
        &self,
        chat: &ChatState,
        opts: EndChatOptions,
    ) -> error::Result<MetaData> {
        let owner = self.chat_owner(chat, "end")?;

        let query_opts = lifecycle_query_options(
            END_CHAT_STATEMENT.to_string(),
            opts.server_timeout,
            opts.retry_strategy,
        )
        .natural_language(NaturalLanguageOptions::new().chat_id(chat.chat_id().to_string()))
        .node_uuid(owner.clone());

        let meta = self
            .execute_chat_lifecycle(query_opts)
            .await
            .map_err(|e| chat_error(e, chat.chat_id(), owner, "end"))?;
        chat.close();
        Ok(meta)
    }

    // The owner of `chat`, checking in order that the chat is open, that the cluster supports
    // chats and that an owner is known.
    fn chat_owner(&self, chat: &ChatState, operation: &'static str) -> error::Result<String> {
        let owner = chat.owner()?;
        self.ensure_chats_supported()?;
        owner.ok_or_else(|| chat_currently_unavailable(chat.chat_id(), None, operation, None))
    }

    fn ensure_chats_supported(&self) -> error::Result<()> {
        let msg = if !self.conversational_query_supported.load(Ordering::SeqCst) {
            "the cluster does not advertise conversational query chat support"
        } else if self.node_uuids.lock().unwrap().shared_endpoint {
            "chats are not supported when query nodes share the same host and port, for example \
             when connecting through Capella Private Link"
        } else {
            return Ok(());
        };
        Err(error::Error::new_feature_not_available_error(
            "conversational query",
            msg,
        ))
    }

    fn is_known_node(&self, node_uuid: &str) -> bool {
        self.node_uuids
            .lock()
            .unwrap()
            .endpoint_ids
            .contains_key(node_uuid)
    }

    fn select_chat_node(&self) -> error::Result<String> {
        let node_uuids = &self.node_uuids.lock().unwrap().endpoint_ids;
        if node_uuids.is_empty() {
            return Err(ErrorKind::ServiceNotAvailable {
                service: ServiceType::QUERY,
            }
            .into());
        }

        let idx = rand::rng().random_range(0..node_uuids.len());
        Ok(node_uuids.keys().nth(idx).cloned().unwrap())
    }

    // Runs a chat request that returns no rows and returns its metadata.
    async fn execute_chat_lifecycle(&self, opts: QueryOptions) -> error::Result<MetaData> {
        lifecycle_metadata(self.query(opts).await?).await
    }

    async fn orchestrate_query_endpoint<Resp, Fut>(
        &self,
        node_uuid: Option<&str>,
        operation: impl Fn(Arc<C>, String, String, String, Auth) -> Fut + Send + Sync,
    ) -> error::Result<Resp>
    where
        Fut: Future<Output = error::Result<Resp>> + Send,
        Resp: Send,
    {
        let Some(node_uuid) = node_uuid else {
            return self
                .http_component
                .orchestrate_endpoint(None, operation)
                .await;
        };

        // Resolved per dispatch attempt so that node UUIDs follow the latest config.
        let (client, endpoint_id, endpoint) = {
            let node_uuids = self.node_uuids.lock().unwrap();
            let endpoint_id = node_uuids.endpoint_ids.get(node_uuid).ok_or_else(|| {
                error::Error::from(ErrorKind::NodeNotKnown {
                    node_uuid: node_uuid.to_string(),
                })
            })?;
            let (client, endpoint) = self.http_component.select_specific_endpoint(endpoint_id)?;
            (client, endpoint_id.clone(), endpoint)
        };

        operation(
            client,
            endpoint_id,
            endpoint.endpoint,
            endpoint.canonical_endpoint,
            endpoint.auth,
        )
        .await
    }

    // A single attempt of a query, without retries.
    async fn query_attempt(
        &self,
        copts: &crate::queryx::query_options::QueryOptions,
        node_uuid: Option<&str>,
    ) -> error::Result<QueryResultStream> {
        self.orchestrate_query_endpoint(
            node_uuid,
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
                .query(copts)
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
    }

    pub async fn query(&self, opts: QueryOptions) -> error::Result<QueryResultStream> {
        let retry_info = RetryRequest::new("query", opts.is_idempotent());

        let retry = opts.retry_strategy.clone();
        let node_uuid = opts.node_uuid.clone();
        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry, retry_info, async || {
            self.query_attempt(&copts, node_uuid.as_deref()).await
        })
        .await
    }

    pub async fn prepared_query(&self, opts: QueryOptions) -> error::Result<QueryResultStream> {
        let retry_info = RetryRequest::new("prepared_query", opts.is_idempotent());

        let retry = opts.retry_strategy.clone();
        let node_uuid = opts.node_uuid.clone();
        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry, retry_info, async || {
            self.orchestrate_query_endpoint(
                node_uuid.as_deref(),
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

#[cfg(test)]
mod tests {
    use super::{owner_after_failed_resume, QueryNodeUuids};
    use crate::componentconfigs::NetworkAndCanonicalEndpoint;
    use crate::error::DispatchOutcome;
    use crate::error::ErrorKind;
    use std::collections::HashMap;

    fn endpoint(url: &str) -> NetworkAndCanonicalEndpoint {
        NetworkAndCanonicalEndpoint {
            network_endpoint: url.to_string(),
            canonical_endpoint: url.to_string(),
        }
    }

    fn node_uuids_with_urls(url_a: &str, url_c: &str) -> QueryNodeUuids {
        QueryNodeUuids::new(
            HashMap::from([
                ("uuid-a".to_string(), "queryep-a-8091".to_string()),
                ("uuid-c".to_string(), "queryep-c-8091".to_string()),
            ]),
            &HashMap::from([
                ("queryep-a-8091".to_string(), endpoint(url_a)),
                ("queryep-c-8091".to_string(), endpoint(url_c)),
            ]),
        )
    }

    fn node_uuids() -> QueryNodeUuids {
        node_uuids_with_urls("http://a:8093", "http://c:8093")
    }

    // Two node UUIDs behind one address, as seen through Capella Private Link.
    fn shared_node_uuids() -> QueryNodeUuids {
        node_uuids_with_urls("https://pl.example:18093", "https://pl.example:18093")
    }

    #[test]
    fn node_uuids_shared_endpoint_detection() {
        assert!(!node_uuids().shared_endpoint);
        assert!(shared_node_uuids().shared_endpoint);
        // Same host, different port is still individually addressable.
        assert!(!node_uuids_with_urls("http://a:8093", "http://a:9093").shared_endpoint);
        assert!(!QueryNodeUuids::default().shared_endpoint);
    }

    #[test]
    fn owner_after_failed_resume_follows_routing_rules() {
        use DispatchOutcome::*;
        let a = Some("a");

        // The outcome is unknown: the chat may now live on the target.
        assert_eq!(
            Some("b".into()),
            owner_after_failed_resume(a, "b", Unknown, None)
        );
        // Already live on the target.
        assert_eq!(
            Some("b".into()),
            owner_after_failed_resume(a, "b", Response, Some(19255))
        );
        // The retained owner could not restore the chat.
        assert_eq!(
            None,
            owner_after_failed_resume(a, "a", Response, Some(19243))
        );
        // A different node could not restore it: the retained owner is unchanged.
        assert_eq!(
            Some("a".into()),
            owner_after_failed_resume(a, "b", Response, Some(19243))
        );
        assert_eq!(
            None,
            owner_after_failed_resume(None, "b", Response, Some(19243))
        );
        // Other server errors and failures before dispatch leave the owner unchanged.
        assert_eq!(
            Some("a".into()),
            owner_after_failed_resume(a, "b", Response, Some(19240))
        );
        assert_eq!(
            Some("a".into()),
            owner_after_failed_resume(a, "b", NotDispatched, None)
        );
    }
}

#[cfg(test)]
mod chat_tests {
    use super::{QueryComponent, QueryComponentConfig, QueryComponentOptions, QueryNodeUuids};
    use crate::authenticator::{Authenticator, PasswordAuthenticator};
    use crate::componentconfigs::NetworkAndCanonicalEndpoint;
    use crate::errmapcomponent::ErrMapComponent;
    use crate::error::{Error, ErrorKind};
    use crate::httpx::client::Client;
    use crate::httpx::error::{Error as HttpError, Result as HttpResult};
    use crate::httpx::request::Request;
    use crate::httpx::response::Response;
    use crate::options::conversational_query::{
        AskChatOptions, BeginChatOptions, ChatState, CollectionKeyspace,
        ConversationalQueryOptions, ConversationalQueryOutput, EndChatOptions, PauseChatOptions,
        ResumeChatOptions,
    };
    use crate::queryx::query_options::NaturalLanguageOptions;
    use crate::retry::{RetryAction, RetryManager, RetryReason, RetryRequest, RetryStrategy};
    use crate::retryfailfast::FailFastRetryStrategy;
    use crate::tracingcomponent::{TracingComponent, TracingComponentConfig};
    use async_trait::async_trait;
    use futures::StreamExt;
    use serde_json::{json, Value};
    use std::collections::{HashMap, VecDeque};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    enum Reply {
        Body(Value),
        ConnectFailed,
        Hang,
    }

    // Answers requests with scripted replies, in order, and records where each was sent.
    #[derive(Default)]
    struct MockClient {
        replies: Mutex<VecDeque<Reply>>,
        requests: Mutex<Vec<(String, Value)>>,
    }

    #[async_trait]
    impl Client for MockClient {
        async fn execute(&self, req: Request) -> HttpResult<Response> {
            let body = serde_json::from_slice(req.body.as_deref().unwrap_or(b"null")).unwrap();
            self.requests.lock().unwrap().push((req.uri.clone(), body));
            let reply = self.replies.lock().unwrap().pop_front();
            match reply.expect("unexpected request") {
                Reply::Body(body) => {
                    // Errors after rows arrive in a successful HTTP response.
                    let status = if body.get("errors").is_some() && body.get("results").is_none() {
                        500
                    } else {
                        200
                    };
                    let res = http::Response::builder()
                        .status(status)
                        .body(body.to_string())
                        .unwrap();
                    Ok(reqwest::Response::from(res).into())
                }
                Reply::ConnectFailed => Err(HttpError::new_connect_error("connection refused")),
                Reply::Hang => std::future::pending().await,
            }
        }
    }

    #[derive(Debug)]
    struct RetryOnce;

    impl RetryStrategy for RetryOnce {
        fn retry_after(&self, request: &RetryRequest, _: &RetryReason) -> Option<RetryAction> {
            (request.retry_attempts() == 0).then(|| RetryAction::new(Duration::from_millis(1)))
        }
    }

    // A component whose config has Query nodes `a` and `b` at `http://a:8093` and `http://b:8093`.
    fn component(
        nodes: &[&str],
        replies: Vec<Reply>,
    ) -> (QueryComponent<MockClient>, Arc<MockClient>) {
        component_with(config(nodes, |n| format!("http://{n}:8093")), replies)
    }

    // A config with a Query node for each of `nodes`, using the node name as its UUID.
    fn config(nodes: &[&str], url: impl Fn(&str) -> String) -> QueryComponentConfig {
        let endpoints: HashMap<_, _> = nodes
            .iter()
            .map(|n| {
                let url = url(n);
                let endpoint = NetworkAndCanonicalEndpoint {
                    network_endpoint: url.clone(),
                    canonical_endpoint: url,
                };
                (format!("queryep-{n}"), endpoint)
            })
            .collect();
        let endpoint_ids = nodes
            .iter()
            .map(|n| (n.to_string(), format!("queryep-{n}")))
            .collect();
        QueryComponentConfig {
            node_uuids: QueryNodeUuids::new(endpoint_ids, &endpoints),
            endpoints,
            authenticator: Authenticator::PasswordAuthenticator(PasswordAuthenticator {
                username: "user".into(),
                password: "pass".into(),
            }),
            conversational_query_supported: true,
        }
    }

    fn component_with(
        config: QueryComponentConfig,
        replies: Vec<Reply>,
    ) -> (QueryComponent<MockClient>, Arc<MockClient>) {
        let client = Arc::new(MockClient {
            replies: Mutex::new(replies.into()),
            ..Default::default()
        });
        let component = QueryComponent::new(
            Arc::new(RetryManager::new(Arc::new(ErrMapComponent::new()))),
            client.clone(),
            Arc::new(TracingComponent::new(TracingComponentConfig::default())),
            config,
            QueryComponentOptions {
                id: "test".into(),
                user_agent: "test".into(),
            },
        );
        (component, client)
    }

    fn success(extra: Value) -> Reply {
        let mut body = json!({"requestID": "r", "results": [], "status": "success"});
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        Reply::Body(body)
    }

    fn errors(errors: &[(u32, bool)]) -> Reply {
        let errors: Vec<_> = errors
            .iter()
            .map(|(code, retry)| json!({"code": code, "msg": "error", "retry": retry}))
            .collect();
        Reply::Body(json!({"requestID": "r", "errors": errors, "status": "fatal"}))
    }

    fn hosts(client: &MockClient) -> Vec<String> {
        client
            .requests
            .lock()
            .unwrap()
            .iter()
            .map(|(uri, _)| uri.trim_start_matches("http://")[..1].to_string())
            .collect()
    }

    fn last_body(client: &MockClient) -> Value {
        client.requests.lock().unwrap().last().unwrap().1.clone()
    }

    fn server_code(err: &Error) -> u32 {
        match err.kind() {
            ErrorKind::Query(e) => match e.kind() {
                crate::queryx::error::ErrorKind::Server(e) => e.code(),
                _ => panic!("unexpected error: {err}"),
            },
            _ => panic!("unexpected error: {err}"),
        }
    }

    fn assert_chat_currently_unavailable(err: &Error, owner: Option<&str>) {
        match err.kind() {
            ErrorKind::ConversationalChatCurrentlyUnavailable { owner: o, .. } => {
                assert_eq!(owner, o.as_deref(), "{err}")
            }
            _ => panic!("unexpected error: {err}"),
        }
    }

    #[tokio::test]
    async fn begin_routes_to_selected_node_and_retains_it() {
        let (component, client) = component(&["a"], vec![success(json!({"chatId": "c1"}))]);
        let keyspaces = vec![CollectionKeyspace::new("b", "s", "c")];

        let chat = component
            .begin_chat(
                BeginChatOptions::new(keyspaces).inactivity_timeout(Duration::from_millis(1500)),
            )
            .await
            .unwrap();

        assert_eq!("c1", chat.chat_id());
        assert_eq!(Some("a".to_string()), chat.owner().unwrap());
        let body = last_body(&client);
        assert_eq!(
            "BEGIN CHAT WITH {\"keyspaces\":[\"`b`.`s`.`c`\"],\"timeout\":2}",
            body["statement"]
        );
        assert_eq!(false, body["metrics"]);
        assert!(body.get("natural_chatid").is_none(), "{body}");
    }

    #[tokio::test]
    async fn begin_selects_a_node_per_attempt_and_retains_the_one_that_answered() {
        let keyspaces = || vec![CollectionKeyspace::new("b", "s", "c")];
        let retry = || Arc::new(RetryOnce) as Arc<dyn RetryStrategy>;

        let (c, client) = component(
            &["a", "b"],
            vec![Reply::ConnectFailed, success(json!({"chatId": "c1"}))],
        );
        let chat = c
            .begin_chat(BeginChatOptions::new(keyspaces()).retry_strategy(retry()))
            .await
            .unwrap();
        let sent = hosts(&client);
        assert_eq!(2, sent.len());
        assert_eq!(Some(sent[1].clone()), chat.owner().unwrap());
    }

    #[tokio::test]
    async fn ask_maps_chat_not_live_and_unknown_owner_to_chat_currently_unavailable() {
        let (component, client) = component(&["a"], vec![errors(&[(19236, false)])]);

        let chat = ChatState::new("c1", Some("a".into()));
        let err = component
            .ask_chat(&chat, AskChatOptions::new("hello"))
            .await
            .err()
            .unwrap();
        assert_chat_currently_unavailable(&err, Some("a"));
        assert_eq!(Some("a".to_string()), chat.owner().unwrap());
        assert_eq!(vec!["a"], hosts(&client));
        assert_eq!("c1", last_body(&client)["natural_chatid"]);

        // The owner left the config: nothing is sent, and the original error is the source.
        let chat = ChatState::new("c1", Some("gone".into()));
        let err = component
            .ask_chat(&chat, AskChatOptions::new("hello"))
            .await
            .err()
            .unwrap();
        assert_chat_currently_unavailable(&err, Some("gone"));
        assert_eq!(Some("gone".to_string()), chat.owner().unwrap());
        assert!(
            matches!(
                err.kind(),
                ErrorKind::ConversationalChatCurrentlyUnavailable { source: Some(e), .. }
                    if matches!(e.kind(), ErrorKind::NodeNotKnown { .. })
            ),
            "{err}"
        );

        // No owner after a pause.
        let chat = ChatState::new("c1", None);
        let err = component
            .ask_chat(&chat, AskChatOptions::new("hello"))
            .await
            .err()
            .unwrap();
        assert_chat_currently_unavailable(&err, None);
        assert_eq!(1, client.requests.lock().unwrap().len());
    }

    #[tokio::test]
    async fn pause_without_summarizing_omits_model_and_clears_owner() {
        let (component, client) = component(&["a"], vec![success(json!({}))]);
        let chat = ChatState::new("c1", Some("a".into()));
        let model = NaturalLanguageOptions::new()
            .model("gpt".to_string())
            .vendor("openai".to_string());

        component
            .pause_chat(
                &chat,
                PauseChatOptions::new()
                    .summarize(false)
                    .natural_language(model),
            )
            .await
            .unwrap();

        let body = last_body(&client);
        assert_eq!("PAUSE CHAT WITH {\"summarize\":false}", body["statement"]);
        assert_eq!("c1", body["natural_chatid"]);
        assert!(body.get("natural_model").is_none(), "{body}");
        assert!(body.get("natural_vendor").is_none(), "{body}");
        assert_eq!(None, chat.owner().unwrap());
    }

    #[tokio::test]
    async fn end_closes_chat() {
        let (component, _) = component(&["a"], vec![success(json!({}))]);
        let chat = ChatState::new("c1", Some("a".into()));

        component
            .end_chat(&chat, EndChatOptions::new())
            .await
            .unwrap();

        assert!(chat.owner().is_err());
        let err = component
            .ask_chat(&chat, AskChatOptions::new("hello"))
            .await
            .err()
            .unwrap();
        assert!(
            matches!(err.kind(), ErrorKind::InvalidArgument { .. }),
            "{err}"
        );
    }

    #[tokio::test]
    async fn resume_selects_another_node_when_retained_owner_left() {
        let (component, client) = component(&["b"], vec![success(json!({}))]);
        let chat = ChatState::new("c1", Some("gone".into()));

        component
            .resume_chat(&chat, ResumeChatOptions::new())
            .await
            .unwrap();

        assert_eq!(vec!["b"], hosts(&client));
        assert_eq!(Some("b".to_string()), chat.owner().unwrap());
    }

    #[tokio::test]
    async fn resume_owner_follows_server_response() {
        // Already live on the newly selected node.
        let (c, _) = component(&["b"], vec![errors(&[(19255, false)])]);
        let chat = ChatState::new("c1", None);
        c.resume_chat(&chat, ResumeChatOptions::new())
            .await
            .unwrap_err();
        assert_eq!(Some("b".to_string()), chat.owner().unwrap());

        // The retained owner could not restore the chat.
        let (c, _) = component(&["a"], vec![errors(&[(19243, false)])]);
        let chat = ChatState::new("c1", Some("a".into()));
        c.resume_chat(&chat, ResumeChatOptions::new())
            .await
            .unwrap_err();
        assert_eq!(None, chat.owner().unwrap());

        // A newly selected node failed with an unrelated error.
        let (c, _) = component(&["b"], vec![errors(&[(5000, false)])]);
        let chat = ChatState::new("c1", None);
        c.resume_chat(&chat, ResumeChatOptions::new())
            .await
            .unwrap_err();
        assert_eq!(None, chat.owner().unwrap());
    }

    #[tokio::test]
    async fn first_error_decides_resume_routing() {
        // 19255 comes first, though 5000 is the first non-retryable error.
        let (component, _) = component(&["b"], vec![errors(&[(19255, true), (5000, false)])]);
        let chat = ChatState::new("c1", None);

        let err = component
            .resume_chat(&chat, ResumeChatOptions::new())
            .await
            .unwrap_err();

        assert_eq!(Some("b".to_string()), chat.owner().unwrap());
        assert_eq!(5000, server_code(&err));
        assert_eq!(19255, server_code(&err.with_first_query_error_chosen()));
    }

    #[tokio::test]
    async fn one_shot_query_is_read_only_and_sends_using_ai_fields() {
        let (component, client) = component(&["a"], vec![success(json!({}))]);
        let opts = ConversationalQueryOptions::new(
            "how many?",
            vec![CollectionKeyspace::new("b", "s", "c")],
        )
        .use_knowledge(true)
        .hint("h".to_string())
        .execute(false)
        .output(ConversationalQueryOutput::FtsSqlpp);

        component.conversational_query(opts).await.err();

        let body = last_body(&client);
        let statement = body["statement"].as_str().unwrap();
        let with = statement
            .strip_prefix("USING AI WITH ")
            .and_then(|s| s.strip_suffix(" how many?"))
            .unwrap_or_else(|| panic!("{statement}"));
        assert_eq!(
            json!({
                "keyspaces": ["`b`.`s`.`c`"],
                "knowledge": true,
                "hint": "h",
                "execute": false,
                "output": "FTSSQL",
            }),
            serde_json::from_str::<Value>(with).unwrap()
        );
        assert_eq!(true, body["readonly"]);
        assert!(body.get("natural_chatid").is_none(), "{body}");
    }

    #[tokio::test]
    async fn ask_sends_generation_fields_without_knowledge() {
        let (component, client) = component(&["a"], vec![success(json!({}))]);
        let chat = ChatState::new("c1", Some("a".into()));

        component
            .ask_chat(
                &chat,
                AskChatOptions::new("and now?").output(ConversationalQueryOutput::JsUdf),
            )
            .await
            .err();

        let body = last_body(&client);
        assert_eq!(
            "USING AI WITH {\"output\":\"JSUDF\"} and now?",
            body["statement"]
        );
        assert!(body.get("readonly").is_none(), "{body}");
    }

    #[tokio::test]
    async fn chats_require_feature_support_before_routing() {
        let assert_feature_not_available = |err: Error| {
            assert!(
                matches!(err.kind(), ErrorKind::FeatureNotAvailable { .. }),
                "{err}"
            )
        };

        let mut unsupported = config(&["a"], |n| format!("http://{n}:8093"));
        unsupported.conversational_query_supported = false;
        // Two nodes behind one address, as through Capella Private Link.
        let shared = config(&["a", "b"], |_| "https://pl.example:18093".to_string());

        for config in [unsupported, shared] {
            let (component, client) = component_with(config, vec![]);
            // No owner would otherwise be ConversationalChatCurrentlyUnavailable.
            let chat = ChatState::new("c1", None);

            let keyspaces = vec![CollectionKeyspace::new("b", "s", "c")];
            assert_feature_not_available(
                component
                    .begin_chat(BeginChatOptions::new(keyspaces))
                    .await
                    .unwrap_err(),
            );
            assert_feature_not_available(
                component
                    .ask_chat(&chat, AskChatOptions::new("hello"))
                    .await
                    .err()
                    .unwrap(),
            );
            assert_feature_not_available(
                component
                    .pause_chat(&chat, PauseChatOptions::new())
                    .await
                    .unwrap_err(),
            );
            assert_feature_not_available(
                component
                    .resume_chat(&chat, ResumeChatOptions::new())
                    .await
                    .unwrap_err(),
            );
            assert_feature_not_available(
                component
                    .end_chat(&chat, EndChatOptions::new())
                    .await
                    .unwrap_err(),
            );
            assert!(client.requests.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn resume_restores_owner_when_not_sent() {
        let (component, client) = component(&["b"], vec![Reply::ConnectFailed]);
        let chat = ChatState::new("c1", None);

        component
            .resume_chat(
                &chat,
                ResumeChatOptions::new().retry_strategy(Arc::new(FailFastRetryStrategy::default())),
            )
            .await
            .unwrap_err();

        assert_eq!(vec!["b"], hosts(&client));
        assert_eq!(None, chat.owner().unwrap());
    }

    #[tokio::test]
    async fn first_error_decides_errors_after_rows() {
        let (component, _) = component(
            &["a"],
            vec![Reply::Body(json!({
                "requestID": "r",
                "results": [1],
                "errors": [
                    {"code": 19220, "msg": "busy", "retry": true},
                    {"code": 5000, "msg": "error", "retry": false}
                ],
                "status": "fatal"
            }))],
        );
        let chat = ChatState::new("c1", Some("a".into()));

        let mut stream = component
            .ask_chat(&chat, AskChatOptions::new("hello"))
            .await
            .unwrap();
        stream.next().await.unwrap().unwrap();
        let err = stream.next().await.unwrap().unwrap_err();
        assert_eq!(19220, server_code(&err.with_first_query_error_chosen()));
        let err = stream.metadata().unwrap_err();
        assert_eq!(19220, server_code(&err.with_first_query_error_chosen()));
    }

    #[tokio::test]
    async fn dropped_resume_keeps_target_as_owner() {
        let (component, client) = component(&["b"], vec![Reply::Hang]);
        let chat = ChatState::new("c1", None);

        let resume = component.resume_chat(&chat, ResumeChatOptions::new());
        tokio::time::timeout(Duration::from_millis(50), resume)
            .await
            .unwrap_err();

        // The request may have been sent, so the chat may now live on the target.
        assert_eq!(vec!["b"], hosts(&client));
        assert_eq!(Some("b".to_string()), chat.owner().unwrap());
    }

    #[tokio::test]
    async fn ask_and_summarizing_pause_send_chat_model() {
        let (component, client) = component(&["a"], vec![success(json!({})), success(json!({}))]);
        let chat = ChatState::new("c1", Some("a".into()));
        let model = NaturalLanguageOptions::new()
            .model("gpt".to_string())
            .vendor("openai".to_string());
        let assert_model_sent = |body: Value| {
            assert_eq!("gpt", body["natural_model"], "{body}");
            assert_eq!("openai", body["natural_vendor"], "{body}");
            assert_eq!("c1", body["natural_chatid"], "{body}");
        };

        component
            .ask_chat(
                &chat,
                AskChatOptions::new("hello").natural_language(model.clone()),
            )
            .await
            .err();
        assert_model_sent(last_body(&client));

        component
            .pause_chat(&chat, PauseChatOptions::new().natural_language(model))
            .await
            .unwrap();
        let body = last_body(&client);
        assert_eq!("PAUSE CHAT", body["statement"]);
        assert_model_sent(body);
    }
}
