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

use crate::address::Address;
use crate::auth_mechanism::AuthMechanism;
use crate::authenticator::Authenticator;
use crate::cbconfig::TerseConfig;
use crate::collection_resolver_cached::{
    CollectionResolverCached, CollectionResolverCachedOptions,
};
use crate::collection_resolver_memd::{CollectionResolverMemd, CollectionResolverMemdOptions};
use crate::compressionmanager::{CompressionManager, StdCompressor};
use crate::configmanager::{
    ConfigManager, ConfigManagerMemd, ConfigManagerMemdConfig, ConfigManagerMemdOptions,
};
use crate::configparser::ConfigParser;
use crate::crudcomponent::CrudComponent;
use crate::diagnosticscomponent::{DiagnosticsComponent, DiagnosticsComponentConfig};
use crate::errmapcomponent::ErrMapComponent;
use crate::error::{Error, ErrorKind, Result};
use crate::features::BucketFeature;
use crate::httpcomponent::HttpComponent;
use crate::httpx::client::{ClientConfig, ReqwestClient};
use crate::kvclient::{
    KvClient, KvClientBootstrapOptions, KvClientOptions, StdKvClient, UnsolicitedPacket,
};
use crate::kvclient_ops::KvClientOps;
use crate::kvclientpool::{KvClientPool, KvClientPoolOptions, StdKvClientPool};
use crate::memdx::client::Client;
use crate::memdx::opcode::OpCode;
use crate::memdx::packet::ResponsePacket;
use crate::memdx::request::GetClusterConfigRequest;
use crate::mgmtcomponent::{MgmtComponent, MgmtComponentConfig, MgmtComponentOptions};
use crate::mgmtx::options::{GetTerseBucketConfigOptions, GetTerseClusterConfigOptions};
use crate::networktypeheuristic::NetworkTypeHeuristic;
use crate::nmvbhandler::{ConfigUpdater, StdNotMyVbucketConfigHandler};
use crate::options::agent::{AgentOptions, ReconfigureAgentOptions};
use crate::parsedconfig::{ParsedConfig, ParsedConfigBucketFeature, ParsedConfigFeature};
use crate::querycomponent::{QueryComponent, QueryComponentConfig, QueryComponentOptions};
use crate::retry::RetryManager;
use crate::searchcomponent::{SearchComponent, SearchComponentConfig, SearchComponentOptions};
use crate::service_type::ServiceType;
use crate::tls_config::TlsConfig;
use crate::util::{get_host_port_from_uri, get_hostname_from_host_port};
use crate::vbucketrouter::{
    StdVbucketRouter, VbucketRouter, VbucketRouterOptions, VbucketRoutingInfo,
};
use crate::{httpx, mgmtx};

use byteorder::BigEndian;
use futures::executor::block_on;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::analyticscomponent::{AnalyticsComponent, AnalyticsComponentOptions};
use crate::componentconfigs::{AgentComponentConfigs, HttpClientConfig};
use crate::httpx::request::{Auth, BasicAuth, BearerAuth};
use crate::kvclient_babysitter::{KvTarget, StdKvClientBabysitter};
use crate::kvendpointclientmanager::{
    KvEndpointClientManager, KvEndpointClientManagerOptions, StdKvEndpointClientManager,
};
use crate::orphan_reporter::OrphanReporter;
use crate::tracingcomponent::{TracingComponent, TracingComponentConfig};
use arc_swap::ArcSwap;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::{format, Display};
use std::io::Cursor;
use std::net::ToSocketAddrs;
use std::ops::{Add, Deref};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net;
use tokio::runtime::{Handle, Runtime};
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::{broadcast, mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout, timeout_at, Instant};

#[derive(Clone)]
struct AgentState {
    bucket: Option<String>,
    tls_config: Option<TlsConfig>,
    authenticator: Authenticator,
    auth_mechanisms: Vec<AuthMechanism>,
    num_pool_connections: usize,
    // http_transport:
    latest_config: ParsedConfig,
    network_type: String,

    disable_error_map: bool,
    disable_mutation_tokens: bool,
    disable_server_durations: bool,
    kv_connect_timeout: Duration,
    kv_connect_throttle_timeout: Duration,
    http_idle_connection_timeout: Duration,
    http_max_idle_connections_per_host: Option<usize>,
    tcp_keep_alive_time: Duration,
}

impl Display for AgentState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ bucket: {:?}, network_type: {}, num_pool_connections: {}, latest_config_rev_id: {}, latest_config_rev_epoch: {}, authenticator: {} }}",
            self.bucket,
            self.network_type,
            self.num_pool_connections,
            self.latest_config.rev_id,
            self.latest_config.rev_epoch,
            self.authenticator,

        )
    }
}

type AgentClientManager = StdKvEndpointClientManager<
    StdKvClientPool<StdKvClientBabysitter<StdKvClient<Client>>, StdKvClient<Client>>,
    StdKvClient<Client>,
>;
type AgentCollectionResolver = CollectionResolverCached<CollectionResolverMemd<AgentClientManager>>;

pub(crate) struct AgentInner {
    state: Arc<Mutex<AgentState>>,
    bucket: Option<String>,

    cfg_manager: Arc<ConfigManagerMemd<AgentClientManager>>,
    conn_mgr: Arc<AgentClientManager>,
    vb_router: Arc<StdVbucketRouter>,
    collections: Arc<AgentCollectionResolver>,
    retry_manager: Arc<RetryManager>,
    http_client: Arc<ReqwestClient>,
    err_map_component: Arc<ErrMapComponent>,

    pub(crate) crud: CrudComponent<
        AgentClientManager,
        StdVbucketRouter,
        StdNotMyVbucketConfigHandler<AgentInner>,
        AgentCollectionResolver,
        StdCompressor,
    >,

    pub(crate) analytics: Arc<AnalyticsComponent<ReqwestClient>>,
    pub(crate) query: Arc<QueryComponent<ReqwestClient>>,
    pub(crate) search: Arc<SearchComponent<ReqwestClient>>,
    pub(crate) mgmt: MgmtComponent<ReqwestClient>,
    pub(crate) diagnostics: DiagnosticsComponent<ReqwestClient, AgentClientManager>,
    pub(crate) tracing: Arc<TracingComponent>,
}

pub struct Agent {
    pub(crate) inner: Arc<AgentInner>,
    user_agent: String,
    id: String,
}

impl AgentInner {
    fn gen_agent_component_configs_locked(state: &AgentState) -> AgentComponentConfigs {
        AgentComponentConfigs::gen_from_config(
            &state.latest_config,
            &state.network_type,
            state.tls_config.clone(),
            state.bucket.clone(),
            state.authenticator.clone(),
        )
    }

    pub async fn unsolicited_packet_handler(&self, up: UnsolicitedPacket) {
        let packet = up.packet;
        if packet.op_code == OpCode::Set {
            if let Some(ref extras) = packet.extras {
                if extras.len() < 16 {
                    warn!("Received Set packet with too short extras: {packet:?}");
                    return;
                }

                let mut cursor = Cursor::new(extras);
                let server_rev_epoch = cursor.read_i64().await.unwrap();
                let server_rev_id = cursor.read_i64().await.unwrap();

                if let Some(config) = self
                    .cfg_manager
                    .out_of_band_version(server_rev_id, server_rev_epoch, up.endpoint_id)
                    .await
                {
                    self.apply_config(config).await;
                }
            } else {
                warn!("Received Set packet with no extras: {packet:?}");
            }
        }
    }

    pub async fn apply_config(&self, config: ParsedConfig) {
        let mut state = self.state.lock().await;

        info!(
            "Agent applying updated config: rev_id={rev_id}, rev_epoch={rev_epoch}",
            rev_id = config.rev_id,
            rev_epoch = config.rev_epoch
        );
        state.latest_config = config;

        self.update_state_locked(&mut state).await;
    }

    async fn update_state_locked(&self, state: &mut AgentState) {
        debug!("Agent updating state {}", state);

        let agent_component_configs = Self::gen_agent_component_configs_locked(state);

        // In order to avoid race conditions between operations selecting the
        // endpoint they need to send the request to, and fetching an actual
        // client which can send to that endpoint.  We must first ensure that
        // all the new endpoints are available in the manager.  Then update
        // the routing table.  Then go back and remove the old entries from
        // the connection manager list.

        if let Err(e) = self
            .conn_mgr
            .update_endpoints(agent_component_configs.kv_targets.clone(), true)
            .await
        {
            error!("Failed to reconfigure connection manager (add-only); {e}");
        };

        self.vb_router
            .update_vbucket_info(agent_component_configs.vbucket_routing_info);

        if let Err(e) = self
            .cfg_manager
            .reconfigure(agent_component_configs.config_manager_memd_config)
        {
            error!("Failed to reconfigure memd config watcher component; {e}");
        }

        if let Err(e) = self
            .conn_mgr
            .update_endpoints(agent_component_configs.kv_targets, false)
            .await
        {
            error!("Failed to reconfigure connection manager; {e}");
        }

        self.analytics
            .reconfigure(agent_component_configs.analytics_config);
        self.query.reconfigure(agent_component_configs.query_config);
        self.search
            .reconfigure(agent_component_configs.search_config);
        self.mgmt.reconfigure(agent_component_configs.mgmt_config);
        self.diagnostics
            .reconfigure(agent_component_configs.diagnostics_config);
        self.tracing
            .reconfigure(agent_component_configs.tracing_config);
    }

    pub async fn bucket_features(&self) -> Result<Vec<BucketFeature>> {
        let guard = self.state.lock().await;

        if let Some(bucket) = &guard.latest_config.bucket {
            let mut features = vec![];

            for feature in &bucket.features {
                match feature {
                    ParsedConfigBucketFeature::CreateAsDeleted => {
                        features.push(BucketFeature::CreateAsDeleted)
                    }
                    ParsedConfigBucketFeature::ReplaceBodyWithXattr => {
                        features.push(BucketFeature::ReplaceBodyWithXattr)
                    }
                    ParsedConfigBucketFeature::RangeScan => features.push(BucketFeature::RangeScan),
                    ParsedConfigBucketFeature::ReplicaRead => {
                        features.push(BucketFeature::ReplicaRead)
                    }
                    ParsedConfigBucketFeature::NonDedupedHistory => {
                        features.push(BucketFeature::NonDedupedHistory)
                    }
                    ParsedConfigBucketFeature::ReviveDocument => {
                        features.push(BucketFeature::ReviveDocument)
                    }
                    _ => {}
                }
            }

            return Ok(features);
        }

        Err(ErrorKind::NoBucket.into())
    }

    pub(crate) fn get_bucket_name(&self) -> Option<String> {
        self.bucket.clone()
    }

    pub async fn reconfigure(&self, opts: ReconfigureAgentOptions) {
        let mut state = self.state.lock().await;
        state.tls_config = opts.tls_config.clone();
        state.authenticator = opts.authenticator.clone();

        // We manually update tls for http as it requires rebuilding the client.
        match self
            .http_client
            .update_tls(httpx::client::UpdateTlsOptions {
                tls_config: opts.tls_config,
            }) {
            Ok(_) => {}
            Err(e) => {
                warn!("Failed to update TLS for HTTP client: {}", e);
            }
        };

        self.conn_mgr.update_auth(opts.authenticator).await;

        self.update_state_locked(&mut state).await;
    }
}

impl ConfigUpdater for AgentInner {
    async fn apply_terse_config(&self, config: TerseConfig, source_hostname: &str) {
        let parsed_config = match ConfigParser::parse_terse_config(config, source_hostname) {
            Ok(cfg) => cfg,
            Err(_e) => {
                // TODO: log
                return;
            }
        };

        if let Some(config) = self.cfg_manager.out_of_band_config(parsed_config) {
            self.apply_config(config).await;
        };
    }
}

impl Agent {
    pub async fn new(opts: AgentOptions) -> Result<Self> {
        let build_version = env!("CARGO_PKG_VERSION");
        let user_agent = format!("cb-rust/{build_version}");
        let agent_id = Uuid::new_v4().to_string();
        info!(
            "Core SDK Version: {} - Agent ID: {}",
            &user_agent, &agent_id
        );
        info!("Agent Options {opts}");

        let auth_mechanisms = if !opts.auth_mechanisms.is_empty() {
            if opts.tls_config.is_none() && opts.auth_mechanisms.contains(&AuthMechanism::Plain) {
                warn!("PLAIN sends credentials in plaintext, this will cause credential leakage on the network");
            } else if opts.tls_config.is_some()
                && (opts.auth_mechanisms.contains(&AuthMechanism::ScramSha512)
                    || opts.auth_mechanisms.contains(&AuthMechanism::ScramSha256)
                    || opts.auth_mechanisms.contains(&AuthMechanism::ScramSha1))
            {
                warn!("Consider using PLAIN for TLS connections, as it is more efficient");
            }

            opts.auth_mechanisms
        } else {
            vec![]
        };

        let mut state = AgentState {
            bucket: opts.bucket_name.clone(),
            authenticator: opts.authenticator.clone(),
            num_pool_connections: opts.kv_config.num_connections,
            latest_config: ParsedConfig::default(),
            network_type: "".to_string(),
            tls_config: opts.tls_config,
            auth_mechanisms: auth_mechanisms.clone(),
            disable_error_map: !opts.kv_config.enable_error_map,
            disable_mutation_tokens: !opts.kv_config.enable_mutation_tokens,
            disable_server_durations: !opts.kv_config.enable_server_durations,
            kv_connect_timeout: opts.kv_config.connect_timeout,
            kv_connect_throttle_timeout: opts.kv_config.connect_throttle_timeout,
            http_idle_connection_timeout: opts.http_config.idle_connection_timeout,
            http_max_idle_connections_per_host: opts.http_config.max_idle_connections_per_host,
            tcp_keep_alive_time: opts
                .tcp_keep_alive_time
                .unwrap_or_else(|| Duration::from_secs(60)),
        };

        let bucket_name = opts.bucket_name.clone();

        let http_client = Arc::new(ReqwestClient::new(ClientConfig {
            tls_config: state.tls_config.clone(),
            idle_connection_timeout: state.http_idle_connection_timeout,
            max_idle_connections_per_host: state.http_max_idle_connections_per_host,
            tcp_keep_alive_time: state.tcp_keep_alive_time,
        })?);

        let err_map_component = Arc::new(ErrMapComponent::new());

        let connect_timeout = opts.kv_config.connect_timeout;

        let first_kv_client_configs =
            Self::gen_first_kv_client_configs(&opts.seed_config.memd_addrs, &state);
        let first_http_client_configs = Self::gen_first_http_endpoints(
            user_agent.clone(),
            &opts.seed_config.http_addrs,
            &state,
        );
        let (first_config, cfg_source_host_port) = Self::get_first_config(
            user_agent.clone(),
            first_kv_client_configs,
            &state,
            first_http_client_configs,
            http_client.clone(),
            err_map_component.clone(),
            connect_timeout,
        )
        .await?;

        state.latest_config = first_config.clone();

        let network_type = if let Some(network) = opts.network {
            if network == "auto" || network.is_empty() {
                NetworkTypeHeuristic::identify(&state.latest_config, &cfg_source_host_port)
            } else {
                network
            }
        } else {
            NetworkTypeHeuristic::identify(&state.latest_config, &cfg_source_host_port)
        };
        info!(
            "Agent {} identified network type: {network_type}",
            &agent_id
        );
        state.network_type = network_type;

        let agent_component_configs = AgentInner::gen_agent_component_configs_locked(&state);

        let tracing = Arc::new(TracingComponent::new(
            agent_component_configs.tracing_config,
        ));

        let err_map_component_conn_mgr = err_map_component.clone();

        let num_pool_connections = state.num_pool_connections;

        let (unsolicited_packet_tx, mut unsolicited_packet_rx) = mpsc::unbounded_channel();

        let conn_mgr_id = Uuid::new_v4().to_string();
        info!(
            "Agent {} creating kv endpoint client manager {}",
            &agent_id, &conn_mgr_id
        );
        let conn_mgr = Arc::new(
            StdKvEndpointClientManager::new(KvEndpointClientManagerOptions {
                id: conn_mgr_id,
                on_close_handler: Arc::new(|_manager_id| {}),
                on_demand_connect: opts.kv_config.on_demand_connect,
                num_pool_connections,
                connect_throttle_period: opts.kv_config.connect_throttle_timeout,
                bootstrap_options: KvClientBootstrapOptions {
                    client_name: user_agent.clone(),
                    disable_error_map: state.disable_error_map,
                    disable_mutation_tokens: state.disable_mutation_tokens,
                    disable_server_durations: state.disable_server_durations,
                    on_err_map_fetched: Some(Arc::new(move |err_map| {
                        err_map_component_conn_mgr.on_err_map(err_map);
                    })),
                    tcp_keep_alive_time: state.tcp_keep_alive_time,
                    auth_mechanisms,
                    connect_timeout,
                },
                unsolicited_packet_tx: Some(unsolicited_packet_tx),
                orphan_handler: opts.orphan_response_handler,
                endpoints: agent_component_configs.kv_targets,
                authenticator: opts.authenticator,
                disable_decompression: opts.compression_config.disable_decompression,
                selected_bucket: opts.bucket_name,
                tracing: tracing.clone(),
            })
            .await?,
        );

        let cfg_manager = Arc::new(ConfigManagerMemd::new(
            agent_component_configs.config_manager_memd_config,
            ConfigManagerMemdOptions {
                polling_period: opts.config_poller_config.poll_interval,
                kv_client_manager: conn_mgr.clone(),
                first_config,
                fetch_timeout: opts.config_poller_config.fetch_timeout,
            },
        ));
        let vb_router = Arc::new(StdVbucketRouter::new(
            agent_component_configs.vbucket_routing_info,
            VbucketRouterOptions {},
        ));

        let nmvb_handler = Arc::new(StdNotMyVbucketConfigHandler::new());

        let memd_resolver = CollectionResolverMemd::new(CollectionResolverMemdOptions {
            conn_mgr: conn_mgr.clone(),
        });

        let collections = Arc::new(CollectionResolverCached::new(
            CollectionResolverCachedOptions {
                resolver: memd_resolver,
            },
        ));

        let retry_manager = Arc::new(RetryManager::new(err_map_component.clone()));
        let compression_manager = Arc::new(CompressionManager::new(opts.compression_config));

        let crud = CrudComponent::new(
            nmvb_handler.clone(),
            vb_router.clone(),
            conn_mgr.clone(),
            collections.clone(),
            retry_manager.clone(),
            compression_manager,
        );

        let mgmt_id = Uuid::new_v4().to_string();
        info!("Agent {} creating mgmt component {}", &agent_id, &mgmt_id);
        let mgmt = MgmtComponent::new(
            retry_manager.clone(),
            http_client.clone(),
            tracing.clone(),
            agent_component_configs.mgmt_config,
            MgmtComponentOptions {
                id: mgmt_id,
                user_agent: user_agent.clone(),
            },
        );

        let analytics_id = Uuid::new_v4().to_string();
        info!(
            "Agent {} creating analytics component {}",
            &agent_id, &analytics_id
        );
        let analytics = Arc::new(AnalyticsComponent::new(
            retry_manager.clone(),
            http_client.clone(),
            agent_component_configs.analytics_config,
            AnalyticsComponentOptions {
                id: analytics_id,
                user_agent: user_agent.clone(),
            },
        ));

        let query_id = Uuid::new_v4().to_string();
        info!("Agent {} creating query component {}", &agent_id, &query_id);
        let query = Arc::new(QueryComponent::new(
            retry_manager.clone(),
            http_client.clone(),
            tracing.clone(),
            agent_component_configs.query_config,
            QueryComponentOptions {
                id: query_id,
                user_agent: user_agent.clone(),
            },
        ));

        let search_id = Uuid::new_v4().to_string();
        info!(
            "Agent {} creating search component {}",
            &agent_id, &search_id
        );
        let search = Arc::new(SearchComponent::new(
            retry_manager.clone(),
            http_client.clone(),
            tracing.clone(),
            agent_component_configs.search_config,
            SearchComponentOptions {
                id: search_id,
                user_agent: user_agent.clone(),
            },
        ));

        let diagnostics = DiagnosticsComponent::new(
            conn_mgr.clone(),
            query.clone(),
            search.clone(),
            retry_manager.clone(),
            agent_component_configs.diagnostics_config,
        );

        let state = Arc::new(Mutex::new(state));

        let inner = Arc::new(AgentInner {
            state,
            bucket: bucket_name,
            cfg_manager: cfg_manager.clone(),
            conn_mgr,
            vb_router,
            crud,
            collections,
            retry_manager,
            http_client,
            err_map_component,

            mgmt,
            analytics,
            query,
            search,
            diagnostics,
            tracing,
        });

        let inner_clone = Arc::downgrade(&inner);
        tokio::spawn(async move {
            while let Some(packet) = unsolicited_packet_rx.recv().await {
                if let Some(inner_clone) = inner_clone.upgrade() {
                    inner_clone.unsolicited_packet_handler(packet).await;
                } else {
                    break;
                }
            }
            debug!("Unsolicited packet handler exited");
        });

        nmvb_handler.set_watcher(Arc::downgrade(&inner)).await;

        Self::start_config_watcher(Arc::downgrade(&inner), cfg_manager);

        let agent = Agent {
            inner,
            user_agent,
            id: agent_id.clone(),
        };

        info!("Agent {agent_id} created");

        Ok(agent)
    }

    // reconfigure allows updating certain aspects of the agent at runtime.
    // Note: toggling TLS on and off is not supported and will result in internal errors.
    pub async fn reconfigure(&self, opts: ReconfigureAgentOptions) {
        self.inner.reconfigure(opts).await
    }

    fn start_config_watcher(
        inner: Weak<AgentInner>,
        config_watcher: Arc<impl ConfigManager>,
    ) -> JoinHandle<()> {
        let mut watch_rx = config_watcher.watch();

        let inner = inner.clone();
        tokio::spawn(async move {
            loop {
                match watch_rx.changed().await {
                    Ok(_) => {
                        let pc = {
                            // apply_config requires an owned ParsedConfig, as it takes ownership of it.
                            // Doing the clone within a block also means we can release the lock that
                            // borrow_and_update() takes as soon as possible.
                            watch_rx.borrow_and_update().clone()
                        };
                        if let Some(i) = inner.upgrade() {
                            i.apply_config(pc).await;
                        } else {
                            debug!("Config watcher inner dropped, exiting");
                            return;
                        }
                    }
                    Err(_e) => {
                        debug!("Config watcher channel closed");
                        return;
                    }
                }
            }
        })
    }

    async fn get_first_config<C: httpx::client::Client>(
        client_name: String,
        kv_targets: HashMap<String, KvTarget>,
        state: &AgentState,
        http_configs: HashMap<String, FirstHttpConfig>,
        http_client: Arc<C>,
        err_map_component: Arc<ErrMapComponent>,
        connect_timeout: Duration,
    ) -> Result<(ParsedConfig, String)> {
        loop {
            for target in kv_targets.values() {
                let host = &target.address;
                let err_map_component_clone = err_map_component.clone();
                let timeout_result = timeout(
                    connect_timeout,
                    StdKvClient::new(KvClientOptions {
                        address: target.clone(),
                        authenticator: state.authenticator.clone(),
                        selected_bucket: state.bucket.clone(),
                        bootstrap_options: KvClientBootstrapOptions {
                            client_name: client_name.clone(),
                            disable_error_map: state.disable_error_map,
                            disable_mutation_tokens: true,
                            disable_server_durations: true,
                            on_err_map_fetched: Some(Arc::new(move |err_map| {
                                err_map_component_clone.on_err_map(err_map);
                            })),
                            tcp_keep_alive_time: state.tcp_keep_alive_time,
                            auth_mechanisms: state.auth_mechanisms.clone(),
                            connect_timeout,
                        },
                        endpoint_id: "".to_string(),
                        unsolicited_packet_tx: None,
                        orphan_handler: None,
                        on_close_tx: None,
                        disable_decompression: false,
                        id: Uuid::new_v4().to_string(),
                        tracing: Default::default(),
                    }),
                )
                .await;

                let client: StdKvClient<Client> = match timeout_result {
                    Ok(client_result) => match client_result {
                        Ok(client) => client,
                        Err(e) => {
                            let mut msg = format!("Failed to connect to endpoint: {e}");
                            if let Some(source) = e.source() {
                                msg = format!("{msg} - {source}");
                            }
                            warn!("{msg}");
                            continue;
                        }
                    },
                    Err(_e) => continue,
                };

                let raw_config = match client
                    .get_cluster_config(GetClusterConfigRequest {
                        known_version: None,
                    })
                    .await
                {
                    Ok(resp) => resp.config,
                    Err(_e) => continue,
                };

                client.close().await?;

                let config: TerseConfig = serde_json::from_slice(&raw_config).map_err(|e| {
                    Error::new_message_error(format!("failed to deserialize config: {e}"))
                })?;

                match ConfigParser::parse_terse_config(config, host.host.as_str()) {
                    Ok(c) => {
                        return Ok((c, format!("{}:{}", host.host, host.port)));
                    }
                    Err(_e) => continue,
                };
            }

            info!("Failed to fetch config over kv, attempting http");
            for endpoint_config in http_configs.values() {
                let endpoint = endpoint_config.endpoint.clone();
                let host_port = get_host_port_from_uri(&endpoint)?;
                let auth = match &endpoint_config.authenticator {
                    Authenticator::PasswordAuthenticator(authenticator) => {
                        let user_pass =
                            authenticator.get_credentials(&ServiceType::MGMT, host_port.clone())?;
                        Auth::BasicAuth(BasicAuth::new(user_pass.username, user_pass.password))
                    }
                    Authenticator::CertificateAuthenticator(_authenticator) => {
                        Auth::BasicAuth(BasicAuth::new("".to_string(), "".to_string()))
                    }
                    Authenticator::JwtAuthenticator(authenticator) => {
                        Auth::BearerAuth(BearerAuth::new(authenticator.get_token()))
                    }
                };

                match Self::fetch_http_config(
                    http_client.clone(),
                    endpoint,
                    endpoint_config.user_agent.clone(),
                    auth,
                    endpoint_config.bucket_name.clone(),
                )
                .await
                {
                    Ok(c) => {
                        return Ok((c, host_port));
                    }
                    Err(_e) => {}
                };
            }

            info!("Failed to fetch config from any source");

            // TODO: Make configurable?
            sleep(Duration::from_secs(1)).await;
        }
    }

    pub(crate) async fn fetch_http_config<C: httpx::client::Client>(
        http_client: Arc<C>,
        endpoint: String,
        user_agent: String,
        auth: Auth,
        bucket_name: Option<String>,
    ) -> Result<ParsedConfig> {
        debug!("Polling config from {}", &endpoint);

        let host_port = get_host_port_from_uri(&endpoint)?;
        let hostname = get_hostname_from_host_port(&host_port)?;

        let parsed = if let Some(bucket_name) = bucket_name {
            let config = mgmtx::mgmt::Management {
                http_client,
                user_agent,
                endpoint: endpoint.clone(),
                canonical_endpoint: endpoint.clone(),
                auth,
                tracing: Default::default(),
            }
            .get_terse_bucket_config(&GetTerseBucketConfigOptions {
                bucket_name: &bucket_name,
                on_behalf_of_info: None,
            })
            .await
            .map_err(Error::from)?;

            ConfigParser::parse_terse_config(config, &hostname)?
        } else {
            let config = mgmtx::mgmt::Management {
                http_client,
                user_agent,
                endpoint: endpoint.clone(),
                canonical_endpoint: endpoint.clone(),
                auth,
                tracing: Default::default(),
            }
            .get_terse_cluster_config(&GetTerseClusterConfigOptions {
                on_behalf_of_info: None,
            })
            .await
            .map_err(Error::from)?;

            ConfigParser::parse_terse_config(config, &hostname)?
        };

        Ok(parsed)
    }

    fn gen_first_kv_client_configs(
        memd_addrs: &Vec<Address>,
        state: &AgentState,
    ) -> HashMap<String, KvTarget> {
        let mut clients = HashMap::new();
        for addr in memd_addrs {
            let node_id = format!("kv-{addr}");
            let target = KvTarget {
                address: addr.clone(),
                tls_config: state.tls_config.clone(),
                canonical_address: addr.clone(),
            };
            clients.insert(node_id, target);
        }

        clients
    }

    fn gen_first_http_endpoints(
        client_name: String,
        mgmt_addrs: &Vec<Address>,
        state: &AgentState,
    ) -> HashMap<String, FirstHttpConfig> {
        let mut clients = HashMap::new();
        for addr in mgmt_addrs {
            let node_id = format!("mgmt{addr}");
            let base = if state.tls_config.is_some() {
                "https"
            } else {
                "http"
            };
            let config = FirstHttpConfig {
                endpoint: format!("{base}://{addr}"),
                tls: state.tls_config.clone(),
                user_agent: client_name.clone(),
                authenticator: state.authenticator.clone(),
                bucket_name: state.bucket.clone(),
            };
            clients.insert(node_id, config);
        }

        clients
    }

    pub(crate) async fn run_with_bucket_feature_check<T, Fut>(
        &self,
        feature: BucketFeature,
        operation: impl FnOnce() -> Fut,
        message: impl Into<String>,
    ) -> Result<T>
    where
        Fut: std::future::Future<Output = Result<T>>,
    {
        let features = self.bucket_features().await?;

        if !features.contains(&feature) {
            return Err(Error::new_feature_not_available_error(
                format!("{feature:?}"),
                message.into(),
            ));
        }

        operation().await
    }

    pub(crate) fn get_bucket_name(&self) -> Option<String> {
        self.inner.get_bucket_name()
    }
}

struct FirstHttpConfig {
    pub endpoint: String,
    pub tls: Option<TlsConfig>,
    pub user_agent: String,
    pub authenticator: Authenticator,
    pub bucket_name: Option<String>,
}

impl Drop for Agent {
    fn drop(&mut self) {
        debug!(
            "Dropping agent {}, {} strong references remain",
            self.id,
            Arc::strong_count(&self.inner)
        );
    }
}
