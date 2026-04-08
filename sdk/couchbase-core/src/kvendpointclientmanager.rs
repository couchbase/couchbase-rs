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
use crate::error;
use crate::kvclient::{KvClient, KvClientBootstrapOptions, UnsolicitedPacketSender};
use crate::kvclient_babysitter::{KvClientBabysitter, KvTarget};
use crate::kvclient_ops::KvClientOps;
use crate::kvclientpool::{KvClientPool, KvClientPoolOptions};
use crate::memdx::dispatcher::OrphanResponseHandler;
use crate::memdx::request::PingRequest;
use crate::memdx::response::PingResponse;
use crate::results::diagnostics::EndpointDiagnostics;
use crate::tracingcomponent::TracingComponent;
use arc_swap::ArcSwap;
use futures::future::join_all;
use futures::AsyncWriteExt;
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, trace};
use uuid::Uuid;

pub(crate) trait KvEndpointClientManager: Sized + Send + Sync {
    type Client: KvClient + KvClientOps + Send + Sync;

    async fn new(opts: KvEndpointClientManagerOptions) -> error::Result<Self>;

    fn get_client(&self) -> impl Future<Output = error::Result<Arc<Self::Client>>> + Send;
    fn get_endpoint_client(
        &self,
        endpoint: &str,
    ) -> impl Future<Output = error::Result<Arc<Self::Client>>> + Send;
    fn update_endpoints(
        &self,
        endpoints: HashMap<String, KvTarget>,
        add_only: bool,
    ) -> impl Future<Output = error::Result<()>> + Send;
    fn update_auth(&self, authenticator: Authenticator) -> impl Future<Output = ()> + Send;
    fn ping_all_clients(
        &self,
        req: PingRequest,
    ) -> impl Future<Output = HashMap<String, Vec<error::Result<PingResponse>>>> + Send;
    fn endpoint_diagnostics(&self) -> impl Future<Output = Vec<EndpointDiagnostics>> + Send;
    fn get_client_per_endpoint(
        &self,
    ) -> impl Future<Output = error::Result<Vec<Arc<Self::Client>>>> + Send;
    // async fn update_selected_bucket(&self, bucket_name: String);
}

pub(crate) type KvEndpointClientManagerCloseHandler = Arc<dyn Fn(String) + Send + Sync>;

pub(crate) struct KvEndpointClientManagerOptions {
    pub id: String,
    pub on_close_handler: KvEndpointClientManagerCloseHandler,

    pub on_demand_connect: bool,
    pub num_pool_connections: usize,
    pub connect_throttle_period: Duration,
    pub disable_decompression: bool,
    pub bootstrap_options: KvClientBootstrapOptions,
    pub unsolicited_packet_tx: Option<UnsolicitedPacketSender>,
    pub orphan_handler: Option<OrphanResponseHandler>,
    pub tracing: Arc<TracingComponent>,

    pub endpoints: HashMap<String, KvTarget>,
    pub authenticator: Authenticator,
    pub selected_bucket: Option<String>,
}

struct KvEndpointClientManagerFastState<P, K>
where
    P: KvClientPool<Client = K>,
    K: KvClient,
{
    client_pools: HashMap<String, Arc<P>>,
}

struct KvEndpointClientManagerSlowState<P, K>
where
    P: KvClientPool<Client = K>,
    K: KvClient,
{
    auth: Authenticator,
    selected_bucket: Option<String>,
    client_pools: HashMap<String, Arc<P>>,
}

pub(crate) struct StdKvEndpointClientManager<P, K>
where
    P: KvClientPool<Client = K>,
    K: KvClient,
{
    id: String,

    on_close_handler: KvEndpointClientManagerCloseHandler,

    on_demand_connect: bool,
    num_pool_connections: usize,
    connect_throttle_period: Duration,
    disable_decompression: bool,
    bootstrap_options: KvClientBootstrapOptions,
    unsolicited_packet_tx: Option<UnsolicitedPacketSender>,
    orphan_handler: Option<OrphanResponseHandler>,
    tracing: Arc<TracingComponent>,

    slow_state: Arc<Mutex<KvEndpointClientManagerSlowState<P, K>>>,
    fast_state: ArcSwap<KvEndpointClientManagerFastState<P, K>>,
}

impl<P, K> KvEndpointClientManager for StdKvEndpointClientManager<P, K>
where
    P: KvClientPool<Client = K>,
    K: KvClient + KvClientOps,
{
    type Client = K;

    async fn new(opts: KvEndpointClientManagerOptions) -> error::Result<Self> {
        let slow_state = KvEndpointClientManagerSlowState {
            auth: opts.authenticator,
            selected_bucket: opts.selected_bucket,
            client_pools: HashMap::new(),
        };

        let fast_state = KvEndpointClientManagerFastState {
            client_pools: HashMap::new(),
        };

        let mgr = StdKvEndpointClientManager {
            id: opts.id,
            on_close_handler: opts.on_close_handler,
            on_demand_connect: opts.on_demand_connect,
            num_pool_connections: opts.num_pool_connections,
            connect_throttle_period: opts.connect_throttle_period,
            disable_decompression: opts.disable_decompression,
            bootstrap_options: opts.bootstrap_options,
            unsolicited_packet_tx: opts.unsolicited_packet_tx,
            orphan_handler: opts.orphan_handler,
            tracing: opts.tracing,

            slow_state: Arc::new(Mutex::new(slow_state)),
            fast_state: ArcSwap::from_pointee(fast_state),
        };

        mgr.update_endpoints(opts.endpoints, false).await?;

        Ok(mgr)
    }

    async fn get_client(&self) -> error::Result<Arc<K>> {
        let state = self.fast_state.load();

        // Just pick one at random for now
        if let Some(pool) = state.client_pools.values().next() {
            return pool.get_client().await;
        }

        Err(error::Error::new_message_error("invalid endpoint"))
    }

    async fn get_endpoint_client(&self, endpoint: &str) -> error::Result<Arc<K>> {
        let state = self.fast_state.load();

        let pool = match state.client_pools.get(endpoint) {
            Some(p) => p,
            None => {
                return Err(error::Error::new_message_error("invalid endpoint"));
            }
        };

        pool.get_client().await
    }

    async fn update_endpoints(
        &self,
        endpoints: HashMap<String, KvTarget>,
        add_only: bool,
    ) -> error::Result<()> {
        debug!(
            "Kvclientmanager {} updating endpoints to {:?}",
            self.id,
            endpoints.keys()
        );

        let mut slow_state = self.slow_state.lock().await;

        let mut old_pools = HashMap::with_capacity(slow_state.client_pools.len());
        for (pool_name, pool) in slow_state.client_pools.drain() {
            old_pools.insert(pool_name, pool);
        }

        let mut new_pools = HashMap::new();

        for (endpoint, target) in endpoints.into_iter() {
            let old_pool = old_pools.remove(&endpoint);
            let pool = if let Some(old_pool) = old_pool {
                old_pool.update_target(target).await;

                old_pool
            } else {
                let pool_id = Uuid::new_v4().to_string();
                info!("Kvclientmanager {} creating pool {}", self.id, &pool_id);
                let pool = P::new(KvClientPoolOptions {
                    id: pool_id,
                    on_demand_connect: self.on_demand_connect,
                    num_connections: self.num_pool_connections,
                    connect_throttle_period: self.connect_throttle_period,
                    disable_decompression: self.disable_decompression,
                    bootstrap_options: self.bootstrap_options.clone(),
                    endpoint_id: endpoint.clone(),
                    target,
                    auth: slow_state.auth.clone(),
                    selected_bucket: slow_state.selected_bucket.clone(),
                    unsolicited_packet_tx: self.unsolicited_packet_tx.clone(),
                    orphan_handler: self.orphan_handler.clone(),
                    tracing: self.tracing.clone(),
                })
                .await;

                Arc::new(pool)
            };

            new_pools.insert(endpoint, pool);
        }

        if add_only {
            // in add-only mode, we keep any existing pools that aren't in the new set
            // this is useful for making sure all routers still work until we've updated
            // the routers separately...
            for (endpoint, pool) in old_pools.into_iter() {
                new_pools.insert(endpoint, pool);
            }
        } else {
            for pool in old_pools.into_values() {
                let id = pool.id();
                if let Err(e) = pool.close().await {
                    debug!("Failed to close pool {id}: {e}");
                };
            }
        }

        slow_state.client_pools = new_pools;

        let mut client_pools = HashMap::with_capacity(slow_state.client_pools.len());
        for (endpoint, pool) in slow_state.client_pools.iter() {
            client_pools.insert(endpoint.clone(), pool.clone());
        }

        self.fast_state
            .store(Arc::new(KvEndpointClientManagerFastState { client_pools }));

        Ok(())
    }

    async fn update_auth(&self, authenticator: Authenticator) {
        let mut state = self.slow_state.lock().await;

        state.auth = authenticator.clone();

        for pool in state.client_pools.values() {
            pool.update_auth(authenticator.clone()).await;
        }
    }

    async fn ping_all_clients(
        &self,
        req: PingRequest<'_>,
    ) -> HashMap<String, Vec<error::Result<PingResponse>>> {
        let state = self.fast_state.load();

        let mut handles = vec![];
        for (endpoint, pool) in state.client_pools.iter() {
            let req = req.clone();

            let handle = async move {
                let pool_id = pool.id();
                trace!("Pinging pool {pool_id}");
                (endpoint, pool.ping_all_clients(req).await)
            };

            handles.push(handle);
        }

        let resp = join_all(handles).await;
        let mut results = HashMap::new();
        for (endpoint, resp) in resp {
            results.insert(endpoint.clone(), resp);
        }

        results
    }

    async fn endpoint_diagnostics(&self) -> Vec<EndpointDiagnostics> {
        let state = self.fast_state.load();

        let mut diags = Vec::with_capacity(state.client_pools.len());
        for pool in state.client_pools.values() {
            diags.extend(pool.endpoint_diagnostics().await);
        }

        diags
    }

    async fn get_client_per_endpoint(&self) -> error::Result<Vec<Arc<Self::Client>>> {
        let state = self.fast_state.load();

        let mut clients = Vec::with_capacity(state.client_pools.len());
        for pool in state.client_pools.values() {
            let client = pool.get_client().await?;
            clients.push(client);
        }

        Ok(clients)
    }
}

impl<P, K> Drop for StdKvEndpointClientManager<P, K>
where
    P: KvClientPool<Client = K>,
    K: KvClient,
{
    fn drop(&mut self) {
        info!("Dropping StdKvEndpointClientManager {}", self.id);
    }
}
