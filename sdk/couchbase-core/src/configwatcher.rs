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

use crate::cbconfig;
use crate::cbconfig::TerseConfig;
use crate::configfetcher::ConfigFetcherMemd;
use crate::configmanager::ConfigVersion;
use crate::configparser::ConfigParser;
use crate::error::{Error, Result};
use crate::kvclient::KvClient;
use crate::kvclient_ops::KvClientOps;
use crate::kvendpointclientmanager::KvEndpointClientManager;
use crate::log_redaction::system_data;
use crate::memdx::hello_feature::HelloFeature;
use crate::memdx::request::{GetClusterConfigKnownVersion, GetClusterConfigRequest};
use crate::parsedconfig::ParsedConfig;
use futures::future::err;
use std::cmp::Ordering;
use std::future::Future;
use std::ops::Deref;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::select;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::{broadcast, watch, Notify};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, trace};

#[derive(Debug, Clone)]
pub(crate) struct ConfigWatcherMemdConfig {
    pub endpoints: Vec<String>,
}

pub(crate) struct ConfigWatcherMemdOptions<M: KvEndpointClientManager> {
    pub polling_period: Duration,
    pub config_fetcher: Arc<ConfigFetcherMemd<M>>,
    pub latest_version_rx: watch::Receiver<ConfigVersion>,
}

pub struct ConfigWatcherMemdInner<M: KvEndpointClientManager> {
    config_fetcher: Arc<ConfigFetcherMemd<M>>,
    polling_period: Duration,
    endpoints: Mutex<Vec<String>>,
    latest_version_rx: watch::Receiver<ConfigVersion>,
}

impl<M: KvEndpointClientManager> ConfigWatcherMemdInner<M> {
    pub fn reconfigure(&self, config: ConfigWatcherMemdConfig) -> Result<()> {
        let mut endpoints = self.endpoints.lock().unwrap();
        *endpoints = config.endpoints;

        Ok(())
    }

    pub fn endpoints(&self) -> Vec<String> {
        let mut endpoints = vec![];
        let endpoints_guard = self.endpoints.lock().unwrap();
        for endpoint in endpoints_guard.iter() {
            endpoints.push(endpoint.clone());
        }

        endpoints
    }

    pub async fn watch(
        &self,
        on_shutdown: CancellationToken,
        on_new_config_tx: Sender<ParsedConfig>,
    ) {
        let mut recent_endpoints = vec![];
        let mut all_endpoints_failed = true;

        loop {
            let endpoints = self.endpoints();

            if endpoints.is_empty() {
                select! {
                    _ = on_shutdown.cancelled() => {
                        return;
                    },
                    _ = sleep(self.polling_period) => {
                        continue;
                    }
                }
            }

            let mut remaining_endpoints = vec![];
            for endpoint in endpoints {
                if !recent_endpoints.contains(&endpoint) {
                    remaining_endpoints.push(endpoint);
                }
            }

            let endpoint = if remaining_endpoints.is_empty() {
                if all_endpoints_failed {
                    select! {
                        _ = on_shutdown.cancelled() => {
                            return;
                        },
                        _ = sleep(self.polling_period) => {}
                    }
                }

                recent_endpoints = vec![];
                all_endpoints_failed = true;
                continue;
            } else {
                remaining_endpoints.remove(0)
            };

            recent_endpoints.push(endpoint.clone());

            let (rev_id, rev_epoch) = {
                let version = self.latest_version_rx.borrow();

                (version.rev_id, version.rev_epoch)
            };

            let parsed_config = match self
                .config_fetcher
                .poll_one(&endpoint, rev_id, rev_epoch, |client| {
                    // If notif brief is supported then we don't actually need to poll.
                    let supported =
                        client.has_feature(HelloFeature::ClusterMapChangeNotificationBrief);
                    if !supported {
                        debug!(
                            "Polling config from {} with rev_id: {rev_id}, rev_epoch: {rev_epoch}",
                            system_data(&endpoint)
                        );
                    }

                    supported
                })
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    select! {
                        _ = on_shutdown.cancelled() => {
                            return;
                        },
                        _ = sleep(self.polling_period) => {}
                    }

                    continue;
                }
            };

            all_endpoints_failed = false;

            if let Some(parsed_config) = parsed_config {
                on_new_config_tx
                    .send(parsed_config.clone())
                    .unwrap_or_default();
            }

            select! {
                _ = on_shutdown.cancelled() => {
                    return;
                },
                _ = sleep(self.polling_period) => {}
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct ConfigWatcherMemd<M: KvEndpointClientManager> {
    inner: Arc<ConfigWatcherMemdInner<M>>,
}

impl<M> ConfigWatcherMemd<M>
where
    M: KvEndpointClientManager + 'static,
{
    pub fn new(config: ConfigWatcherMemdConfig, opts: ConfigWatcherMemdOptions<M>) -> Self {
        Self {
            inner: Arc::new(ConfigWatcherMemdInner {
                config_fetcher: opts.config_fetcher,
                polling_period: opts.polling_period,
                endpoints: Mutex::new(config.endpoints),
                latest_version_rx: opts.latest_version_rx,
            }),
        }
    }

    pub fn watch(&self, on_shutdown: CancellationToken) -> Receiver<ParsedConfig> {
        let (on_new_config_tx, on_new_config_rx) = broadcast::channel::<ParsedConfig>(1);

        let inner = self.inner.clone();
        tokio::spawn(async move {
            inner.watch(on_shutdown, on_new_config_tx).await;
            debug!("Config poller exited")
        });

        on_new_config_rx
    }

    pub fn reconfigure(&self, config: ConfigWatcherMemdConfig) -> Result<()> {
        self.inner.reconfigure(config)
    }

    pub fn endpoints(&self) -> Vec<String> {
        self.inner.endpoints()
    }
}
