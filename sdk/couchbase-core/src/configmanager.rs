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

use crate::agent::AgentInner;
use crate::cbconfig::TerseConfig;
use crate::configfetcher::{ConfigFetcherMemd, ConfigFetcherMemdOptions};
use crate::configparser::ConfigParser;
use crate::configwatcher::{ConfigWatcherMemd, ConfigWatcherMemdConfig, ConfigWatcherMemdOptions};
use crate::kvendpointclientmanager::KvEndpointClientManager;
use crate::log_redaction::{metadata, not_sensitive, system_data, user_data};
use crate::nmvbhandler::ConfigUpdater;
use crate::parsedconfig::{ParsedConfig, ParsedConfigBucket};
use std::cmp::Ordering;
use std::future::Future;
use std::ops::Deref;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{watch, Notify};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

pub(crate) trait ConfigManager: Sized + Send + Sync {
    fn watch(&self) -> watch::Receiver<ParsedConfig>;
    fn reconfigure(&self, config: ConfigManagerMemdConfig) -> crate::error::Result<()>;
    fn out_of_band_version(
        &self,
        rev_id: i64,
        rev_epoch: i64,
        endpoint_id: String,
    ) -> impl Future<Output = Option<ParsedConfig>> + Send;
    fn out_of_band_config(&self, config: ParsedConfig) -> Option<ParsedConfig>;
}

#[derive(Debug, Clone)]
pub(crate) struct ConfigVersion {
    pub rev_epoch: i64,
    pub rev_id: i64,
}

pub(crate) struct ConfigManagerMemdConfig {
    pub endpoints: Vec<String>,
}

pub(crate) struct ConfigManagerMemdOptions<M: KvEndpointClientManager> {
    pub polling_period: Duration,
    pub first_config: ParsedConfig,
    pub kv_client_manager: Arc<M>,
    pub fetch_timeout: Duration,
}

pub(crate) struct ConfigManagerMemd<M: KvEndpointClientManager> {
    inner: Arc<ConfigManagerMemdInner<M>>,
}

pub(crate) struct ConfigManagerMemdInner<M: KvEndpointClientManager> {
    fetcher: Arc<ConfigFetcherMemd<M>>,
    watcher: Arc<ConfigWatcherMemd<M>>,

    out_of_band_notify: Notify,
    performing_out_of_band_fetch: AtomicBool,

    latest_config: Arc<Mutex<ParsedConfig>>,
    latest_version_tx: watch::Sender<ConfigVersion>,
    bucket: Mutex<Option<ParsedConfigBucket>>,

    on_new_config_tx: watch::Sender<ParsedConfig>,

    // When this gets dropped it cancels the wrapped CancellationToken.
    watcher_shutdown_guard: tokio_util::sync::DropGuard,
}

impl<M: KvEndpointClientManager + 'static> ConfigManagerMemdInner<M> {
    pub fn watch(&self) -> watch::Receiver<ParsedConfig> {
        self.on_new_config_tx.subscribe()
    }

    pub fn reconfigure(&self, config: ConfigManagerMemdConfig) -> crate::error::Result<()> {
        self.watcher.reconfigure(ConfigWatcherMemdConfig {
            endpoints: config.endpoints,
        })
    }

    async fn perform_out_of_band_fetch(
        &self,
        rev_id: i64,
        rev_epoch: i64,
        endpoint_id: String,
    ) -> Option<ParsedConfig> {
        loop {
            let (latest_rev_epoch, latest_rev_id) = {
                let latest_config = self.latest_config.lock().unwrap();
                (latest_config.rev_epoch, latest_config.rev_id)
            };

            if rev_epoch < latest_rev_epoch
                || (rev_epoch == latest_rev_epoch && rev_id <= latest_rev_id)
            {
                debug!(
                    "Skipping out-of-band fetch, already have newer or same version: new: \
                    rev_epoch={rev_epoch}, rev_id={rev_id}, old: rev_epoch={latest_rev_epoch}, \
                    rev_id={latest_rev_id}"
                );
                // No need to poll, we already have a newer version.
                return None;
            }

            if self.performing_out_of_band_fetch.compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            ) == Ok(true)
            {
                // Right now we don't reach here because unsolicited packets are sent on a channel
                // so read serially.
                self.out_of_band_notify.notified().await;
                continue;
            } else {
                let parsed_config = match self
                    .fetcher
                    .poll_one(&endpoint_id, latest_rev_id, latest_rev_epoch, |_c| false)
                    .await
                {
                    Ok(c) => c,
                    Err(e) => {
                        debug!(
                            "Out-of-band fetch from {} failed: {}",
                            system_data(&endpoint_id),
                            user_data(&e)
                        );
                        return None;
                    }
                };

                if let Some(parsed_config) = parsed_config {
                    if let Some(cfg) = Self::handle_config(
                        self.latest_config.lock().unwrap(),
                        parsed_config,
                        self.latest_version_tx.clone(),
                    ) {
                        self.performing_out_of_band_fetch
                            .store(false, std::sync::atomic::Ordering::SeqCst);
                        self.out_of_band_notify.notify_waiters();

                        return Some(cfg);
                    };
                }

                debug!("No newer config found during out-of-band fetch");

                self.performing_out_of_band_fetch
                    .store(false, std::sync::atomic::Ordering::SeqCst);
                self.out_of_band_notify.notify_waiters();

                return None;
            }
        }
    }

    pub async fn out_of_band_version(
        &self,
        rev_id: i64,
        rev_epoch: i64,
        endpoint_id: String,
    ) -> Option<ParsedConfig> {
        self.perform_out_of_band_fetch(rev_id, rev_epoch, endpoint_id)
            .await
    }

    pub fn out_of_band_config(&self, parsed_config: ParsedConfig) -> Option<ParsedConfig> {
        Self::handle_config(
            self.latest_config.lock().unwrap(),
            parsed_config,
            self.latest_version_tx.clone(),
        )
    }

    fn handle_config(
        mut latest_config: MutexGuard<ParsedConfig>,
        parsed_config: ParsedConfig,
        latest_version_tx: watch::Sender<ConfigVersion>,
    ) -> Option<ParsedConfig> {
        if Self::can_update_config(&parsed_config, latest_config.deref()) {
            let new_latest_version = ConfigVersion {
                rev_epoch: parsed_config.rev_epoch,
                rev_id: parsed_config.rev_id,
            };
            *latest_config = parsed_config.clone();
            drop(latest_config);

            if let Err(e) = latest_version_tx.send(new_latest_version) {
                warn!(
                    "Failed to update config watcher with latest version: {}",
                    user_data(&e)
                );
            }

            return Some(parsed_config);
        }

        None
    }

    fn bucket_type_changed(a: &Option<ParsedConfigBucket>, b: &Option<ParsedConfigBucket>) -> bool {
        match (a, b) {
            (None, None) => false,
            (Some(_), None) | (None, Some(_)) => true,
            (Some(a_bucket), Some(b_bucket)) => a_bucket.bucket_type != b_bucket.bucket_type,
        }
    }

    fn can_update_config(new_config: &ParsedConfig, old_config: &ParsedConfig) -> bool {
        if Self::bucket_type_changed(&new_config.bucket, &old_config.bucket) {
            debug!(
                "Switching config due to changed bucket type (bucket takeover) old: {:?} new: {:?}",
                metadata(&old_config.bucket),
                metadata(&new_config.bucket)
            );
            return true;
        } else if let Some(cmp) = new_config.partial_cmp(old_config) {
            if cmp == Ordering::Less {
                debug!("Skipping config due to new config being an older revision old: rev_epoch={}, rev_id={} new: rev_epoch={}, rev_id={}",
                    old_config.rev_epoch, old_config.rev_id, new_config.rev_epoch, new_config.rev_id);
            } else if cmp == Ordering::Equal {
                debug!("Skipping config due to matching revisions old: rev_epoch={}, rev_id={} new: rev_epoch={}, rev_id={}",
                    old_config.rev_epoch, old_config.rev_id, new_config.rev_epoch, new_config.rev_id);
            } else {
                return true;
            }
        }

        false
    }

    pub fn start_watcher(&self, watcher_shutdown: CancellationToken) {
        let mut rx = self.watcher.watch(watcher_shutdown);
        let latest_version_tx = self.latest_version_tx.clone();
        let guard = self.latest_config.clone();
        let new_config_tx = self.on_new_config_tx.clone();

        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(cfg) => {
                        if let Some(new_cfg) = Self::handle_config(
                            guard.lock().unwrap(),
                            cfg,
                            latest_version_tx.clone(),
                        ) {
                            new_config_tx.send_replace(new_cfg);
                        }
                    }
                    Err(e) => {
                        if e == RecvError::Closed {
                            debug!("Config watcher exited");
                            return;
                        } else {
                            // A channel error renders as a fixed message, never its payload.
                            warn!("Config watcher channel error: {}", not_sensitive(&e));
                        }
                    }
                }
            }
        });
    }
}

impl<M: KvEndpointClientManager + 'static> ConfigManagerMemd<M> {
    pub fn new(
        config: ConfigManagerMemdConfig,
        opts: ConfigManagerMemdOptions<M>,
    ) -> ConfigManagerMemd<M> {
        let latest_version = ConfigVersion {
            rev_epoch: opts.first_config.rev_epoch,
            rev_id: opts.first_config.rev_id,
        };

        let (latest_version_tx, latest_version_rx) = watch::channel(latest_version.clone());

        let fetcher = Arc::new(ConfigFetcherMemd::new(ConfigFetcherMemdOptions {
            kv_client_manager: opts.kv_client_manager,
            fetch_timeout: opts.fetch_timeout,
        }));
        let watcher = Arc::new(ConfigWatcherMemd::new(
            ConfigWatcherMemdConfig {
                endpoints: config.endpoints,
            },
            ConfigWatcherMemdOptions {
                polling_period: opts.polling_period,
                config_fetcher: fetcher.clone(),
                latest_version_rx,
            },
        ));

        let watcher_shutdown = CancellationToken::new();
        let watcher_shutdown_guard = watcher_shutdown.clone().drop_guard();
        let bucket = opts.first_config.bucket.clone();

        let (on_new_config_tx, _on_new_config_rx) =
            watch::channel::<ParsedConfig>(opts.first_config.clone());

        let inner = Arc::new(ConfigManagerMemdInner {
            fetcher,
            watcher: watcher.clone(),

            out_of_band_notify: Notify::new(),
            performing_out_of_band_fetch: AtomicBool::new(false),

            latest_config: Arc::new(Mutex::new(opts.first_config)),
            latest_version_tx,
            bucket: Mutex::new(bucket),

            on_new_config_tx,

            watcher_shutdown_guard,
        });

        inner.start_watcher(watcher_shutdown);

        ConfigManagerMemd { inner }
    }
}

impl<M: KvEndpointClientManager + 'static> ConfigManager for ConfigManagerMemd<M> {
    fn watch(&self) -> watch::Receiver<ParsedConfig> {
        self.inner.watch()
    }

    fn reconfigure(&self, config: ConfigManagerMemdConfig) -> crate::error::Result<()> {
        self.inner.reconfigure(config)
    }

    async fn out_of_band_version(
        &self,
        rev_id: i64,
        rev_epoch: i64,
        endpoint_id: String,
    ) -> Option<ParsedConfig> {
        let inner = self.inner.clone();
        inner
            .out_of_band_version(rev_id, rev_epoch, endpoint_id)
            .await
    }

    fn out_of_band_config(&self, config: ParsedConfig) -> Option<ParsedConfig> {
        self.inner.out_of_band_config(config)
    }
}
