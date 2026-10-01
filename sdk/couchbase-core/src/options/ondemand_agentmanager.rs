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

use crate::auth_mechanism::AuthMechanism;
use crate::authenticator::Authenticator;
use crate::memdx::dispatcher::OrphanResponseHandler;
use crate::options::agent::{
    AgentOptions, CompressionConfig, ConfigPollerConfig, HttpConfig, KvConfig, SeedConfig,
};
use crate::tls_config::TlsConfig;
use std::time::Duration;

#[derive(Clone)]
#[non_exhaustive]
pub struct OnDemandAgentManagerOptions {
    pub seed_config: SeedConfig,
    pub authenticator: Authenticator,

    pub auth_mechanisms: Vec<AuthMechanism>,
    pub tls_config: Option<TlsConfig>,
    pub network: Option<String>,

    pub compression_config: CompressionConfig,
    pub config_poller_config: ConfigPollerConfig,
    pub kv_config: KvConfig,
    pub http_config: HttpConfig,
    pub tcp_keep_alive_time: Option<Duration>,
    pub orphan_response_handler: Option<OrphanResponseHandler>,
    /// Wrap sensitive values in log output in redaction tags. See
    /// [`log_redaction`](crate::log_redaction). Redaction is process-wide, so creating an agent
    /// with this set turns it on for every agent, and creating one without it never turns it off.
    pub log_redaction: bool,
}

impl OnDemandAgentManagerOptions {
    pub fn new(seed_config: SeedConfig, authenticator: Authenticator) -> Self {
        Self {
            tls_config: None,
            authenticator,
            seed_config,
            compression_config: CompressionConfig::default(),
            config_poller_config: ConfigPollerConfig::default(),
            auth_mechanisms: vec![],
            network: None,
            kv_config: KvConfig::default(),
            http_config: HttpConfig::default(),
            tcp_keep_alive_time: None,
            orphan_response_handler: None,
            log_redaction: false,
        }
    }

    pub fn seed_config(mut self, seed_config: SeedConfig) -> Self {
        self.seed_config = seed_config;
        self
    }

    pub fn authenticator(mut self, authenticator: Authenticator) -> Self {
        self.authenticator = authenticator;
        self
    }

    pub fn tls_config(mut self, tls_config: impl Into<Option<TlsConfig>>) -> Self {
        self.tls_config = tls_config.into();
        self
    }

    pub fn compression_config(mut self, compression_config: CompressionConfig) -> Self {
        self.compression_config = compression_config;
        self
    }

    pub fn config_poller_config(mut self, config_poller_config: ConfigPollerConfig) -> Self {
        self.config_poller_config = config_poller_config;
        self
    }

    pub fn auth_mechanisms(mut self, auth_mechanisms: impl Into<Vec<AuthMechanism>>) -> Self {
        self.auth_mechanisms = auth_mechanisms.into();
        self
    }

    pub fn network(mut self, network: impl Into<String>) -> Self {
        self.network = Some(network.into());
        self
    }

    pub fn kv_config(mut self, kv_config: KvConfig) -> Self {
        self.kv_config = kv_config;
        self
    }

    pub fn http_config(mut self, http_config: HttpConfig) -> Self {
        self.http_config = http_config;
        self
    }

    pub fn tcp_keep_alive_time(mut self, tcp_keep_alive: Duration) -> Self {
        self.tcp_keep_alive_time = Some(tcp_keep_alive);
        self
    }

    pub fn orphan_reporter_handler(
        mut self,
        orphan_response_handler: Option<OrphanResponseHandler>,
    ) -> Self {
        self.orphan_response_handler = orphan_response_handler;
        self
    }

    pub fn log_redaction(mut self, log_redaction: bool) -> Self {
        self.log_redaction = log_redaction;
        self
    }
}

impl From<OnDemandAgentManagerOptions> for AgentOptions {
    fn from(opts: OnDemandAgentManagerOptions) -> Self {
        AgentOptions {
            tls_config: opts.tls_config,
            authenticator: opts.authenticator,
            bucket_name: None,
            network: opts.network,
            seed_config: opts.seed_config,
            compression_config: opts.compression_config,
            config_poller_config: opts.config_poller_config,
            auth_mechanisms: opts.auth_mechanisms,
            kv_config: opts.kv_config,
            http_config: opts.http_config,
            tcp_keep_alive_time: opts.tcp_keep_alive_time,
            orphan_response_handler: opts.orphan_response_handler,
            log_redaction: opts.log_redaction,
        }
    }
}

impl From<AgentOptions> for OnDemandAgentManagerOptions {
    fn from(opts: AgentOptions) -> Self {
        OnDemandAgentManagerOptions {
            authenticator: opts.authenticator,
            network: opts.network,
            tls_config: opts.tls_config,
            seed_config: opts.seed_config,
            compression_config: opts.compression_config,
            config_poller_config: opts.config_poller_config,
            auth_mechanisms: opts.auth_mechanisms,
            kv_config: opts.kv_config,
            http_config: opts.http_config,
            tcp_keep_alive_time: opts.tcp_keep_alive_time,
            orphan_response_handler: opts.orphan_response_handler,
            log_redaction: opts.log_redaction,
        }
    }
}
