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
use crate::log_redaction::{metadata, system_data, user_data};
use crate::memdx::dispatcher::OrphanResponseHandler;
use crate::tls_config::TlsConfig;
use std::fmt::{Debug, Display};
use std::time::Duration;

#[derive(Clone)]
#[non_exhaustive]
pub struct AgentOptions {
    pub seed_config: SeedConfig,
    pub authenticator: Authenticator,

    // By default, the SDK will default to using the mechanisms provided by the
    // Authenticator, but this can be overridden here.
    pub auth_mechanisms: Vec<AuthMechanism>,
    pub tls_config: Option<TlsConfig>,
    pub bucket_name: Option<String>,
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

impl Debug for AgentOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentOptions")
            .field("seed_config", &self.seed_config)
            .field("auth_mechanisms", &self.auth_mechanisms)
            .field("tls_config", &self.tls_config)
            .field("bucket_name", &self.bucket_name)
            .field("network", &self.network)
            .field("compression_config", &self.compression_config)
            .field("config_poller_config", &self.config_poller_config)
            .field("kv_config", &self.kv_config)
            .field("http_config", &self.http_config)
            .field("tcp_keep_alive_time", &self.tcp_keep_alive_time)
            .field("log_redaction", &self.log_redaction)
            .finish()
    }
}

impl AgentOptions {
    pub fn new(seed_config: SeedConfig, authenticator: Authenticator) -> Self {
        Self {
            tls_config: None,
            authenticator,
            bucket_name: None,
            network: None,
            seed_config,
            compression_config: CompressionConfig::default(),
            config_poller_config: ConfigPollerConfig::default(),
            auth_mechanisms: vec![],
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

    pub fn bucket_name(mut self, bucket_name: impl Into<Option<String>>) -> Self {
        self.bucket_name = bucket_name.into();
        self
    }

    pub fn network(mut self, network: impl Into<Option<String>>) -> Self {
        self.network = network.into();
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

    pub fn auth_mechanisms(mut self, auth_mechanisms: Vec<AuthMechanism>) -> Self {
        self.auth_mechanisms = auth_mechanisms;
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

    pub fn log_redaction(mut self, log_redaction: bool) -> Self {
        self.log_redaction = log_redaction;
        self
    }

    pub fn orphan_reporter_handler(
        mut self,
        orphan_response_handler: OrphanResponseHandler,
    ) -> Self {
        self.orphan_response_handler = Some(orphan_response_handler);
        self
    }
}

#[derive(Default, Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct SeedConfig {
    pub http_addrs: Vec<Address>,
    pub memd_addrs: Vec<Address>,
}

impl SeedConfig {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn http_addrs(mut self, http_addrs: Vec<Address>) -> Self {
        self.http_addrs = http_addrs;
        self
    }

    pub fn memd_addrs(mut self, memd_addrs: Vec<Address>) -> Self {
        self.memd_addrs = memd_addrs;
        self
    }
}

#[derive(Default, Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct CompressionConfig {
    pub disable_decompression: bool,
    pub mode: CompressionMode,
}

impl CompressionConfig {
    pub fn new(mode: CompressionMode) -> Self {
        Self {
            disable_decompression: false,
            mode,
        }
    }

    pub fn disable_decompression(mut self, disable_decompression: bool) -> Self {
        self.disable_decompression = disable_decompression;
        self
    }

    pub fn mode(mut self, mode: CompressionMode) -> Self {
        self.mode = mode;
        self
    }
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum CompressionMode {
    Enabled { min_size: usize, min_ratio: f64 },
    Disabled,
}

impl Default for CompressionMode {
    fn default() -> Self {
        Self::Enabled {
            min_size: 32,
            min_ratio: 0.83,
        }
    }
}

impl Display for CompressionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionMode::Enabled {
                min_size,
                min_ratio,
            } => {
                write!(f, "{{ min_size: {}, min_ratio: {} }}", min_size, min_ratio)
            }
            CompressionMode::Disabled => write!(f, "disabled"),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct ConfigPollerConfig {
    pub poll_interval: Duration,
    pub fetch_timeout: Duration,
}

impl ConfigPollerConfig {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    pub fn fetch_timeout(mut self, fetch_timeout: Duration) -> Self {
        self.fetch_timeout = fetch_timeout;
        self
    }
}

impl Default for ConfigPollerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(2500),
            fetch_timeout: Duration::from_millis(2500),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct KvConfig {
    pub on_demand_connect: bool,
    pub enable_error_map: bool,
    pub enable_mutation_tokens: bool,
    pub enable_server_durations: bool,
    pub num_connections: usize,
    pub connect_timeout: Duration,
    pub connect_throttle_timeout: Duration,
}

impl KvConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn on_demand_connect(mut self, on_demand_connect: bool) -> Self {
        self.on_demand_connect = on_demand_connect;
        self
    }

    pub fn enable_error_map(mut self, enable: bool) -> Self {
        self.enable_error_map = enable;
        self
    }

    pub fn enable_mutation_tokens(mut self, enable: bool) -> Self {
        self.enable_mutation_tokens = enable;
        self
    }

    pub fn enable_server_durations(mut self, enable: bool) -> Self {
        self.enable_server_durations = enable;
        self
    }

    pub fn connect_timeout(mut self, connect_timeout: Duration) -> Self {
        self.connect_timeout = connect_timeout;
        self
    }

    pub fn connect_throttle_timeout(mut self, connect_throttle_timeout: Duration) -> Self {
        self.connect_throttle_timeout = connect_throttle_timeout;
        self
    }

    pub fn num_connections(mut self, num: usize) -> Self {
        self.num_connections = num;
        self
    }
}

impl Default for KvConfig {
    fn default() -> Self {
        Self {
            on_demand_connect: false,
            enable_error_map: true,
            enable_mutation_tokens: true,
            enable_server_durations: true,
            num_connections: 1,
            connect_timeout: Duration::from_secs(10),
            connect_throttle_timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct HttpConfig {
    pub max_idle_connections_per_host: Option<usize>,
    pub idle_connection_timeout: Duration,
}

impl HttpConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn max_idle_connections_per_host(mut self, max_idle_connections_per_host: usize) -> Self {
        self.max_idle_connections_per_host = Some(max_idle_connections_per_host);
        self
    }

    pub fn idle_connection_timeout(mut self, idle_connection_timeout: Duration) -> Self {
        self.idle_connection_timeout = idle_connection_timeout;
        self
    }
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            max_idle_connections_per_host: None,
            idle_connection_timeout: Duration::from_secs(1),
        }
    }
}

#[derive(Clone)]
#[non_exhaustive]
pub struct ReconfigureAgentOptions {
    pub authenticator: Authenticator,
    pub tls_config: Option<TlsConfig>,
}

impl ReconfigureAgentOptions {
    pub fn new(authenticator: Authenticator) -> Self {
        Self {
            tls_config: None,
            authenticator,
        }
    }

    pub fn tls_config(mut self, tls_config: impl Into<Option<TlsConfig>>) -> Self {
        self.tls_config = tls_config.into();
        self
    }
}

impl Display for SeedConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ http_addrs: {:?}, memd_addrs: {:?} }}",
            self.http_addrs, self.memd_addrs
        )
    }
}

impl Display for CompressionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ disable_decompression: {}, mode: {} }}",
            self.disable_decompression, self.mode
        )
    }
}

impl Display for ConfigPollerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ poll_interval: {:?}, fetch_timeout: {:?} }}",
            self.poll_interval, self.fetch_timeout
        )
    }
}

impl Display for KvConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ on_demand_connect: {}, enable_error_map: {}, enable_mutation_tokens: {}, enable_server_durations: {}, num_connections: {}, connect_timeout: {:?}, connect_throttle_timeout: {:?} }}",
            self.on_demand_connect,
            self.enable_error_map,
            self.enable_mutation_tokens,
            self.enable_server_durations,
            self.num_connections,
            self.connect_timeout,
            self.connect_throttle_timeout
        )
    }
}

impl Display for HttpConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ max_idle_connections_per_host: {:?}, idle_connection_timeout: {:?} }}",
            self.max_idle_connections_per_host, self.idle_connection_timeout
        )
    }
}

impl Display for AgentOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.write_fields(f, &self.seed_config, &self.bucket_name, &self.network)
    }
}

impl AgentOptions {
    pub(crate) fn to_string_redacted(&self) -> String {
        let mut out = String::new();
        // Writing to a String cannot fail.
        let _ = self.write_fields(
            &mut out,
            &system_data(&self.seed_config),
            &metadata(&self.bucket_name),
            &user_data(&self.network),
        );
        out
    }

    // The rendering shared by Display and to_string_redacted, which differ only in whether the
    // three values that need redaction are passed in plain or wrapped.
    fn write_fields(
        &self,
        out: &mut impl std::fmt::Write,
        seed_config: &dyn Display,
        bucket_name: &dyn Debug,
        network: &dyn Debug,
    ) -> std::fmt::Result {
        let tls_config = if cfg!(feature = "rustls-tls") {
            "rustls-tls"
        } else if cfg!(feature = "native-tls") {
            "native-tls"
        } else {
            "none"
        };

        write!(
            out,
            "{{ seed_config: {}, auth_mechanisms: {:?}, tls_config: {}, bucket_name: {:?}, network: {:?}, compression_config: {}, config_poller_config: {}, kv_config: {}, http_config: {}, tcp_keep_alive_time: {:?}, orphan_response_handler: {}, log_redaction: {} }}",
            seed_config,
            self.auth_mechanisms,
            tls_config,
            bucket_name,
            network,
            self.compression_config,
            self.config_poller_config,
            self.kv_config,
            self.http_config,
            self.tcp_keep_alive_time,
            if self.orphan_response_handler.is_some() { "Some" } else { "None" },
            self.log_redaction,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authenticator::PasswordAuthenticator;
    use crate::log_redaction::tests::ScopedLogRedaction;

    fn options() -> AgentOptions {
        AgentOptions::new(
            SeedConfig::new().memd_addrs(vec![Address {
                host: "10.0.0.1".to_string(),
                port: 11210,
            }]),
            Authenticator::PasswordAuthenticator(PasswordAuthenticator {
                username: "user".to_string(),
                password: "pass".to_string(),
            }),
        )
        .bucket_name("travel-sample".to_string())
        .network("external".to_string())
    }

    #[test]
    fn to_string_redacted_matches_display_while_redaction_is_disabled() {
        let _r = ScopedLogRedaction::new(false);
        let opts = options();

        assert_eq!(opts.to_string_redacted(), opts.to_string());
    }

    #[test]
    fn to_string_redacted_tags_only_the_values_that_need_it() {
        let _r = ScopedLogRedaction::new(true);
        let rendered = options().to_string_redacted();

        assert!(
            rendered.contains(
                r#"seed_config: <sd>{ http_addrs: [], memd_addrs: [Address { host: "10.0.0.1", port: 11210 }] }</sd>"#
            ),
            "{rendered}"
        );
        assert!(
            rendered.contains(r#"bucket_name: <md>Some("travel-sample")</md>"#),
            "{rendered}"
        );
        assert!(
            rendered.contains(r#"network: <ud>Some("external")</ud>"#),
            "{rendered}"
        );
        assert_eq!(rendered.matches("</").count(), 3, "{rendered}");
    }

    #[test]
    fn display_never_carries_tags() {
        let _r = ScopedLogRedaction::new(true);
        let rendered = options().to_string();

        assert!(rendered.contains("10.0.0.1"), "{rendered}");
        assert!(!rendered.contains('<'), "{rendered}");
    }
}
