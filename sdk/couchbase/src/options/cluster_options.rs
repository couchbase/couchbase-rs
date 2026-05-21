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

//! Options for configuring a connection to a Couchbase cluster.
//!
//! The primary type is [`ClusterOptions`], which holds authentication credentials, TLS settings,
//! compression, timeouts, and other connection-level configuration. Pass it to
//! [`Cluster::connect`](crate::cluster::Cluster::connect) to establish a connection.

use crate::authenticator::Authenticator;
use crate::capella_ca::CAPELLA_CERT;
use crate::error;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

#[cfg(feature = "native-tls")]
use tokio_native_tls::native_tls::Identity;

#[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
use {
    couchbase_core::insecure_certverfier::InsecureCertVerifier,
    tokio_rustls::rustls::crypto::aws_lc_rs::default_provider,
    tokio_rustls::rustls::pki_types::pem::{PemObject, SectionKind},
    tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer},
    tokio_rustls::rustls::RootCertStore,
    webpki_roots::TLS_SERVER_ROOTS,
};

/// Configuration options for connecting to a Couchbase cluster.
///
/// Pass a `ClusterOptions` instance to [`Cluster::connect`](crate::cluster::Cluster::connect).
/// The only required field is the [`authenticator`](ClusterOptions::authenticator).
///
/// # Example
///
/// ```rust
/// use couchbase::authenticator::PasswordAuthenticator;
/// use couchbase::options::cluster_options::{ClusterOptions, CompressionMode};
/// use std::time::Duration;
///
/// let opts = ClusterOptions::new(
///     PasswordAuthenticator::new("user", "pass").into(),
/// )
/// .compression_mode(CompressionMode::Enabled { min_size: 32, min_ratio: 0.83 })
/// .tcp_keep_alive_time(Duration::from_secs(30));
/// ```
#[derive(Clone)]
#[non_exhaustive]
pub struct ClusterOptions {
    /// The authenticator to use when connecting to the cluster.
    pub authenticator: Authenticator,
    /// Compression mode for KV operations.
    pub compression_mode: Option<CompressionMode>,
    /// TLS configuration. Set this when using `couchbases://` connections.
    pub tls_options: Option<TlsOptions>,
    /// TCP keep-alive interval.
    pub tcp_keep_alive_time: Option<Duration>,
    /// Configuration for the cluster map poller.
    pub poller_options: PollerOptions,
    /// Configuration for the HTTP client used by query, search, and management services.
    pub http_options: HttpOptions,
    /// Configuration for the key-value (memcached) connections.
    pub kv_options: KvOptions,
    /// DNS configuration. **Volatile: This feature is subject to change at any time**.
    #[cfg(feature = "dns-srv")]
    pub dns_options: Option<DnsOptions>,
    /// Configuration for the orphan response reporter.
    pub orphan_reporter_options: OrphanReporterOptions,
    /// The default retry strategy for all operations. Individual operations can override
    /// this with their own per-operation retry strategy option.
    pub default_retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

impl Debug for ClusterOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterOptions")
            .field("authenticator", &self.authenticator)
            .field("compression_mode", &self.compression_mode)
            .field("has_tls_options", &self.tls_options.is_some())
            .field("tcp_keep_alive_time", &self.tcp_keep_alive_time)
            .field("poller_options", &self.poller_options)
            .field("http_options", &self.http_options)
            .field("kv_options", &self.kv_options)
            .field("orphan_reporter_options", &self.orphan_reporter_options)
            .finish()
    }
}

impl ClusterOptions {
    /// Creates a new `ClusterOptions` with the given authenticator and default settings.
    pub fn new(authenticator: Authenticator) -> Self {
        Self {
            authenticator,
            compression_mode: None,
            tls_options: None,
            tcp_keep_alive_time: None,
            poller_options: PollerOptions::new(),
            http_options: HttpOptions::new(),
            kv_options: KvOptions::new(),
            #[cfg(feature = "dns-srv")]
            dns_options: None,
            orphan_reporter_options: OrphanReporterOptions::new(),
            default_retry_strategy: None,
        }
    }

    /// Sets the compression mode for KV operations.
    pub fn compression_mode(mut self, compression_mode: CompressionMode) -> Self {
        self.compression_mode = Some(compression_mode);
        self
    }

    /// Sets the TLS configuration.
    pub fn tls_options(mut self, tls_options: TlsOptions) -> Self {
        self.tls_options = Some(tls_options);
        self
    }

    /// Sets the TCP keep-alive interval.
    pub fn tcp_keep_alive_time(mut self, val: Duration) -> Self {
        self.tcp_keep_alive_time = Some(val);
        self
    }

    /// Sets the cluster map poller configuration.
    pub fn poller_options(mut self, poller_options: PollerOptions) -> Self {
        self.poller_options = poller_options;
        self
    }

    /// Sets the HTTP client configuration.
    pub fn http_options(mut self, http_options: HttpOptions) -> Self {
        self.http_options = http_options;
        self
    }

    /// Sets the key-value connection configuration.
    pub fn kv_options(mut self, kv_options: KvOptions) -> Self {
        self.kv_options = kv_options;
        self
    }

    /// Sets the DNS configuration. **Volatile: This feature is subject to change at any time**.
    #[cfg(feature = "dns-srv")]
    pub fn dns_options(mut self, dns_options: DnsOptions) -> Self {
        self.dns_options = Some(dns_options);
        self
    }

    /// Sets the orphan response reporter configuration.
    pub fn orphan_reporter_options(
        mut self,
        orphan_reporter_options: OrphanReporterOptions,
    ) -> Self {
        self.orphan_reporter_options = orphan_reporter_options;
        self
    }

    /// Sets the default retry strategy for all operations.
    ///
    /// Individual operations can override this with their own per-operation retry strategy.
    pub fn default_retry_strategy(
        mut self,
        retry_strategy: Arc<dyn crate::retry::RetryStrategy>,
    ) -> Self {
        self.default_retry_strategy = Some(retry_strategy);
        self
    }
}

/// Controls whether the SDK compresses KV request/response bodies.
///
/// When enabled, values above `min_size` bytes are compressed using Snappy if the
/// compressed size is at most `min_ratio` of the original size.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum CompressionMode {
    /// Enable Snappy compression for KV bodies.
    ///
    /// * `min_size` — minimum body size in bytes before compression is attempted.
    /// * `min_ratio` — maximum compressed-to-original ratio (e.g. `0.83`).
    Enabled {
        /// Minimum body size in bytes before compression is attempted.
        min_size: usize,
        /// Maximum compressed-to-original size ratio. Values that do not compress
        /// below this ratio are sent uncompressed.
        min_ratio: f64,
    },
    /// Disable compression entirely.
    Disabled,
}

impl From<CompressionMode> for couchbase_core::options::agent::CompressionMode {
    fn from(mode: CompressionMode) -> Self {
        match mode {
            CompressionMode::Enabled {
                min_size,
                min_ratio,
            } => couchbase_core::options::agent::CompressionMode::Enabled {
                min_size,
                min_ratio,
            },
            CompressionMode::Disabled => couchbase_core::options::agent::CompressionMode::Disabled,
        }
    }
}

impl Display for CompressionMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            CompressionMode::Enabled {
                min_size,
                min_ratio,
            } => {
                write!(f, "{{ min_size: {min_size}, min_ratio: {min_ratio} }}")
            }
            CompressionMode::Disabled => write!(f, "disabled"),
        }
    }
}

/// Configuration for the cluster map configuration poller.
///
/// The SDK periodically polls the server for an updated cluster map so it can
/// route operations to the correct nodes.
#[derive(Default, Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct PollerOptions {
    /// How often to poll for a new cluster map configuration.
    pub config_poll_interval: Option<Duration>,
}

impl PollerOptions {
    /// Creates a new `PollerOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the interval between cluster map configuration polls.
    pub fn config_poll_interval(mut self, interval: Duration) -> Self {
        self.config_poll_interval = Some(interval);
        self
    }
}

impl From<PollerOptions> for couchbase_core::options::agent::ConfigPollerConfig {
    fn from(opts: PollerOptions) -> Self {
        let mut core_opts = couchbase_core::options::agent::ConfigPollerConfig::default();
        if let Some(interval) = opts.config_poll_interval {
            core_opts = core_opts.poll_interval(interval);
        }

        core_opts
    }
}

impl Display for PollerOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "{{ config_poll_interval: {:?} }}",
            self.config_poll_interval
        )
    }
}

/// Configuration for the HTTP client used by query, search, analytics, and management services.
#[derive(Default, Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct HttpOptions {
    /// Maximum number of idle HTTP connections to keep open per host.
    pub max_idle_connections_per_host: Option<usize>,
    /// How long an idle HTTP connection may remain open before being closed.
    pub idle_connection_timeout: Option<Duration>,
}

impl HttpOptions {
    /// Creates a new `HttpOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum number of idle connections to keep open per host.
    pub fn max_idle_connections_per_host(mut self, max: usize) -> Self {
        self.max_idle_connections_per_host = Some(max);
        self
    }

    /// Sets how long an idle connection may remain open before being closed.
    pub fn idle_connection_timeout(mut self, timeout: Duration) -> Self {
        self.idle_connection_timeout = Some(timeout);
        self
    }
}

impl From<HttpOptions> for couchbase_core::options::agent::HttpConfig {
    fn from(opts: HttpOptions) -> Self {
        let mut core_opts = couchbase_core::options::agent::HttpConfig::default();
        if let Some(max) = opts.max_idle_connections_per_host {
            core_opts = core_opts.max_idle_connections_per_host(max);
        }

        if let Some(timeout) = opts.idle_connection_timeout {
            core_opts = core_opts.idle_connection_timeout(timeout);
        }

        core_opts
    }
}

impl Display for HttpOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "{{ max_idle_connections_per_host: {:?}, idle_connection_timeout: {:?} }}",
            self.max_idle_connections_per_host, self.idle_connection_timeout
        )
    }
}

/// Configuration for key-value (memcached protocol) connections.
#[derive(Default, Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct KvOptions {
    /// Whether to request mutation tokens from the server. Mutation tokens are required
    /// for [`MutationState`](crate::mutation_state::MutationState)-based query consistency.
    pub enable_mutation_tokens: Option<bool>,
    /// Whether to request server-side operation duration metrics.
    pub enable_server_durations: Option<bool>,
    /// The number of KV connections to open per node.
    pub num_connections: Option<usize>,
    /// Timeout for establishing a single KV connection.
    pub connect_timeout: Option<Duration>,
    /// Throttle timeout applied when many connections are being opened concurrently.
    pub connect_throttle_timeout: Option<Duration>,
}

impl KvOptions {
    /// Creates a new `KvOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enables or disables mutation tokens.
    ///
    /// Mutation tokens are required for
    /// [`MutationState`](crate::mutation_state::MutationState)-based query consistency.
    pub fn enable_mutation_tokens(mut self, enable: bool) -> Self {
        self.enable_mutation_tokens = Some(enable);
        self
    }

    /// Enables or disables server-side operation duration metrics.
    pub fn enable_server_durations(mut self, enable: bool) -> Self {
        self.enable_server_durations = Some(enable);
        self
    }

    /// Sets the number of KV connections to open per node.
    pub fn num_connections(mut self, num: usize) -> Self {
        self.num_connections = Some(num);
        self
    }

    /// Sets the timeout for establishing a single KV connection.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Sets the throttle timeout for concurrent connection establishment.
    pub fn connect_throttle_timeout(mut self, timeout: Duration) -> Self {
        self.connect_throttle_timeout = Some(timeout);
        self
    }
}

impl From<KvOptions> for couchbase_core::options::agent::KvConfig {
    fn from(opts: KvOptions) -> Self {
        let mut core_opts =
            couchbase_core::options::agent::KvConfig::default().enable_error_map(true);
        if let Some(enable) = opts.enable_mutation_tokens {
            core_opts = core_opts.enable_mutation_tokens(enable);
        }

        if let Some(enable) = opts.enable_server_durations {
            core_opts = core_opts.enable_server_durations(enable);
        }

        if let Some(num) = opts.num_connections {
            core_opts = core_opts.num_connections(num);
        }

        if let Some(timeout) = opts.connect_timeout {
            core_opts = core_opts.connect_timeout(timeout);
        }

        if let Some(timeout) = opts.connect_throttle_timeout {
            core_opts = core_opts.connect_throttle_timeout(timeout);
        }

        core_opts
    }
}

impl Display for KvOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "{{ enable_mutation_tokens: {:?}, enable_server_durations: {:?}, num_connections: {:?}, connect_timeout: {:?}, connect_throttle_timeout: {:?} }}",
            self.enable_mutation_tokens,
            self.enable_server_durations,
            self.num_connections,
            self.connect_timeout,
            self.connect_throttle_timeout
        )
    }
}

/// TLS configuration for secure connections.
///
/// By default the SDK trusts the system root CAs plus the Couchbase Capella root CA.
/// Use [`add_ca_certificate`](TlsOptions::add_ca_certificate) to add custom CAs
/// (e.g. for self-signed certificates).
///
/// # Safety
///
/// Setting [`danger_accept_invalid_certs`](TlsOptions::danger_accept_invalid_certs) to
/// `true` disables all certificate verification. **Do not use in production.**
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct TlsOptions {
    /// If `true`, skip server certificate verification entirely.
    ///
    /// # Warning
    ///
    /// This is **insecure** and should only be used for development or testing.
    pub danger_accept_invalid_certs: Option<bool>,

    /// Custom CA certificates to trust. When set, only these CAs (plus the system
    /// roots) are trusted. Overrides the default Capella CA.
    #[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
    pub ca_certificates: Option<Vec<CertificateDer<'static>>>,

    /// Custom CA certificates to trust. When set, only these CAs (plus the system
    /// roots) are trusted. Overrides the default Capella CA.
    #[cfg(feature = "native-tls")]
    pub ca_certificates: Option<Vec<tokio_native_tls::native_tls::Certificate>>,

    /// If `true`, skip hostname verification. Only available with the `native-tls` feature.
    ///
    /// # Warning
    ///
    /// This is **insecure** and should only be used for development or testing.
    #[cfg(feature = "native-tls")]
    pub danger_accept_invalid_hostnames: Option<bool>,
}

impl TlsOptions {
    /// Creates a new `TlsOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Disables server certificate verification when set to `true`.
    ///
    /// # Warning
    ///
    /// This is **insecure** and should only be used for development or testing.
    pub fn danger_accept_invalid_certs(mut self, danger: bool) -> Self {
        self.danger_accept_invalid_certs = Some(danger);
        self
    }

    /// Adds a single CA certificate to the trust store.
    #[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
    pub fn add_ca_certificate(mut self, cert: CertificateDer<'static>) -> Self {
        self.ca_certificates.get_or_insert_with(Vec::new).push(cert);
        self
    }

    /// Adds multiple CA certificates to the trust store.
    #[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
    pub fn add_ca_certificates<T: IntoIterator<Item = CertificateDer<'static>>>(
        mut self,
        certs: T,
    ) -> Self {
        self.ca_certificates
            .get_or_insert_with(Vec::new)
            .extend(certs);
        self
    }

    /// Adds a single CA certificate to the trust store.
    #[cfg(feature = "native-tls")]
    pub fn add_ca_certificate(mut self, cert: tokio_native_tls::native_tls::Certificate) -> Self {
        self.ca_certificates.get_or_insert_with(Vec::new).push(cert);
        self
    }

    /// Adds multiple CA certificates to the trust store.
    #[cfg(feature = "native-tls")]
    pub fn add_ca_certificates<
        T: IntoIterator<Item = tokio_native_tls::native_tls::Certificate>,
    >(
        mut self,
        certs: T,
    ) -> Self {
        self.ca_certificates
            .get_or_insert_with(Vec::new)
            .extend(certs);
        self
    }

    /// Disables hostname verification when set to `true`. Only available with the
    /// `native-tls` feature.
    ///
    /// # Warning
    ///
    /// This is **insecure** and should only be used for development or testing.
    #[cfg(feature = "native-tls")]
    pub fn danger_accept_invalid_hostnames(mut self, danger: bool) -> Self {
        self.danger_accept_invalid_hostnames = Some(danger);
        self
    }

    #[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
    pub(crate) fn try_into_tls_config(
        self,
        auth: &Authenticator,
    ) -> Result<Arc<tokio_rustls::rustls::ClientConfig>, error::Error> {
        let store = match self.ca_certificates {
            Some(certs) if certs.is_empty() => {
                return Err(error::Error::invalid_argument(
                    "ca_certificates",
                    "ca_certificates list was provided but is empty",
                ));
            }
            Some(certs) => {
                let mut store = RootCertStore::empty();
                for cert in certs {
                    store.add(cert).map_err(|e| {
                        error::Error::other_failure(format!("failed to add cert: {e}"))
                    })?;
                }
                store
            }
            None => {
                let mut store = RootCertStore {
                    roots: TLS_SERVER_ROOTS.to_vec(),
                };

                debug!("Adding Capella root CA to trust store");
                let certs =
                    CertificateDer::from_pem_slice(CAPELLA_CERT.as_bytes()).map_err(|e| {
                        error::Error::other_failure(format!("failed to parse capella cert: {e}"))
                    })?;

                store.add(certs).map_err(|e| {
                    error::Error::other_failure(format!(
                        "failed to add capella cert to root store: {e}"
                    ))
                })?;
                store
            }
        };

        let mut builder =
            tokio_rustls::rustls::ClientConfig::builder_with_provider(Arc::new(default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(|e| {
                    error::Error::other_failure(format!(
                        "failed to set safe default protocol versions: {e}"
                    ))
                })?;

        let builder = if let Some(true) = self.danger_accept_invalid_certs {
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(InsecureCertVerifier {}))
        } else {
            builder.with_root_certificates(store)
        };

        let config = match auth {
            Authenticator::CertificateAuthenticator(a) => {
                let clone = a.clone();
                builder
                    .with_client_auth_cert(clone.cert_chain, clone.private_key)
                    .map_err(|e| {
                        error::Error::other_failure(format!(
                            "failed to setup client auth cert: {e}"
                        ))
                    })?
            }
            _ => builder.with_no_client_auth(),
        };

        Ok(Arc::new(config))
    }

    #[cfg(feature = "native-tls")]
    pub(crate) fn try_into_tls_config(
        self,
        auth: &Authenticator,
    ) -> Result<tokio_native_tls::native_tls::TlsConnector, error::Error> {
        let mut builder = tokio_native_tls::native_tls::TlsConnector::builder();
        if let Some(true) = self.danger_accept_invalid_certs {
            builder.danger_accept_invalid_certs(true);
        }
        if let Some(true) = self.danger_accept_invalid_hostnames {
            builder.danger_accept_invalid_hostnames(true);
        }

        match self.ca_certificates {
            Some(certs) if certs.is_empty() => {
                return Err(error::Error::invalid_argument(
                    "ca_certificates",
                    "ca_certificates list was provided but is empty",
                ));
            }
            Some(certs) => {
                for cert in certs {
                    builder.add_root_certificate(cert);
                }
            }
            None => {
                debug!("Adding Capella root CA to trust store");
                let capella_ca =
                    tokio_native_tls::native_tls::Certificate::from_pem(CAPELLA_CERT.as_ref())
                        .map_err(|e| {
                            error::Error::other_failure(format!("failed to add capella cert: {e}"))
                        })?;
                builder.add_root_certificate(capella_ca);
            }
        }

        match auth {
            Authenticator::CertificateAuthenticator(a) => {
                builder.identity(a.identity.clone());
            }
            Authenticator::PasswordAuthenticator(_) => {}
            Authenticator::JwtAuthenticator(_) => {}
        };

        builder
            .build()
            .map_err(|e| error::Error::other_failure(format!("failed to build client config: {e}")))
    }
}

#[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
impl Display for TlsOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "rustls-tls")
    }
}

#[cfg(feature = "native-tls")]
impl Display for TlsOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "native-tls")
    }
}

/// Custom DNS resolver configuration.  **Volatile: This feature is subject to change at any time**.
#[cfg(feature = "dns-srv")]
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct DnsOptions {
    /// The DNS server address to use for SRV and A/AAAA lookups.
    pub namespace: SocketAddr,
    /// Timeout for DNS resolution.
    pub timeout: Option<Duration>,
}

#[cfg(feature = "dns-srv")]
impl DnsOptions {
    /// Creates a new `DnsOptions` with the given DNS server address.
    pub fn new(namespace: SocketAddr) -> Self {
        Self {
            namespace,
            timeout: None,
        }
    }

    /// Sets the DNS resolution timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}
#[cfg(feature = "dns-srv")]
impl From<DnsOptions> for couchbase_connstr::DnsConfig {
    fn from(opts: DnsOptions) -> Self {
        couchbase_connstr::DnsConfig {
            namespace: opts.namespace,
            timeout: opts.timeout,
        }
    }
}

/// Configuration for the orphan response reporter.
///
/// The orphan reporter periodically logs operations whose responses arrived after the
/// client-side already dropped the request future.
#[derive(Default, Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct OrphanReporterOptions {
    /// Whether the orphan reporter is enabled.
    pub enabled: Option<bool>,
    /// How often the reporter emits a summary of orphaned responses.
    pub reporter_interval: Option<Duration>,
    /// Maximum number of orphaned responses to keep per reporting interval.
    pub sample_size: Option<usize>,
}

impl OrphanReporterOptions {
    /// Creates a new `OrphanReporterOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enables or disables the orphan reporter.
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = Some(enabled);
        self
    }

    /// Sets how often the reporter emits a summary of orphaned responses.
    pub fn reporter_interval(mut self, reporter_interval: Duration) -> Self {
        self.reporter_interval = Some(reporter_interval);
        self
    }

    /// Sets the maximum number of orphaned responses to keep per reporting interval.
    pub fn sample_size(mut self, sample_size: usize) -> Self {
        self.sample_size = Some(sample_size);
        self
    }
}

impl Display for OrphanReporterOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "{{ enabled: {:?}, reporter_interval: {:?}, sample_size: {:?} }}",
            self.enabled, self.reporter_interval, self.sample_size
        )
    }
}

impl From<OrphanReporterOptions>
    for couchbase_core::options::orphan_reporter::OrphanReporterConfig
{
    fn from(opts: OrphanReporterOptions) -> Self {
        let mut core_opts =
            couchbase_core::options::orphan_reporter::OrphanReporterConfig::default();

        if let Some(reporter_interval) = opts.reporter_interval {
            core_opts = core_opts.reporter_interval(reporter_interval);
        }

        if let Some(sample_size) = opts.sample_size {
            core_opts = core_opts.sample_size(sample_size);
        }

        core_opts
    }
}

impl Display for ClusterOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "{{ authenticator: {}, compression_mode: {:?}, tls_options: {}, tcp_keep_alive_time: {:?}, poller_options: {}, http_options: {}, kv_options: {}, orphan_reporter_options: {} }}",
            self.authenticator,
            self.compression_mode,
            if let Some(tls) = &self.tls_options {tls.to_string()} else {"none".to_string()},
            self.tcp_keep_alive_time,
            self.poller_options,
            self.http_options,
            self.kv_options,
            self.orphan_reporter_options
        )
    }
}
