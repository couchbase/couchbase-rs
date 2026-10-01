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
use crate::authenticator::{Authenticator, UserPassPair};
use crate::error::Error;
use crate::error::ErrorKind::Memdx;
use crate::error::{MemdxError, Result};
use crate::kvclient_babysitter::KvTarget;
use crate::log_redaction::{not_sensitive, system_data};
use crate::memdx::connection::{ConnectOptions, ConnectionType, TcpConnection, TlsConnection};
use crate::memdx::dispatcher::{
    Dispatcher, DispatcherOptions, OrphanResponseHandler, UnsolicitedPacketHandler,
};
use crate::memdx::hello_feature::HelloFeature;
use crate::memdx::op_auth_saslauto::{Credentials, SASLAuthAutoOptions};
use crate::memdx::op_bootstrap::BootstrapOptions;
use crate::memdx::packet::ResponsePacket;
use crate::memdx::request::{GetErrorMapRequest, HelloRequest, SelectBucketRequest};
use crate::service_type::ServiceType;
use crate::tls_config::TlsConfig;
use crate::tracingcomponent::TracingComponent;
use crate::util::hostname_from_addr_str;
use crate::{error, memdx};
use arc_swap::ArcSwap;
use chrono::{DateTime, FixedOffset, Local, NaiveDateTime, Utc};
use futures::future::BoxFuture;
use std::future::Future;
use std::net::SocketAddr;
use std::ops::{Add, Deref};
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicPtr, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct KvClientBootstrapOptions {
    pub client_name: String,

    pub disable_error_map: bool,
    pub disable_mutation_tokens: bool,
    pub disable_server_durations: bool,

    pub on_err_map_fetched: Option<OnErrMapFetchedHandler>,
    pub tcp_keep_alive_time: Duration,
    pub auth_mechanisms: Vec<AuthMechanism>,
    pub connect_timeout: Duration,
}

impl PartialEq for KvClientBootstrapOptions {
    fn eq(&self, other: &Self) -> bool {
        self.client_name == other.client_name
            && self.disable_error_map == other.disable_error_map
            && self.disable_server_durations == other.disable_server_durations
            && self.disable_mutation_tokens == other.disable_mutation_tokens
    }
}

#[derive(Clone)]
pub(crate) struct KvClientOptions {
    pub address: KvTarget,
    pub authenticator: Authenticator,
    pub selected_bucket: Option<String>,

    pub bootstrap_options: KvClientBootstrapOptions,
    pub endpoint_id: String,

    pub unsolicited_packet_tx: Option<UnsolicitedPacketSender>,
    pub orphan_handler: Option<OrphanResponseHandler>,
    pub on_close_tx: Option<OnKvClientCloseHandler>,
    pub disable_decompression: bool,
    pub id: String,
    pub tracing: Arc<TracingComponent>,
}

pub(crate) type OnKvClientCloseHandler = mpsc::Sender<()>;

pub(crate) type OnErrMapFetchedHandler = Arc<dyn Fn(&[u8]) + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct UnsolicitedPacket {
    pub packet: ResponsePacket,
    pub endpoint_id: String,
}

pub(crate) type UnsolicitedPacketSender = mpsc::UnboundedSender<UnsolicitedPacket>;

pub(crate) trait KvClient: Sized + PartialEq + Send + Sync {
    fn new(opts: KvClientOptions) -> impl Future<Output = Result<Self>> + Send;
    fn select_bucket(&self, bucket_name: String) -> impl Future<Output = Result<()>> + Send;
    fn has_feature(&self, feature: HelloFeature) -> bool;
    fn remote_hostname(&self) -> &str;
    fn remote_addr(&self) -> SocketAddr;
    fn local_addr(&self) -> SocketAddr;
    fn canonical_addr(&self) -> &Address;
    fn last_activity(&self) -> DateTime<FixedOffset>;
    fn close(&self) -> impl Future<Output = Result<()>> + Send;
    fn id(&self) -> &str;
}

// TODO: connect timeout
pub(crate) struct StdKvClient<D: Dispatcher> {
    remote_addr: SocketAddr,
    local_addr: SocketAddr,
    remote_hostname: String,
    endpoint_id: String,
    canonical_addr: Address,

    // Pre-formatted address strings cached at construction to avoid
    // per-operation allocations in tracing dispatch spans.
    pub(crate) remote_ip_str: String,
    pub(crate) remote_port_str: String,
    pub(crate) canonical_port_str: String,

    cli: D,
    closed: Arc<AtomicBool>,
    on_close_tx: Option<OnKvClientCloseHandler>,
    client_close_handle: tokio::task::JoinHandle<()>,

    supported_features: Vec<HelloFeature>,

    pub(crate) selected_bucket: std::sync::Mutex<Option<String>>,

    pub(crate) last_activity_timestamp_micros: AtomicI64,

    pub(crate) id: String,

    pub(crate) tracing: Arc<TracingComponent>,
}

impl<D> StdKvClient<D>
where
    D: Dispatcher,
{
    pub fn client(&self) -> &D {
        &self.cli
    }
}

impl<D> KvClient for StdKvClient<D>
where
    D: Dispatcher,
{
    async fn new(opts: KvClientOptions) -> Result<StdKvClient<D>> {
        let mut requested_features = vec![
            HelloFeature::DataType,
            HelloFeature::Xattr,
            HelloFeature::Xerror,
            HelloFeature::Snappy,
            HelloFeature::SnappyEverywhere,
            HelloFeature::Json,
            HelloFeature::UnorderedExec,
            HelloFeature::SyncReplication,
            HelloFeature::ReplaceBodyWithXattr,
            HelloFeature::SelectBucket,
            HelloFeature::CreateAsDeleted,
            HelloFeature::AltRequests,
            HelloFeature::Collections,
            HelloFeature::ClusterMapKnownVersion,
            HelloFeature::DedupeNotMyVbucketClustermap,
            HelloFeature::ClusterMapChangeNotificationBrief,
            HelloFeature::Duplex,
            HelloFeature::PreserveExpiry,
        ];

        if !opts.bootstrap_options.disable_mutation_tokens {
            requested_features.push(HelloFeature::SeqNo)
        }

        if !opts.bootstrap_options.disable_server_durations {
            requested_features.push(HelloFeature::Durations);
        }

        let boostrap_hello = if !opts.bootstrap_options.client_name.is_empty() {
            Some(HelloRequest {
                client_name: Vec::from(opts.bootstrap_options.client_name.clone()),
                requested_features,
            })
        } else {
            None
        };

        let bootstrap_get_error_map = if !opts.bootstrap_options.disable_error_map {
            Some(GetErrorMapRequest { version: 2 })
        } else {
            None
        };

        let address = opts.address.address;

        let credentials = match &opts.authenticator {
            Authenticator::PasswordAuthenticator(a) => {
                let creds = a.get_credentials(&ServiceType::MEMD, address.to_string())?;
                Some(Credentials::UserPass {
                    username: creds.username,
                    password: creds.password,
                })
            }
            Authenticator::CertificateAuthenticator(_a) => None,
            Authenticator::JwtAuthenticator(a) => {
                Some(Credentials::JwtToken(a.get_token().to_string()))
            }
        };

        let bootstrap_auth = if let Some(credentials) = credentials {
            let enabled_mechs: Vec<memdx::auth_mechanism::AuthMechanism> =
                if !opts.bootstrap_options.auth_mechanisms.is_empty() {
                    opts.bootstrap_options
                        .auth_mechanisms
                        .iter()
                        .cloned()
                        .map(memdx::auth_mechanism::AuthMechanism::from)
                        .collect()
                } else {
                    match opts.authenticator {
                        Authenticator::PasswordAuthenticator(a) => {
                            a.get_auth_mechanisms(opts.address.tls_config.is_some())
                        }
                        Authenticator::JwtAuthenticator(a) => a.get_auth_mechanisms(),
                        _ => vec![],
                    }
                    .into_iter()
                    .map(memdx::auth_mechanism::AuthMechanism::from)
                    .collect()
                };

            Some(SASLAuthAutoOptions {
                credentials,
                enabled_mechs,
            })
        } else {
            None
        };

        let bootstrap_select_bucket =
            opts.selected_bucket
                .as_ref()
                .map(|bucket_name| SelectBucketRequest {
                    bucket_name: bucket_name.clone(),
                });

        let should_bootstrap = boostrap_hello.is_some()
            || bootstrap_auth.is_some()
            || bootstrap_get_error_map.is_some();

        let closed = Arc::new(AtomicBool::new(false));
        let closed_clone = closed.clone();
        let id = opts.id;
        let read_id = id.clone();

        let client_id = Uuid::new_v4().to_string();

        info!(
            "Kvclient {} assigning client id {} for {}",
            &id,
            &client_id,
            system_data(&address)
        );

        let (on_read_close_tx, mut on_read_close_rx) = oneshot::channel::<()>();

        let unsolicited_packet_tx = opts.unsolicited_packet_tx.clone();
        let endpoint_id = opts.endpoint_id.clone();
        let unsolicited_client_id = client_id.clone();
        let memdx_client_opts = DispatcherOptions {
            on_read_close_tx,
            orphan_handler: opts.orphan_handler,
            unsolicited_packet_handler: Arc::new(move |p| {
                let unsolicited_packet_tx = unsolicited_packet_tx.clone();
                let endpoint_id = endpoint_id.clone();
                let unsolicited_client_id = unsolicited_client_id.clone();
                Box::pin(async move {
                    if let Some(sender) = unsolicited_packet_tx {
                        if let Err(e) = sender.send(UnsolicitedPacket {
                            packet: p,
                            endpoint_id,
                        }) {
                            // A channel error renders as a fixed message, never its payload.
                            warn!(
                                "Failed to send unsolicited packet {} on {}",
                                not_sensitive(&e),
                                &unsolicited_client_id
                            );
                        };
                    }
                })
            }),
            disable_decompression: opts.disable_decompression,
            id: client_id,
        };

        let conn = if let Some(tls) = opts.address.tls_config {
            let conn = match TlsConnection::connect(
                address.clone(),
                tls,
                ConnectOptions {
                    deadline: Instant::now().add(opts.bootstrap_options.connect_timeout),
                    tcp_keep_alive_time: opts.bootstrap_options.tcp_keep_alive_time,
                },
            )
            .await
            {
                Ok(conn) => conn,
                Err(e) => {
                    return Err(Error::new_contextual_memdx_error(
                        MemdxError::new(e).with_dispatched_to(address.to_string()),
                    ))
                }
            };
            ConnectionType::Tls(conn)
        } else {
            let conn = match TcpConnection::connect(
                address.clone(),
                ConnectOptions {
                    deadline: Instant::now().add(opts.bootstrap_options.connect_timeout),
                    tcp_keep_alive_time: opts.bootstrap_options.tcp_keep_alive_time,
                },
            )
            .await
            {
                Ok(conn) => conn,
                Err(e) => {
                    return Err(Error::new_contextual_memdx_error(
                        MemdxError::new(e).with_dispatched_to(address.to_string()),
                    ))
                }
            };
            ConnectionType::Tcp(conn)
        };

        let remote_addr = *conn.peer_addr();
        let local_addr = *conn.local_addr();
        let remote_hostname = hostname_from_addr_str(address.host.as_str());

        let mut cli = D::new(conn, memdx_client_opts);

        let on_close = opts.on_close_tx.clone();
        let close_handle = tokio::spawn(async move {
            let _ = on_read_close_rx.await;

            closed_clone.store(true, Ordering::SeqCst);

            if let Some(on_close) = on_close {
                // If we get here via the drop chain then this will probably be closed so don't
                // send on it.
                if !on_close.is_closed() {
                    if let Err(e) = on_close.send(()).await {
                        // A channel error renders as a fixed message, never its payload.
                        debug!(
                            "Failed to send on_close for kvclient {}: {}",
                            &read_id,
                            not_sensitive(&e)
                        );
                    }
                }
            }
        });

        let canonical_addr = opts.address.canonical_address;
        let remote_ip_str = remote_addr.ip().to_string();
        let remote_port_str = remote_addr.port().to_string();
        let canonical_port_str = canonical_addr.port.to_string();

        let mut kv_cli = StdKvClient {
            remote_addr,
            local_addr,
            remote_hostname,
            endpoint_id: opts.endpoint_id,
            canonical_addr,
            remote_ip_str,
            remote_port_str,
            canonical_port_str,
            cli,
            closed,
            on_close_tx: opts.on_close_tx,
            supported_features: vec![],
            selected_bucket: std::sync::Mutex::new(None),
            id: id.clone(),
            last_activity_timestamp_micros: AtomicI64::new(Utc::now().timestamp_micros()),
            client_close_handle: close_handle,
            tracing: opts.tracing,
        };

        if should_bootstrap {
            if let Some(b) = &bootstrap_select_bucket {
                let mut guard = kv_cli.selected_bucket.lock().unwrap();
                *guard = Some(b.bucket_name.clone());
            };

            let res = match kv_cli
                .bootstrap(BootstrapOptions {
                    hello: boostrap_hello,
                    get_error_map: bootstrap_get_error_map,
                    auth: bootstrap_auth,
                    select_bucket: bootstrap_select_bucket,
                    deadline: Instant::now().add(Duration::from_secs(7)),
                    get_cluster_config: None,
                })
                .await
            {
                Ok(res) => res,
                Err(e) => {
                    kv_cli.close().await.unwrap_or_default();
                    return Err(Error::new_contextual_memdx_error(e));
                }
            };

            if let Some(hello) = res.hello {
                info!(
                    "Enabled hello features: {:?}",
                    not_sensitive(&hello.enabled_features)
                );
                kv_cli.supported_features = hello.enabled_features;
            }

            if let Some(handler) = opts.bootstrap_options.on_err_map_fetched {
                if let Some(err_map) = res.error_map {
                    handler(&err_map.error_map);
                }
            }
        }

        Ok(kv_cli)
    }

    async fn select_bucket(&self, bucket_name: String) -> Result<()> {
        debug!("Selecting bucket on KvClient {}", &self.id);

        {
            let mut guard = self.selected_bucket.lock().unwrap();
            let selected_bucket = guard.as_ref();
            if selected_bucket.is_some() {
                return Err(Error::new_invalid_argument_error(
                    "cannot select bucket when a bucket is already selected",
                    Some("bucket_name".to_string()),
                ));
            }

            *guard = Some(bucket_name.clone());
        }

        match self
            .select_bucket(SelectBucketRequest {
                bucket_name: bucket_name.clone(),
            })
            .await
        {
            Ok(_r) => Ok(()),
            Err(e) => {
                let mut guard = self.selected_bucket.lock().unwrap();
                *guard = None;
                Err(Error::new(Memdx(e)))
            }
        }
    }

    fn has_feature(&self, feature: HelloFeature) -> bool {
        self.supported_features.contains(&feature)
    }

    fn remote_hostname(&self) -> &str {
        &self.remote_hostname
    }

    fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    fn canonical_addr(&self) -> &Address {
        &self.canonical_addr
    }

    fn last_activity(&self) -> DateTime<FixedOffset> {
        let last_activity = self.last_activity_timestamp_micros.load(SeqCst);

        DateTime::from_timestamp_micros(last_activity)
            .unwrap_or_default()
            .fixed_offset()
    }

    async fn close(&self) -> Result<()> {
        if self
            .closed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            != Ok(false)
        {
            return Ok(());
        }

        info!("Kvclient {} closing", self.id);

        self.cli
            .close()
            .await
            .map_err(|e| Error::new_contextual_memdx_error(MemdxError::new(e)))?;

        Ok(())
    }

    fn id(&self) -> &str {
        &self.id
    }
}

impl<D> PartialEq for StdKvClient<D>
where
    D: Dispatcher,
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<D> Drop for StdKvClient<D>
where
    D: Dispatcher,
{
    fn drop(&mut self) {
        // This basically just prevents the client read close handler from attempting to
        // signal upstream that we've closed.
        self.closed.store(true, Ordering::SeqCst);
        info!("Dropping kvclient {}", self.id);
    }
}
