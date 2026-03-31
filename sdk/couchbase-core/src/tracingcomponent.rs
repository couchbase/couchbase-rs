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
use crate::clusterlabels::ClusterLabels;
use crate::memdx::durability_level::DurabilityLevel;
use crate::util::get_host_port_from_uri;
use arc_swap::ArcSwap;
use std::fmt::Display;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{instrument, span, trace_span, Instrument, Level, Span};
use url::Url;

pub const SPAN_NAME_DISPATCH_TO_SERVER: &str = "dispatch_to_server";
pub const SPAN_NAME_REQUEST_ENCODING: &str = "request_encoding";

pub const SPAN_ATTRIB_DB_SYSTEM_KEY: &str = "db.system.name";
pub const SPAN_ATTRIB_DB_SYSTEM_VALUE: &str = "couchbase";
pub const SPAN_ATTRIB_OPERATION_ID_KEY: &str = "couchbase.operation_id";
pub const SPAN_ATTRIB_OPERATION_KEY: &str = "db.operation.name";
pub const SPAN_ATTRIB_RETRIES: &str = "couchbase.retries";
pub const SPAN_ATTRIB_LOCAL_ID_KEY: &str = "couchbase.local_id";
pub const SPAN_ATTRIB_NET_TRANSPORT_KEY: &str = "network.transport";
pub const SPAN_ATTRIB_NET_TRANSPORT_VALUE: &str = "tcp";
pub const SPAN_ATTRIB_NET_REMOTE_ADDRESS_KEY: &str = "server.address";
pub const SPAN_ATTRIB_NET_REMOTE_PORT_KEY: &str = "server.port";
pub const SPAN_ATTRIB_NET_PEER_ADDRESS_KEY: &str = "network.peer.address";
pub const SPAN_ATTRIB_NET_PEER_PORT_KEY: &str = "network.peer.port";
pub const SPAN_ATTRIB_SERVER_DURATION_KEY: &str = "couchbase.server_duration";
pub const SPAN_ATTRIB_SERVICE_KEY: &str = "couchbase.service";
pub const SPAN_ATTRIB_DB_NAME_KEY: &str = "db.namespace";
pub const SPAN_ATTRIB_DB_COLLECTION_NAME_KEY: &str = "couchbase.collection.name";
pub const SPAN_ATTRIB_DB_SCOPE_NAME_KEY: &str = "couchbase.scope.name";
pub const SPAN_ATTRIB_DB_DURABILITY: &str = "couchbase.durability";
pub const SPAN_ATTRIB_NUM_RETRIES: &str = "couchbase.retries";
pub const SPAN_ATTRIB_CLUSTER_UUID_KEY: &str = "couchbase.cluster.uuid";
pub const SPAN_ATTRIB_CLUSTER_NAME_KEY: &str = "couchbase.cluster.name";

pub const METER_NAME_CB_OPERATION_DURATION: &str = "db.client.operation.duration";
pub const METER_ATTRIB_SERVICE_KEY: &str = "couchbase.service";
pub const METER_ATTRIB_OPERATION_KEY: &str = "db.operation.name";
pub const METER_ATTRIB_BUCKET_NAME_KEY: &str = "db.namespace";
pub const METER_ATTRIB_SCOPE_NAME_KEY: &str = "couchbase.scope.name";
pub const METER_ATTRIB_COLLECTION_NAME_KEY: &str = "couchbase.collection.name";
pub const METER_ATTRIB_ERROR_KEY: &str = "error.type";
pub const METER_ATTRIB_CLUSTER_UUID_KEY: &str = "couchbase.cluster.uuid";
pub const METER_ATTRIB_CLUSTER_NAME_KEY: &str = "couchbase.cluster.name";

pub const SERVICE_VALUE_KV: &str = "kv";
pub const SERVICE_VALUE_QUERY: &str = "query";
pub const SERVICE_VALUE_ANALYTICS: &str = "analytics";
pub const SERVICE_VALUE_SEARCH: &str = "search";
pub const SERVICE_VALUE_MANAGEMENT: &str = "management";
pub const SERVICE_VALUE_EVENTING: &str = "eventing";

pub const SPAN_ATTRIB_OTEL_KIND_KEY: &str = "otel.kind";
pub const SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE: &str = "client";

macro_rules! record_operation_metric_event {
    (
        $duration:expr,
        $operation:expr,
        $cluster_name:expr,
        $cluster_uuid:expr,
        $error:expr
        $(, $($field:ident).+ = $value:expr )*
    ) => {{
        let cluster_name = $cluster_name;
        let cluster_uuid = $cluster_uuid;
        let error = $error;
        if let Some(error) = error {
            match (cluster_name, cluster_uuid) {
                (Some(name), Some(uuid)) => tracing::event!(
                    target: "couchbase::metrics",
                    Level::TRACE,
                    histogram.db.client.operation.duration = $duration,
                    otel.unit = "s",
                    db.operation.name = $operation,
                    db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
                    $( $($field).+ = $value, )*
                    couchbase.cluster.name = name,
                    couchbase.cluster.uuid = uuid,
                    error.type = error,
                ),
                (Some(name), None) => tracing::event!(
                    target: "couchbase::metrics",
                    Level::TRACE,
                    histogram.db.client.operation.duration = $duration,
                    otel.unit = "s",
                    db.operation.name = $operation,
                    db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
                    $( $($field).+ = $value, )*
                    couchbase.cluster.name = name,
                    error.type = error,
                ),
                (None, Some(uuid)) => tracing::event!(
                    target: "couchbase::metrics",
                    Level::TRACE,
                    histogram.db.client.operation.duration = $duration,
                    otel.unit = "s",
                    db.operation.name = $operation,
                    db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
                    $( $($field).+ = $value, )*
                    couchbase.cluster.uuid = uuid,
                    error.type = error,
                ),
                (None, None) => tracing::event!(
                    target: "couchbase::metrics",
                    Level::TRACE,
                    histogram.db.client.operation.duration = $duration,
                    otel.unit = "s",
                    db.operation.name = $operation,
                    db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
                    $( $($field).+ = $value, )*
                    error.type = error,
                ),
            }
        } else {
            match (cluster_name, cluster_uuid) {
                (Some(name), Some(uuid)) => tracing::event!(
                    target: "couchbase::metrics",
                    Level::TRACE,
                    histogram.db.client.operation.duration = $duration,
                    otel.unit = "s",
                    db.operation.name = $operation,
                    db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
                    $( $($field).+ = $value, )*
                    couchbase.cluster.name = name,
                    couchbase.cluster.uuid = uuid,
                ),
                (Some(name), None) => tracing::event!(
                    target: "couchbase::metrics",
                    Level::TRACE,
                    histogram.db.client.operation.duration = $duration,
                    otel.unit = "s",
                    db.operation.name = $operation,
                    db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
                    $( $($field).+ = $value, )*
                    couchbase.cluster.name = name,
                ),
                (None, Some(uuid)) => tracing::event!(
                    target: "couchbase::metrics",
                    Level::TRACE,
                    histogram.db.client.operation.duration = $duration,
                    otel.unit = "s",
                    db.operation.name = $operation,
                    db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
                    $( $($field).+ = $value, )*
                    couchbase.cluster.uuid = uuid,
                ),
                (None, None) => tracing::event!(
                    target: "couchbase::metrics",
                    Level::TRACE,
                    histogram.db.client.operation.duration = $duration,
                    otel.unit = "s",
                    db.operation.name = $operation,
                    db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
                    $( $($field).+ = $value, )*
                ),
            }
        }
    }};
}

#[macro_export]
macro_rules! create_span {
    ($name:literal) => {
        $crate::tracingcomponent::SpanBuilder::new(
            $name,
            tracing::trace_span!(
                target: "couchbase::tracing",
                $name,
                otel.kind = $crate::tracingcomponent::SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
                otel.status_code = tracing::field::Empty,
                db.operation.name = $name,
                db.system.name = $crate::tracingcomponent::SPAN_ATTRIB_DB_SYSTEM_VALUE,
                couchbase.retries = 0,
                couchbase.cluster.name = tracing::field::Empty,
                couchbase.cluster.uuid = tracing::field::Empty,
                couchbase.service = tracing::field::Empty,
                db.namespace = tracing::field::Empty,
                couchbase.scope.name = tracing::field::Empty,
                couchbase.collection.name = tracing::field::Empty,
                couchbase.durability = tracing::field::Empty,
            ),
        )
    };
}

pub(crate) fn build_keyspace<'a>(
    bucket: Option<&'a str>,
    scope: Option<&'a str>,
    collection_name: Option<&'a str>,
) -> Keyspace<'a> {
    match (bucket, scope, collection_name) {
        (Some(bucket_str), Some(scope_name), Some(collection)) => Keyspace::Collection {
            bucket: bucket_str,
            scope: scope_name,
            collection,
        },
        (Some(bucket_str), Some(scope_name), None) => Keyspace::Scope {
            bucket: bucket_str,
            scope: scope_name,
        },
        (Some(bucket_str), None, None) => Keyspace::Bucket { bucket: bucket_str },
        _ => Keyspace::Cluster,
    }
}

#[derive(Debug)]
pub(crate) struct TracingComponent {
    cluster_labels: ArcSwap<Option<ClusterLabels>>,
}

impl Default for TracingComponent {
    fn default() -> Self {
        Self {
            cluster_labels: ArcSwap::new(Arc::new(None)),
        }
    }
}

impl TracingComponent {
    pub(crate) fn new(config: TracingComponentConfig) -> Self {
        Self {
            cluster_labels: ArcSwap::new(Arc::new(config.cluster_labels)),
        }
    }

    pub(crate) fn reconfigure(&self, state: TracingComponentConfig) {
        self.cluster_labels.store(Arc::new(state.cluster_labels));
    }

    pub(crate) fn get_cluster_labels(&self) -> Option<ClusterLabels> {
        (**self.cluster_labels.load()).clone()
    }

    pub(crate) fn create_dispatch_span(&self, fields: &BeginDispatchFields<'_>) -> Span {
        let span = trace_span!(
            target: "couchbase::tracing",
            SPAN_NAME_DISPATCH_TO_SERVER,
            otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
            db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
            network.transport = SPAN_ATTRIB_NET_TRANSPORT_VALUE,
            couchbase.cluster.uuid = tracing::field::Empty,
            couchbase.cluster.name = tracing::field::Empty,
            couchbase.server_duration = tracing::field::Empty,
            couchbase.local_id = fields.client_id,
            network.peer.address = fields.peer_addr.0,
            network.peer.port = fields.peer_addr.1,
            server.address = fields.canonical_addr.0,
            server.port = fields.canonical_addr.1,
            couchbase.operation_id = tracing::field::Empty,
        );

        self.record_cluster_labels(&span);
        span
    }

    pub(crate) fn record_cluster_labels(&self, span: &Span) {
        let cluster_labels = self.cluster_labels.load();
        if let Some(ref cluster_labels) = **cluster_labels {
            if let Some(ref cluster_uuid) = cluster_labels.cluster_uuid {
                span.record(SPAN_ATTRIB_CLUSTER_UUID_KEY, cluster_uuid.as_str());
            }
            if let Some(ref cluster_name) = cluster_labels.cluster_name {
                span.record(SPAN_ATTRIB_CLUSTER_NAME_KEY, cluster_name.as_str());
            }
        }
    }

    pub(crate) async fn orchestrate_dispatch_span<Fut, T, F>(
        &self,
        begin_fields: BeginDispatchFields<'_>,
        operation: Fut,
        end_fields_provider: F,
    ) -> T
    where
        Fut: Future<Output = T> + Send,
        F: FnOnce(&T) -> EndDispatchFields + Send,
    {
        // Fast path: skip tracing overhead when no subscriber is listening
        if !tracing::span_enabled!(tracing::Level::TRACE) {
            return operation.await;
        }

        let span = self.create_dispatch_span(&begin_fields);
        let result = operation.instrument(span.clone()).await;
        let end_fields = end_fields_provider(&result);
        end_dispatch_span(span, end_fields);
        result
    }
}

#[derive(Debug, Clone, Default)]
pub struct TracingComponentConfig {
    pub cluster_labels: Option<ClusterLabels>,
}

pub enum OperationId {
    String(String),
    Number(u64),
}

impl OperationId {
    pub fn from_u32(n: u32) -> Self {
        Self::Number(n as u64)
    }

    pub fn from_string(s: String) -> Self {
        Self::String(s)
    }
}

pub struct EndDispatchFields {
    pub server_duration: Option<Duration>,
    pub operation_id: Option<OperationId>,
}

impl EndDispatchFields {
    pub fn new(server_duration: Option<Duration>, operation_id: Option<OperationId>) -> Self {
        Self {
            server_duration,
            operation_id,
        }
    }

    pub fn server_duration(mut self, server_duration: Option<Duration>) -> Self {
        self.server_duration = server_duration;
        self
    }

    pub fn operation_id(mut self, operation_id: Option<OperationId>) -> Self {
        self.operation_id = operation_id;
        self
    }
}

pub fn end_dispatch_span(span: Span, fields: EndDispatchFields) {
    if let Some(server_duration) = fields.server_duration {
        span.record(SPAN_ATTRIB_SERVER_DURATION_KEY, server_duration.as_micros());
    }

    if let Some(operation_id) = fields.operation_id {
        match operation_id {
            OperationId::String(s) => span.record(SPAN_ATTRIB_OPERATION_ID_KEY, s),
            OperationId::Number(n) => span.record(SPAN_ATTRIB_OPERATION_ID_KEY, n),
        };
    }

    drop(span);
}

#[derive(Debug)]
pub(crate) struct BeginDispatchFields<'a> {
    // (network.peer.address, network.peer.port)
    pub peer_addr: (&'a str, &'a str),
    pub client_id: Option<&'a str>,
    // (server.address / server.port)
    pub canonical_addr: (&'a str, &'a str),
}

impl<'a> BeginDispatchFields<'a> {
    pub fn new(
        peer_addr: (&'a str, &'a str),
        canonical_addr: (&'a str, &'a str),
        client_id: Option<&'a str>,
    ) -> Self {
        Self {
            peer_addr,
            client_id,
            canonical_addr,
        }
    }
}

pub struct SpanBuilder {
    name: &'static str,
    span: tracing::Span,
}

#[derive(Clone, Debug)]
pub enum Keyspace<'a> {
    Cluster,
    Bucket {
        bucket: &'a str,
    },
    Scope {
        bucket: &'a str,
        scope: &'a str,
    },
    Collection {
        bucket: &'a str,
        scope: &'a str,
        collection: &'a str,
    },
}

impl Display for Keyspace<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Keyspace::Cluster => write!(f, "cluster"),
            Keyspace::Bucket { bucket } => write!(f, "bucket:{}", bucket),
            Keyspace::Scope { bucket, scope } => write!(f, "bucket:{}/scope:{}", bucket, scope),
            Keyspace::Collection {
                bucket,
                scope,
                collection,
            } => write!(
                f,
                "bucket:{}/scope:{}/collection:{}",
                bucket, scope, collection
            ),
        }
    }
}

impl SpanBuilder {
    pub fn new(name: &'static str, span: tracing::Span) -> Self {
        Self { span, name }
    }

    pub fn span(&self) -> &tracing::Span {
        &self.span
    }

    pub fn with_cluster_labels(self, cluster_labels: &Option<ClusterLabels>) -> Self {
        if let Some(labels) = cluster_labels {
            if let Some(uuid) = &labels.cluster_uuid {
                self.span
                    .record(SPAN_ATTRIB_CLUSTER_UUID_KEY, uuid.as_str());
            }
            if let Some(name) = &labels.cluster_name {
                self.span
                    .record(SPAN_ATTRIB_CLUSTER_NAME_KEY, name.as_str());
            }
        }
        self
    }

    pub fn with_durability<D>(self, durability: Option<&D>) -> Self
    where
        for<'a> &'a D: Into<u8>,
    {
        let durability_str = if let Some(durability) = durability {
            match durability.into() {
                1 => "majority",
                2 => "majority_and_persist_active",
                3 => "persist_majority",
                _ => return self,
            }
        } else {
            return self;
        };

        self.span.record(SPAN_ATTRIB_DB_DURABILITY, durability_str);
        self
    }

    pub fn with_keyspace(self, keyspace: &Keyspace<'_>) -> Self {
        match keyspace {
            Keyspace::Cluster => {}
            Keyspace::Bucket { bucket } => {
                self.span.record("db.namespace", *bucket);
            }
            Keyspace::Scope { bucket, scope } => {
                self.span.record("db.namespace", *bucket);
                self.span.record("couchbase.scope.name", *scope);
            }
            Keyspace::Collection {
                bucket,
                scope,
                collection,
            } => {
                self.span.record("db.namespace", *bucket);
                self.span.record("couchbase.scope.name", *scope);
                self.span.record("couchbase.collection.name", *collection);
            }
        }
        self
    }

    pub fn with_service(self, service: Option<&'static str>) -> Self {
        if let Some(service) = service {
            self.span.record(SPAN_ATTRIB_SERVICE_KEY, service);
        }
        self
    }

    pub fn with_statement(self, statement: &str) -> Self {
        self.span.record("db.query.text", statement);
        self
    }

    pub fn build(self) -> tracing::Span {
        self.span
    }

    pub fn name(&self) -> &'static str {
        self.name
    }
}

pub trait MetricsName {
    fn metrics_name(&self) -> &'static str;
}

pub fn record_metrics<E>(
    operation_name: &str,
    service: Option<&str>,
    keyspace: &Keyspace<'_>,
    cluster_labels: &Option<ClusterLabels>,
    start: Instant,
    error: Option<&E>,
) where
    E: MetricsName,
{
    let duration = start.elapsed().as_secs_f64();

    let cluster_name = cluster_labels
        .as_ref()
        .and_then(|labels| labels.cluster_name.as_deref());
    let cluster_uuid = cluster_labels
        .as_ref()
        .and_then(|labels| labels.cluster_uuid.as_deref());
    let error_name = error.map(|err| err.metrics_name());

    match keyspace {
        Keyspace::Cluster => {
            if let Some(service) = service {
                record_operation_metric_event!(
                    duration,
                    operation_name,
                    cluster_name,
                    cluster_uuid,
                    error_name,
                    couchbase.service = service
                );
            } else {
                record_operation_metric_event!(
                    duration,
                    operation_name,
                    cluster_name,
                    cluster_uuid,
                    error_name
                );
            }
        }
        Keyspace::Bucket { bucket } => {
            if let Some(service) = service {
                record_operation_metric_event!(
                    duration,
                    operation_name,
                    cluster_name,
                    cluster_uuid,
                    error_name,
                    couchbase.service = service,
                    db.namespace = bucket
                );
            } else {
                record_operation_metric_event!(
                    duration,
                    operation_name,
                    cluster_name,
                    cluster_uuid,
                    error_name,
                    db.namespace = bucket
                );
            }
        }
        Keyspace::Scope { bucket, scope } => {
            if let Some(service) = service {
                record_operation_metric_event!(
                    duration,
                    operation_name,
                    cluster_name,
                    cluster_uuid,
                    error_name,
                    couchbase.service = service,
                    db.namespace = bucket,
                    couchbase.scope.name = scope
                );
            } else {
                record_operation_metric_event!(
                    duration,
                    operation_name,
                    cluster_name,
                    cluster_uuid,
                    error_name,
                    db.namespace = bucket,
                    couchbase.scope.name = scope
                );
            }
        }
        Keyspace::Collection {
            bucket,
            scope,
            collection,
        } => {
            if let Some(service) = service {
                record_operation_metric_event!(
                    duration,
                    operation_name,
                    cluster_name,
                    cluster_uuid,
                    error_name,
                    couchbase.service = service,
                    db.namespace = bucket,
                    couchbase.scope.name = scope,
                    couchbase.collection.name = collection
                );
            } else {
                record_operation_metric_event!(
                    duration,
                    operation_name,
                    cluster_name,
                    cluster_uuid,
                    error_name,
                    db.namespace = bucket,
                    couchbase.scope.name = scope,
                    couchbase.collection.name = collection
                );
            }
        }
    }
}
