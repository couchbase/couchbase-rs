use crate::commands::helpers::create_run_result;
use crate::errors::error::{Error, Result};
use crate::proto::protocol::run::result;
use crate::proto::protocol::shared::content_as::As;
use crate::proto::protocol::shared::{ContentTypes, ScanConsistency};
use crate::proto::protocol::{run, sdk, shared};
use crate::translations::common::{sdk_error_to_proto_run_result, sdk_error_to_proto_sdk_result};
use couchbase::cluster::Cluster;
use couchbase::options::query_options::ScanConsistency::{NotBounded, RequestPlus};
use couchbase::options::query_options::{QueryOptions, ReplicaLevel};
use couchbase::results::query_results::{QueryMetrics, QueryStatus, QueryWarning};
use couchbase::scope::Scope;
use log::info;
use prost_types::Timestamp;
use serde_json::Value;
use std::time::Duration;
use tonic::codegen::tokio_stream::StreamExt;
use tracing::Instrument;

#[derive(Clone)]
pub enum Location {
    Cluster(Cluster),
    Scope(Scope),
}

pub struct QueryCommand {
    location: Location,
    statement: String,
    return_result: bool,
    initiated: Timestamp,
    content_as: Option<As>,
    options: Option<QueryOptions>,
    parent_span: Option<tracing::Span>,
}

impl QueryCommand {
    pub fn new(
        location: Location,
        statement: String,
        return_result: bool,
        initiated: Timestamp,
        content_as: Option<As>,
        options: Option<QueryOptions>,
        parent_span: Option<tracing::Span>,
    ) -> Self {
        QueryCommand {
            location,
            statement,
            return_result,
            initiated,
            content_as,
            options,
            parent_span,
        }
    }

    pub async fn execute(&self, batcher: &crate::common::batcher::Batcher) -> Result<bool> {
        let start = std::time::Instant::now();
        let mut res = match self.parent_span.clone() {
            Some(span) => match &self.location {
                Location::Cluster(cluster) => {
                    cluster
                        .query(&self.statement, self.options.clone())
                        .instrument(span)
                        .await
                }
                Location::Scope(scope) => {
                    scope
                        .query(&self.statement, self.options.clone())
                        .instrument(span)
                        .await
                }
            },
            None => match &self.location {
                Location::Cluster(cluster) => {
                    cluster.query(&self.statement, self.options.clone()).await
                }
                Location::Scope(scope) => scope.query(&self.statement, self.options.clone()).await,
            },
        };
        let end = std::time::Instant::now();

        let mut top_level_result = create_run_result(self.initiated, end - start);

        match res {
            Ok(ref mut ok) => {
                if self.return_result {
                    top_level_result.result = Some(result::Result::Sdk(
                        Self::sdk_result_from_query_result(self.content_as.as_ref(), ok).await?,
                    ));
                } else {
                    top_level_result.result = Some(run::result::Result::Sdk(sdk::Result {
                        result: Some(sdk::result::Result::Success(true)),
                    }));
                }
            }
            Err(err) => {
                top_level_result.result = Some(sdk_error_to_proto_run_result(err));
                batcher.send(top_level_result).await;
                return Ok(false);
            }
        }

        batcher.send(top_level_result).await;
        Ok(true)
    }

    async fn sdk_result_from_query_result(
        content_as: Option<&As>,
        res: &mut couchbase::results::query_results::QueryResult,
    ) -> Result<sdk::Result> {
        let content: Vec<ContentTypes> = match content_as {
            Some(As::AsString(_)) => {
                let mut content = vec![];
                let mut rows = res.rows::<String>();
                while let Some(row) = rows.next().await {
                    match row {
                        Ok(parsed) => content.push(ContentTypes {
                            content: Some(shared::content_types::Content::ContentAsString(parsed)),
                        }),
                        Err(err) => return Ok(sdk_error_to_proto_sdk_result(err)),
                    }
                }
                content
            }
            Some(As::AsByteArray(_)) => {
                let mut content = vec![];
                let mut rows = res.rows::<Vec<u8>>();
                while let Some(row) = rows.next().await {
                    match row {
                        Ok(parsed) => content.push(ContentTypes {
                            content: Some(shared::content_types::Content::ContentAsBytes(parsed)),
                        }),
                        Err(err) => return Ok(sdk_error_to_proto_sdk_result(err)),
                    }
                }
                content
            }
            Some(As::AsJsonObject(_)) | Some(As::AsJsonArray(_)) => {
                let mut content = vec![];
                let mut rows = res.rows::<Value>();
                while let Some(row) = rows.next().await {
                    match row {
                        Ok(parsed) => {
                            let bytes = serde_json::to_vec(&parsed).map_err(|e| {
                                Error::internal(format!("Failed to serialize JSON value: {e}"))
                            })?;
                            content.push(ContentTypes {
                                content: Some(shared::content_types::Content::ContentAsBytes(
                                    bytes,
                                )),
                            });
                        }
                        Err(err) => return Ok(sdk_error_to_proto_sdk_result(err)),
                    }
                }
                content
            }
            Some(As::AsBoolean(_)) => {
                let mut content = vec![];
                let mut rows = res.rows::<bool>();
                while let Some(row) = rows.next().await {
                    match row {
                        Ok(parsed) => content.push(ContentTypes {
                            content: Some(shared::content_types::Content::ContentAsBool(parsed)),
                        }),
                        Err(err) => return Ok(sdk_error_to_proto_sdk_result(err)),
                    }
                }
                content
            }
            Some(As::AsInteger(_)) => {
                let mut content = vec![];
                let mut rows = res.rows::<i64>();
                while let Some(row) = rows.next().await {
                    match row {
                        Ok(parsed) => content.push(ContentTypes {
                            content: Some(shared::content_types::Content::ContentAsInt64(parsed)),
                        }),
                        Err(err) => return Ok(sdk_error_to_proto_sdk_result(err)),
                    }
                }
                content
            }
            Some(As::AsFloatingPoint(_)) => {
                let mut content = vec![];
                let mut rows = res.rows::<f64>();
                while let Some(row) = rows.next().await {
                    match row {
                        Ok(parsed) => content.push(ContentTypes {
                            content: Some(shared::content_types::Content::ContentAsDouble(parsed)),
                        }),
                        Err(err) => return Ok(sdk_error_to_proto_sdk_result(err)),
                    }
                }
                content
            }
            None => {
                let mut content = vec![];
                let mut rows = res.rows::<Vec<u8>>();
                while let Some(row) = rows.next().await {
                    match row {
                        Ok(parsed) => content.push(ContentTypes {
                            content: Some(shared::content_types::Content::ContentAsBytes(parsed)),
                        }),
                        Err(err) => return Ok(sdk_error_to_proto_sdk_result(err)),
                    }
                }
                content
            }
        };

        let meta_data = match res.metadata() {
            Ok(metadata) => Some((&metadata).try_into()?),
            Err(err) => return Ok(sdk_error_to_proto_sdk_result(err)),
        };

        Ok(sdk::Result {
            result: Some(sdk::result::Result::QueryResult(sdk::query::QueryResult {
                content,
                meta_data,
            })),
        })
    }
}

// Request conversions
impl TryFrom<sdk::query::QueryOptions> for QueryOptions {
    type Error = Box<Error>;

    fn try_from(proto: sdk::query::QueryOptions) -> Result<Self> {
        let mut options = QueryOptions::new();

        if let Some(scan_consistency) = proto.scan_consistency {
            match ScanConsistency::try_from(scan_consistency) {
                Ok(ScanConsistency::RequestPlus) => {
                    options = options.scan_consistency(RequestPlus);
                }
                Ok(ScanConsistency::NotBounded) => {
                    options = options.scan_consistency(NotBounded);
                }
                Err(_) => {
                    return Err(Error::invalid_argument(
                        "Invalid scan consistency specified",
                    ))
                }
            }
        }
        for (k, v) in proto.raw {
            options = options
                .add_raw(k, v)
                .map_err(|e| Error::internal(format!("Failed to add raw option: {e}")))?;
        }
        if let Some(adhoc) = proto.adhoc {
            options = options.ad_hoc(adhoc);
        }
        if let Some(profile) = proto.profile {
            match profile.as_str() {
                "off" => {
                    options = options.profile(couchbase::options::query_options::ProfileMode::Off)
                }
                "phases" => {
                    options =
                        options.profile(couchbase::options::query_options::ProfileMode::Phases)
                }
                "timings" => {
                    options =
                        options.profile(couchbase::options::query_options::ProfileMode::Timings)
                }
                _ => return Err(Error::invalid_argument("Invalid profile mode specified")),
            }
        }
        if let Some(readonly) = proto.readonly {
            options = options.read_only(readonly);
        }
        for param in proto.parameters_positional {
            options = options
                .add_positional_parameter(param)
                .map_err(|e| Error::internal(format!("Failed to add positional parameter: {e}")))?;
        }
        for (k, v) in proto.parameters_named {
            options = options
                .add_named_parameter(k, v)
                .map_err(|e| Error::internal(format!("Failed to add named parameter: {e}")))?;
        }
        if let Some(flex_index) = proto.flex_index {
            options = options.flex_index(flex_index);
        }
        if let Some(pipeline_cap) = proto.pipeline_cap {
            options = options.pipeline_cap(pipeline_cap as u32);
        }
        if let Some(pipeline_batch) = proto.pipeline_batch {
            options = options.pipeline_batch(pipeline_batch as u32);
        }
        if let Some(scan_cap) = proto.scan_cap {
            options = options.scan_cap(scan_cap as u32);
        }
        if let Some(scan_wait_millis) = proto.scan_wait_millis {
            options = options.scan_wait(Duration::from_millis(scan_wait_millis as u64));
        }
        if let Some(timeout_millis) = proto.timeout_millis {
            options = options.server_timeout(Duration::from_millis(timeout_millis as u64));
        }
        if let Some(max_parallelism) = proto.max_parallelism {
            options = options.max_parallelism(max_parallelism as u32);
        }
        if let Some(metrics) = proto.metrics {
            options = options.metrics(metrics);
        }
        if let Some(_single_query_transaction_options) = proto.single_query_transaction_options {
            return Err(Error::unimplemented(
                "Single query transaction options are not implemented yet",
            ));
        }
        if let Some(use_replica) = proto.use_replica {
            options = match use_replica {
                true => options.use_replica(ReplicaLevel::On),
                false => options.use_replica(ReplicaLevel::Off),
            };
        }
        if let Some(client_context_id) = proto.client_context_id {
            options = options.client_context_id(client_context_id);
        }
        if let Some(consistent_with) = proto.consistent_with {
            options = options.scan_consistency(
                couchbase::options::query_options::ScanConsistency::AtPlus(consistent_with.into()),
            );
        }
        if let Some(preserve_expiry) = proto.preserve_expiry {
            options = options.preserve_expiry(preserve_expiry);
        }

        Ok(options)
    }
}

// Result conversions
impl TryFrom<&couchbase::results::query_results::QueryMetaData<'_>> for sdk::query::QueryMetaData {
    type Error = Box<Error>;

    fn try_from(metadata: &couchbase::results::query_results::QueryMetaData) -> Result<Self> {
        info!("couchbase metrics: {:?}", metadata.metrics);

        Ok(Self {
            request_id: metadata.request_id.to_string(),
            client_context_id: metadata.client_context_id.to_string(),
            status: sdk::query::QueryStatus::from(metadata.status) as i32,
            signature: metadata
                .signature
                .as_ref()
                .map(|raw| raw.get().as_bytes().to_vec()),
            warnings: metadata.warnings.iter().cloned().map(Into::into).collect(),
            metrics: metadata
                .metrics
                .as_ref()
                .map(|m| m.try_into())
                .transpose()?,
            profile: metadata
                .profile
                .as_ref()
                .map(|raw| raw.get().as_bytes().to_vec()),
        })
    }
}

impl From<QueryStatus> for sdk::query::QueryStatus {
    fn from(status: QueryStatus) -> Self {
        match status {
            QueryStatus::Running => Self::Running,
            QueryStatus::Success => Self::Success,
            QueryStatus::Errors => Self::Errors,
            QueryStatus::Completed => Self::Completed,
            QueryStatus::Stopped => Self::Stopped,
            QueryStatus::Timeout => Self::Timeout,
            QueryStatus::Closed => Self::Closed,
            QueryStatus::Fatal => Self::Fatal,
            QueryStatus::Aborted => Self::Aborted,
            QueryStatus::Unknown => Self::Unknown,
            _ => Self::Unknown,
        }
    }
}

impl From<QueryWarning> for sdk::query::QueryWarning {
    fn from(value: QueryWarning) -> Self {
        Self {
            code: value.code as i32,
            message: value.message,
        }
    }
}

impl TryFrom<&QueryMetrics> for sdk::query::QueryMetrics {
    type Error = Box<Error>;

    fn try_from(value: &QueryMetrics) -> Result<Self> {
        Ok(Self {
            elapsed_time: Some(
                value
                    .elapsed_time
                    .try_into()
                    .map_err(|e| Error::internal(format!("Duration conversion failed: {e}")))?,
            ),
            execution_time: Some(
                value
                    .execution_time
                    .try_into()
                    .map_err(|e| Error::internal(format!("Duration conversion failed: {e}")))?,
            ),
            sort_count: value.sort_count,
            result_count: value.result_count,
            result_size: value.result_size,
            mutation_count: value.mutation_count,
            error_count: value.error_count,
            warning_count: value.warning_count,
        })
    }
}
