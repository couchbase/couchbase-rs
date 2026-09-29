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

use std::pin::{pin, Pin};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::analyticsx::error;
use crate::analyticsx::error::ErrorKind::Server;
use crate::analyticsx::error::{Error, ErrorDesc, ServerError, ServerErrorKind};
use crate::analyticsx::query_json::{
    QueryError, QueryErrorResponse, QueryMetaData, QueryMetrics, QueryWarning,
};
use crate::analyticsx::query_result::{MetaData, MetadataPlans, Metrics, Warning};
use crate::helpers::durations::parse_duration_from_golang_string;
use crate::httpx;
use crate::httpx::decoder::Decoder;
use crate::httpx::raw_json_row_streamer::{RawJsonRowItem, RawJsonRowStreamer};
use crate::httpx::response::Response;
use arc_swap::ArcSwap;
use bytes::Bytes;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt};
use http::StatusCode;
use tracing::debug;

pub struct QueryRespReader {
    endpoint: String,
    statement: String,
    client_context_id: String,
    status_code: StatusCode,

    streamer: Pin<Box<dyn Stream<Item = httpx::error::Result<RawJsonRowItem>> + Send>>,
    meta_data: Option<MetaData>,
    meta_data_error: Option<Error>,
}

impl Stream for QueryRespReader {
    type Item = error::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match this.streamer.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(RawJsonRowItem::Row(row_data)))) => {
                Poll::Ready(Some(Ok(Bytes::from(row_data))))
            }
            Poll::Ready(Some(Ok(RawJsonRowItem::Metadata(metadata)))) => {
                match this.read_final_metadata(metadata) {
                    Ok(meta) => this.meta_data = Some(meta),
                    Err(e) => {
                        this.meta_data_error = Some(e.clone());
                        return Poll::Ready(Some(Err(e)));
                    }
                };
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(Error::new_http_error(
                e,
                this.endpoint.to_string(),
                Some(this.statement.clone()),
                this.client_context_id.clone(),
            )))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl QueryRespReader {
    pub async fn new(
        resp: Response,
        endpoint: impl Into<String>,
        statement: impl Into<String>,
        client_context_id: impl Into<String>,
    ) -> error::Result<Self> {
        let status_code = resp.status();
        let endpoint = endpoint.into();
        let statement = statement.into();
        let client_context_id = client_context_id.into();
        if status_code != 200 {
            let body = match resp.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    debug!("Failed to read response body on error {}", &e);
                    return Err(Error::new_http_error(
                        e,
                        endpoint,
                        statement,
                        client_context_id,
                    ));
                }
            };

            let errors: QueryErrorResponse = match serde_json::from_slice(&body) {
                Ok(e) => e,
                Err(e) => {
                    return Err(Error::new_message_error(
                        format!(
                        "non-200 status code received {status_code} but parsing error response body failed {e}"
                    ),
                        None,
                        None,
                        None,
                    ));
                }
            };

            if errors.errors.is_empty() {
                return Err(Error::new_message_error(
                    format!(
                        "Non-200 status code received {status_code} but response body contained no errors",
                    ),
                    None,
                    None,
                    None,
                ));
            }

            return Err(Self::parse_errors(
                &errors.errors,
                endpoint,
                statement,
                client_context_id,
                status_code,
            ));
        }

        let stream = resp.bytes_stream();
        let mut streamer = RawJsonRowStreamer::new(Decoder::new(stream), "results");
        // There is no actual prelude we need to hold onto, but we need to trigger the reading of
        // the stream to advance the streamer to the rows.
        streamer.read_prelude().await.map_err(|e| {
            Error::new_http_error(
                e,
                endpoint.clone(),
                statement.to_string(),
                client_context_id.to_string(),
            )
        })?;

        let has_more_rows = streamer.has_more_rows().await;
        let mut epilog = None;
        if !has_more_rows {
            epilog = match streamer.epilog() {
                Ok(epilog) => Some(epilog),
                Err(e) => {
                    return Err(Error::new_http_error(
                        e,
                        endpoint.clone(),
                        statement,
                        client_context_id,
                    ));
                }
            };
        }

        let mut reader = Self {
            endpoint,
            statement,
            client_context_id,
            status_code,
            streamer: Box::pin(streamer.into_stream()),
            meta_data: None,
            meta_data_error: None,
        };

        if let Some(epilog) = epilog {
            let meta = reader.read_final_metadata(epilog)?;

            reader.meta_data = Some(meta);
        }

        Ok(reader)
    }

    pub fn metadata(&self) -> error::Result<&MetaData> {
        if let Some(e) = &self.meta_data_error {
            return Err(e.clone());
        }

        if let Some(meta) = &self.meta_data {
            return Ok(meta);
        }

        Err(Error::new_message_error(
            "cannot read meta-data until after all rows are read",
            None,
            None,
            None,
        ))
    }

    fn read_final_metadata(&mut self, epilog: Vec<u8>) -> error::Result<MetaData> {
        let metadata: QueryMetaData = match serde_json::from_slice(&epilog) {
            Ok(m) => m,
            Err(e) => {
                return Err(Error::new_message_error(
                    format!("failed to parse analytics metadata from epilog: {e}"),
                    self.endpoint.clone(),
                    self.statement.clone(),
                    self.client_context_id.clone(),
                ));
            }
        };

        self.parse_metadata(metadata)
    }

    fn parse_metadata(&self, metadata: QueryMetaData) -> error::Result<MetaData> {
        if !metadata.errors.is_empty() {
            return Err(Self::parse_errors(
                &metadata.errors,
                &self.endpoint,
                &self.statement,
                &self.client_context_id,
                self.status_code,
            ));
        }

        let metrics = self.parse_metrics(metadata.metrics);
        let warnings = self.parse_warnings(metadata.warnings);

        Ok(MetaData {
            request_id: metadata.request_id,
            client_context_id: metadata.client_context_id,
            status: metadata.status,
            metrics,
            signature: metadata.signature,
            warnings,
            plans: metadata.plans.map(|p| MetadataPlans {
                logical_plan: p.logical_plan,
                optimized_logical_plan: p.optimized_logical_plan,
                rewritten_expression_tree: p.rewritten_expression_tree,
                expression_tree: p.expression_tree,
                job: p.job,
            }),
        })
    }

    fn parse_metrics(&self, metrics: Option<QueryMetrics>) -> Option<Metrics> {
        metrics.map(|m| {
            let elapsed_time = if let Some(elapsed) = m.elapsed_time {
                parse_duration_from_golang_string(&elapsed).unwrap_or_default()
            } else {
                Duration::default()
            };

            let execution_time = if let Some(execution) = m.execution_time {
                parse_duration_from_golang_string(&execution).unwrap_or_default()
            } else {
                Duration::default()
            };

            Metrics {
                elapsed_time,
                execution_time,
                result_count: m.result_count.unwrap_or_default(),
                result_size: m.result_size.unwrap_or_default(),
                error_count: m.error_count.unwrap_or_default(),
                warning_count: m.warning_count.unwrap_or_default(),
                processed_objects: m.processed_objects.unwrap_or_default(),
            }
        })
    }

    fn parse_warnings(&self, warnings: Vec<QueryWarning>) -> Vec<Warning> {
        let mut converted = vec![];
        for w in warnings {
            converted.push(Warning {
                code: w.code.unwrap_or_default(),
                message: w.msg.unwrap_or_default(),
            });
        }

        converted
    }

    fn parse_errors(
        errors: &[QueryError],
        endpoint: impl Into<String>,
        statement: impl Into<String>,
        client_context_id: impl Into<String>,
        status_code: StatusCode,
    ) -> Error {
        let error_descs: Vec<ErrorDesc> = errors
            .iter()
            .map(|error| {
                ErrorDesc::new(Self::parse_error_kind(error), error.code, error.msg.clone())
            })
            .collect();

        let chosen_desc = &error_descs[0];

        let mut server_error = ServerError::new(
            chosen_desc.kind().clone(),
            endpoint,
            status_code,
            chosen_desc.code(),
            chosen_desc.message(),
        )
        .with_client_context_id(client_context_id)
        .with_statement(statement);

        if error_descs.len() > 1 {
            server_error = server_error.with_error_descs(error_descs);
        }

        Error::new_server_error(server_error)
    }

    fn parse_error_kind(error: &QueryError) -> ServerErrorKind {
        let err_code = error.code;
        let err_code_group = err_code / 1000;

        if err_code_group == 20 {
            ServerErrorKind::AuthenticationFailure
        } else if err_code_group == 24 {
            if err_code == 24000 {
                ServerErrorKind::ParsingFailure
            } else if err_code == 24006 {
                ServerErrorKind::LinkNotFound
            } else if err_code == 24025 || err_code == 24044 || err_code == 24045 {
                ServerErrorKind::DatasetNotFound
            } else if err_code == 24034 {
                ServerErrorKind::DataverseNotFound
            } else if err_code == 24039 {
                ServerErrorKind::DataverseExists
            } else if err_code == 24040 {
                ServerErrorKind::DatasetExists
            } else if err_code == 24047 {
                ServerErrorKind::IndexNotFound
            } else if err_code == 24048 {
                ServerErrorKind::IndexExists
            } else if err_code == 24055 {
                ServerErrorKind::LinkExists
            } else {
                ServerErrorKind::CompilationFailure
            }
        } else if err_code_group == 25 {
            ServerErrorKind::Internal
        } else if err_code == 23000 || err_code == 23003 {
            ServerErrorKind::TemporaryFailure
        } else if err_code == 23007 {
            ServerErrorKind::JobQueueFull
        } else {
            ServerErrorKind::Unknown
        }
    }
}
