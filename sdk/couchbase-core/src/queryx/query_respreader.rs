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

use crate::log_redaction::user_data;
use std::pin::{pin, Pin};
use std::ptr::read;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::err;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt};
use http::StatusCode;
use regex::Regex;
use serde_json::Value;
use tokio::sync::Mutex;
use tracing::debug;

use crate::helpers::durations::parse_duration_from_golang_string;
use crate::httpx;
use crate::httpx::decoder::Decoder;
use crate::httpx::raw_json_row_streamer::{RawJsonRowItem, RawJsonRowStreamer};
use crate::httpx::response::Response;
use crate::memdx::magic::Magic::Res;
use crate::queryx::error;
use crate::queryx::error::{
    Error, ErrorDesc, ErrorKind, ResourceError, ServerError, ServerErrorKind,
};
use crate::queryx::query_json::{
    QueryEarlyMetaData, QueryError, QueryErrorResponse, QueryMetaData, QueryMetrics,
    QueryTokenUsage, QueryWarning,
};
use crate::queryx::query_result::{EarlyMetaData, MetaData, Metrics, TokenUsage, Warning};

pub struct QueryRespReader {
    endpoint: String,
    statement: String,
    client_context_id: String,
    status_code: StatusCode,

    streamer: Pin<Box<dyn Stream<Item = httpx::error::Result<RawJsonRowItem>> + Send>>,
    early_meta_data: EarlyMetaData,
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
                this.endpoint.clone(),
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
                    debug!("Failed to read response body on error {}", user_data(&e));
                    return Err(Error::new_http_error(
                        e,
                        endpoint,
                        statement,
                        client_context_id,
                    ));
                }
            };

            let msg = match serde_json::from_slice::<QueryErrorResponse>(&body) {
                Ok(resp) if !resp.errors.is_empty() => {
                    return Err(Self::parse_errors(
                        &resp.errors,
                        endpoint,
                        statement,
                        client_context_id,
                        status_code,
                    ));
                }
                Ok(_) => format!(
                    "Non-200 status code received {status_code} but response body contained no errors"
                ),
                Err(e) => format!(
                    "non-200 status code received {status_code} but parsing error response body failed {e}"
                ),
            };

            if status_code == StatusCode::BAD_REQUEST {
                return Err(Error::new_invalid_argument_error(
                    String::from_utf8_lossy(&body),
                    None,
                ));
            }

            return Err(Error::new_message_error(msg, None, None, None));
        }

        let stream = resp.bytes_stream();
        let mut streamer = RawJsonRowStreamer::new(Decoder::new(stream), "results");

        let early_meta_data =
            Self::read_early_metadata(&mut streamer, &endpoint, &statement, &client_context_id)
                .await?;

        let has_more_rows = streamer.has_more_rows().await;
        let mut epilog = None;
        if !has_more_rows {
            epilog = match streamer.epilog() {
                Ok(epilog) => Some(epilog),
                Err(e) => {
                    return Err(Error::new_http_error(
                        e,
                        endpoint,
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
            early_meta_data,
            meta_data: None,
            meta_data_error: None,
        };

        if let Some(epilog) = epilog {
            let meta = reader.read_final_metadata(epilog)?;

            reader.meta_data = Some(meta);
        }

        Ok(reader)
    }

    pub fn early_metadata(&self) -> &EarlyMetaData {
        &self.early_meta_data
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

    async fn read_early_metadata(
        streamer: &mut RawJsonRowStreamer,
        endpoint: &str,
        statement: &str,
        client_context_id: &str,
    ) -> error::Result<EarlyMetaData> {
        let prelude = streamer.read_prelude().await.map_err(|e| {
            Error::new_http_error(
                e,
                endpoint,
                statement.to_string(),
                client_context_id.to_string(),
            )
        })?;

        let early_metadata: QueryEarlyMetaData = serde_json::from_slice(&prelude).map_err(|e| {
            Error::new_message_error(
                format!("failed to parse metadata from response: {e}"),
                endpoint.to_string(),
                statement.to_string(),
                client_context_id.to_string(),
            )
        })?;

        Ok(EarlyMetaData {
            prepared: early_metadata.prepared,
            generated_statement: early_metadata.generated_statement,
        })
    }

    fn read_final_metadata(&mut self, epilog: Vec<u8>) -> error::Result<MetaData> {
        let metadata: QueryMetaData = match serde_json::from_slice(&epilog) {
            Ok(m) => m,
            Err(e) => {
                return Err(Error::new_message_error(
                    format!("failed to parse query metadata from epilog: {e}"),
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
            prepared: metadata.early_meta_data.prepared,
            request_id: metadata.request_id.unwrap_or_default(),
            client_context_id: metadata.client_context_id.unwrap_or_default(),
            status: metadata.status,
            metrics,
            signature: metadata.signature,
            warnings,
            profile: metadata.profile,
            chat_id: metadata.chat_id,
            request_tokens: metadata.request_tokens.map(Self::parse_token_usage),
            chat_tokens: metadata.chat_tokens.map(Self::parse_token_usage),
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
                mutation_count: m.mutation_count.unwrap_or_default(),
                sort_count: m.sort_count.unwrap_or_default(),
                error_count: m.error_count.unwrap_or_default(),
                warning_count: m.warning_count.unwrap_or_default(),
                natural_language_processing_time: m
                    .natural_language_processing_time
                    .and_then(|t| parse_duration_from_golang_string(&t).ok()),
                generated_statement_executed: m.generated_statement_executed,
            }
        })
    }

    fn parse_token_usage(usage: QueryTokenUsage) -> TokenUsage {
        TokenUsage {
            prompt_tokens: usage.prompt_tokens,
            completion_tokens: usage.completion_tokens,
            total_tokens: usage.total_tokens,
        }
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
                let kind = match Self::parse_error_kind(error) {
                    ServerErrorKind::Unknown if status_code == StatusCode::BAD_REQUEST => {
                        ServerErrorKind::InvalidArgument {
                            argument: None,
                            reason: error.msg.clone(),
                        }
                    }
                    kind => kind,
                };
                ErrorDesc::new(
                    kind,
                    error.code,
                    error.msg.clone(),
                    error.retry.unwrap_or_default(),
                    error.reason.clone(),
                )
            })
            .collect();

        let chosen_desc = error_descs
            .iter()
            .find(|desc| !desc.retry())
            .unwrap_or(&error_descs[0]);

        let server_error = ServerError::new(
            chosen_desc.kind().clone(),
            endpoint,
            status_code,
            chosen_desc.code(),
            chosen_desc.retry(),
            chosen_desc.message(),
        )
        .with_client_context_id(client_context_id)
        .with_statement(statement)
        .with_error_descs(error_descs);

        Error::from_server_error(server_error)
    }

    fn parse_error_kind(error: &QueryError) -> ServerErrorKind {
        let err_code = error.code;
        let err_code_group = err_code / 1000;

        if err_code_group == 4 {
            if err_code == 4040
                || err_code == 4050
                || err_code == 4060
                || err_code == 4070
                || err_code == 4080
                || err_code == 4090
            {
                ServerErrorKind::PreparedStatementFailure
            } else if err_code == 4300 {
                ServerErrorKind::IndexExists
            } else {
                ServerErrorKind::PlanningFailure
            }
        } else if err_code_group == 5 {
            let msg = error.msg.to_lowercase();
            if msg.contains("not enough") && msg.contains("replica") {
                ServerErrorKind::InvalidArgument {
                    argument: Some("num_replicas".to_string()),
                    reason: "not enough indexer nodes to create index with replica count"
                        .to_string(),
                }
            } else if msg.contains("build already in progress") {
                ServerErrorKind::BuildAlreadyInProgress
            } else if Regex::new(".*?ndex .*? already exist.*")
                .unwrap()
                .is_match(&error.msg)
            {
                ServerErrorKind::IndexExists
            } else if Regex::new(".*?ndex .*? not exist.*")
                .unwrap()
                .is_match(&error.msg)
            {
                ServerErrorKind::IndexNotFound
            } else {
                ServerErrorKind::Internal
            }
        } else if err_code_group == 12 {
            if err_code == 12003 {
                ServerErrorKind::CollectionNotFound
            } else if err_code == 12004 {
                ServerErrorKind::IndexNotFound
            } else if err_code == 12009 {
                let reason = error.reason.as_ref().and_then(Value::as_object);
                if let Some(reason) = reason.filter(|reason| !reason.is_empty()) {
                    if let Some(code) = reason.get("code") {
                        if code == 12033 {
                            ServerErrorKind::CasMismatch
                        } else if code == 17014 {
                            ServerErrorKind::DocNotFound
                        } else if code == 17012 {
                            ServerErrorKind::DocExists
                        } else {
                            ServerErrorKind::DMLFailure
                        }
                    } else {
                        ServerErrorKind::DMLFailure
                    }
                } else if error.msg.to_lowercase().contains("cas mismatch") {
                    ServerErrorKind::CasMismatch
                } else {
                    ServerErrorKind::DMLFailure
                }
            } else if err_code == 12016 {
                ServerErrorKind::IndexNotFound
            } else if err_code == 12021 {
                ServerErrorKind::ScopeNotFound
            } else {
                ServerErrorKind::IndexFailure
            }
        } else if err_code_group == 14 {
            ServerErrorKind::IndexFailure
        } else if err_code_group == 10 {
            ServerErrorKind::AuthenticationFailure
        } else if err_code == 1000 {
            ServerErrorKind::WriteInReadOnlyMode
        } else if err_code == 1080 {
            ServerErrorKind::Timeout
        } else if err_code == 3000 {
            ServerErrorKind::ParsingFailure
        } else if err_code == 13014 || err_code == 2120 {
            ServerErrorKind::AuthenticationFailure
        } else {
            ServerErrorKind::Unknown
        }
    }
}

#[cfg(test)]
mod tests {
    use super::QueryRespReader;
    use crate::queryx::error::{ErrorKind, ServerErrorKind};
    use crate::queryx::query_json::{QueryError, QueryMetaData};
    use crate::queryx::query_result::{Status, TokenUsage};
    use http::StatusCode;
    use serde_json::json;

    fn parse_errors(errors: serde_json::Value) -> crate::queryx::error::Error {
        let errors: Vec<QueryError> = serde_json::from_value(errors).unwrap();
        QueryRespReader::parse_errors(&errors, "endpoint", "statement", "ccid", StatusCode::OK)
    }

    #[test]
    fn parse_errors_with_object_reason() {
        let err = parse_errors(json!([{
            "code": 12009,
            "msg": "DML Error",
            "reason": {"code": 12033, "msg": "cas mismatch"}
        }]));

        let ErrorKind::Server(server_error) = err.kind() else {
            panic!("unexpected error: {err}");
        };
        assert_eq!(&ServerErrorKind::CasMismatch, server_error.kind());

        let desc = &server_error.all_error_descs()[0];
        assert_eq!(
            Some(&json!({"code": 12033, "msg": "cas mismatch"})),
            desc.reason()
        );
    }

    #[test]
    fn parse_errors_with_string_reason() {
        let err = parse_errors(json!([{
            "code": 19243,
            "msg": "could not resume chat",
            "reason": "chat not found"
        }]));

        let ErrorKind::Server(server_error) = err.kind() else {
            panic!("unexpected error: {err}");
        };
        assert_eq!(19243, server_error.code());

        let desc = &server_error.all_error_descs()[0];
        assert_eq!(Some(&json!("chat not found")), desc.reason());
    }

    #[test]
    fn parse_errors_without_reason() {
        let err = parse_errors(json!([{"code": 3000, "msg": "syntax error"}]));

        let ErrorKind::Server(server_error) = err.kind() else {
            panic!("unexpected error: {err}");
        };
        let desc = &server_error.all_error_descs()[0];
        assert_eq!(None, desc.reason());
    }

    async fn reader(body: serde_json::Value) -> QueryRespReader {
        let resp = http::Response::builder()
            .status(200)
            .body(serde_json::to_vec(&body).unwrap())
            .unwrap();
        QueryRespReader::new(
            reqwest::Response::from(resp).into(),
            "endpoint",
            "statement",
            "ccid",
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn conversational_response_with_results() {
        let mut reader = reader(json!({
            "requestID": "req",
            "generated_statement": "SELECT 1",
            "results": [{"a": 1}],
            "status": "success",
            "requestTokens": {"promptTokens": 1, "completionTokens": 2, "totalTokens": 3},
            "chatTokens": {"totalTokens": 30},
            "metrics": {
                "elapsedTime": "1s",
                "naturalLanguageProcessingTime": "1.5s",
                "generatedStatementExecuted": true
            }
        }))
        .await;

        // Available before any rows are read.
        assert_eq!(
            Some("SELECT 1"),
            reader.early_metadata().generated_statement.as_deref()
        );

        let rows: Vec<_> = futures::StreamExt::collect(&mut reader).await;
        assert_eq!(1, rows.len());

        let meta = reader.metadata().unwrap();
        assert_eq!(None, meta.chat_id);
        assert_eq!(
            Some(TokenUsage {
                prompt_tokens: Some(1),
                completion_tokens: Some(2),
                total_tokens: Some(3),
            }),
            meta.request_tokens
        );
        assert_eq!(Some(30), meta.chat_tokens.as_ref().unwrap().total_tokens);
        assert_eq!(None, meta.chat_tokens.as_ref().unwrap().prompt_tokens);

        let metrics = meta.metrics.as_ref().unwrap();
        assert_eq!(
            Some(std::time::Duration::from_millis(1500)),
            metrics.natural_language_processing_time
        );
        assert_eq!(Some(true), metrics.generated_statement_executed);
    }

    #[tokio::test]
    async fn conversational_response_without_results_or_signature() {
        let reader = reader(json!({
            "requestID": "req",
            "chatId": "chat-1",
            "status": "success",
            "metrics": {"elapsedTime": "1ms"}
        }))
        .await;

        // No rows, so metadata is available immediately.
        let meta = reader.metadata().unwrap();
        assert_eq!(Some("chat-1"), meta.chat_id.as_deref());
        assert!(meta.signature.is_none());
        assert_eq!(None, meta.request_tokens);

        let metrics = meta.metrics.as_ref().unwrap();
        assert_eq!(None, metrics.natural_language_processing_time);
        assert_eq!(None, metrics.generated_statement_executed);
    }

    #[tokio::test]
    async fn generated_statement_without_execution() {
        let reader = reader(json!({
            "requestID": "req",
            "generated_statement": "SELECT 2",
            "status": "success"
        }))
        .await;

        assert_eq!(
            Some("SELECT 2"),
            reader.early_metadata().generated_statement.as_deref()
        );
    }

    #[test]
    fn parse_metadata_with_unrecognized_status() {
        let metadata: QueryMetaData = serde_json::from_value(json!({
            "requestID": "abc",
            "status": "somethingnew"
        }))
        .unwrap();

        assert_eq!(Status::Unknown, metadata.status);
    }
}
