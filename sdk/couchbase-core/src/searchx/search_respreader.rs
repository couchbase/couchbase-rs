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

use crate::httpx;
use crate::httpx::decoder::Decoder;
use crate::httpx::raw_json_row_streamer::{RawJsonRowItem, RawJsonRowStreamer};
use crate::httpx::response::Response;
use crate::searchx::error::{ErrorKind, ServerError, ServerErrorKind};
use crate::searchx::search::{decode_common_error, Search};
use crate::searchx::search_result::{FacetResult, MetaData, Metrics, ResultHit};
use crate::searchx::{error, search_json};
use bytes::Bytes;
use futures::{FutureExt, Stream, StreamExt};
use http::StatusCode;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tracing::debug;

pub struct SearchRespReader {
    endpoint: String,
    status_code: StatusCode,

    index_name: String,
    streamer: Pin<Box<dyn Stream<Item = httpx::error::Result<RawJsonRowItem>> + Send>>,
    meta_data: Option<MetaData>,
    epilogue_error: Option<error::Error>,
    facets: Option<HashMap<String, FacetResult>>,
}

impl Stream for SearchRespReader {
    type Item = error::Result<ResultHit>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match this.streamer.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(RawJsonRowItem::Row(row_data)))) => {
                let row: search_json::Row = match serde_json::from_slice(&row_data).map_err(|e| {
                    error::Error::new_message_error(
                        format!("failed to parse row: {}", e),
                        this.endpoint.clone(),
                    )
                }) {
                    Ok(row) => row,
                    Err(e) => return Poll::Ready(Some(Err(e))),
                };

                Poll::Ready(Some(Ok(ResultHit::from(row))))
            }
            Poll::Ready(Some(Ok(RawJsonRowItem::Metadata(metadata)))) => {
                match this.read_final_metadata(metadata) {
                    Ok((meta, facets)) => {
                        this.meta_data = Some(meta);
                        this.facets = Some(facets);
                    }
                    Err(e) => {
                        this.epilogue_error = Some(e.clone());
                        return Poll::Ready(Some(Err(e)));
                    }
                }
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(e))) => {
                Poll::Ready(Some(Err(error::Error::new_http_error(e, &this.endpoint))))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl SearchRespReader {
    pub async fn new(
        resp: Response,
        index_name: impl Into<String>,
        endpoint: impl Into<String>,
    ) -> error::Result<Self> {
        let endpoint = endpoint.into();
        let index_name = index_name.into();

        let status_code = resp.status();
        if status_code != 200 {
            let body = match resp.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    debug!("Failed to read response body on error {e}");
                    return Err(error::Error::new_http_error(e, endpoint));
                }
            };

            let err: search_json::ErrorResponse = match serde_json::from_slice(&body) {
                Ok(e) => e,
                Err(e) => {
                    return Err(error::Error::new_message_error(
                        format!(
                        "non-200 status code received {status_code} but parsing error response body failed {e}"
                    ),
                        endpoint,
                    ));
                }
            };

            return Err(decode_common_error(
                index_name,
                status_code,
                &err.error,
                endpoint,
            ));
        }

        let stream = resp.bytes_stream();
        let mut streamer = RawJsonRowStreamer::new(Decoder::new(stream), "hits");

        match streamer.read_prelude().await {
            Ok(_) => {}
            Err(e) => {
                return Err(error::Error::new_http_error(e, endpoint));
            }
        };

        let has_more_rows = streamer.has_more_rows().await;
        let mut epilog = None;
        if !has_more_rows {
            epilog = match streamer.epilog() {
                Ok(epilog) => Some(epilog),
                Err(e) => {
                    return Err(error::Error::new_http_error(e, endpoint));
                }
            };
        }

        let mut reader = Self {
            endpoint,
            status_code,
            index_name,
            streamer: Box::pin(streamer.into_stream()),
            meta_data: None,
            facets: None,
            epilogue_error: None,
        };

        if let Some(epilog) = epilog {
            let (meta, facets) = reader.read_final_metadata(epilog)?;

            reader.meta_data = Some(meta);
            reader.facets = Some(facets);
        }

        Ok(reader)
    }

    pub fn metadata(&self) -> error::Result<&MetaData> {
        if let Some(e) = &self.epilogue_error {
            return Err(e.clone());
        }

        if let Some(meta) = &self.meta_data {
            return Ok(meta);
        }

        Err(error::Error::new_message_error(
            "cannot read meta-data until after all rows are read",
            None,
        ))
    }

    pub fn facets(&self) -> error::Result<&HashMap<String, FacetResult>> {
        if let Some(e) = &self.epilogue_error {
            return Err(e.clone());
        }

        if let Some(facets) = &self.facets {
            return Ok(facets);
        }

        Err(error::Error::new_message_error(
            "cannot read facets until after all rows are read",
            None,
        ))
    }

    fn read_final_metadata(
        &mut self,
        epilog: Vec<u8>,
    ) -> error::Result<(MetaData, HashMap<String, FacetResult>)> {
        let metadata_json: search_json::SearchMetaData = match serde_json::from_slice(&epilog) {
            Ok(m) => m,
            Err(e) => {
                return Err(error::Error::new_message_error(
                    format!("failed to parse metadata: {}", e),
                    self.endpoint.clone(),
                ));
            }
        };

        let metadata = MetaData {
            errors: metadata_json.status.errors,
            metrics: Metrics {
                failed_partition_count: metadata_json.status.failed,
                max_score: metadata_json.max_score,
                successful_partition_count: metadata_json.status.successful,
                took: Duration::from_nanos(metadata_json.took),
                total_hits: metadata_json.total_hits,
                total_partition_count: metadata_json.status.total,
            },
        };

        let mut facets: HashMap<String, FacetResult> = HashMap::new();
        if let Some(resp_facets) = metadata_json.facets {
            for (facet_name, facet_data) in resp_facets {
                facets.insert(facet_name, facet_data.try_into()?);
            }
        }

        Ok((metadata, facets))
    }
}
