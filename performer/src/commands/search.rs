use crate::commands::helpers::create_run_result;
use crate::common::batcher::Batcher;
use crate::errors::error::{Error, Result};
use crate::proto::protocol::sdk::search;
use crate::proto::protocol::sdk::search::{
    streaming_search_result, SearchFacets, SearchFragments, SearchRow,
};
use crate::proto::protocol::shared::content_as::As;
use crate::proto::protocol::shared::ContentTypes;
use crate::proto::protocol::streams::config::StreamWhen;
use crate::proto::protocol::streams::{signal, Config, Created, Signal, Type};
use crate::proto::protocol::{run, sdk, shared};
use crate::streams::stream::Stream;
use crate::streams::stream_owner::StreamOwner;
use crate::translations::common::sdk_error_to_proto_run_result;
use couchbase::options::search_options::SearchOptions;
use couchbase::results::search_results::{
    SearchFacetResult, SearchMetaData, SearchResult, SearchRowLocation,
};
use couchbase::scope::Scope;
use couchbase::search::request::SearchRequest;
use prost_types::Timestamp;
use std::collections::HashMap;
use std::sync::Arc;
use std::time;
use tokio::sync::oneshot;
use tonic::codegen::tokio_stream::StreamExt;
use tracing::Instrument;

pub struct SearchCommand {
    scope: Scope,
    index_name: String,
    request: SearchRequest,
    initiated: Timestamp,
    options: Option<SearchOptions>,
    content_as: Option<As>,
    stream_config: Config,
    parent_span: Option<tracing::Span>,
}

impl SearchCommand {
    pub fn new(
        scope: Scope,
        index_name: String,
        request: SearchRequest,
        initiated: Timestamp,
        options: Option<SearchOptions>,
        content_as: Option<As>,
        stream_config: Config,
        parent_span: Option<tracing::Span>,
    ) -> Self {
        Self {
            scope,
            index_name,
            request,
            initiated,
            options,
            content_as,
            stream_config,
            parent_span,
        }
    }

    pub async fn execute(
        &self,
        batcher: &Batcher,
        stream_owner: &Arc<StreamOwner>,
        run_id: String,
    ) -> Result<bool> {
        let start = time::Instant::now();
        let res = match self.parent_span.clone() {
            Some(span) => {
                self.scope
                    .search(&self.index_name, self.request.clone(), self.options.clone())
                    .instrument(span)
                    .await
            }
            None => {
                self.scope
                    .search(&self.index_name, self.request.clone(), self.options.clone())
                    .await
            }
        };
        let end = time::Instant::now();

        let mut top_level_result = create_run_result(self.initiated, end - start);

        match res {
            Ok(ok) => {
                let stream_id = self.stream_config.stream_id.clone();
                top_level_result.result = Some(run::result::Result::Stream(Signal {
                    signal: Some(signal::Signal::Created(Created {
                        stream_id: stream_id.clone(),
                        r#type: Type::StreamFullTextSearch as i32,
                    })),
                }));

                batcher.send(top_level_result).await;

                let mut search_stream = SearchStream::new(
                    ok,
                    self.content_as,
                    run_id,
                    stream_id.clone(),
                    batcher.clone(),
                );

                match self.stream_config.stream_when.unwrap() {
                    StreamWhen::Automatically(_) => {
                        while let Some(res) = search_stream.next_row().await? {
                            batcher.send(res).await;
                        }

                        let facets = search_stream.facets();
                        batcher.send(facets).await;
                        let metadata = search_stream.metadata();
                        batcher.send(metadata).await;
                        Ok(true)
                    }
                    StreamWhen::OnDemand(_) => {
                        stream_owner
                            .add(stream_id.clone(), Stream::Search(search_stream))
                            .await;
                        Ok(true)
                    }
                }
            }
            Err(err) => {
                top_level_result.result = Some(sdk_error_to_proto_run_result(err));
                batcher.send(top_level_result).await;
                Ok(false)
            }
        }
    }
}
// Result

pub struct SearchStream {
    result: SearchResult,
    content_as: Option<As>,
    run_id: String,
    stream_id: String,
    batcher: Batcher,

    complete_tx: Option<oneshot::Sender<()>>,
    complete_rx: oneshot::Receiver<()>,

    facets_sent: bool,
    metadata_sent: bool,
}

impl SearchStream {
    fn new(
        result: SearchResult,
        content_as: Option<As>,
        run_id: String,
        stream_id: String,
        batcher: Batcher,
    ) -> Self {
        let (tx, rx) = oneshot::channel();

        Self {
            result,
            content_as,
            run_id,
            stream_id,
            batcher,
            complete_tx: Some(tx),
            complete_rx: rx,
            facets_sent: false,
            metadata_sent: false,
        }
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn completion(&mut self) -> &mut oneshot::Receiver<()> {
        &mut self.complete_rx
    }

    pub fn finish(&mut self) {
        if let Some(tx) = self.complete_tx.take() {
            let _ = tx.send(());
        }
    }

    pub async fn request_items(&mut self, num_items: i32) -> Result<()> {
        for _ in 0..num_items {
            match self.next_item().await? {
                Some(item) => self.batcher.send(item).await,
                None => break,
            }
        }
        Ok(())
    }

    pub async fn next_item(&mut self) -> Result<Option<run::Result>> {
        if let Some(row) = self.next_row().await? {
            return Ok(Some(row));
        }

        if !self.facets_sent {
            self.facets_sent = true;
            return Ok(Some(self.facets()));
        }

        if !self.metadata_sent {
            self.metadata_sent = true;
            return Ok(Some(self.metadata()));
        }

        self.finish();
        Ok(None)
    }

    pub async fn next_row(&mut self) -> Result<Option<run::Result>> {
        let search_row = self.result.rows().next().await;
        match search_row {
            Some(row) => {
                let mut run_result = run::Result {
                    elapsed_nanos: 0,
                    initiated: None,
                    result: None,
                };
                match row {
                    Ok(r) => match self.run_result_from_search_row(r) {
                        Ok(res) => run_result.result = Some(res),
                        Err(err) => {
                            return match *err {
                                Error::Status(_) => Err(err),
                                Error::Sdk(e) => Ok(Some(*e)),
                            }
                        }
                    },
                    Err(e) => run_result.result = Some(sdk_error_to_proto_run_result(e)),
                }
                Ok(Some(run_result))
            }
            None => Ok(None),
        }
    }

    pub fn facets(&self) -> run::Result {
        let mut run_result = run::Result {
            elapsed_nanos: 0,
            initiated: None,
            result: None,
        };
        match self.result.facets() {
            Ok(f) => run_result.result = Some(self.run_result_from_facets(f)),
            Err(e) => run_result.result = Some(sdk_error_to_proto_run_result(e)),
        }
        run_result
    }

    pub fn metadata(&self) -> run::Result {
        let mut run_result = run::Result {
            elapsed_nanos: 0,
            initiated: None,
            result: None,
        };
        match self.result.metadata() {
            Ok(m) => run_result.result = Some(self.run_result_from_metadata(m)),
            Err(e) => run_result.result = Some(sdk_error_to_proto_run_result(e)),
        }
        run_result
    }

    fn run_result_from_facets(
        &self,
        facets: HashMap<&String, SearchFacetResult<'_>>,
    ) -> run::result::Result {
        run::result::Result::Sdk(sdk::Result {
            result: Some(sdk::result::Result::SearchStreamingResult(
                search::StreamingSearchResult {
                    stream_id: self.stream_id.clone(),
                    result: Some(streaming_search_result::Result::Facets(SearchFacets {
                        facets: facets
                            .iter()
                            .map(|(key, value)| {
                                let proto = sdk::search::SearchFacetResult {
                                    name: value.name.to_string(),
                                    field: value.field.to_string(),
                                    total: value.total,
                                    missing: value.missing,
                                    other: value.other,
                                };
                                (key.to_string(), proto)
                            })
                            .collect(),
                    })),
                },
            )),
        })
    }

    fn run_result_from_metadata(&self, metadata: SearchMetaData<'_>) -> run::result::Result {
        run::result::Result::Sdk(sdk::Result {
            result: Some(sdk::result::Result::SearchStreamingResult(
                search::StreamingSearchResult {
                    stream_id: self.stream_id.clone(),
                    result: Some(streaming_search_result::Result::MetaData(
                        search::SearchMetaData {
                            metrics: Some(search::SearchMetrics {
                                took_msec: metadata.metrics.took.as_millis() as i64,
                                total_rows: metadata.metrics.total_hits as i64,
                                max_score: metadata.metrics.max_score,
                                total_partition_count: metadata.metrics.total_partition_count
                                    as i64,
                                success_partition_count: metadata.metrics.successful_partition_count
                                    as i64,
                                error_partition_count: metadata.metrics.failed_partition_count
                                    as i64,
                            }),
                            errors: metadata.errors.clone(),
                        },
                    )),
                },
            )),
        })
    }

    fn run_result_from_search_row(
        &self,
        row: couchbase::results::search_results::SearchRow,
    ) -> Result<run::result::Result> {
        let fields = self
            .content_as
            .as_ref()
            .map(|as_| fields_to_content(row.clone(), *as_))
            .transpose()?;

        Ok(run::result::Result::Sdk(sdk::Result {
            result: Some(sdk::result::Result::SearchStreamingResult(
                search::StreamingSearchResult {
                    stream_id: self.stream_id.clone(),
                    result: Some(streaming_search_result::Result::Row(SearchRow {
                        fields,
                        index: row.index,
                        id: row.id,
                        score: row.score,
                        explanation: row
                            .explanation
                            .as_ref()
                            .map(|e| e.get().as_bytes().to_vec())
                            .unwrap_or_default(),
                        locations: row
                            .locations
                            .as_ref()
                            .map(|locs| locs.get_all().iter().cloned().map(Into::into).collect())
                            .unwrap_or_default(),
                        fragments: row
                            .fragments
                            .unwrap_or_default()
                            .into_iter()
                            .map(|(k, v)| (k, SearchFragments { fragments: v }))
                            .collect(),
                    })),
                },
            )),
        }))
    }
}

impl Drop for SearchStream {
    fn drop(&mut self) {
        self.finish();
    }
}

fn fields_to_content(
    row: couchbase::results::search_results::SearchRow,
    content_as: As,
) -> Result<ContentTypes> {
    match content_as {
        As::AsString(_) => {
            let content = row.fields::<String>()?;
            Ok(ContentTypes {
                content: Some(shared::content_types::Content::ContentAsString(content)),
            })
        }
        As::AsByteArray(_) => {
            let content = row.fields::<Vec<u8>>()?;
            Ok(ContentTypes {
                content: Some(shared::content_types::Content::ContentAsBytes(content)),
            })
        }
        As::AsJsonObject(_) | As::AsJsonArray(_) => {
            let content = row.fields::<serde_json::Value>()?;
            let bytes = serde_json::to_vec(&content)
                .map_err(|e| Error::internal(format!("Failed to serialize JSON value: {e}")))?;
            Ok(ContentTypes {
                content: Some(shared::content_types::Content::ContentAsBytes(bytes)),
            })
        }
        As::AsBoolean(_) => {
            let content = row.fields::<bool>()?;
            Ok(ContentTypes {
                content: Some(shared::content_types::Content::ContentAsBool(content)),
            })
        }
        As::AsInteger(_) => {
            let content = row.fields::<i64>()?;
            Ok(ContentTypes {
                content: Some(shared::content_types::Content::ContentAsInt64(content)),
            })
        }
        As::AsFloatingPoint(_) => {
            let content = row.fields::<f64>()?;
            Ok(ContentTypes {
                content: Some(shared::content_types::Content::ContentAsDouble(content)),
            })
        }
    }
}

impl From<SearchRowLocation> for search::SearchRowLocation {
    fn from(value: SearchRowLocation) -> Self {
        search::SearchRowLocation {
            field: value.field,
            term: value.term,
            position: value.position,
            start: value.start,
            end: value.end,
            array_positions: value.array_positions.unwrap_or_default(),
        }
    }
}
