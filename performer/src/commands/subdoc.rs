use crate::commands::execution::execute_simple;
use crate::errors::error;
use crate::proto::protocol::sdk::kv::lookup_in::{
    boolean_or_error, BooleanOrError, LookupInSpecResult,
};
use crate::proto::protocol::sdk::kv::mutate_in::MutateInSpecResult;
use crate::proto::protocol::shared::content_as::As;
use crate::proto::protocol::shared::{content_or_error, ContentOrError, ContentTypes};
use crate::proto::protocol::{run, sdk, shared};
use crate::translations::common::sdk_error_to_proto_exception;
use couchbase::collection::Collection;
use couchbase::options::kv_options::{LookupInOptions, MutateInOptions};
use couchbase::results::kv_results::{LookupInResult, MutateInResult};
use couchbase::subdoc::lookup_in_specs::LookupInSpec;
use couchbase::subdoc::mutate_in_specs::MutateInSpec;
use prost::bytes::Bytes;
use prost_types::Timestamp;
use serde_json::Value;
use tracing::{Instrument, Span};

pub struct LookupInCommand {
    pub collection: Collection,
    pub doc_id: String,
    pub return_result: bool,
    pub initiated: Timestamp,
    pub specs: Vec<LookupInSpec>,
    pub contents_as: Vec<As>,
    pub options: Option<LookupInOptions>,
    pub parent_span: Option<Span>,
}

impl LookupInCommand {
    pub fn new(
        cb: Collection,
        doc_id: String,
        return_result: bool,
        initiated: Timestamp,
        specs: Vec<LookupInSpec>,
        contents_as: Vec<As>,
        options: Option<LookupInOptions>,
        parent_span: Option<Span>,
    ) -> Self {
        Self {
            collection: cb,
            doc_id,
            return_result,
            initiated,
            specs,
            contents_as,
            options,
            parent_span,
        }
    }

    pub async fn execute(self, batcher: &crate::common::batcher::Batcher) -> error::Result<bool> {
        let fut = execute_simple(
            self.initiated,
            batcher,
            self.return_result,
            self.collection
                .lookup_in(&self.doc_id, &self.specs, self.options),
            |res| run_result_from_lookup_in_result(self.contents_as, res),
        );
        match self.parent_span {
            Some(span) => fut.instrument(span).await,
            None => fut.await,
        }
    }
}

pub struct MutateInCommand {
    pub collection: Collection,
    pub doc_id: String,
    pub return_result: bool,
    pub initiated: Timestamp,
    pub specs: Vec<MutateInSpec>,
    pub contents_as: Vec<Option<As>>,
    pub options: Option<MutateInOptions>,
    pub parent_span: Option<Span>,
}

impl MutateInCommand {
    pub fn new(
        cb: Collection,
        doc_id: String,
        return_result: bool,
        initiated: Timestamp,
        specs: Vec<MutateInSpec>,
        contents_as: Vec<Option<As>>,
        options: Option<MutateInOptions>,
        parent_span: Option<Span>,
    ) -> Self {
        Self {
            collection: cb,
            doc_id,
            return_result,
            initiated,
            specs,
            contents_as,
            options,
            parent_span,
        }
    }

    pub async fn execute(self, batcher: &crate::common::batcher::Batcher) -> error::Result<bool> {
        let fut = execute_simple(
            self.initiated,
            batcher,
            self.return_result,
            self.collection
                .mutate_in(&self.doc_id, &self.specs, self.options),
            |res| run_result_from_mutate_in_result(self.contents_as, res),
        );
        match self.parent_span {
            Some(span) => fut.instrument(span).await,
            None => fut.await,
        }
    }
}

fn run_result_from_mutate_in_result(
    contents_as: Vec<Option<As>>,
    res: MutateInResult,
) -> error::Result<run::result::Result> {
    let mut results = vec![];
    for (i, content_as) in contents_as.into_iter().enumerate() {
        let content_as_result = content_as.map(|ca| match decode(&ca, i, &res) {
            Ok(c) => ContentOrError {
                result: Some(content_or_error::Result::Content(c)),
            },
            Err(e) => ContentOrError {
                result: Some(content_or_error::Result::Exception(shared::Exception {
                    exception: Some(sdk_error_to_proto_exception(e)),
                })),
            },
        });

        results.push(MutateInSpecResult { content_as_result });
    }

    let mutation_token = res.mutation_token().map(|tok| shared::MutationToken {
        partition_id: tok.partition_id() as i32,
        partition_uuid: tok.partition_uuid() as i64,
        sequence_number: tok.sequence_number() as i64,
        bucket_name: tok.bucket_name().to_string(),
    });

    Ok(run::result::Result::Sdk(sdk::Result {
        result: Some(sdk::result::Result::MutateInResult(
            sdk::kv::mutate_in::MutateInResult {
                cas: res.cas() as i64,
                mutation_token,
                results,
            },
        )),
    }))
}

fn run_result_from_lookup_in_result(
    contents_as: Vec<As>,
    res: LookupInResult,
) -> error::Result<run::result::Result> {
    let mut results = vec![];
    for (i, content_as) in contents_as.into_iter().enumerate() {
        let exists = res.exists(i);

        let exists_result = match exists {
            Ok(e) => BooleanOrError {
                result: Some(boolean_or_error::Result::Value(e)),
            },
            Err(e) => BooleanOrError {
                result: Some(boolean_or_error::Result::Exception(shared::Exception {
                    exception: Some(sdk_error_to_proto_exception(e)),
                })),
            },
        };

        let content_result = match decode(&content_as, i, &res) {
            Ok(c) => ContentOrError {
                result: Some(content_or_error::Result::Content(c)),
            },
            Err(e) => ContentOrError {
                result: Some(content_or_error::Result::Exception(shared::Exception {
                    exception: Some(sdk_error_to_proto_exception(e)),
                })),
            },
        };

        results.push(LookupInSpecResult {
            content_as_result: Some(content_result),
            exists_result: Some(exists_result),
        });
    }

    Ok(run::result::Result::Sdk(sdk::Result {
        result: Some(sdk::result::Result::LookupInResult(
            sdk::kv::lookup_in::LookupInResult {
                cas: res.cas() as i64,
                results,
            },
        )),
    }))
}

fn decode<R>(
    content_as: &As,
    index: usize,
    result: R,
) -> Result<ContentTypes, couchbase::error::Error>
where
    R: ContentAs,
{
    let content = match content_as {
        As::AsString(_) => {
            let res = result.content::<String>(index)?;
            shared::content_types::Content::ContentAsString(res)
        }
        As::AsByteArray(_) => {
            let res = result.content_raw(index)?;
            shared::content_types::Content::ContentAsBytes(res.to_vec())
        }
        As::AsJsonObject(_) | As::AsJsonArray(_) => {
            let json = result.content::<Value>(index)?;
            let bytes = serde_json::to_vec(&json).unwrap();
            shared::content_types::Content::ContentAsBytes(bytes)
        }
        As::AsBoolean(_) => {
            let res = result.content::<bool>(index)?;
            shared::content_types::Content::ContentAsBool(res)
        }
        As::AsInteger(_) => {
            let res = result.content::<i64>(index)?;
            shared::content_types::Content::ContentAsInt64(res)
        }
        As::AsFloatingPoint(_) => {
            let res = result.content::<f64>(index)?;
            shared::content_types::Content::ContentAsDouble(res)
        }
    };

    Ok(ContentTypes {
        content: Some(content),
    })
}

trait ContentAs {
    fn content<T: serde::de::DeserializeOwned>(
        &self,
        index: usize,
    ) -> Result<T, couchbase::error::Error>;

    fn content_raw(&self, mutate_index: usize) -> couchbase::error::Result<Bytes>;
}

impl ContentAs for &LookupInResult {
    fn content<T: serde::de::DeserializeOwned>(
        &self,
        index: usize,
    ) -> Result<T, couchbase::error::Error> {
        self.content_as(index)
    }
    fn content_raw(&self, mutate_index: usize) -> couchbase::error::Result<Bytes> {
        self.content_as_raw(mutate_index)
    }
}

impl ContentAs for &MutateInResult {
    fn content<T: serde::de::DeserializeOwned>(
        &self,
        index: usize,
    ) -> Result<T, couchbase::error::Error> {
        self.content_as(index)
    }

    fn content_raw(&self, mutate_index: usize) -> couchbase::error::Result<Bytes> {
        self.content_as_raw(mutate_index)
    }
}
