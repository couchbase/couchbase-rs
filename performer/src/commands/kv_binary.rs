use crate::commands::execution::execute_simple;
use crate::proto::protocol::{run, sdk, shared};
use couchbase::collection::BinaryCollection;
use couchbase::options::kv_binary_options::{
    AppendOptions, DecrementOptions, IncrementOptions, PrependOptions,
};
use couchbase::results::kv_binary_results::CounterResult;
use prost_types::Timestamp;
use tracing::{Instrument, Span};

pub struct AppendCommand {
    pub collection: BinaryCollection,
    pub doc_id: String,
    pub return_result: bool,
    pub initiated: Timestamp,
    pub content: Vec<u8>,
    pub options: Option<AppendOptions>,
    pub parent_span: Option<Span>,
}

impl AppendCommand {
    pub fn new(
        cb: BinaryCollection,
        doc_id: String,
        return_result: bool,
        initiated: Timestamp,
        content: Vec<u8>,
        options: Option<AppendOptions>,
        parent_span: Option<Span>,
    ) -> Self {
        Self {
            collection: cb,
            doc_id,
            return_result,
            initiated,
            content,
            options,
            parent_span,
        }
    }

    pub async fn execute(
        self,
        batcher: &crate::common::batcher::Batcher,
    ) -> crate::errors::error::Result<bool> {
        let fut = execute_simple(
            self.initiated,
            batcher,
            self.return_result,
            self.collection
                .append(&self.doc_id, &self.content, self.options.clone()),
            |res| Ok(res.into()),
        );
        match self.parent_span {
            Some(span) => fut.instrument(span).await,
            None => fut.await,
        }
    }
}

pub struct PrependCommand {
    pub collection: BinaryCollection,
    pub doc_id: String,
    pub return_result: bool,
    pub initiated: Timestamp,
    pub content: Vec<u8>,
    pub options: Option<PrependOptions>,
    pub parent_span: Option<Span>,
}

impl PrependCommand {
    pub fn new(
        cb: BinaryCollection,
        doc_id: String,
        return_result: bool,
        initiated: Timestamp,
        content: Vec<u8>,
        options: Option<PrependOptions>,
        parent_span: Option<Span>,
    ) -> Self {
        Self {
            collection: cb,
            doc_id,
            return_result,
            initiated,
            content,
            options,
            parent_span,
        }
    }

    pub async fn execute(
        self,
        batcher: &crate::common::batcher::Batcher,
    ) -> crate::errors::error::Result<bool> {
        let fut = execute_simple(
            self.initiated,
            batcher,
            self.return_result,
            self.collection
                .prepend(&self.doc_id, &self.content, self.options.clone()),
            |res| Ok(res.into()),
        );
        match self.parent_span {
            Some(span) => fut.instrument(span).await,
            None => fut.await,
        }
    }
}

pub struct IncrementCommand {
    pub collection: BinaryCollection,
    pub doc_id: String,
    pub return_result: bool,
    pub initiated: Timestamp,
    pub options: Option<IncrementOptions>,
    pub parent_span: Option<Span>,
}

impl IncrementCommand {
    pub fn new(
        cb: BinaryCollection,
        doc_id: String,
        return_result: bool,
        initiated: Timestamp,
        options: Option<IncrementOptions>,
        parent_span: Option<Span>,
    ) -> Self {
        Self {
            collection: cb,
            doc_id,
            return_result,
            initiated,
            options,
            parent_span,
        }
    }

    pub async fn execute(
        self,
        batcher: &crate::common::batcher::Batcher,
    ) -> crate::errors::error::Result<bool> {
        let fut = execute_simple(
            self.initiated,
            batcher,
            self.return_result,
            self.collection
                .increment(&self.doc_id, self.options.clone()),
            |res| Ok(res.into()),
        );
        match self.parent_span {
            Some(span) => fut.instrument(span).await,
            None => fut.await,
        }
    }
}

pub struct DecrementCommand {
    pub collection: BinaryCollection,
    pub doc_id: String,
    pub return_result: bool,
    pub initiated: Timestamp,
    pub options: Option<DecrementOptions>,
    pub parent_span: Option<Span>,
}

impl DecrementCommand {
    pub fn new(
        cb: BinaryCollection,
        doc_id: String,
        return_result: bool,
        initiated: Timestamp,
        options: Option<DecrementOptions>,
        parent_span: Option<Span>,
    ) -> Self {
        Self {
            collection: cb,
            doc_id,
            return_result,
            initiated,
            options,
            parent_span,
        }
    }

    pub async fn execute(
        self,
        batcher: &crate::common::batcher::Batcher,
    ) -> crate::errors::error::Result<bool> {
        let fut = execute_simple(
            self.initiated,
            batcher,
            self.return_result,
            self.collection
                .decrement(&self.doc_id, self.options.clone()),
            |res| Ok(res.into()),
        );
        match self.parent_span {
            Some(span) => fut.instrument(span).await,
            None => fut.await,
        }
    }
}

impl From<CounterResult> for run::result::Result {
    fn from(res: CounterResult) -> Self {
        let mut counter_result = sdk::kv::CounterResult {
            cas: res.cas() as i64,
            mutation_token: None,
            content: res.content() as i64,
        };

        if let Some(tok) = &res.mutation_token() {
            let shared_token = shared::MutationToken {
                partition_id: tok.partition_id() as i32,
                partition_uuid: tok.partition_uuid() as i64,
                sequence_number: tok.sequence_number() as i64,
                bucket_name: tok.bucket_name().to_string(),
            };
            counter_result.mutation_token = Some(shared_token);
        }

        run::result::Result::Sdk(sdk::Result {
            result: Some(sdk::result::Result::CounterResult(counter_result)),
        })
    }
}
