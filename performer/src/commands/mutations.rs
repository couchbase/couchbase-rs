use crate::commands::kv::{InsertCommand, ReplaceCommand, UpsertCommand};
use crate::common::content::{SdkContent, Transcoder};
use crate::proto::protocol::{run, sdk, shared};
use couchbase::results::kv_results::MutationResult;
use prost_types::Timestamp;
use serde::Serialize;

pub trait MutationCommand {
    type Options: Clone;

    fn doc_id(&self) -> &str;
    fn return_result(&self) -> bool;
    fn initiated(&self) -> Timestamp;
    fn content(&self) -> &SdkContent;
    fn transcoder(&self) -> &Option<Transcoder>;
    fn options(&self) -> Option<Self::Options>;

    async fn execute_operation<V: Serialize>(
        &self,
        doc_id: impl AsRef<str>,
        content: V,
        options: Option<Self::Options>,
    ) -> couchbase::error::Result<MutationResult>;

    async fn execute_raw_operation(
        &self,
        doc_id: &str,
        content: &[u8],
        flags: u32,
        options: Option<Self::Options>,
    ) -> couchbase::error::Result<MutationResult>;
}

impl MutationCommand for InsertCommand {
    type Options = couchbase::options::kv_options::InsertOptions;

    fn doc_id(&self) -> &str {
        &self.doc_id
    }
    fn return_result(&self) -> bool {
        self.return_result
    }
    fn initiated(&self) -> Timestamp {
        self.initiated
    }
    fn content(&self) -> &SdkContent {
        &self.content
    }
    fn transcoder(&self) -> &Option<Transcoder> {
        &self.transcoder
    }
    fn options(&self) -> Option<Self::Options> {
        self.options.clone()
    }

    async fn execute_operation<V: Serialize>(
        &self,
        doc_id: impl AsRef<str>,
        content: V,
        options: Option<Self::Options>,
    ) -> couchbase::error::Result<MutationResult> {
        self.collection.insert(doc_id, content, options).await
    }

    async fn execute_raw_operation(
        &self,
        doc_id: &str,
        content: &[u8],
        flags: u32,
        options: Option<Self::Options>,
    ) -> couchbase::error::Result<MutationResult> {
        self.collection
            .insert_raw(doc_id, content, flags, options)
            .await
    }
}

impl MutationCommand for ReplaceCommand {
    type Options = couchbase::options::kv_options::ReplaceOptions;

    fn doc_id(&self) -> &str {
        &self.doc_id
    }
    fn return_result(&self) -> bool {
        self.return_result
    }
    fn initiated(&self) -> Timestamp {
        self.initiated
    }
    fn content(&self) -> &SdkContent {
        &self.content
    }
    fn transcoder(&self) -> &Option<Transcoder> {
        &self.transcoder
    }
    fn options(&self) -> Option<Self::Options> {
        self.options.clone()
    }

    async fn execute_operation<V: Serialize>(
        &self,
        doc_id: impl AsRef<str>,
        content: V,
        options: Option<Self::Options>,
    ) -> couchbase::error::Result<MutationResult> {
        self.collection.replace(doc_id, content, options).await
    }

    async fn execute_raw_operation(
        &self,
        doc_id: &str,
        content: &[u8],
        flags: u32,
        options: Option<Self::Options>,
    ) -> couchbase::error::Result<MutationResult> {
        self.collection
            .replace_raw(doc_id, content, flags, options)
            .await
    }
}

impl MutationCommand for UpsertCommand {
    type Options = couchbase::options::kv_options::UpsertOptions;

    fn doc_id(&self) -> &str {
        &self.doc_id
    }
    fn return_result(&self) -> bool {
        self.return_result
    }
    fn initiated(&self) -> Timestamp {
        self.initiated
    }
    fn content(&self) -> &SdkContent {
        &self.content
    }
    fn transcoder(&self) -> &Option<Transcoder> {
        &self.transcoder
    }
    fn options(&self) -> Option<Self::Options> {
        self.options.clone()
    }

    async fn execute_operation<V: Serialize>(
        &self,
        doc_id: impl AsRef<str>,
        content: V,
        options: Option<Self::Options>,
    ) -> couchbase::error::Result<MutationResult> {
        self.collection.upsert(doc_id, content, options).await
    }

    async fn execute_raw_operation(
        &self,
        doc_id: &str,
        content: &[u8],
        flags: u32,
        options: Option<Self::Options>,
    ) -> couchbase::error::Result<MutationResult> {
        self.collection
            .upsert_raw(doc_id, content, flags, options)
            .await
    }
}

impl From<MutationResult> for run::result::Result {
    fn from(res: MutationResult) -> Self {
        let mut mutation_result = sdk::kv::MutationResult {
            cas: res.cas() as i64,
            mutation_token: None,
        };

        if let Some(tok) = &res.mutation_token() {
            let shared_token = shared::MutationToken {
                partition_id: tok.partition_id() as i32,
                partition_uuid: tok.partition_uuid() as i64,
                sequence_number: tok.sequence_number() as i64,
                bucket_name: tok.bucket_name().to_string(),
            };
            mutation_result.mutation_token = Some(shared_token);
        }

        run::result::Result::Sdk(sdk::Result {
            result: Some(sdk::result::Result::MutationResult(mutation_result)),
        })
    }
}
