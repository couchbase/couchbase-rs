use crate::commands::execution::{execute_mutation, execute_simple};
use crate::commands::helpers::create_success_sdk_result;
use crate::commands::kv_binary::{
    AppendCommand, DecrementCommand, IncrementCommand, PrependCommand,
};
use crate::commands::subdoc::{LookupInCommand, MutateInCommand};
use crate::common::content::{parse_content_as, SdkContent, Transcoder};
use crate::errors::error::Result;
use crate::proto::protocol::shared::content_as::As;
use crate::proto::protocol::{run, sdk};
use couchbase::collection::Collection;
use couchbase::options::kv_options::{
    ExistsOptions, GetAndLockOptions, GetAndTouchOptions, GetOptions, InsertOptions, RemoveOptions,
    ReplaceOptions, TouchOptions, UnlockOptions, UpsertOptions,
};
use couchbase::results::kv_results::GetResult;
use prost_types::Timestamp;
use std::time::Duration;
use tracing::{Instrument, Span};

pub enum KvCommand {
    Get(GetCommand),
    Insert(InsertCommand),
    Replace(ReplaceCommand),
    Upsert(UpsertCommand),
    Remove(RemoveCommand),
    GetAndLock(GetAndLockCommand),
    GetAndTouch(GetAndTouchCommand),
    Unlock(UnlockCommand),
    Exists(ExistsCommand),
    Touch(TouchCommand),
    Append(AppendCommand),
    Prepend(PrependCommand),
    Increment(IncrementCommand),
    Decrement(DecrementCommand),
    LookupIn(LookupInCommand),
    MutateIn(MutateInCommand),
}

impl KvCommand {
    pub async fn execute(self, batcher: &crate::common::batcher::Batcher) -> Result<bool> {
        match self {
            KvCommand::Get(cmd) => cmd.execute(batcher).await,
            KvCommand::Insert(cmd) => cmd.execute(batcher).await,
            KvCommand::Replace(cmd) => cmd.execute(batcher).await,
            KvCommand::Upsert(cmd) => cmd.execute(batcher).await,
            KvCommand::Remove(cmd) => cmd.execute(batcher).await,
            KvCommand::GetAndLock(cmd) => cmd.execute(batcher).await,
            KvCommand::GetAndTouch(cmd) => cmd.execute(batcher).await,
            KvCommand::Unlock(cmd) => cmd.execute(batcher).await,
            KvCommand::Exists(cmd) => cmd.execute(batcher).await,
            KvCommand::Touch(cmd) => cmd.execute(batcher).await,
            KvCommand::Append(cmd) => cmd.execute(batcher).await,
            KvCommand::Prepend(cmd) => cmd.execute(batcher).await,
            KvCommand::Increment(cmd) => cmd.execute(batcher).await,
            KvCommand::Decrement(cmd) => cmd.execute(batcher).await,
            KvCommand::LookupIn(cmd) => cmd.execute(batcher).await,
            KvCommand::MutateIn(cmd) => cmd.execute(batcher).await,
        }
    }
}

pub struct GetCommand {
    collection: Collection,
    doc_id: String,
    return_result: bool,
    initiated: Timestamp,
    content_as: Option<As>,
    transcoder: Option<Transcoder>,
    options: Option<GetOptions>,
    parent_span: Option<Span>,
}

impl GetCommand {
    pub fn new(
        cb: Collection,
        doc_id: String,
        return_result: bool,
        initiated: Timestamp,
        content_as: Option<As>,
        transcoder: Option<Transcoder>,
        options: Option<GetOptions>,
        parent_span: Option<Span>,
    ) -> Self {
        GetCommand {
            collection: cb,
            doc_id,
            return_result,
            initiated,
            content_as,
            transcoder,
            options,
            parent_span,
        }
    }

    pub async fn execute(self, batcher: &crate::common::batcher::Batcher) -> Result<bool> {
        let fut = execute_simple(
            self.initiated,
            batcher,
            self.return_result,
            self.collection.get(&self.doc_id, self.options.clone()),
            |res| run_result_from_get_result(&self.content_as.unwrap(), &self.transcoder, &res),
        );

        match self.parent_span {
            Some(span) => fut.instrument(span).await,
            None => fut.await,
        }
    }
}

pub struct InsertCommand {
    pub collection: Collection,
    pub doc_id: String,
    pub return_result: bool,
    pub initiated: Timestamp,
    pub content: SdkContent,
    pub transcoder: Option<Transcoder>,
    pub options: Option<InsertOptions>,
    pub parent_span: Option<Span>,
}

impl InsertCommand {
    pub fn new(
        cb: Collection,
        doc_id: String,
        return_result: bool,
        initiated: Timestamp,
        content: SdkContent,
        transcoder: Option<Transcoder>,
        options: Option<InsertOptions>,
        parent_span: Option<Span>,
    ) -> Self {
        Self {
            collection: cb,
            doc_id,
            return_result,
            initiated,
            content,
            transcoder,
            options,
            parent_span,
        }
    }

    pub async fn execute(mut self, batcher: &crate::common::batcher::Batcher) -> Result<bool> {
        let parent_span = self.parent_span.take();
        let fut = execute_mutation(self, batcher);
        match parent_span {
            Some(span) => fut.instrument(span).await,
            None => fut.await,
        }
    }
}

pub struct ReplaceCommand {
    pub collection: Collection,
    pub doc_id: String,
    pub return_result: bool,
    pub initiated: Timestamp,
    pub content: SdkContent,
    pub transcoder: Option<Transcoder>,
    pub options: Option<ReplaceOptions>,
    pub parent_span: Option<Span>,
}

impl ReplaceCommand {
    pub fn new(
        cb: Collection,
        doc_id: String,
        return_result: bool,
        initiated: Timestamp,
        content: SdkContent,
        transcoder: Option<Transcoder>,
        options: Option<ReplaceOptions>,
        parent_span: Option<Span>,
    ) -> Self {
        Self {
            collection: cb,
            doc_id,
            return_result,
            initiated,
            content,
            transcoder,
            options,
            parent_span,
        }
    }

    pub async fn execute(mut self, batcher: &crate::common::batcher::Batcher) -> Result<bool> {
        let parent_span = self.parent_span.take();
        let fut = execute_mutation(self, batcher);
        match parent_span {
            Some(span) => fut.instrument(span).await,
            None => fut.await,
        }
    }
}

pub struct UpsertCommand {
    pub collection: Collection,
    pub doc_id: String,
    pub return_result: bool,
    pub initiated: Timestamp,
    pub content: SdkContent,
    pub transcoder: Option<Transcoder>,
    pub options: Option<UpsertOptions>,
    pub parent_span: Option<Span>,
}

impl UpsertCommand {
    pub fn new(
        cb: Collection,
        doc_id: String,
        return_result: bool,
        initiated: Timestamp,
        content: SdkContent,
        transcoder: Option<Transcoder>,
        options: Option<UpsertOptions>,
        parent_span: Option<Span>,
    ) -> Self {
        Self {
            collection: cb,
            doc_id,
            return_result,
            initiated,
            content,
            transcoder,
            options,
            parent_span,
        }
    }

    pub async fn execute(mut self, batcher: &crate::common::batcher::Batcher) -> Result<bool> {
        let parent_span = self.parent_span.take();
        let fut = execute_mutation(self, batcher);
        match parent_span {
            Some(span) => fut.instrument(span).await,
            None => fut.await,
        }
    }
}

pub struct RemoveCommand {
    pub collection: Collection,
    pub doc_id: String,
    pub return_result: bool,
    pub initiated: Timestamp,
    pub options: Option<RemoveOptions>,
    pub parent_span: Option<Span>,
}

impl RemoveCommand {
    pub fn new(
        cb: Collection,
        doc_id: String,
        return_result: bool,
        initiated: Timestamp,
        options: Option<RemoveOptions>,
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

    pub async fn execute(self, batcher: &crate::common::batcher::Batcher) -> Result<bool> {
        let fut = execute_simple(
            self.initiated,
            batcher,
            self.return_result,
            self.collection.remove(&self.doc_id, self.options.clone()),
            |res| Ok(res.into()),
        );
        match self.parent_span {
            Some(span) => fut.instrument(span).await,
            None => fut.await,
        }
    }
}

pub struct GetAndLockCommand {
    collection: Collection,
    doc_id: String,
    duration: Duration,
    return_result: bool,
    initiated: Timestamp,
    content_as: Option<As>,
    transcoder: Option<Transcoder>,
    options: Option<GetAndLockOptions>,
    parent_span: Option<Span>,
}

impl GetAndLockCommand {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cb: Collection,
        doc_id: String,
        duration: Duration,
        return_result: bool,
        initiated: Timestamp,
        content_as: Option<As>,
        transcoder: Option<Transcoder>,
        options: Option<GetAndLockOptions>,
        parent_span: Option<Span>,
    ) -> Self {
        GetAndLockCommand {
            collection: cb,
            doc_id,
            duration,
            return_result,
            initiated,
            content_as,
            transcoder,
            options,
            parent_span,
        }
    }

    pub async fn execute(self, batcher: &crate::common::batcher::Batcher) -> Result<bool> {
        let fut = execute_simple(
            self.initiated,
            batcher,
            self.return_result,
            self.collection
                .get_and_lock(&self.doc_id, self.duration, self.options.clone()),
            |res| run_result_from_get_result(&self.content_as.unwrap(), &self.transcoder, &res),
        );
        match self.parent_span {
            Some(span) => fut.instrument(span).await,
            None => fut.await,
        }
    }
}

pub struct GetAndTouchCommand {
    collection: Collection,
    doc_id: String,
    expiry: Duration,
    return_result: bool,
    initiated: Timestamp,
    content_as: Option<As>,
    transcoder: Option<Transcoder>,
    options: Option<GetAndTouchOptions>,
    parent_span: Option<Span>,
}

impl GetAndTouchCommand {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cb: Collection,
        doc_id: String,
        expiry: Duration,
        return_result: bool,
        initiated: Timestamp,
        content_as: Option<As>,
        transcoder: Option<Transcoder>,
        options: Option<GetAndTouchOptions>,
        parent_span: Option<Span>,
    ) -> Self {
        GetAndTouchCommand {
            collection: cb,
            doc_id,
            expiry,
            return_result,
            initiated,
            content_as,
            transcoder,
            options,
            parent_span,
        }
    }

    pub async fn execute(self, batcher: &crate::common::batcher::Batcher) -> Result<bool> {
        let fut = execute_simple(
            self.initiated,
            batcher,
            self.return_result,
            self.collection
                .get_and_touch(&self.doc_id, self.expiry, self.options.clone()),
            |res| run_result_from_get_result(&self.content_as.unwrap(), &self.transcoder, &res),
        );
        match self.parent_span {
            Some(span) => fut.instrument(span).await,
            None => fut.await,
        }
    }
}

pub struct TouchCommand {
    collection: Collection,
    doc_id: String,
    expiry: Duration,
    return_result: bool,
    initiated: Timestamp,
    options: Option<TouchOptions>,
    parent_span: Option<Span>,
}

impl TouchCommand {
    pub fn new(
        cb: Collection,
        doc_id: String,
        expiry: Duration,
        return_result: bool,
        initiated: Timestamp,
        options: Option<TouchOptions>,
        parent_span: Option<Span>,
    ) -> Self {
        TouchCommand {
            collection: cb,
            doc_id,
            expiry,
            return_result,
            initiated,
            options,
            parent_span,
        }
    }

    pub async fn execute(self, batcher: &crate::common::batcher::Batcher) -> Result<bool> {
        let fut = execute_simple(
            self.initiated,
            batcher,
            self.return_result,
            self.collection
                .touch(&self.doc_id, self.expiry, self.options.clone()),
            |res| Ok(res.into()),
        );
        match self.parent_span {
            Some(span) => fut.instrument(span).await,
            None => fut.await,
        }
    }
}

pub struct UnlockCommand {
    collection: Collection,
    doc_id: String,
    cas: u64,
    return_result: bool,
    initiated: Timestamp,
    options: Option<UnlockOptions>,
    parent_span: Option<Span>,
}

impl UnlockCommand {
    pub fn new(
        cb: Collection,
        doc_id: String,
        cas: u64,
        return_result: bool,
        initiated: Timestamp,
        options: Option<UnlockOptions>,
        parent_span: Option<Span>,
    ) -> Self {
        UnlockCommand {
            collection: cb,
            doc_id,
            cas,
            return_result,
            initiated,
            options,
            parent_span,
        }
    }

    pub async fn execute(self, batcher: &crate::common::batcher::Batcher) -> Result<bool> {
        let fut = execute_simple(
            self.initiated,
            batcher,
            self.return_result,
            self.collection
                .unlock(&self.doc_id, self.cas, self.options.clone()),
            |_| Ok(create_success_sdk_result()),
        );
        match self.parent_span {
            Some(span) => fut.instrument(span).await,
            None => fut.await,
        }
    }
}

pub struct ExistsCommand {
    collection: Collection,
    doc_id: String,
    return_result: bool,
    initiated: Timestamp,
    options: Option<ExistsOptions>,
    parent_span: Option<Span>,
}

impl ExistsCommand {
    pub fn new(
        cb: Collection,
        doc_id: String,
        return_result: bool,
        initiated: Timestamp,
        options: Option<ExistsOptions>,
        parent_span: Option<Span>,
    ) -> Self {
        ExistsCommand {
            collection: cb,
            doc_id,
            return_result,
            initiated,
            options,
            parent_span,
        }
    }

    pub async fn execute(self, batcher: &crate::common::batcher::Batcher) -> Result<bool> {
        let fut = execute_simple(
            self.initiated,
            batcher,
            self.return_result,
            self.collection.exists(&self.doc_id, self.options.clone()),
            |res| Ok(res.into()),
        );
        match self.parent_span {
            Some(span) => fut.instrument(span).await,
            None => fut.await,
        }
    }
}

fn run_result_from_get_result(
    content_as: &As,
    transcoder: &Option<Transcoder>,
    res: &GetResult,
) -> Result<run::result::Result> {
    Ok(run::result::Result::Sdk(sdk::Result {
        result: Some(sdk::result::Result::GetResult(sdk::kv::GetResult {
            cas: res.cas() as i64,
            content: Some(parse_content_as(content_as, transcoder, res)?),
            expiry_time: res.expiry_time().map(|ex| ex.timestamp()),
        })),
    }))
}

impl From<couchbase::results::kv_results::TouchResult> for run::result::Result {
    fn from(res: couchbase::results::kv_results::TouchResult) -> Self {
        run::result::Result::Sdk(sdk::Result {
            result: Some(sdk::result::Result::MutationResult(
                sdk::kv::MutationResult {
                    cas: res.cas() as i64,
                    mutation_token: None,
                },
            )),
        })
    }
}

impl From<couchbase::results::kv_results::ExistsResult> for run::result::Result {
    fn from(res: couchbase::results::kv_results::ExistsResult) -> Self {
        run::result::Result::Sdk(sdk::Result {
            result: Some(sdk::result::Result::ExistsResult(sdk::kv::ExistsResult {
                cas: res.cas() as i64,
                exists: res.exists(),
            })),
        })
    }
}
