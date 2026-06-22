use crate::commands::execution::execute_simple;
use crate::commands::helpers::create_success_sdk_result;
use crate::errors::error::{Error, Result};
use crate::observability::span_owner::SpanOwner;
use crate::proto::protocol::sdk::scope::search::index_manager::command::Command;
use crate::proto::protocol::sdk::search::index_manager;
use crate::proto::protocol::sdk::search::index_manager::command::Command as SharedCommand;
use crate::proto::protocol::{run, sdk};
use couchbase::management::search::index::SearchIndex;
use couchbase::management::search::search_index_manager::SearchIndexManager;
use couchbase::options::search_index_mgmt_options::{
    AllowQueryingSearchIndexOptions, AnalyzeDocumentOptions, DisallowQueryingSearchIndexOptions,
    DropSearchIndexOptions, FreezePlanSearchIndexOptions, GetAllSearchIndexesOptions,
    GetIndexedDocumentsCountOptions, GetSearchIndexOptions, PauseIngestSearchIndexOptions,
    ResumeIngestSearchIndexOptions, UnfreezePlanSearchIndexOptions, UpsertSearchIndexOptions,
};
use couchbase::scope::Scope;
use prost_types::Timestamp;
use serde_json::Value;
use std::sync::Arc;
use tracing::Instrument;

#[derive(Clone)]
pub struct SearchIndexManagerCommand {
    scope: Scope,
    return_result: bool,
    initiated: Timestamp,
    command_type: SearchIndexManagerCommandType,
    parent_span: Option<tracing::Span>,
}

#[derive(Clone)]
pub enum SearchIndexManagerCommandType {
    GetIndex(GetIndexCommandType),
    GetAllIndexes(GetAllIndexesCommandType),
    UpsertIndex(UpsertIndexCommandType),
    DropIndex(DropIndexCommandType),
    GetIndexedDocumentsCount(GetIndexedDocumentsCountCommandType),
    PauseIngest(PauseIngestCommandType),
    ResumeIngest(ResumeIngestCommandType),
    AllowQuerying(AllowQueryingCommandType),
    DisallowQuerying(DisallowQueryingCommandType),
    FreezePlan(FreezePlanCommandType),
    UnfreezePlan(UnfreezePlanCommandType),
    AnalyzeDocument(AnalyzeDocumentCommandType),
}

impl SearchIndexManagerCommand {
    pub fn new(
        scope: Scope,
        command: Command,
        return_result: bool,
        span_owner: Arc<SpanOwner>,
    ) -> Result<Self> {
        let Command::Shared(ref shared) = command;
        let parent_span = shared
            .command
            .as_ref()
            .and_then(|inner| match inner {
                SharedCommand::GetIndex(cmd) => cmd
                    .options
                    .as_ref()
                    .and_then(|o| o.parent_span_id.as_deref()),
                SharedCommand::GetAllIndexes(cmd) => cmd
                    .options
                    .as_ref()
                    .and_then(|o| o.parent_span_id.as_deref()),
                SharedCommand::UpsertIndex(cmd) => cmd
                    .options
                    .as_ref()
                    .and_then(|o| o.parent_span_id.as_deref()),
                SharedCommand::DropIndex(cmd) => cmd
                    .options
                    .as_ref()
                    .and_then(|o| o.parent_span_id.as_deref()),
                SharedCommand::GetIndexedDocumentsCount(cmd) => cmd
                    .options
                    .as_ref()
                    .and_then(|o| o.parent_span_id.as_deref()),
                SharedCommand::PauseIngest(cmd) => cmd
                    .options
                    .as_ref()
                    .and_then(|o| o.parent_span_id.as_deref()),
                SharedCommand::ResumeIngest(cmd) => cmd
                    .options
                    .as_ref()
                    .and_then(|o| o.parent_span_id.as_deref()),
                SharedCommand::AllowQuerying(cmd) => cmd
                    .options
                    .as_ref()
                    .and_then(|o| o.parent_span_id.as_deref()),
                SharedCommand::DisallowQuerying(cmd) => cmd
                    .options
                    .as_ref()
                    .and_then(|o| o.parent_span_id.as_deref()),
                SharedCommand::FreezePlan(cmd) => cmd
                    .options
                    .as_ref()
                    .and_then(|o| o.parent_span_id.as_deref()),
                SharedCommand::UnfreezePlan(cmd) => cmd
                    .options
                    .as_ref()
                    .and_then(|o| o.parent_span_id.as_deref()),
                SharedCommand::AnalyzeDocument(cmd) => cmd
                    .options
                    .as_ref()
                    .and_then(|o| o.parent_span_id.as_deref()),
            })
            .and_then(|id| span_owner.get(id));

        let command_type = match command {
            Command::Shared(shared) => match shared.command.unwrap() {
                SharedCommand::GetIndex(cmd) => {
                    let opts = cmd.options.map(|o| o.try_into()).transpose()?;
                    SearchIndexManagerCommandType::GetIndex(GetIndexCommandType::new(
                        cmd.index_name,
                        opts,
                    ))
                }
                SharedCommand::GetAllIndexes(cmd) => {
                    let opts = cmd.options.map(|o| o.try_into()).transpose()?;
                    SearchIndexManagerCommandType::GetAllIndexes(GetAllIndexesCommandType::new(
                        opts,
                    ))
                }
                SharedCommand::UpsertIndex(cmd) => {
                    let opts = cmd.options.map(|o| o.try_into()).transpose()?;
                    let index: SearchIndex = serde_json::from_slice(&cmd.index_definition)
                        .map_err(|e| {
                            Error::internal(format!("Failed to parse index definition: {e}"))
                        })?;
                    SearchIndexManagerCommandType::UpsertIndex(UpsertIndexCommandType::new(
                        index, opts,
                    ))
                }
                SharedCommand::DropIndex(cmd) => {
                    let opts = cmd.options.map(|o| o.try_into()).transpose()?;
                    SearchIndexManagerCommandType::DropIndex(DropIndexCommandType::new(
                        cmd.index_name,
                        opts,
                    ))
                }
                SharedCommand::GetIndexedDocumentsCount(cmd) => {
                    let opts = cmd.options.map(|o| o.try_into()).transpose()?;
                    SearchIndexManagerCommandType::GetIndexedDocumentsCount(
                        GetIndexedDocumentsCountCommandType::new(cmd.index_name, opts),
                    )
                }
                SharedCommand::PauseIngest(cmd) => {
                    let opts = cmd.options.map(|o| o.try_into()).transpose()?;
                    SearchIndexManagerCommandType::PauseIngest(PauseIngestCommandType::new(
                        cmd.index_name,
                        opts,
                    ))
                }
                SharedCommand::ResumeIngest(cmd) => {
                    let opts = cmd.options.map(|o| o.try_into()).transpose()?;
                    SearchIndexManagerCommandType::ResumeIngest(ResumeIngestCommandType::new(
                        cmd.index_name,
                        opts,
                    ))
                }
                SharedCommand::AllowQuerying(cmd) => {
                    let opts = cmd.options.map(|o| o.try_into()).transpose()?;
                    SearchIndexManagerCommandType::AllowQuerying(AllowQueryingCommandType::new(
                        cmd.index_name,
                        opts,
                    ))
                }
                SharedCommand::DisallowQuerying(cmd) => {
                    let opts = cmd.options.map(|o| o.try_into()).transpose()?;
                    SearchIndexManagerCommandType::DisallowQuerying(
                        DisallowQueryingCommandType::new(cmd.index_name, opts),
                    )
                }
                SharedCommand::FreezePlan(cmd) => {
                    let opts = cmd.options.map(|o| o.try_into()).transpose()?;
                    SearchIndexManagerCommandType::FreezePlan(FreezePlanCommandType::new(
                        cmd.index_name,
                        opts,
                    ))
                }
                SharedCommand::UnfreezePlan(cmd) => {
                    let opts = cmd.options.map(|o| o.try_into()).transpose()?;
                    SearchIndexManagerCommandType::UnfreezePlan(UnfreezePlanCommandType::new(
                        cmd.index_name,
                        opts,
                    ))
                }
                SharedCommand::AnalyzeDocument(cmd) => {
                    let opts = cmd.options.map(|o| o.try_into()).transpose()?;
                    let document: Value = serde_json::from_slice(cmd.document.as_slice())
                        .map_err(|e| Error::internal(format!("Failed to parse document: {e}")))?;
                    SearchIndexManagerCommandType::AnalyzeDocument(AnalyzeDocumentCommandType::new(
                        cmd.index_name,
                        document,
                        opts,
                    ))
                }
            },
        };

        Ok(Self {
            scope,
            return_result,
            initiated: Timestamp::default(),
            command_type,
            parent_span,
        })
    }

    pub async fn execute(self, batcher: &crate::common::batcher::Batcher) -> Result<bool> {
        let mgr = self.scope.search_indexes();
        match self.command_type {
            SearchIndexManagerCommandType::GetIndex(cmd) => {
                let fut = cmd.execute(mgr, self.initiated, batcher, self.return_result);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
            SearchIndexManagerCommandType::GetAllIndexes(cmd) => {
                let fut = cmd.execute(mgr, self.initiated, batcher, self.return_result);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
            SearchIndexManagerCommandType::UpsertIndex(cmd) => {
                let fut = cmd.execute(mgr, self.initiated, batcher, self.return_result);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
            SearchIndexManagerCommandType::DropIndex(cmd) => {
                let fut = cmd.execute(mgr, self.initiated, batcher, self.return_result);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
            SearchIndexManagerCommandType::GetIndexedDocumentsCount(cmd) => {
                let fut = cmd.execute(mgr, self.initiated, batcher, self.return_result);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
            SearchIndexManagerCommandType::PauseIngest(cmd) => {
                let fut = cmd.execute(mgr, self.initiated, batcher, self.return_result);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
            SearchIndexManagerCommandType::ResumeIngest(cmd) => {
                let fut = cmd.execute(mgr, self.initiated, batcher, self.return_result);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
            SearchIndexManagerCommandType::AllowQuerying(cmd) => {
                let fut = cmd.execute(mgr, self.initiated, batcher, self.return_result);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
            SearchIndexManagerCommandType::DisallowQuerying(cmd) => {
                let fut = cmd.execute(mgr, self.initiated, batcher, self.return_result);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
            SearchIndexManagerCommandType::FreezePlan(cmd) => {
                let fut = cmd.execute(mgr, self.initiated, batcher, self.return_result);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
            SearchIndexManagerCommandType::UnfreezePlan(cmd) => {
                let fut = cmd.execute(mgr, self.initiated, batcher, self.return_result);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
            SearchIndexManagerCommandType::AnalyzeDocument(cmd) => {
                let fut = cmd.execute(mgr, self.initiated, batcher, self.return_result);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct GetIndexCommandType {
    index_name: String,
    options: Option<GetSearchIndexOptions>,
}

impl GetIndexCommandType {
    pub fn new(index_name: String, options: Option<GetSearchIndexOptions>) -> Self {
        Self {
            index_name,
            options,
        }
    }

    pub async fn execute(
        self,
        mgr: SearchIndexManager,
        initiated: Timestamp,
        batcher: &crate::common::batcher::Batcher,
        return_result: bool,
    ) -> Result<bool> {
        execute_simple(
            initiated,
            batcher,
            return_result,
            mgr.get_index(self.index_name, self.options),
            |index| {
                Ok(run::result::Result::Sdk(sdk::Result {
                    result: Some(sdk::result::Result::SearchIndexManagerResult(
                        index_manager::Result {
                            result: Some(index_manager::result::Result::Index(index.into())),
                        },
                    )),
                }))
            },
        )
        .await
    }
}

#[derive(Clone)]
pub struct GetAllIndexesCommandType {
    options: Option<GetAllSearchIndexesOptions>,
}

impl GetAllIndexesCommandType {
    pub fn new(options: Option<GetAllSearchIndexesOptions>) -> Self {
        Self { options }
    }

    pub async fn execute(
        self,
        mgr: SearchIndexManager,
        initiated: Timestamp,
        batcher: &crate::common::batcher::Batcher,
        return_result: bool,
    ) -> Result<bool> {
        execute_simple(
            initiated,
            batcher,
            return_result,
            mgr.get_all_indexes(self.options),
            |indexes| {
                let proto_indexes = indexes.into_iter().map(Into::into).collect();

                Ok(run::result::Result::Sdk(sdk::Result {
                    result: Some(sdk::result::Result::SearchIndexManagerResult(
                        index_manager::Result {
                            result: Some(index_manager::result::Result::Indexes(
                                index_manager::SearchIndexes {
                                    indexes: proto_indexes,
                                },
                            )),
                        },
                    )),
                }))
            },
        )
        .await
    }
}

#[derive(Clone)]
pub struct UpsertIndexCommandType {
    index: SearchIndex,
    options: Option<UpsertSearchIndexOptions>,
}

impl UpsertIndexCommandType {
    pub fn new(index: SearchIndex, options: Option<UpsertSearchIndexOptions>) -> Self {
        Self { index, options }
    }

    pub async fn execute(
        self,
        mgr: SearchIndexManager,
        initiated: Timestamp,
        batcher: &crate::common::batcher::Batcher,
        return_result: bool,
    ) -> Result<bool> {
        execute_simple(
            initiated,
            batcher,
            return_result,
            mgr.upsert_index(self.index, self.options),
            |_| Ok(create_success_sdk_result()),
        )
        .await
    }
}

#[derive(Clone)]
pub struct DropIndexCommandType {
    index_name: String,
    options: Option<DropSearchIndexOptions>,
}

impl DropIndexCommandType {
    pub fn new(index_name: String, options: Option<DropSearchIndexOptions>) -> Self {
        Self {
            index_name,
            options,
        }
    }

    pub async fn execute(
        self,
        mgr: SearchIndexManager,
        initiated: Timestamp,
        batcher: &crate::common::batcher::Batcher,
        return_result: bool,
    ) -> Result<bool> {
        execute_simple(
            initiated,
            batcher,
            return_result,
            mgr.drop_index(self.index_name, self.options),
            |_| Ok(create_success_sdk_result()),
        )
        .await
    }
}

#[derive(Clone)]
pub struct GetIndexedDocumentsCountCommandType {
    index_name: String,
    options: Option<GetIndexedDocumentsCountOptions>,
}

impl GetIndexedDocumentsCountCommandType {
    pub fn new(index_name: String, options: Option<GetIndexedDocumentsCountOptions>) -> Self {
        Self {
            index_name,
            options,
        }
    }

    pub async fn execute(
        self,
        mgr: SearchIndexManager,
        initiated: Timestamp,
        batcher: &crate::common::batcher::Batcher,
        return_result: bool,
    ) -> Result<bool> {
        execute_simple(
            initiated,
            batcher,
            return_result,
            mgr.get_indexed_documents_count(self.index_name, self.options),
            |count| {
                Ok(run::result::Result::Sdk(sdk::Result {
                    result: Some(sdk::result::Result::SearchIndexManagerResult(
                        index_manager::Result {
                            result: Some(index_manager::result::Result::IndexedDocumentCounts(
                                count as i32,
                            )),
                        },
                    )),
                }))
            },
        )
        .await
    }
}

#[derive(Clone)]
pub struct PauseIngestCommandType {
    index_name: String,
    options: Option<PauseIngestSearchIndexOptions>,
}

impl PauseIngestCommandType {
    pub fn new(index_name: String, options: Option<PauseIngestSearchIndexOptions>) -> Self {
        Self {
            index_name,
            options,
        }
    }

    pub async fn execute(
        self,
        mgr: SearchIndexManager,
        initiated: Timestamp,
        batcher: &crate::common::batcher::Batcher,
        return_result: bool,
    ) -> Result<bool> {
        execute_simple(
            initiated,
            batcher,
            return_result,
            mgr.pause_ingest(self.index_name, self.options),
            |_| Ok(create_success_sdk_result()),
        )
        .await
    }
}

#[derive(Clone)]
pub struct ResumeIngestCommandType {
    index_name: String,
    options: Option<ResumeIngestSearchIndexOptions>,
}

impl ResumeIngestCommandType {
    pub fn new(index_name: String, options: Option<ResumeIngestSearchIndexOptions>) -> Self {
        Self {
            index_name,
            options,
        }
    }

    pub async fn execute(
        self,
        mgr: SearchIndexManager,
        initiated: Timestamp,
        batcher: &crate::common::batcher::Batcher,
        return_result: bool,
    ) -> Result<bool> {
        execute_simple(
            initiated,
            batcher,
            return_result,
            mgr.resume_ingest(self.index_name, self.options),
            |_| Ok(create_success_sdk_result()),
        )
        .await
    }
}

#[derive(Clone)]
pub struct AllowQueryingCommandType {
    index_name: String,
    options: Option<AllowQueryingSearchIndexOptions>,
}

impl AllowQueryingCommandType {
    pub fn new(index_name: String, options: Option<AllowQueryingSearchIndexOptions>) -> Self {
        Self {
            index_name,
            options,
        }
    }

    pub async fn execute(
        self,
        mgr: SearchIndexManager,
        initiated: Timestamp,
        batcher: &crate::common::batcher::Batcher,
        return_result: bool,
    ) -> Result<bool> {
        execute_simple(
            initiated,
            batcher,
            return_result,
            mgr.allow_querying(self.index_name, self.options),
            |_| Ok(create_success_sdk_result()),
        )
        .await
    }
}

#[derive(Clone)]
pub struct DisallowQueryingCommandType {
    index_name: String,
    options: Option<DisallowQueryingSearchIndexOptions>,
}

impl DisallowQueryingCommandType {
    pub fn new(index_name: String, options: Option<DisallowQueryingSearchIndexOptions>) -> Self {
        Self {
            index_name,
            options,
        }
    }

    pub async fn execute(
        self,
        mgr: SearchIndexManager,
        initiated: Timestamp,
        batcher: &crate::common::batcher::Batcher,
        return_result: bool,
    ) -> Result<bool> {
        execute_simple(
            initiated,
            batcher,
            return_result,
            mgr.disallow_querying(self.index_name, self.options),
            |_| Ok(create_success_sdk_result()),
        )
        .await
    }
}

#[derive(Clone)]
pub struct FreezePlanCommandType {
    index_name: String,
    options: Option<FreezePlanSearchIndexOptions>,
}

impl FreezePlanCommandType {
    pub fn new(index_name: String, options: Option<FreezePlanSearchIndexOptions>) -> Self {
        Self {
            index_name,
            options,
        }
    }

    pub async fn execute(
        self,
        mgr: SearchIndexManager,
        initiated: Timestamp,
        batcher: &crate::common::batcher::Batcher,
        return_result: bool,
    ) -> Result<bool> {
        execute_simple(
            initiated,
            batcher,
            return_result,
            mgr.freeze_plan(self.index_name, self.options),
            |_| Ok(create_success_sdk_result()),
        )
        .await
    }
}

#[derive(Clone)]
pub struct UnfreezePlanCommandType {
    index_name: String,
    options: Option<UnfreezePlanSearchIndexOptions>,
}

impl UnfreezePlanCommandType {
    pub fn new(index_name: String, options: Option<UnfreezePlanSearchIndexOptions>) -> Self {
        Self {
            index_name,
            options,
        }
    }

    pub async fn execute(
        self,
        mgr: SearchIndexManager,
        initiated: Timestamp,
        batcher: &crate::common::batcher::Batcher,
        return_result: bool,
    ) -> Result<bool> {
        execute_simple(
            initiated,
            batcher,
            return_result,
            mgr.unfreeze_plan(self.index_name, self.options),
            |_| Ok(create_success_sdk_result()),
        )
        .await
    }
}

#[derive(Clone)]
pub struct AnalyzeDocumentCommandType {
    index_name: String,
    document: Value,
    options: Option<AnalyzeDocumentOptions>,
}

impl AnalyzeDocumentCommandType {
    pub fn new(
        index_name: String,
        document: Value,
        options: Option<AnalyzeDocumentOptions>,
    ) -> Self {
        Self {
            index_name,
            document,
            options,
        }
    }

    pub async fn execute(
        self,
        mgr: SearchIndexManager,
        initiated: Timestamp,
        batcher: &crate::common::batcher::Batcher,
        return_result: bool,
    ) -> Result<bool> {
        execute_simple(
            initiated,
            batcher,
            return_result,
            mgr.analyze_document(self.index_name, self.document, self.options),
            |results| {
                let byte_results = results
                    .as_array()
                    .unwrap_or(&vec![])
                    .iter()
                    .map(|r| serde_json::to_vec(r).unwrap_or_default())
                    .collect::<Vec<Vec<u8>>>();

                Ok(run::result::Result::Sdk(sdk::Result {
                    result: Some(sdk::result::Result::SearchIndexManagerResult(
                        index_manager::Result {
                            result: Some(index_manager::result::Result::AnalyzeDocument(
                                index_manager::AnalyzeDocumentResult {
                                    results: byte_results,
                                },
                            )),
                        },
                    )),
                }))
            },
        )
        .await
    }
}

impl From<SearchIndex> for index_manager::SearchIndex {
    fn from(index: SearchIndex) -> Self {
        Self {
            uuid: index.uuid.unwrap_or_default(),
            name: index.name,
            r#type: index.index_type,
            source_uuid: index.source_uuid.unwrap_or_default(),
            source_type: index.source_type.unwrap_or_default(),
            params: serde_json::to_vec(&index.params).unwrap_or_default(),
            source_params: serde_json::to_vec(&index.source_params).unwrap_or_default(),
            plan_params: serde_json::to_vec(&index.plan_params).unwrap_or_default(),
        }
    }
}
