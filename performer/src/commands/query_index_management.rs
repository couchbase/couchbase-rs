use crate::commands::helpers::{create_run_result, current_timestamp};
use crate::errors;
use crate::observability::span_owner::SpanOwner;
use crate::proto::protocol::sdk::collection::query::index_manager as collection_index_manager;
use crate::proto::protocol::sdk::collection::query::index_manager::command::Command::Shared;
use crate::proto::protocol::sdk::query::index_manager;
use crate::proto::protocol::sdk::query::index_manager::QueryIndex;
use crate::proto::protocol::{run, sdk};
use crate::translations::common::sdk_error_to_proto_sdk_result;
use couchbase::collection::Collection;
use couchbase::management::query::query_index_manager::QueryIndexManager;
use couchbase::options::query_index_mgmt_options::{
    BuildQueryIndexOptions, CreatePrimaryQueryIndexOptions, CreateQueryIndexOptions,
    DropPrimaryQueryIndexOptions, DropQueryIndexOptions, GetAllQueryIndexesOptions,
    WatchQueryIndexOptions,
};
use couchbase::results::query_index_mgmt_results::QueryIndexType;
use prost_types::Timestamp;
use std::sync::Arc;
use tracing::Instrument;

#[derive(Clone)]
pub struct QueryIndexManagerCommand {
    collection: Collection,
    return_result: bool,
    initiated: Timestamp,
    command_type: QueryIndexManagerCommandType,
    parent_span: Option<tracing::Span>,
}

#[derive(Clone)]
pub enum QueryIndexManagerCommandType {
    GetAllIndexes(GetAllIndexesCommandType),
    CreatePrimaryIndex(CreatePrimaryIndexCommandType),
    CreateIndex(CreateIndexCommandType),
    DropPrimaryIndex(DropPrimaryIndexCommandType),
    DropIndex(DropIndexCommandType),
    WatchIndexes(WatchIndexesCommandType),
    BuildDeferredIndexes(BuildDeferredIndexesCommandType),
}

impl QueryIndexManagerCommand {
    pub fn new(
        collection: Collection,
        command: collection_index_manager::command::Command,
        return_result: bool,
        span_owner: Arc<SpanOwner>,
    ) -> errors::error::Result<Self> {
        let Shared(ref shared_cmd) = command;
        let parent_span = shared_cmd
            .command
            .as_ref()
            .and_then(|inner| match inner {
                index_manager::command::Command::GetAllIndexes(c) => {
                    c.options.as_ref().and_then(|o| o.parent_span_id.as_deref())
                }
                index_manager::command::Command::CreatePrimaryIndex(c) => {
                    c.options.as_ref().and_then(|o| o.parent_span_id.as_deref())
                }
                index_manager::command::Command::CreateIndex(c) => {
                    c.options.as_ref().and_then(|o| o.parent_span_id.as_deref())
                }
                index_manager::command::Command::DropPrimaryIndex(c) => {
                    c.options.as_ref().and_then(|o| o.parent_span_id.as_deref())
                }
                index_manager::command::Command::DropIndex(c) => {
                    c.options.as_ref().and_then(|o| o.parent_span_id.as_deref())
                }
                index_manager::command::Command::WatchIndexes(c) => {
                    c.options.as_ref().and_then(|o| o.parent_span_id.as_deref())
                }
                index_manager::command::Command::BuildDeferredIndexes(c) => {
                    c.options.as_ref().and_then(|o| o.parent_span_id.as_deref())
                }
            })
            .and_then(|id| span_owner.get(id));

        let command_type = match command {
            Shared(cmd) => match cmd.command.unwrap() {
                index_manager::command::Command::GetAllIndexes(cmd) => {
                    let opts = cmd.options.map(|o| o.try_into()).transpose()?;

                    QueryIndexManagerCommandType::GetAllIndexes(GetAllIndexesCommandType::new(opts))
                }
                index_manager::command::Command::CreatePrimaryIndex(cmd) => {
                    let opts = cmd.options.map(|o| o.try_into()).transpose()?;

                    QueryIndexManagerCommandType::CreatePrimaryIndex(
                        CreatePrimaryIndexCommandType::new(opts),
                    )
                }
                index_manager::command::Command::CreateIndex(cmd) => {
                    let opts = cmd.options.map(|o| o.try_into()).transpose()?;

                    QueryIndexManagerCommandType::CreateIndex(CreateIndexCommandType::new(
                        cmd.index_name,
                        cmd.fields,
                        opts,
                    ))
                }
                index_manager::command::Command::DropPrimaryIndex(cmd) => {
                    let opts = cmd.options.map(|o| o.try_into()).transpose()?;

                    QueryIndexManagerCommandType::DropPrimaryIndex(
                        DropPrimaryIndexCommandType::new(opts),
                    )
                }
                index_manager::command::Command::DropIndex(cmd) => {
                    let opts = cmd.options.map(|o| o.try_into()).transpose()?;

                    QueryIndexManagerCommandType::DropIndex(DropIndexCommandType::new(
                        cmd.index_name,
                        opts,
                    ))
                }
                index_manager::command::Command::WatchIndexes(cmd) => {
                    let opts = cmd.options.map(|o| o.try_into()).transpose()?;

                    QueryIndexManagerCommandType::WatchIndexes(WatchIndexesCommandType::new(
                        cmd.index_names,
                        opts,
                    ))
                }
                index_manager::command::Command::BuildDeferredIndexes(cmd) => {
                    let opts = cmd.options.map(|o| o.try_into()).transpose()?;

                    QueryIndexManagerCommandType::BuildDeferredIndexes(
                        BuildDeferredIndexesCommandType::new(opts),
                    )
                }
            },
        };

        Ok(Self {
            collection,
            return_result,
            initiated: current_timestamp(),
            command_type,
            parent_span,
        })
    }

    pub async fn execute(
        self,
        batcher: &crate::common::batcher::Batcher,
    ) -> errors::error::Result<bool> {
        let start = std::time::Instant::now();

        let mgr = self.collection.query_indexes();
        let (res, success) = match self.command_type {
            QueryIndexManagerCommandType::GetAllIndexes(cmd) => {
                let fut = cmd.execute(mgr, self.return_result);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
            QueryIndexManagerCommandType::CreatePrimaryIndex(cmd) => {
                let fut = cmd.execute(mgr);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
            QueryIndexManagerCommandType::CreateIndex(cmd) => {
                let fut = cmd.execute(mgr);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
            QueryIndexManagerCommandType::DropPrimaryIndex(cmd) => {
                let fut = cmd.execute(mgr);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
            QueryIndexManagerCommandType::DropIndex(cmd) => {
                let fut = cmd.execute(mgr);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
            QueryIndexManagerCommandType::WatchIndexes(cmd) => {
                let fut = cmd.execute(mgr);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
            QueryIndexManagerCommandType::BuildDeferredIndexes(cmd) => {
                let fut = cmd.execute(mgr);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
        };

        let end = std::time::Instant::now();

        let mut top_level_result = create_run_result(self.initiated, end - start);
        top_level_result.result = Some(run::result::Result::Sdk(res));
        batcher.send(top_level_result).await;

        Ok(success)
    }
}

#[derive(Clone)]
pub struct GetAllIndexesCommandType {
    options: Option<GetAllQueryIndexesOptions>,
}

impl GetAllIndexesCommandType {
    pub fn new(options: Option<GetAllQueryIndexesOptions>) -> Self {
        Self { options }
    }

    pub async fn execute(self, mgr: QueryIndexManager, return_result: bool) -> (sdk::Result, bool) {
        let res = mgr.get_all_indexes(self.options).await;

        match res {
            Ok(indexes) => {
                if return_result {
                    (
                        sdk::Result {
                            result: Some(sdk::result::Result::QueryIndexes(
                                index_manager::QueryIndexes {
                                    indexes: indexes
                                        .into_iter()
                                        .map(|index| {
                                            let index_type = match index.index_type() {
                                                QueryIndexType::Unknown => {
                                                    unreachable!()
                                                }
                                                QueryIndexType::View => {
                                                    index_manager::QueryIndexType::View
                                                }
                                                QueryIndexType::Gsi => {
                                                    index_manager::QueryIndexType::Gsi
                                                }
                                                _ => {
                                                    unreachable!()
                                                }
                                            };

                                            QueryIndex {
                                                name: index.name().to_string(),
                                                is_primary: index.is_primary(),
                                                r#type: index_type.into(),
                                                state: index.state().to_string(),
                                                keyspace: index.keyspace().to_string(),
                                                index_key: index.index_key().to_vec(),
                                                condition: index
                                                    .condition()
                                                    .map(|cond| cond.into()),
                                                partition: index
                                                    .partition()
                                                    .map(|part| part.into()),
                                                bucket_name: index.bucket_name().to_string(),
                                                scope_name: index
                                                    .scope_name()
                                                    .map(|s| s.to_string()),
                                                collection_name: index
                                                    .collection_name()
                                                    .map(|c| c.to_string()),
                                            }
                                        })
                                        .collect(),
                                },
                            )),
                        },
                        true,
                    )
                } else {
                    (
                        sdk::Result {
                            result: Some(sdk::result::Result::Success(true)),
                        },
                        true,
                    )
                }
            }
            Err(err) => (sdk_error_to_proto_sdk_result(err), false),
        }
    }
}

#[derive(Clone)]
pub struct CreatePrimaryIndexCommandType {
    options: Option<CreatePrimaryQueryIndexOptions>,
}

impl CreatePrimaryIndexCommandType {
    pub fn new(options: Option<CreatePrimaryQueryIndexOptions>) -> Self {
        Self { options }
    }

    pub async fn execute(self, mgr: QueryIndexManager) -> (sdk::Result, bool) {
        match mgr.create_primary_index(self.options).await {
            Ok(_) => (
                sdk::Result {
                    result: Some(sdk::result::Result::Success(true)),
                },
                true,
            ),
            Err(err) => (sdk_error_to_proto_sdk_result(err), false),
        }
    }
}

#[derive(Clone)]
pub struct CreateIndexCommandType {
    index_name: String,
    fields: Vec<String>,
    options: Option<CreateQueryIndexOptions>,
}

impl CreateIndexCommandType {
    pub fn new(
        index_name: String,
        fields: Vec<String>,
        options: Option<CreateQueryIndexOptions>,
    ) -> Self {
        Self {
            index_name,
            fields,
            options,
        }
    }

    pub async fn execute(self, mgr: QueryIndexManager) -> (sdk::Result, bool) {
        match mgr
            .create_index(self.index_name, self.fields, self.options)
            .await
        {
            Ok(_) => (
                sdk::Result {
                    result: Some(sdk::result::Result::Success(true)),
                },
                true,
            ),
            Err(err) => (sdk_error_to_proto_sdk_result(err), false),
        }
    }
}

#[derive(Clone)]
pub struct DropPrimaryIndexCommandType {
    options: Option<DropPrimaryQueryIndexOptions>,
}

impl DropPrimaryIndexCommandType {
    pub fn new(options: Option<DropPrimaryQueryIndexOptions>) -> Self {
        Self { options }
    }

    pub async fn execute(self, mgr: QueryIndexManager) -> (sdk::Result, bool) {
        match mgr.drop_primary_index(self.options).await {
            Ok(_) => (
                sdk::Result {
                    result: Some(sdk::result::Result::Success(true)),
                },
                true,
            ),
            Err(err) => (sdk_error_to_proto_sdk_result(err), false),
        }
    }
}

#[derive(Clone)]
pub struct DropIndexCommandType {
    index_name: String,
    options: Option<DropQueryIndexOptions>,
}

impl DropIndexCommandType {
    pub fn new(index_name: String, options: Option<DropQueryIndexOptions>) -> Self {
        Self {
            index_name,
            options,
        }
    }

    pub async fn execute(self, mgr: QueryIndexManager) -> (sdk::Result, bool) {
        match mgr.drop_index(self.index_name, self.options).await {
            Ok(_) => (
                sdk::Result {
                    result: Some(sdk::result::Result::Success(true)),
                },
                true,
            ),
            Err(err) => (sdk_error_to_proto_sdk_result(err), false),
        }
    }
}

#[derive(Clone)]
pub struct WatchIndexesCommandType {
    index_names: Vec<String>,
    options: Option<WatchQueryIndexOptions>,
}

impl WatchIndexesCommandType {
    pub fn new(index_names: Vec<String>, options: Option<WatchQueryIndexOptions>) -> Self {
        Self {
            index_names,
            options,
        }
    }

    pub async fn execute(self, mgr: QueryIndexManager) -> (sdk::Result, bool) {
        match mgr.watch_indexes(self.index_names, self.options).await {
            Ok(_) => (
                sdk::Result {
                    result: Some(sdk::result::Result::Success(true)),
                },
                true,
            ),
            Err(err) => (sdk_error_to_proto_sdk_result(err), false),
        }
    }
}

#[derive(Clone)]
pub struct BuildDeferredIndexesCommandType {
    options: Option<BuildQueryIndexOptions>,
}

impl BuildDeferredIndexesCommandType {
    pub fn new(options: Option<BuildQueryIndexOptions>) -> Self {
        Self { options }
    }

    pub async fn execute(self, mgr: QueryIndexManager) -> (sdk::Result, bool) {
        match mgr.build_deferred_indexes(self.options).await {
            Ok(_) => (
                sdk::Result {
                    result: Some(sdk::result::Result::Success(true)),
                },
                true,
            ),
            Err(err) => (sdk_error_to_proto_sdk_result(err), false),
        }
    }
}
