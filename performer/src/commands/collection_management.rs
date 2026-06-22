use crate::commands::execution::execute_simple;
use crate::commands::helpers::create_success_sdk_result;
use crate::errors;
use crate::observability::span_owner::SpanOwner;
use crate::proto::protocol::sdk::bucket::collection_manager;
use crate::proto::protocol::sdk::bucket::collection_manager::command::Command;
use crate::proto::protocol::{run, sdk};
use couchbase::bucket::Bucket;
use couchbase::management::collections::collection_manager::CollectionManager;
use couchbase::management::collections::collection_settings::UpdateCollectionSettings;
use couchbase::management::collections::collection_settings::{
    CreateCollectionSettings, MaxExpiryValue,
};
use couchbase::options::collection_mgmt_options::{
    CreateCollectionOptions, CreateScopeOptions, DropCollectionOptions, DropScopeOptions,
    GetAllScopesOptions, UpdateCollectionOptions,
};
use prost_types::Timestamp;
use std::sync::Arc;
use tracing::Instrument;

#[derive(Clone)]
pub struct CollectionManagerCommand {
    bucket: Bucket,
    return_result: bool,
    initiated: Timestamp,
    command_type: CollectionManagerCommandType,
    parent_span: Option<tracing::Span>,
}

#[derive(Clone)]
pub enum CollectionManagerCommandType {
    GetAllScopes(GetAllScopesCommandType),
    CreateScope(CreateScopeCommandType),
    DropScope(DropScopeCommandType),
    CreateCollection(CreateCollectionCommandType),
    UpdateCollection(UpdateCollectionCommandType),
    DropCollection(DropCollectionCommandType),
}

impl CollectionManagerCommand {
    pub fn new(
        bucket: Bucket,
        command: Command,
        return_result: bool,
        span_owner: Arc<SpanOwner>,
    ) -> errors::error::Result<Self> {
        let parent_span = match &command {
            Command::GetAllScopes(cmd) => cmd
                .options
                .as_ref()
                .and_then(|o| o.parent_span_id.as_deref()),
            Command::CreateScope(cmd) => cmd
                .options
                .as_ref()
                .and_then(|o| o.parent_span_id.as_deref()),
            Command::DropScope(cmd) => cmd
                .options
                .as_ref()
                .and_then(|o| o.parent_span_id.as_deref()),
            Command::CreateCollection(cmd) => cmd
                .options
                .as_ref()
                .and_then(|o| o.parent_span_id.as_deref()),
            Command::UpdateCollection(cmd) => cmd
                .options
                .as_ref()
                .and_then(|o| o.parent_span_id.as_deref()),
            Command::DropCollection(cmd) => cmd
                .options
                .as_ref()
                .and_then(|o| o.parent_span_id.as_deref()),
        }
        .and_then(|id| span_owner.get(id));

        let command_type =
            match command {
                Command::GetAllScopes(cmd) => {
                    let opts = cmd.options.map(|o| o.try_into()).transpose()?;

                    CollectionManagerCommandType::GetAllScopes(GetAllScopesCommandType::new(opts))
                }
                Command::CreateScope(cmd) => {
                    let opts = cmd.options.map(|o| o.try_into()).transpose()?;

                    CollectionManagerCommandType::CreateScope(CreateScopeCommandType::new(
                        cmd.name, opts,
                    ))
                }
                Command::DropScope(cmd) => {
                    let opts = cmd.options.map(|o| o.try_into()).transpose()?;

                    CollectionManagerCommandType::DropScope(DropScopeCommandType::new(
                        cmd.name, opts,
                    ))
                }
                Command::CreateCollection(cmd) => {
                    let opts = cmd.options.map(|o| o.try_into()).transpose()?;
                    let settings = cmd
                        .settings
                        .map(CreateCollectionSettings::try_from)
                        .transpose()?;

                    CollectionManagerCommandType::CreateCollection(
                        CreateCollectionCommandType::new(cmd.scope_name, cmd.name, settings, opts),
                    )
                }
                Command::UpdateCollection(cmd) => {
                    let opts = cmd.options.map(|o| o.try_into()).transpose()?;
                    let settings = cmd
                        .settings
                        .ok_or_else(|| {
                            errors::error::Error::unimplemented("collection settings is required")
                        })?
                        .try_into()?;

                    CollectionManagerCommandType::UpdateCollection(
                        UpdateCollectionCommandType::new(cmd.scope_name, cmd.name, settings, opts),
                    )
                }
                Command::DropCollection(cmd) => {
                    let opts = cmd.options.map(|o| o.try_into()).transpose()?;

                    CollectionManagerCommandType::DropCollection(DropCollectionCommandType::new(
                        cmd.scope_name,
                        cmd.name,
                        opts,
                    ))
                }
            };

        Ok(Self {
            bucket,
            return_result,
            initiated: Timestamp::default(),
            command_type,
            parent_span,
        })
    }

    pub async fn execute(
        self,
        batcher: &crate::common::batcher::Batcher,
    ) -> errors::error::Result<bool> {
        let mgr = self.bucket.collections();
        match self.command_type {
            CollectionManagerCommandType::GetAllScopes(cmd) => {
                let fut = cmd.execute(mgr, self.initiated, batcher, self.return_result);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
            CollectionManagerCommandType::CreateScope(cmd) => {
                let fut = cmd.execute(mgr, self.initiated, batcher, self.return_result);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
            CollectionManagerCommandType::DropScope(cmd) => {
                let fut = cmd.execute(mgr, self.initiated, batcher, self.return_result);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
            CollectionManagerCommandType::CreateCollection(cmd) => {
                let fut = cmd.execute(mgr, self.initiated, batcher, self.return_result);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
            CollectionManagerCommandType::UpdateCollection(cmd) => {
                let fut = cmd.execute(mgr, self.initiated, batcher, self.return_result);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
            CollectionManagerCommandType::DropCollection(cmd) => {
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
pub struct GetAllScopesCommandType {
    options: Option<GetAllScopesOptions>,
}

impl GetAllScopesCommandType {
    pub fn new(options: Option<GetAllScopesOptions>) -> Self {
        Self { options }
    }

    pub async fn execute(
        self,
        mgr: CollectionManager,
        initiated: Timestamp,
        batcher: &crate::common::batcher::Batcher,
        return_result: bool,
    ) -> errors::error::Result<bool> {
        execute_simple(
            initiated,
            batcher,
            return_result,
            mgr.get_all_scopes(self.options),
            |scopes| {
                let result_scopes = scopes
                    .into_iter()
                    .map(|scope| {
                        let collections = scope
                            .collections()
                            .iter()
                            .map(|collection| collection_manager::CollectionSpec {
                                name: collection.name().to_string(),
                                scope_name: collection.scope_name().to_string(),
                                expiry_secs: match collection.max_expiry() {
                                    MaxExpiryValue::Never => None,
                                    MaxExpiryValue::Seconds(s) => Some(s.as_secs() as i32),
                                    MaxExpiryValue::InheritFromBucket => Some(-1),
                                    _ => None,
                                },
                                history: Some(collection.history()),
                            })
                            .collect();

                        collection_manager::ScopeSpec {
                            name: scope.name().to_string(),
                            collections,
                        }
                    })
                    .collect();

                Ok(run::result::Result::Sdk(sdk::Result {
                    result: Some(sdk::result::Result::CollectionManagerResult(
                        collection_manager::Result {
                            result: Some(collection_manager::result::Result::GetAllScopesResult(
                                collection_manager::GetAllScopesResult {
                                    result: result_scopes,
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
pub struct CreateScopeCommandType {
    name: String,
    options: Option<CreateScopeOptions>,
}

impl CreateScopeCommandType {
    pub fn new(name: String, options: Option<CreateScopeOptions>) -> Self {
        Self { name, options }
    }

    pub async fn execute(
        self,
        mgr: CollectionManager,
        initiated: Timestamp,
        batcher: &crate::common::batcher::Batcher,
        return_result: bool,
    ) -> errors::error::Result<bool> {
        execute_simple(
            initiated,
            batcher,
            return_result,
            mgr.create_scope(self.name, self.options),
            |_| Ok(create_success_sdk_result()),
        )
        .await
    }
}

#[derive(Clone)]
pub struct DropScopeCommandType {
    name: String,
    options: Option<DropScopeOptions>,
}

impl DropScopeCommandType {
    pub fn new(name: String, options: Option<DropScopeOptions>) -> Self {
        Self { name, options }
    }

    pub async fn execute(
        self,
        mgr: CollectionManager,
        initiated: Timestamp,
        batcher: &crate::common::batcher::Batcher,
        return_result: bool,
    ) -> errors::error::Result<bool> {
        execute_simple(
            initiated,
            batcher,
            return_result,
            mgr.drop_scope(self.name, self.options),
            |_| Ok(create_success_sdk_result()),
        )
        .await
    }
}

#[derive(Clone)]
pub struct CreateCollectionCommandType {
    collection_name: String,
    scope_name: String,
    settings: Option<CreateCollectionSettings>,
    options: Option<CreateCollectionOptions>,
}

impl CreateCollectionCommandType {
    pub fn new(
        scope_name: String,
        collection_name: String,
        settings: Option<CreateCollectionSettings>,
        options: Option<CreateCollectionOptions>,
    ) -> Self {
        Self {
            collection_name,
            scope_name,
            settings,
            options,
        }
    }

    pub async fn execute(
        self,
        mgr: CollectionManager,
        initiated: Timestamp,
        batcher: &crate::common::batcher::Batcher,
        return_result: bool,
    ) -> errors::error::Result<bool> {
        execute_simple(
            initiated,
            batcher,
            return_result,
            mgr.create_collection(
                self.scope_name,
                self.collection_name,
                self.settings,
                self.options,
            ),
            |_| Ok(create_success_sdk_result()),
        )
        .await
    }
}

#[derive(Clone)]
pub struct UpdateCollectionCommandType {
    collection_name: String,
    scope_name: String,
    settings: UpdateCollectionSettings,
    options: Option<UpdateCollectionOptions>,
}

impl UpdateCollectionCommandType {
    pub fn new(
        scope_name: String,
        collection_name: String,
        settings: UpdateCollectionSettings,
        options: Option<UpdateCollectionOptions>,
    ) -> Self {
        Self {
            collection_name,
            scope_name,
            settings,
            options,
        }
    }

    pub async fn execute(
        self,
        mgr: CollectionManager,
        initiated: Timestamp,
        batcher: &crate::common::batcher::Batcher,
        return_result: bool,
    ) -> errors::error::Result<bool> {
        execute_simple(
            initiated,
            batcher,
            return_result,
            mgr.update_collection(
                self.scope_name,
                self.collection_name,
                self.settings,
                self.options,
            ),
            |_| Ok(create_success_sdk_result()),
        )
        .await
    }
}

#[derive(Clone)]
pub struct DropCollectionCommandType {
    scope_name: String,
    collection_name: String,
    options: Option<DropCollectionOptions>,
}

impl DropCollectionCommandType {
    pub fn new(
        scope_name: String,
        collection_name: String,
        options: Option<DropCollectionOptions>,
    ) -> Self {
        Self {
            scope_name,
            collection_name,
            options,
        }
    }

    pub async fn execute(
        self,
        mgr: CollectionManager,
        initiated: Timestamp,
        batcher: &crate::common::batcher::Batcher,
        return_result: bool,
    ) -> errors::error::Result<bool> {
        execute_simple(
            initiated,
            batcher,
            return_result,
            mgr.drop_collection(self.scope_name, self.collection_name, self.options),
            |_| Ok(create_success_sdk_result()),
        )
        .await
    }
}
