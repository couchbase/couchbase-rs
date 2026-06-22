use crate::commands::execution::execute_simple;
use crate::commands::helpers::create_success_sdk_result;
use crate::errors;
use crate::observability::span_owner::SpanOwner;
use crate::proto::protocol::sdk::cluster::bucket_manager;
use crate::proto::protocol::sdk::cluster::bucket_manager::command::Command;
use crate::proto::protocol::sdk::cluster::bucket_manager::GetAllBucketsResult;
use crate::proto::protocol::{run, sdk};
use couchbase::cluster::Cluster;
use couchbase::management::buckets::bucket_manager::BucketManager;
use couchbase::management::buckets::bucket_settings::BucketSettings;
use couchbase::options::bucket_mgmt_options::{
    CreateBucketOptions, DropBucketOptions, FlushBucketOptions, GetAllBucketsOptions,
    GetBucketOptions, UpdateBucketOptions,
};
use prost_types::Timestamp;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::Instrument;

#[derive(Clone)]
pub struct BucketManagerCommand {
    cluster: Cluster,
    return_result: bool,
    initiated: Timestamp,
    command_type: BucketManagerCommandType,
    parent_span: Option<tracing::Span>,
}

#[derive(Clone)]
pub enum BucketManagerCommandType {
    GetBucket(GetBucketCommandType),
    GetAllBuckets(GetAllBucketsCommandType),
    CreateBucket(CreateBucketCommandType),
    DropBucket(DropBucketCommandType),
    FlushBucket(FlushBucketCommandType),
    UpdateBucket(UpdateBucketCommandType),
}

impl BucketManagerCommand {
    pub fn new(
        cluster: Cluster,
        command: Command,
        return_result: bool,
        span_owner: Arc<SpanOwner>,
    ) -> errors::error::Result<Self> {
        let parent_span = match &command {
            Command::GetBucket(cmd) => cmd
                .options
                .as_ref()
                .and_then(|o| o.parent_span_id.as_deref()),
            Command::GetAllBuckets(cmd) => cmd
                .options
                .as_ref()
                .and_then(|o| o.parent_span_id.as_deref()),
            Command::CreateBucket(cmd) => cmd
                .options
                .as_ref()
                .and_then(|o| o.parent_span_id.as_deref()),
            Command::DropBucket(cmd) => cmd
                .options
                .as_ref()
                .and_then(|o| o.parent_span_id.as_deref()),
            Command::FlushBucket(cmd) => cmd
                .options
                .as_ref()
                .and_then(|o| o.parent_span_id.as_deref()),
            Command::UpdateBucket(cmd) => cmd
                .options
                .as_ref()
                .and_then(|o| o.parent_span_id.as_deref()),
        }
        .and_then(|id| span_owner.get(id));

        let command_type = match command {
            Command::GetBucket(cmd) => {
                let opts = cmd.options.map(|o| o.try_into()).transpose()?;
                BucketManagerCommandType::GetBucket(GetBucketCommandType::new(
                    cmd.bucket_name,
                    opts,
                ))
            }
            Command::GetAllBuckets(cmd) => {
                let opts = cmd.options.map(|o| o.try_into()).transpose()?;
                BucketManagerCommandType::GetAllBuckets(GetAllBucketsCommandType::new(opts))
            }
            Command::CreateBucket(cmd) => {
                let opts = cmd.options.map(|o| o.try_into()).transpose()?;
                let settings = cmd
                    .settings
                    .ok_or_else(|| {
                        errors::error::Error::unimplemented("bucket settings is required")
                    })?
                    .try_into()?;

                BucketManagerCommandType::CreateBucket(CreateBucketCommandType::new(settings, opts))
            }
            Command::DropBucket(cmd) => {
                let opts = cmd.options.map(|o| o.try_into()).transpose()?;
                BucketManagerCommandType::DropBucket(DropBucketCommandType::new(
                    cmd.bucket_name,
                    opts,
                ))
            }
            Command::FlushBucket(cmd) => {
                let opts = cmd.options.map(|o| o.try_into()).transpose()?;
                BucketManagerCommandType::FlushBucket(FlushBucketCommandType::new(
                    cmd.bucket_name,
                    opts,
                ))
            }
            Command::UpdateBucket(cmd) => {
                let opts = cmd.options.map(|o| o.try_into()).transpose()?;
                let settings = cmd
                    .settings
                    .ok_or_else(|| {
                        errors::error::Error::unimplemented("bucket settings is required")
                    })?
                    .try_into()?;

                BucketManagerCommandType::UpdateBucket(UpdateBucketCommandType::new(settings, opts))
            }
        };

        Ok(Self {
            cluster,
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
        let mgr = self.cluster.buckets();
        match self.command_type {
            BucketManagerCommandType::GetBucket(cmd) => {
                let fut = cmd.execute(mgr, self.initiated, batcher, self.return_result);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
            BucketManagerCommandType::GetAllBuckets(cmd) => {
                let fut = cmd.execute(mgr, self.initiated, batcher, self.return_result);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
            BucketManagerCommandType::CreateBucket(cmd) => {
                let fut = cmd.execute(mgr, self.initiated, batcher, self.return_result);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
            BucketManagerCommandType::DropBucket(cmd) => {
                let fut = cmd.execute(mgr, self.initiated, batcher, self.return_result);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
            BucketManagerCommandType::FlushBucket(cmd) => {
                let fut = cmd.execute(mgr, self.initiated, batcher, self.return_result);
                match self.parent_span {
                    Some(span) => fut.instrument(span).await,
                    None => fut.await,
                }
            }
            BucketManagerCommandType::UpdateBucket(cmd) => {
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
pub struct GetBucketCommandType {
    bucket_name: String,
    options: Option<GetBucketOptions>,
}

impl GetBucketCommandType {
    pub fn new(bucket_name: String, options: Option<GetBucketOptions>) -> Self {
        Self {
            bucket_name,
            options,
        }
    }

    pub async fn execute(
        self,
        mgr: BucketManager,
        initiated: Timestamp,
        batcher: &crate::common::batcher::Batcher,
        return_result: bool,
    ) -> errors::error::Result<bool> {
        execute_simple(
            initiated,
            batcher,
            return_result,
            mgr.get_bucket(&self.bucket_name, self.options),
            |bucket| {
                Ok(run::result::Result::Sdk(sdk::Result {
                    result: Some(sdk::result::Result::BucketManagerResult(
                        bucket_manager::Result {
                            result: Some(bucket_manager::result::Result::BucketSettings(
                                bucket.try_into()?,
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
pub struct GetAllBucketsCommandType {
    options: Option<GetAllBucketsOptions>,
}

impl GetAllBucketsCommandType {
    pub fn new(options: Option<GetAllBucketsOptions>) -> Self {
        Self { options }
    }

    pub async fn execute(
        self,
        mgr: BucketManager,
        initiated: Timestamp,
        batcher: &crate::common::batcher::Batcher,
        return_result: bool,
    ) -> errors::error::Result<bool> {
        execute_simple(
            initiated,
            batcher,
            return_result,
            mgr.get_all_buckets(self.options),
            |buckets| {
                let mut result = HashMap::new();
                for bucket in buckets {
                    result.insert(bucket.name.clone(), bucket.try_into()?);
                }

                Ok(run::result::Result::Sdk(sdk::Result {
                    result: Some(sdk::result::Result::BucketManagerResult(
                        bucket_manager::Result {
                            result: Some(bucket_manager::result::Result::GetAllBucketsResult(
                                GetAllBucketsResult { result },
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
pub struct CreateBucketCommandType {
    settings: BucketSettings,
    options: Option<CreateBucketOptions>,
}

impl CreateBucketCommandType {
    pub fn new(settings: BucketSettings, options: Option<CreateBucketOptions>) -> Self {
        Self { settings, options }
    }

    pub async fn execute(
        self,
        mgr: BucketManager,
        initiated: Timestamp,
        batcher: &crate::common::batcher::Batcher,
        return_result: bool,
    ) -> errors::error::Result<bool> {
        execute_simple(
            initiated,
            batcher,
            return_result,
            mgr.create_bucket(self.settings, self.options.clone()),
            |_| Ok(create_success_sdk_result()),
        )
        .await
    }
}

#[derive(Clone)]
pub struct DropBucketCommandType {
    bucket_name: String,
    options: Option<DropBucketOptions>,
}

impl DropBucketCommandType {
    pub fn new(bucket_name: String, options: Option<DropBucketOptions>) -> Self {
        Self {
            bucket_name,
            options,
        }
    }

    pub async fn execute(
        self,
        mgr: BucketManager,
        initiated: Timestamp,
        batcher: &crate::common::batcher::Batcher,
        return_result: bool,
    ) -> errors::error::Result<bool> {
        execute_simple(
            initiated,
            batcher,
            return_result,
            mgr.drop_bucket(&self.bucket_name, self.options),
            |_| Ok(create_success_sdk_result()),
        )
        .await
    }
}

#[derive(Clone)]
pub struct FlushBucketCommandType {
    bucket_name: String,
    options: Option<FlushBucketOptions>,
}

impl FlushBucketCommandType {
    pub fn new(bucket_name: String, options: Option<FlushBucketOptions>) -> Self {
        Self {
            bucket_name,
            options,
        }
    }

    pub async fn execute(
        self,
        mgr: BucketManager,
        initiated: Timestamp,
        batcher: &crate::common::batcher::Batcher,
        return_result: bool,
    ) -> errors::error::Result<bool> {
        execute_simple(
            initiated,
            batcher,
            return_result,
            mgr.flush_bucket(&self.bucket_name, self.options),
            |_| Ok(create_success_sdk_result()),
        )
        .await
    }
}

#[derive(Clone)]
pub struct UpdateBucketCommandType {
    settings: BucketSettings,
    options: Option<UpdateBucketOptions>,
}

impl UpdateBucketCommandType {
    pub fn new(settings: BucketSettings, options: Option<UpdateBucketOptions>) -> Self {
        Self { settings, options }
    }

    pub async fn execute(
        self,
        mgr: BucketManager,
        initiated: Timestamp,
        batcher: &crate::common::batcher::Batcher,
        return_result: bool,
    ) -> errors::error::Result<bool> {
        execute_simple(
            initiated,
            batcher,
            return_result,
            mgr.update_bucket(self.settings, self.options),
            |_| Ok(create_success_sdk_result()),
        )
        .await
    }
}
