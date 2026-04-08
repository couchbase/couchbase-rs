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

use crate::authenticator::Authenticator;
use crate::cbconfig::{CollectionManifest, FullBucketConfig, FullClusterConfig};
use crate::componentconfigs::NetworkAndCanonicalEndpoint;
use crate::error::ErrorKind;
use crate::httpcomponent::{HttpComponent, HttpComponentState};
use crate::httpx::client::Client;
use crate::httpx::request::Auth;
use crate::mgmtx::bucket_helper::EnsureBucketHelper;
use crate::mgmtx::bucket_settings::BucketDef;
use crate::mgmtx::group_helper::EnsureGroupHelper;
use crate::mgmtx::manifest_helper::EnsureManifestHelper;
use crate::mgmtx::mgmt::AutoFailoverSettings;
use crate::mgmtx::mgmt_query::IndexStatus;
use crate::mgmtx::node_target::NodeTarget;
use crate::mgmtx::options::{
    EnsureBucketPollOptions, EnsureGroupPollOptions, EnsureManifestPollOptions,
    EnsureUserPollOptions,
};
use crate::mgmtx::responses::{
    CreateCollectionResponse, CreateScopeResponse, DeleteCollectionResponse, DeleteScopeResponse,
    UpdateCollectionResponse,
};
use crate::mgmtx::user::{Group, RoleAndDescription, UserAndMetadata};
use crate::mgmtx::user_helper::EnsureUserHelper;
use crate::options::management::{
    ChangePasswordOptions, CreateBucketOptions, CreateCollectionOptions, CreateScopeOptions,
    DeleteBucketOptions, DeleteCollectionOptions, DeleteGroupOptions, DeleteScopeOptions,
    DeleteUserOptions, EnsureBucketOptions, EnsureGroupOptions, EnsureManifestOptions,
    EnsureUserOptions, FlushBucketOptions, GetAllBucketsOptions, GetAllGroupsOptions,
    GetAllUsersOptions, GetAutoFailoverSettingsOptions, GetBucketOptions, GetBucketStatsOptions,
    GetCollectionManifestOptions, GetFullBucketConfigOptions, GetFullClusterConfigOptions,
    GetGroupOptions, GetRolesOptions, GetUserOptions, IndexStatusOptions, LoadSampleBucketOptions,
    UpdateBucketOptions, UpdateCollectionOptions, UpsertGroupOptions, UpsertUserOptions,
};
use crate::retry::{orchestrate_retries, RetryManager, RetryRequest};
use crate::retrybesteffort::ExponentialBackoffCalculator;
use crate::service_type::ServiceType;
use crate::tracingcomponent::TracingComponent;
use crate::{error, mgmtx};
use serde_json::value::RawValue;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

pub(crate) struct MgmtComponent<C: Client> {
    id: String,
    http_component: HttpComponent<C>,
    tracing: Arc<TracingComponent>,

    retry_manager: Arc<RetryManager>,
}

pub(crate) struct MgmtComponentConfig {
    pub endpoints: HashMap<String, NetworkAndCanonicalEndpoint>,
    pub authenticator: Authenticator,
}

pub(crate) struct MgmtComponentOptions {
    pub id: String,
    pub user_agent: String,
}

impl<C: Client> MgmtComponent<C> {
    pub fn new(
        retry_manager: Arc<RetryManager>,
        http_client: Arc<C>,
        tracing: Arc<TracingComponent>,
        config: MgmtComponentConfig,
        opts: MgmtComponentOptions,
    ) -> Self {
        Self {
            id: opts.id,
            http_component: HttpComponent::new(
                ServiceType::MGMT,
                opts.user_agent,
                http_client,
                HttpComponentState::new(config.endpoints, config.authenticator),
            ),
            tracing,
            retry_manager,
        }
    }

    pub fn reconfigure(&self, config: MgmtComponentConfig) {
        debug!(
            "Management component {} updating endpoints to {:?}",
            self.id,
            &config.endpoints.keys().collect::<Vec<_>>()
        );

        self.http_component.reconfigure(HttpComponentState::new(
            config.endpoints,
            config.authenticator,
        ))
    }

    pub async fn get_collection_manifest(
        &self,
        opts: &GetCollectionManifestOptions<'_>,
    ) -> error::Result<CollectionManifest> {
        let retry_info = RetryRequest::new("get_collection_manifest", false);

        let copts = opts.into();

        orchestrate_retries(
            self.retry_manager.clone(),
            opts.retry_strategy.clone(),
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        None,
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            let res = match (mgmtx::mgmt::Management::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: self.tracing.clone(),
                            }
                            .get_collection_manifest(&copts)
                            .await)
                            {
                                Ok(r) => r,
                                Err(e) => return Err(ErrorKind::Mgmt(e).into()),
                            };

                            Ok(res)
                        },
                    )
                    .await
            },
        )
        .await
    }

    pub async fn create_scope(
        &self,
        opts: &CreateScopeOptions<'_>,
    ) -> error::Result<CreateScopeResponse> {
        let retry_info = RetryRequest::new("create_scope", false);

        let copts = opts.into();

        orchestrate_retries(
            self.retry_manager.clone(),
            opts.retry_strategy.clone(),
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        None,
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            let res = match (mgmtx::mgmt::Management::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: self.tracing.clone(),
                            }
                            .create_scope(&copts)
                            .await)
                            {
                                Ok(r) => r,
                                Err(e) => return Err(ErrorKind::Mgmt(e).into()),
                            };

                            Ok(res)
                        },
                    )
                    .await
            },
        )
        .await
    }

    pub async fn delete_scope(
        &self,
        opts: &DeleteScopeOptions<'_>,
    ) -> error::Result<DeleteScopeResponse> {
        let retry_info = RetryRequest::new("delete_scope", false);

        let copts = opts.into();

        orchestrate_retries(
            self.retry_manager.clone(),
            opts.retry_strategy.clone(),
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        None,
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            let res = match (mgmtx::mgmt::Management::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: self.tracing.clone(),
                            }
                            .delete_scope(&copts)
                            .await)
                            {
                                Ok(r) => r,
                                Err(e) => return Err(ErrorKind::Mgmt(e).into()),
                            };

                            Ok(res)
                        },
                    )
                    .await
            },
        )
        .await
    }

    pub async fn create_collection(
        &self,
        opts: &CreateCollectionOptions<'_>,
    ) -> error::Result<CreateCollectionResponse> {
        let retry_info = RetryRequest::new("create_collection", false);

        let copts = opts.into();

        orchestrate_retries(
            self.retry_manager.clone(),
            opts.retry_strategy.clone(),
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        None,
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            let res = match (mgmtx::mgmt::Management::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: self.tracing.clone(),
                            }
                            .create_collection(&copts)
                            .await)
                            {
                                Ok(r) => r,
                                Err(e) => return Err(ErrorKind::Mgmt(e).into()),
                            };

                            Ok(res)
                        },
                    )
                    .await
            },
        )
        .await
    }

    pub async fn delete_collection(
        &self,
        opts: &DeleteCollectionOptions<'_>,
    ) -> error::Result<DeleteCollectionResponse> {
        let retry_info = RetryRequest::new("delete_collection", false);

        let copts = opts.into();

        orchestrate_retries(
            self.retry_manager.clone(),
            opts.retry_strategy.clone(),
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        None,
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            let res = match (mgmtx::mgmt::Management::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: self.tracing.clone(),
                            }
                            .delete_collection(&copts)
                            .await)
                            {
                                Ok(r) => r,
                                Err(e) => return Err(ErrorKind::Mgmt(e).into()),
                            };

                            Ok(res)
                        },
                    )
                    .await
            },
        )
        .await
    }

    pub async fn update_collection(
        &self,
        opts: &UpdateCollectionOptions<'_>,
    ) -> error::Result<UpdateCollectionResponse> {
        let retry_info = RetryRequest::new("update_collection", false);

        let copts = opts.into();

        orchestrate_retries(
            self.retry_manager.clone(),
            opts.retry_strategy.clone(),
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        None,
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            let res = match (mgmtx::mgmt::Management::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: self.tracing.clone(),
                            }
                            .update_collection(&copts)
                            .await)
                            {
                                Ok(r) => r,
                                Err(e) => return Err(ErrorKind::Mgmt(e).into()),
                            };

                            Ok(res)
                        },
                    )
                    .await
            },
        )
        .await
    }

    pub async fn get_all_buckets(
        &self,
        opts: &GetAllBucketsOptions<'_>,
    ) -> error::Result<Vec<BucketDef>> {
        let retry_info = RetryRequest::new("get_all_buckets", false);

        let copts = opts.into();

        orchestrate_retries(
            self.retry_manager.clone(),
            opts.retry_strategy.clone(),
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        None,
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            mgmtx::mgmt::Management::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: self.tracing.clone(),
                            }
                            .get_all_buckets(&copts)
                            .await
                            .map_err(|e| ErrorKind::Mgmt(e).into())
                        },
                    )
                    .await
            },
        )
        .await
    }

    pub async fn get_bucket(&self, opts: &GetBucketOptions<'_>) -> error::Result<BucketDef> {
        let retry_info = RetryRequest::new("get_bucket", false);

        let copts = opts.into();

        orchestrate_retries(
            self.retry_manager.clone(),
            opts.retry_strategy.clone(),
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        None,
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            mgmtx::mgmt::Management::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: self.tracing.clone(),
                            }
                            .get_bucket(&copts)
                            .await
                            .map_err(|e| ErrorKind::Mgmt(e).into())
                        },
                    )
                    .await
            },
        )
        .await
    }

    pub async fn create_bucket(&self, opts: &CreateBucketOptions<'_>) -> error::Result<()> {
        let retry_info = RetryRequest::new("create_bucket", false);

        let copts = opts.into();

        orchestrate_retries(
            self.retry_manager.clone(),
            opts.retry_strategy.clone(),
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        None,
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            mgmtx::mgmt::Management::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: self.tracing.clone(),
                            }
                            .create_bucket(&copts)
                            .await
                            .map_err(|e| ErrorKind::Mgmt(e).into())
                        },
                    )
                    .await
            },
        )
        .await
    }

    pub async fn update_bucket(&self, opts: &UpdateBucketOptions<'_>) -> error::Result<()> {
        let retry_info = RetryRequest::new("update_bucket", false);

        let copts = opts.into();

        orchestrate_retries(
            self.retry_manager.clone(),
            opts.retry_strategy.clone(),
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        None,
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            mgmtx::mgmt::Management::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: self.tracing.clone(),
                            }
                            .update_bucket(&copts)
                            .await
                            .map_err(|e| ErrorKind::Mgmt(e).into())
                        },
                    )
                    .await
            },
        )
        .await
    }

    pub async fn delete_bucket(&self, opts: &DeleteBucketOptions<'_>) -> error::Result<()> {
        let retry_info = RetryRequest::new("delete_bucket", false);

        let copts = opts.into();

        orchestrate_retries(
            self.retry_manager.clone(),
            opts.retry_strategy.clone(),
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        None,
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            mgmtx::mgmt::Management::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: self.tracing.clone(),
                            }
                            .delete_bucket(&copts)
                            .await
                            .map_err(|e| ErrorKind::Mgmt(e).into())
                        },
                    )
                    .await
            },
        )
        .await
    }

    pub async fn flush_bucket(&self, opts: &FlushBucketOptions<'_>) -> error::Result<()> {
        let retry_info = RetryRequest::new("flush_bucket", false);

        let copts = opts.into();

        orchestrate_retries(
            self.retry_manager.clone(),
            opts.retry_strategy.clone(),
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        None,
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            mgmtx::mgmt::Management::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: self.tracing.clone(),
                            }
                            .flush_bucket(&copts)
                            .await
                            .map_err(|e| ErrorKind::Mgmt(e).into())
                        },
                    )
                    .await
            },
        )
        .await
    }

    pub async fn ensure_manifest(&self, opts: &EnsureManifestOptions<'_>) -> error::Result<()> {
        let mut helper = EnsureManifestHelper::new(
            self.http_component.user_agent(),
            opts.bucket_name,
            opts.manifest_uid,
            opts.on_behalf_of_info,
        );

        let backoff = ExponentialBackoffCalculator::new(
            Duration::from_millis(100),
            Duration::from_millis(1000),
            1.5,
        );

        self.http_component
            .ensure_resource(backoff, async |client: Arc<C>, targets: Vec<NodeTarget>| {
                helper
                    .clone()
                    .poll(&EnsureManifestPollOptions { client, targets })
                    .await
                    .map_err(error::Error::from)
            })
            .await
    }

    pub async fn ensure_bucket(&self, opts: &EnsureBucketOptions<'_>) -> error::Result<()> {
        let mut helper = EnsureBucketHelper::new(
            self.http_component.user_agent(),
            opts.bucket_name,
            opts.bucket_uuid,
            opts.want_missing,
            opts.on_behalf_of_info,
        );

        let backoff = ExponentialBackoffCalculator::new(
            Duration::from_millis(100),
            Duration::from_millis(1000),
            1.5,
        );

        self.http_component
            .ensure_resource(backoff, async |client: Arc<C>, targets: Vec<NodeTarget>| {
                helper
                    .clone()
                    .poll(&EnsureBucketPollOptions { client, targets })
                    .await
                    .map_err(error::Error::from)
            })
            .await
    }

    pub async fn get_user(&self, opts: &GetUserOptions<'_>) -> error::Result<UserAndMetadata> {
        let retry_info = RetryRequest::new("get_user", false);
        let copts = opts.into();

        orchestrate_retries(
            self.retry_manager.clone(),
            opts.retry_strategy.clone(),
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        None,
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            mgmtx::mgmt::Management::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: self.tracing.clone(),
                            }
                            .get_user(&copts)
                            .await
                            .map_err(|e| ErrorKind::Mgmt(e).into())
                        },
                    )
                    .await
            },
        )
        .await
    }

    pub async fn get_all_users(
        &self,
        opts: &GetAllUsersOptions<'_>,
    ) -> error::Result<Vec<UserAndMetadata>> {
        let retry_info = RetryRequest::new("get_all_users", false);
        let copts = opts.into();

        orchestrate_retries(
            self.retry_manager.clone(),
            opts.retry_strategy.clone(),
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        None,
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            mgmtx::mgmt::Management::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: self.tracing.clone(),
                            }
                            .get_all_users(&copts)
                            .await
                            .map_err(|e| ErrorKind::Mgmt(e).into())
                        },
                    )
                    .await
            },
        )
        .await
    }

    pub async fn upsert_user(&self, opts: &UpsertUserOptions<'_>) -> error::Result<()> {
        let retry_info = RetryRequest::new("upsert_user", false);
        let copts = opts.into();

        orchestrate_retries(
            self.retry_manager.clone(),
            opts.retry_strategy.clone(),
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        None,
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            mgmtx::mgmt::Management::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: self.tracing.clone(),
                            }
                            .upsert_user(&copts)
                            .await
                            .map_err(|e| ErrorKind::Mgmt(e).into())
                        },
                    )
                    .await
            },
        )
        .await
    }

    pub async fn delete_user(&self, opts: &DeleteUserOptions<'_>) -> error::Result<()> {
        let retry_info = RetryRequest::new("delete_user", false);
        let copts = opts.into();

        orchestrate_retries(
            self.retry_manager.clone(),
            opts.retry_strategy.clone(),
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        None,
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            mgmtx::mgmt::Management::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: self.tracing.clone(),
                            }
                            .delete_user(&copts)
                            .await
                            .map_err(|e| ErrorKind::Mgmt(e).into())
                        },
                    )
                    .await
            },
        )
        .await
    }

    pub async fn get_roles(
        &self,
        opts: &GetRolesOptions<'_>,
    ) -> error::Result<Vec<RoleAndDescription>> {
        let retry_info = RetryRequest::new("get_roles", false);
        let copts = opts.into();

        orchestrate_retries(
            self.retry_manager.clone(),
            opts.retry_strategy.clone(),
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        None,
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            mgmtx::mgmt::Management::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: self.tracing.clone(),
                            }
                            .get_roles(&copts)
                            .await
                            .map_err(|e| ErrorKind::Mgmt(e).into())
                        },
                    )
                    .await
            },
        )
        .await
    }

    pub async fn get_group(&self, opts: &GetGroupOptions<'_>) -> error::Result<Group> {
        let retry_info = RetryRequest::new("get_group", false);
        let copts = opts.into();

        orchestrate_retries(
            self.retry_manager.clone(),
            opts.retry_strategy.clone(),
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        None,
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            mgmtx::mgmt::Management::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: self.tracing.clone(),
                            }
                            .get_group(&copts)
                            .await
                            .map_err(|e| ErrorKind::Mgmt(e).into())
                        },
                    )
                    .await
            },
        )
        .await
    }

    pub async fn get_all_groups(
        &self,
        opts: &GetAllGroupsOptions<'_>,
    ) -> error::Result<Vec<Group>> {
        let retry_info = RetryRequest::new("get_all_groups", false);
        let copts = opts.into();

        orchestrate_retries(
            self.retry_manager.clone(),
            opts.retry_strategy.clone(),
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        None,
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            mgmtx::mgmt::Management::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: self.tracing.clone(),
                            }
                            .get_all_groups(&copts)
                            .await
                            .map_err(|e| ErrorKind::Mgmt(e).into())
                        },
                    )
                    .await
            },
        )
        .await
    }

    pub async fn upsert_group(&self, opts: &UpsertGroupOptions<'_>) -> error::Result<()> {
        let retry_info = RetryRequest::new("upsert_group", false);
        let copts = opts.into();

        orchestrate_retries(
            self.retry_manager.clone(),
            opts.retry_strategy.clone(),
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        None,
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            mgmtx::mgmt::Management::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: self.tracing.clone(),
                            }
                            .upsert_group(&copts)
                            .await
                            .map_err(|e| ErrorKind::Mgmt(e).into())
                        },
                    )
                    .await
            },
        )
        .await
    }

    pub async fn delete_group(&self, opts: &DeleteGroupOptions<'_>) -> error::Result<()> {
        let retry_info = RetryRequest::new("delete_group", false);
        let copts = opts.into();

        orchestrate_retries(
            self.retry_manager.clone(),
            opts.retry_strategy.clone(),
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        None,
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            mgmtx::mgmt::Management::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: self.tracing.clone(),
                            }
                            .delete_group(&copts)
                            .await
                            .map_err(|e| ErrorKind::Mgmt(e).into())
                        },
                    )
                    .await
            },
        )
        .await
    }

    pub async fn change_password(&self, opts: &ChangePasswordOptions<'_>) -> error::Result<()> {
        let retry_info = RetryRequest::new("change_password", false);
        let copts = opts.into();

        orchestrate_retries(
            self.retry_manager.clone(),
            opts.retry_strategy.clone(),
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        None,
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            mgmtx::mgmt::Management::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: self.tracing.clone(),
                            }
                            .change_password(&copts)
                            .await
                            .map_err(|e| ErrorKind::Mgmt(e).into())
                        },
                    )
                    .await
            },
        )
        .await
    }

    pub async fn ensure_user(&self, opts: &EnsureUserOptions<'_>) -> error::Result<()> {
        let mut helper = EnsureUserHelper::new(
            self.http_component.user_agent(),
            opts.username,
            opts.auth_domain,
            opts.want_missing,
            opts.on_behalf_of_info,
        );

        let backoff = ExponentialBackoffCalculator::new(
            Duration::from_millis(100),
            Duration::from_millis(1000),
            1.5,
        );

        self.http_component
            .ensure_resource(backoff, async |client: Arc<C>, targets: Vec<NodeTarget>| {
                helper
                    .clone()
                    .poll(&EnsureUserPollOptions { client, targets })
                    .await
                    .map_err(error::Error::from)
            })
            .await
    }

    pub async fn ensure_group(&self, opts: &EnsureGroupOptions<'_>) -> error::Result<()> {
        let mut helper = EnsureGroupHelper::new(
            self.http_component.user_agent(),
            opts.group_name,
            opts.want_missing,
            opts.on_behalf_of_info,
        );

        let backoff = ExponentialBackoffCalculator::new(
            Duration::from_millis(100),
            Duration::from_millis(1000),
            1.5,
        );

        self.http_component
            .ensure_resource(backoff, async |client: Arc<C>, targets: Vec<NodeTarget>| {
                helper
                    .clone()
                    .poll(&EnsureGroupPollOptions { client, targets })
                    .await
                    .map_err(error::Error::from)
            })
            .await
    }

    pub async fn get_full_cluster_config(
        &self,
        opts: &GetFullClusterConfigOptions<'_>,
    ) -> error::Result<FullClusterConfig> {
        let retry_info = RetryRequest::new("get_full_cluster_config", false);
        let copts = opts.into();

        orchestrate_retries(
            self.retry_manager.clone(),
            opts.retry_strategy.clone(),
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        None,
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            mgmtx::mgmt::Management::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: Default::default(),
                            }
                            .get_full_cluster_config(&copts)
                            .await
                            .map_err(|e| ErrorKind::Mgmt(e).into())
                        },
                    )
                    .await
            },
        )
        .await
    }

    pub async fn get_full_bucket_config(
        &self,
        opts: &GetFullBucketConfigOptions<'_>,
    ) -> error::Result<FullBucketConfig> {
        let retry_info = RetryRequest::new("get_full_bucket_config", false);
        let copts = opts.into();

        orchestrate_retries(
            self.retry_manager.clone(),
            opts.retry_strategy.clone(),
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        None,
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            mgmtx::mgmt::Management::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: Default::default(),
                            }
                            .get_full_bucket_config(&copts)
                            .await
                            .map_err(|e| ErrorKind::Mgmt(e).into())
                        },
                    )
                    .await
            },
        )
        .await
    }

    pub async fn load_sample_bucket(
        &self,
        opts: &LoadSampleBucketOptions<'_>,
    ) -> error::Result<()> {
        let retry_info = RetryRequest::new("load_sample_bucket", false);
        let copts = opts.into();

        orchestrate_retries(
            self.retry_manager.clone(),
            opts.retry_strategy.clone(),
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        None,
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            mgmtx::mgmt::Management::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: Default::default(),
                            }
                            .load_sample_bucket(&copts)
                            .await
                            .map_err(|e| ErrorKind::Mgmt(e).into())
                        },
                    )
                    .await
            },
        )
        .await
    }

    pub async fn index_status(&self, opts: &IndexStatusOptions<'_>) -> error::Result<IndexStatus> {
        let retry_info = RetryRequest::new("index_status", false);
        let copts = opts.into();

        orchestrate_retries(
            self.retry_manager.clone(),
            opts.retry_strategy.clone(),
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        None,
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            mgmtx::mgmt::Management::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: Default::default(),
                            }
                            .index_status(&copts)
                            .await
                            .map_err(|e| ErrorKind::Mgmt(e).into())
                        },
                    )
                    .await
            },
        )
        .await
    }

    pub async fn get_auto_failover_settings(
        &self,
        opts: &GetAutoFailoverSettingsOptions<'_>,
    ) -> error::Result<AutoFailoverSettings> {
        let retry_info = RetryRequest::new("get_auto_failover_settings", false);
        let copts = opts.into();

        orchestrate_retries(
            self.retry_manager.clone(),
            opts.retry_strategy.clone(),
            retry_info,
            async || {
                self.http_component
                    .orchestrate_endpoint(
                        None,
                        async |client: Arc<C>,
                               endpoint_id: String,
                               endpoint: String,
                               canonical_endpoint: String,
                               auth: Auth| {
                            mgmtx::mgmt::Management::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint,
                                canonical_endpoint,
                                auth,
                                tracing: Default::default(),
                            }
                            .get_auto_failover_settings(&copts)
                            .await
                            .map_err(|e| ErrorKind::Mgmt(e).into())
                        },
                    )
                    .await
            },
        )
        .await
    }

    pub async fn get_bucket_stats(
        &self,
        opts: &GetBucketStatsOptions<'_>,
    ) -> error::Result<Box<RawValue>> {
        let retry_info = RetryRequest::new("get_bucket_stats", false);
        let retry = opts.retry_strategy.clone();
        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry, retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>,
                           endpoint_id: String,
                           endpoint: String,
                           canonical_endpoint: String,
                           auth: Auth| {
                        mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint,
                            canonical_endpoint,
                            auth,
                            tracing: Default::default(),
                        }
                        .get_bucket_stats(&copts)
                        .await
                        .map_err(|e| ErrorKind::Mgmt(e).into())
                    },
                )
                .await
        })
        .await
    }
}
