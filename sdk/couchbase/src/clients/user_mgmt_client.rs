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

use crate::clients::agent_provider::CouchbaseAgentProvider;
use crate::clients::tracing_client::{CouchbaseTracingClient, TracingClient, TracingClientBackend};
use crate::error;
use crate::management::users::user::{Group, RoleAndDescription, User, UserAndMetadata};
use crate::options::user_mgmt_options::{
    ChangePasswordOptions, DropGroupOptions, DropUserOptions, GetAllGroupsOptions,
    GetAllUsersOptions, GetGroupOptions, GetRolesOptions, GetUserOptions, UpsertGroupOptions,
    UpsertUserOptions,
};
use crate::retry::RetryStrategy;
use crate::timeouts::{with_timeout, Timeouts};
use std::sync::Arc;

const DEFAULT_AUTH_DOMAIN: &str = "local";

pub(crate) struct UserMgmtClient {
    backend: UserMgmtClientBackend,
}

impl UserMgmtClient {
    pub fn new(backend: UserMgmtClientBackend) -> Self {
        Self { backend }
    }

    pub async fn get_user(
        &self,
        username: String,
        opts: GetUserOptions,
    ) -> error::Result<UserAndMetadata> {
        match &self.backend {
            UserMgmtClientBackend::CouchbaseUserMgmtClientBackend(client) => {
                client.get_user(username, opts).await
            }
            UserMgmtClientBackend::Couchbase2UserMgmtClientBackend(client) => {
                client.get_user(username, opts).await
            }
        }
    }

    pub async fn get_all_users(
        &self,
        opts: GetAllUsersOptions,
    ) -> error::Result<Vec<UserAndMetadata>> {
        match &self.backend {
            UserMgmtClientBackend::CouchbaseUserMgmtClientBackend(client) => {
                client.get_all_users(opts).await
            }
            UserMgmtClientBackend::Couchbase2UserMgmtClientBackend(client) => {
                client.get_all_users(opts).await
            }
        }
    }

    pub async fn upsert_user(&self, user: User, opts: UpsertUserOptions) -> error::Result<()> {
        match &self.backend {
            UserMgmtClientBackend::CouchbaseUserMgmtClientBackend(client) => {
                client.upsert_user(user, opts).await
            }
            UserMgmtClientBackend::Couchbase2UserMgmtClientBackend(client) => {
                client.upsert_user(user, opts).await
            }
        }
    }

    pub async fn drop_user(&self, username: String, opts: DropUserOptions) -> error::Result<()> {
        match &self.backend {
            UserMgmtClientBackend::CouchbaseUserMgmtClientBackend(client) => {
                client.drop_user(username, opts).await
            }
            UserMgmtClientBackend::Couchbase2UserMgmtClientBackend(client) => {
                client.drop_user(username, opts).await
            }
        }
    }

    pub async fn get_roles(&self, opts: GetRolesOptions) -> error::Result<Vec<RoleAndDescription>> {
        match &self.backend {
            UserMgmtClientBackend::CouchbaseUserMgmtClientBackend(client) => {
                client.get_roles(opts).await
            }
            UserMgmtClientBackend::Couchbase2UserMgmtClientBackend(client) => {
                client.get_roles(opts).await
            }
        }
    }

    pub async fn get_group(
        &self,
        group_name: String,
        opts: GetGroupOptions,
    ) -> error::Result<Group> {
        match &self.backend {
            UserMgmtClientBackend::CouchbaseUserMgmtClientBackend(client) => {
                client.get_group(group_name, opts).await
            }
            UserMgmtClientBackend::Couchbase2UserMgmtClientBackend(client) => {
                client.get_group(group_name, opts).await
            }
        }
    }

    pub async fn get_all_groups(&self, opts: GetAllGroupsOptions) -> error::Result<Vec<Group>> {
        match &self.backend {
            UserMgmtClientBackend::CouchbaseUserMgmtClientBackend(client) => {
                client.get_all_groups(opts).await
            }
            UserMgmtClientBackend::Couchbase2UserMgmtClientBackend(client) => {
                client.get_all_groups(opts).await
            }
        }
    }

    pub async fn upsert_group(&self, group: Group, opts: UpsertGroupOptions) -> error::Result<()> {
        match &self.backend {
            UserMgmtClientBackend::CouchbaseUserMgmtClientBackend(client) => {
                client.upsert_group(group, opts).await
            }
            UserMgmtClientBackend::Couchbase2UserMgmtClientBackend(client) => {
                client.upsert_group(group, opts).await
            }
        }
    }

    pub async fn drop_group(
        &self,
        group_name: String,
        opts: DropGroupOptions,
    ) -> error::Result<()> {
        match &self.backend {
            UserMgmtClientBackend::CouchbaseUserMgmtClientBackend(client) => {
                client.drop_group(group_name, opts).await
            }
            UserMgmtClientBackend::Couchbase2UserMgmtClientBackend(client) => {
                client.drop_group(group_name, opts).await
            }
        }
    }

    pub async fn change_password(
        &self,
        new_password: String,
        opts: ChangePasswordOptions,
    ) -> error::Result<()> {
        match &self.backend {
            UserMgmtClientBackend::CouchbaseUserMgmtClientBackend(client) => {
                client.change_password(new_password, opts).await
            }
            UserMgmtClientBackend::Couchbase2UserMgmtClientBackend(client) => {
                client.change_password(new_password, opts).await
            }
        }
    }

    pub fn tracing_client(&self) -> TracingClient {
        match &self.backend {
            UserMgmtClientBackend::CouchbaseUserMgmtClientBackend(client) => TracingClient::new(
                TracingClientBackend::CouchbaseTracingClientBackend(client.tracing_client()),
            ),
            UserMgmtClientBackend::Couchbase2UserMgmtClientBackend(_) => unimplemented!(),
        }
    }
}

pub(crate) enum UserMgmtClientBackend {
    CouchbaseUserMgmtClientBackend(CouchbaseUserMgmtClient),
    Couchbase2UserMgmtClientBackend(Couchbase2UserMgmtClient),
}

pub(crate) struct CouchbaseUserMgmtClient {
    agent_provider: CouchbaseAgentProvider,
    default_retry_strategy: Arc<dyn RetryStrategy>,
    default_timeouts: Timeouts,
}

impl CouchbaseUserMgmtClient {
    pub fn new(
        agent_provider: CouchbaseAgentProvider,
        default_retry_strategy: Arc<dyn RetryStrategy>,
        default_timeouts: Timeouts,
    ) -> Self {
        Self {
            agent_provider,
            default_retry_strategy,
            default_timeouts,
        }
    }

    pub async fn get_all_users(
        &self,
        opts: GetAllUsersOptions,
    ) -> error::Result<Vec<UserAndMetadata>> {
        let timeout = opts.timeout.unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, false, async move {
            let agent = self.agent_provider.get_agent().await;
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());
            let opts = couchbase_core::options::management::GetAllUsersOptions::new(
                opts.auth_domain.as_deref().unwrap_or(DEFAULT_AUTH_DOMAIN),
            )
            .retry_strategy(retry);

            let users = CouchbaseAgentProvider::upgrade_agent(agent)?
                .get_all_users(&opts)
                .await?;

            Ok(users.into_iter().map(|u| u.into()).collect())
        })
        .await
    }

    pub async fn get_user(
        &self,
        username: String,
        opts: GetUserOptions,
    ) -> error::Result<UserAndMetadata> {
        let timeout = opts.timeout.unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, false, async move {
            let agent = self.agent_provider.get_agent().await;
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());
            let opts = couchbase_core::options::management::GetUserOptions::new(
                &username,
                opts.auth_domain.as_deref().unwrap_or(DEFAULT_AUTH_DOMAIN),
            )
            .retry_strategy(retry);

            let user = CouchbaseAgentProvider::upgrade_agent(agent)?
                .get_user(&opts)
                .await?;

            Ok(user.into())
        })
        .await
    }

    pub async fn upsert_user(&self, user: User, opts: UpsertUserOptions) -> error::Result<()> {
        let timeout = opts.timeout.unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, true, async move {
            let agent = self.agent_provider.get_agent().await;
            let cuser = user.into();

            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());
            let opts = couchbase_core::options::management::UpsertUserOptions::new(
                &cuser,
                opts.auth_domain.as_deref().unwrap_or(DEFAULT_AUTH_DOMAIN),
            )
            .retry_strategy(retry);

            CouchbaseAgentProvider::upgrade_agent(agent)?
                .upsert_user(&opts)
                .await?;

            Ok(())
        })
        .await
    }

    pub async fn drop_user(&self, username: String, opts: DropUserOptions) -> error::Result<()> {
        let timeout = opts.timeout.unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, true, async move {
            let agent = self.agent_provider.get_agent().await;
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());
            let opts = couchbase_core::options::management::DeleteUserOptions::new(
                &username,
                opts.auth_domain.as_deref().unwrap_or(DEFAULT_AUTH_DOMAIN),
            )
            .retry_strategy(retry);

            CouchbaseAgentProvider::upgrade_agent(agent)?
                .delete_user(&opts)
                .await?;

            Ok(())
        })
        .await
    }

    pub async fn get_roles(&self, opts: GetRolesOptions) -> error::Result<Vec<RoleAndDescription>> {
        let timeout = opts.timeout.unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, false, async move {
            let agent = self.agent_provider.get_agent().await;
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());
            let opts =
                couchbase_core::options::management::GetRolesOptions::new().retry_strategy(retry);

            let roles = CouchbaseAgentProvider::upgrade_agent(agent)?
                .get_roles(&opts)
                .await?;

            Ok(roles.into_iter().map(|r| r.into()).collect())
        })
        .await
    }

    pub async fn get_group(
        &self,
        group_name: String,
        opts: GetGroupOptions,
    ) -> error::Result<Group> {
        let timeout = opts.timeout.unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, false, async move {
            let agent = self.agent_provider.get_agent().await;
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());
            let opts = couchbase_core::options::management::GetGroupOptions::new(&group_name)
                .retry_strategy(retry);

            let group = CouchbaseAgentProvider::upgrade_agent(agent)?
                .get_group(&opts)
                .await?;

            Ok(group.into())
        })
        .await
    }

    pub async fn get_all_groups(&self, opts: GetAllGroupsOptions) -> error::Result<Vec<Group>> {
        let timeout = opts.timeout.unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, false, async move {
            let agent = self.agent_provider.get_agent().await;
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());
            let opts = couchbase_core::options::management::GetAllGroupsOptions::new()
                .retry_strategy(retry);

            let groups = CouchbaseAgentProvider::upgrade_agent(agent)?
                .get_all_groups(&opts)
                .await?;

            Ok(groups.into_iter().map(|g| g.into()).collect())
        })
        .await
    }

    pub async fn upsert_group(&self, group: Group, opts: UpsertGroupOptions) -> error::Result<()> {
        let timeout = opts.timeout.unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, true, async move {
            let agent = self.agent_provider.get_agent().await;
            let cgroup = group.into();

            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());
            let opts = couchbase_core::options::management::UpsertGroupOptions::new(&cgroup)
                .retry_strategy(retry);

            CouchbaseAgentProvider::upgrade_agent(agent)?
                .upsert_group(&opts)
                .await?;

            Ok(())
        })
        .await
    }

    pub async fn drop_group(
        &self,
        group_name: String,
        opts: DropGroupOptions,
    ) -> error::Result<()> {
        let timeout = opts.timeout.unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, true, async move {
            let agent = self.agent_provider.get_agent().await;
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());
            let opts = couchbase_core::options::management::DeleteGroupOptions::new(&group_name)
                .retry_strategy(retry);

            CouchbaseAgentProvider::upgrade_agent(agent)?
                .delete_group(&opts)
                .await?;

            Ok(())
        })
        .await
    }

    pub async fn change_password(
        &self,
        new_password: String,
        opts: ChangePasswordOptions,
    ) -> error::Result<()> {
        let timeout = opts.timeout.unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, true, async move {
            let agent = self.agent_provider.get_agent().await;
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());
            let opts =
                couchbase_core::options::management::ChangePasswordOptions::new(&new_password)
                    .retry_strategy(retry);

            CouchbaseAgentProvider::upgrade_agent(agent)?
                .change_password(&opts)
                .await?;

            Ok(())
        })
        .await
    }

    pub fn tracing_client(&self) -> CouchbaseTracingClient {
        CouchbaseTracingClient::new(self.agent_provider.clone())
    }
}

pub(crate) struct Couchbase2UserMgmtClient {}

impl Couchbase2UserMgmtClient {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn get_user(
        &self,
        _username: String,
        _opts: GetUserOptions,
    ) -> error::Result<UserAndMetadata> {
        unimplemented!()
    }

    pub async fn get_all_users(
        &self,
        _opts: GetAllUsersOptions,
    ) -> error::Result<Vec<UserAndMetadata>> {
        unimplemented!()
    }

    pub async fn upsert_user(&self, _user: User, _opts: UpsertUserOptions) -> error::Result<()> {
        unimplemented!()
    }

    pub async fn drop_user(&self, _username: String, _opts: DropUserOptions) -> error::Result<()> {
        unimplemented!()
    }

    pub async fn get_roles(
        &self,
        _opts: GetRolesOptions,
    ) -> error::Result<Vec<RoleAndDescription>> {
        unimplemented!()
    }

    pub async fn get_group(
        &self,
        _group_name: String,
        _opts: GetGroupOptions,
    ) -> error::Result<Group> {
        unimplemented!()
    }

    pub async fn get_all_groups(&self, _opts: GetAllGroupsOptions) -> error::Result<Vec<Group>> {
        unimplemented!()
    }

    pub async fn upsert_group(
        &self,
        _group: Group,
        _opts: UpsertGroupOptions,
    ) -> error::Result<()> {
        unimplemented!()
    }

    pub async fn drop_group(
        &self,
        _group_name: String,
        _opts: DropGroupOptions,
    ) -> error::Result<()> {
        unimplemented!()
    }

    pub async fn change_password(
        &self,
        _new_password: String,
        _opts: ChangePasswordOptions,
    ) -> error::Result<()> {
        unimplemented!()
    }
}
