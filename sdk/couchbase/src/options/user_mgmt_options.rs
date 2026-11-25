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

//! Options for user and group management operations.

use crate::retry::RetryStrategy;
use std::sync::Arc;
use std::time::Duration;

/// Options for retrieving a single user.
///
/// * `auth_domain` — The authentication domain to look up the user in (e.g. `"local"` or `"external"`).
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetUserOptions {
    /// The authentication domain (e.g. `"local"` or `"external"`).
    pub auth_domain: Option<String>,
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl GetUserOptions {
    /// Creates a new instance with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the authentication domain.
    pub fn auth_domain(mut self, auth_domain: impl Into<String>) -> Self {
        self.auth_domain = Some(auth_domain.into());
        self
    }

    /// Sets a custom retry strategy for this operation.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }

    /// Sets the timeout for this operation, including retries.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

/// Options for listing all users.
///
/// * `auth_domain` — The authentication domain to list users from.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetAllUsersOptions {
    /// The authentication domain to list users from.
    pub auth_domain: Option<String>,
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl GetAllUsersOptions {
    /// Creates a new instance with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the authentication domain.
    pub fn auth_domain(mut self, auth_domain: impl Into<String>) -> Self {
        self.auth_domain = Some(auth_domain.into());
        self
    }

    /// Sets a custom retry strategy for this operation.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }

    /// Sets the timeout for this operation, including retries.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

/// Options for creating or updating a user.
///
/// * `auth_domain` — The authentication domain for the user.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct UpsertUserOptions {
    /// The authentication domain for the user.
    pub auth_domain: Option<String>,
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl UpsertUserOptions {
    /// Creates a new instance with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the authentication domain.
    pub fn auth_domain(mut self, auth_domain: impl Into<String>) -> Self {
        self.auth_domain = Some(auth_domain.into());
        self
    }

    /// Sets a custom retry strategy for this operation.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }

    /// Sets the timeout for this operation, including retries.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

/// Options for dropping a user.
///
/// * `auth_domain` — The authentication domain of the user to drop.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DropUserOptions {
    /// The authentication domain of the user to drop.
    pub auth_domain: Option<String>,
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl DropUserOptions {
    /// Creates a new instance with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the authentication domain.
    pub fn auth_domain(mut self, auth_domain: impl Into<String>) -> Self {
        self.auth_domain = Some(auth_domain.into());
        self
    }

    /// Sets a custom retry strategy for this operation.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }

    /// Sets the timeout for this operation, including retries.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

/// Options for listing all available roles.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetRolesOptions {
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl GetRolesOptions {
    /// Creates a new instance with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a custom retry strategy for this operation.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }

    /// Sets the timeout for this operation, including retries.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

/// Options for retrieving a single group.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetGroupOptions {
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl GetGroupOptions {
    /// Creates a new instance with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a custom retry strategy for this operation.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }

    /// Sets the timeout for this operation, including retries.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

/// Options for listing all groups.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetAllGroupsOptions {
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl GetAllGroupsOptions {
    /// Creates a new instance with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a custom retry strategy for this operation.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }

    /// Sets the timeout for this operation, including retries.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

/// Options for creating or updating a group.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct UpsertGroupOptions {
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl UpsertGroupOptions {
    /// Creates a new instance with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a custom retry strategy for this operation.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }

    /// Sets the timeout for this operation, including retries.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

/// Options for dropping a group.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DropGroupOptions {
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl DropGroupOptions {
    /// Creates a new instance with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a custom retry strategy for this operation.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }

    /// Sets the timeout for this operation, including retries.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

/// Options for changing the current user's password.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct ChangePasswordOptions {
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl ChangePasswordOptions {
    /// Creates a new instance with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a custom retry strategy for this operation.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }

    /// Sets the timeout for this operation, including retries.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}
