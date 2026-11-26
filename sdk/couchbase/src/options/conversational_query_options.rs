/*
 *
 *  * Copyright (c) 2026 Couchbase, Inc.
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

//! Options for Conversational Query operations.
//!
//! See [`ConversationalQuery`](crate::conversational_query::ConversationalQuery) for the
//! operations these options configure.

use crate::error;
use crate::error::Error;
use crate::options::query_options::{ProfileMode, ReplicaLevel, ScanConsistency};
use crate::retry::RetryStrategy;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;

const REDACTED: &str = "<redacted>";

/// Credentials that Query uses to call the model provider.
///
/// These are separate from the Couchbase credentials used to connect to the cluster.
#[derive(Clone, PartialEq, Eq)]
pub struct ConversationalModelCredential {
    pub(crate) inner: ModelCredentialKind,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) enum ModelCredentialKind {
    Capella {
        email: String,
        password: String,
        organization_id: String,
    },
    CredentialStore {
        credential_name: String,
    },
    ApiKey {
        key: String,
    },
}

impl ConversationalModelCredential {
    /// Authenticates through Capella iQ using a Capella account.
    pub fn capella(
        email: impl Into<String>,
        password: impl Into<String>,
        organization_id: impl Into<String>,
    ) -> Self {
        Self {
            inner: ModelCredentialKind::Capella {
                email: email.into(),
                password: password.into(),
                organization_id: organization_id.into(),
            },
        }
    }

    /// Uses a credential stored in the Query credential store, sent directly to the provider.
    pub fn credential_store(credential_name: impl Into<String>) -> Self {
        Self {
            inner: ModelCredentialKind::CredentialStore {
                credential_name: credential_name.into(),
            },
        }
    }

    /// Uses a model-provider API key, sent directly to the provider.
    pub fn api_key(key: impl Into<String>) -> Self {
        Self {
            inner: ModelCredentialKind::ApiKey { key: key.into() },
        }
    }
}

impl Debug for ConversationalModelCredential {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            ModelCredentialKind::Capella {
                email,
                organization_id,
                ..
            } => f
                .debug_struct("ConversationalModelCredential::Capella")
                .field("email", email)
                .field("password", &REDACTED)
                .field("organization_id", organization_id)
                .finish(),
            ModelCredentialKind::CredentialStore { credential_name } => f
                .debug_struct("ConversationalModelCredential::CredentialStore")
                .field("credential_name", credential_name)
                .finish(),
            ModelCredentialKind::ApiKey { .. } => f
                .debug_struct("ConversationalModelCredential::ApiKey")
                .field("key", &REDACTED)
                .finish(),
        }
    }
}

/// Model configuration for Conversational Query.
///
/// `provider` and `name` are open strings and `endpoint` is a provider-specific URL; all
/// are sent as supplied. Only `provider` and `name` are supported with a
/// [`ConversationalModelCredential::capella`] credential.
#[derive(Default, Clone, PartialEq)]
#[non_exhaustive]
pub struct ConversationalModelOptions {
    /// The model provider.
    pub provider: Option<String>,
    /// The model name.
    pub name: Option<String>,
    /// A provider-specific endpoint URL.
    pub endpoint: Option<String>,
    /// The maximum number of tokens the model may generate.
    pub output_token_limit: Option<u32>,
    /// Whether the provider's moderation is enabled.
    pub moderation: Option<bool>,
    /// Additional `natural_config` fields, for forward compatibility. Typed options take
    /// precedence over entries with the same key. `cred_id` and `api_key` are rejected.
    pub raw: Option<HashMap<String, Value>>,
}

impl ConversationalModelOptions {
    /// Creates a new instance of `ConversationalModelOptions`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the model provider.
    pub fn provider(mut self, provider: impl Into<String>) -> Self {
        self.provider = Some(provider.into());
        self
    }

    /// Sets the model name.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Sets a provider-specific endpoint URL.
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Sets the maximum number of tokens the model may generate.
    pub fn output_token_limit(mut self, output_token_limit: u32) -> Self {
        self.output_token_limit = Some(output_token_limit);
        self
    }

    /// Sets whether the provider's moderation is enabled.
    pub fn moderation(mut self, moderation: bool) -> Self {
        self.moderation = Some(moderation);
        self
    }

    /// Adds an additional `natural_config` field.
    pub fn add_raw<T: Serialize>(
        mut self,
        key: impl Into<String>,
        value: T,
    ) -> error::Result<Self> {
        let value = serde_json::to_value(&value).map_err(Error::encoding_failure_from_serde)?;
        self.raw
            .get_or_insert_with(HashMap::new)
            .insert(key.into(), value);
        Ok(self)
    }
}

impl Debug for ConversationalModelOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConversationalModelOptions")
            .field("provider", &self.provider)
            .field("name", &self.name)
            .field("endpoint", &self.endpoint)
            .field("output_token_limit", &self.output_token_limit)
            .field("moderation", &self.moderation)
            .field("raw", &self.raw.as_ref().map(RedactedValues))
            .finish()
    }
}

// Prints the keys of a parameter map and redacts every value.
struct RedactedValues<'a>(&'a HashMap<String, Value>);

impl Debug for RedactedValues<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut keys: Vec<&String> = self.0.keys().collect();
        keys.sort();
        f.debug_map()
            .entries(keys.into_iter().map(|key| (key, REDACTED)))
            .finish()
    }
}

/// The kind of statement Conversational Query generates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ConversationalOutput {
    /// A SQL++ statement (the server default).
    Sqlpp,
    /// A SQL++ statement that uses Full-Text Search.
    FtsSqlpp,
    /// A JavaScript user-defined function. Creating the function requires a separate
    /// ordinary query.
    JsUdf,
}

impl From<ConversationalOutput>
    for couchbase_core::options::conversational_query::ConversationalQueryOutput
{
    fn from(output: ConversationalOutput) -> Self {
        match output {
            ConversationalOutput::Sqlpp => Self::Sqlpp,
            ConversationalOutput::FtsSqlpp => Self::FtsSqlpp,
            ConversationalOutput::JsUdf => Self::JsUdf,
        }
    }
}

/// Options for [`ConversationalQuery::query`](crate::conversational_query::ConversationalQuery::query).
#[derive(Default, Clone)]
#[non_exhaustive]
pub struct ConversationalQueryOptions {
    /// The model to use.
    pub model: Option<ConversationalModelOptions>,
    /// The model-provider credential.
    pub credential: Option<ConversationalModelCredential>,
    /// A hint for statement generation.
    pub hint: Option<String>,
    /// Whether an eligible generated statement is executed (the server default is `true`).
    /// `true` does not guarantee execution.
    pub execute: Option<bool>,
    /// The kind of statement to generate.
    pub output: Option<ConversationalOutput>,
    /// Whether Knowledge entries configured in Query are used (the server default is
    /// `false`).
    pub use_knowledge: Option<bool>,
    /// The scan consistency.
    pub scan_consistency: Option<ScanConsistency>,
    /// The maximum time the query engine waits for indexes to catch up.
    pub scan_wait: Option<Duration>,
    /// Whether index replicas may be used.
    pub use_replica: Option<ReplicaLevel>,
    /// The client context id.
    pub client_context_id: Option<String>,
    /// Whether metrics are returned.
    pub metrics: Option<bool>,
    /// The profiling mode.
    pub profile: Option<ProfileMode>,
    /// The server-side timeout.
    pub server_timeout: Option<Duration>,
    /// The retry strategy.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Raw Query request parameters.
    pub raw: Option<HashMap<String, Value>>,
}

impl ConversationalQueryOptions {
    /// Creates a new instance of `ConversationalQueryOptions`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the model to use.
    pub fn model(mut self, model: ConversationalModelOptions) -> Self {
        self.model = Some(model);
        self
    }

    /// Sets the model-provider credential.
    pub fn credential(mut self, credential: ConversationalModelCredential) -> Self {
        self.credential = Some(credential);
        self
    }

    /// Sets a hint for statement generation.
    pub fn hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    /// Sets whether an eligible generated statement is executed.
    pub fn execute(mut self, execute: bool) -> Self {
        self.execute = Some(execute);
        self
    }

    /// Sets the kind of statement to generate.
    pub fn output(mut self, output: ConversationalOutput) -> Self {
        self.output = Some(output);
        self
    }

    /// Sets whether Knowledge entries configured in Query are used.
    pub fn use_knowledge(mut self, use_knowledge: bool) -> Self {
        self.use_knowledge = Some(use_knowledge);
        self
    }

    /// Sets the scan consistency.
    pub fn scan_consistency(mut self, scan_consistency: ScanConsistency) -> Self {
        self.scan_consistency = Some(scan_consistency);
        self
    }

    /// Sets the maximum time the query engine waits for indexes to catch up.
    pub fn scan_wait(mut self, scan_wait: Duration) -> Self {
        self.scan_wait = Some(scan_wait);
        self
    }

    /// Sets whether index replicas may be used.
    pub fn use_replica(mut self, use_replica: ReplicaLevel) -> Self {
        self.use_replica = Some(use_replica);
        self
    }

    /// Sets the client context id. A random id is generated when unset.
    pub fn client_context_id(mut self, client_context_id: impl Into<String>) -> Self {
        self.client_context_id = Some(client_context_id.into());
        self
    }

    /// Sets whether metrics are returned.
    pub fn metrics(mut self, metrics: bool) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Sets the profiling mode.
    pub fn profile(mut self, profile: ProfileMode) -> Self {
        self.profile = Some(profile);
        self
    }

    /// Sets the server-side timeout.
    pub fn server_timeout(mut self, server_timeout: Duration) -> Self {
        self.server_timeout = Some(server_timeout);
        self
    }

    /// Sets the retry strategy.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }

    /// Adds a raw Query request parameter.
    pub fn add_raw<T: Serialize>(
        mut self,
        key: impl Into<String>,
        value: T,
    ) -> error::Result<Self> {
        let value = serde_json::to_value(&value).map_err(Error::encoding_failure_from_serde)?;
        self.raw
            .get_or_insert_with(HashMap::new)
            .insert(key.into(), value);
        Ok(self)
    }
}

impl Debug for ConversationalQueryOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConversationalQueryOptions")
            .field("model", &self.model)
            .field("credential", &self.credential)
            .field("hint", &self.hint)
            .field("execute", &self.execute)
            .field("output", &self.output)
            .field("use_knowledge", &self.use_knowledge)
            .field("scan_consistency", &self.scan_consistency)
            .field("scan_wait", &self.scan_wait)
            .field("use_replica", &self.use_replica)
            .field("client_context_id", &self.client_context_id)
            .field("metrics", &self.metrics)
            .field("profile", &self.profile)
            .field("server_timeout", &self.server_timeout)
            .field("retry_strategy", &self.retry_strategy)
            .field("raw", &self.raw.as_ref().map(RedactedValues))
            .finish()
    }
}

/// Options for [`ConversationalQuery::begin_chat`](crate::conversational_query::ConversationalQuery::begin_chat).
///
/// The model and credential are kept by the returned
/// [`ConversationalQueryChat`](crate::conversational_query::ConversationalQueryChat) for its `ask`
/// and summarizing `pause` calls.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct ConversationalBeginChatOptions {
    /// The model used by the chat's ASK and summarizing PAUSE requests.
    pub model: Option<ConversationalModelOptions>,
    /// The model-provider credential used by the chat's ASK and summarizing PAUSE requests.
    pub credential: Option<ConversationalModelCredential>,
    /// How long the chat may be inactive before Query discards it, sent in whole seconds
    /// (rounded up). Query's default and minimum are currently one hour.
    pub inactivity_timeout: Option<Duration>,
    /// Whether the chat uses Knowledge entries configured in Query (the server default is
    /// `false`).
    pub use_knowledge: Option<bool>,
    /// The server-side timeout.
    pub server_timeout: Option<Duration>,
    /// The retry strategy.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl ConversationalBeginChatOptions {
    /// Creates a new instance of `ConversationalBeginChatOptions`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the model used by the chat.
    pub fn model(mut self, model: ConversationalModelOptions) -> Self {
        self.model = Some(model);
        self
    }

    /// Sets the model-provider credential used by the chat.
    pub fn credential(mut self, credential: ConversationalModelCredential) -> Self {
        self.credential = Some(credential);
        self
    }

    /// Sets how long the chat may be inactive before Query discards it.
    pub fn inactivity_timeout(mut self, inactivity_timeout: Duration) -> Self {
        self.inactivity_timeout = Some(inactivity_timeout);
        self
    }

    /// Sets whether the chat uses Knowledge entries configured in Query.
    pub fn use_knowledge(mut self, use_knowledge: bool) -> Self {
        self.use_knowledge = Some(use_knowledge);
        self
    }

    /// Sets the server-side timeout.
    pub fn server_timeout(mut self, server_timeout: Duration) -> Self {
        self.server_timeout = Some(server_timeout);
        self
    }

    /// Sets the retry strategy.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

/// Options for [`ConversationalQuery::attach_chat`](crate::conversational_query::ConversationalQuery::attach_chat).
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct ConversationalAttachChatOptions {
    /// The model used by the chat's ASK and summarizing PAUSE requests.
    pub model: Option<ConversationalModelOptions>,
    /// The model-provider credential used by the chat's ASK and summarizing PAUSE requests.
    pub credential: Option<ConversationalModelCredential>,
}

impl ConversationalAttachChatOptions {
    /// Creates a new instance of `ConversationalAttachChatOptions`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the model used by the chat.
    pub fn model(mut self, model: ConversationalModelOptions) -> Self {
        self.model = Some(model);
        self
    }

    /// Sets the model-provider credential used by the chat.
    pub fn credential(mut self, credential: ConversationalModelCredential) -> Self {
        self.credential = Some(credential);
        self
    }
}

/// Options for [`ConversationalQueryChat::ask`](crate::conversational_query::ConversationalQueryChat::ask).
///
/// The [`Debug`] implementation never prints raw parameter values.
#[derive(Default, Clone)]
#[non_exhaustive]
pub struct ConversationalAskOptions {
    /// A hint for statement generation.
    pub hint: Option<String>,
    /// Whether an eligible generated statement is executed (the server default is `true`).
    /// `true` does not guarantee execution.
    pub execute: Option<bool>,
    /// The kind of statement to generate.
    pub output: Option<ConversationalOutput>,
    /// The scan consistency.
    pub scan_consistency: Option<ScanConsistency>,
    /// The maximum time the query engine waits for indexes to catch up.
    pub scan_wait: Option<Duration>,
    /// Whether index replicas may be used.
    pub use_replica: Option<ReplicaLevel>,
    /// The client context id.
    pub client_context_id: Option<String>,
    /// Whether metrics are returned.
    pub metrics: Option<bool>,
    /// The profiling mode.
    pub profile: Option<ProfileMode>,
    /// The server-side timeout.
    pub server_timeout: Option<Duration>,
    /// The retry strategy.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Raw Query request parameters.
    pub raw: Option<HashMap<String, Value>>,
}

impl ConversationalAskOptions {
    /// Creates a new instance of `ConversationalAskOptions`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a hint for statement generation.
    pub fn hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    /// Sets whether an eligible generated statement is executed.
    pub fn execute(mut self, execute: bool) -> Self {
        self.execute = Some(execute);
        self
    }

    /// Sets the kind of statement to generate.
    pub fn output(mut self, output: ConversationalOutput) -> Self {
        self.output = Some(output);
        self
    }

    /// Sets the scan consistency.
    pub fn scan_consistency(mut self, scan_consistency: ScanConsistency) -> Self {
        self.scan_consistency = Some(scan_consistency);
        self
    }

    /// Sets the maximum time the query engine waits for indexes to catch up.
    pub fn scan_wait(mut self, scan_wait: Duration) -> Self {
        self.scan_wait = Some(scan_wait);
        self
    }

    /// Sets whether index replicas may be used.
    pub fn use_replica(mut self, use_replica: ReplicaLevel) -> Self {
        self.use_replica = Some(use_replica);
        self
    }

    /// Sets the client context id. A random id is generated when unset.
    pub fn client_context_id(mut self, client_context_id: impl Into<String>) -> Self {
        self.client_context_id = Some(client_context_id.into());
        self
    }

    /// Sets whether metrics are returned.
    pub fn metrics(mut self, metrics: bool) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Sets the profiling mode.
    pub fn profile(mut self, profile: ProfileMode) -> Self {
        self.profile = Some(profile);
        self
    }

    /// Sets the server-side timeout.
    pub fn server_timeout(mut self, server_timeout: Duration) -> Self {
        self.server_timeout = Some(server_timeout);
        self
    }

    /// Sets the retry strategy.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }

    /// Adds a raw Query request parameter.
    pub fn add_raw<T: Serialize>(
        mut self,
        key: impl Into<String>,
        value: T,
    ) -> error::Result<Self> {
        let value = serde_json::to_value(&value).map_err(Error::encoding_failure_from_serde)?;
        self.raw
            .get_or_insert_with(HashMap::new)
            .insert(key.into(), value);
        Ok(self)
    }
}

impl Debug for ConversationalAskOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConversationalAskOptions")
            .field("hint", &self.hint)
            .field("execute", &self.execute)
            .field("output", &self.output)
            .field("scan_consistency", &self.scan_consistency)
            .field("scan_wait", &self.scan_wait)
            .field("use_replica", &self.use_replica)
            .field("client_context_id", &self.client_context_id)
            .field("metrics", &self.metrics)
            .field("profile", &self.profile)
            .field("server_timeout", &self.server_timeout)
            .field("retry_strategy", &self.retry_strategy)
            .field("raw", &self.raw.as_ref().map(RedactedValues))
            .finish()
    }
}

/// Options for [`ConversationalQueryChat::pause`](crate::conversational_query::ConversationalQueryChat::pause).
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct ConversationalPauseOptions {
    /// Whether Query may summarize the conversation (invoking the model) before saving it.
    /// When `false`, the chat's model and credential are not sent.
    pub summarize: Option<bool>,
    /// The server-side timeout.
    pub server_timeout: Option<Duration>,
    /// The retry strategy.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl ConversationalPauseOptions {
    /// Creates a new instance of `ConversationalPauseOptions`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets whether Query may summarize the conversation before saving it.
    pub fn summarize(mut self, summarize: bool) -> Self {
        self.summarize = Some(summarize);
        self
    }

    /// Sets the server-side timeout.
    pub fn server_timeout(mut self, server_timeout: Duration) -> Self {
        self.server_timeout = Some(server_timeout);
        self
    }

    /// Sets the retry strategy.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

/// Options for [`ConversationalQueryChat::resume`](crate::conversational_query::ConversationalQueryChat::resume).
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct ConversationalResumeOptions {
    /// The server-side timeout.
    pub server_timeout: Option<Duration>,
    /// The retry strategy.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl ConversationalResumeOptions {
    /// Creates a new instance of `ConversationalResumeOptions`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the server-side timeout.
    pub fn server_timeout(mut self, server_timeout: Duration) -> Self {
        self.server_timeout = Some(server_timeout);
        self
    }

    /// Sets the retry strategy.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

/// Options for [`ConversationalQueryChat::end`](crate::conversational_query::ConversationalQueryChat::end).
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct ConversationalEndOptions {
    /// The server-side timeout.
    pub server_timeout: Option<Duration>,
    /// The retry strategy.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl ConversationalEndOptions {
    /// Creates a new instance of `ConversationalEndOptions`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the server-side timeout.
    pub fn server_timeout(mut self, server_timeout: Duration) -> Self {
        self.server_timeout = Some(server_timeout);
        self
    }

    /// Sets the retry strategy.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}
