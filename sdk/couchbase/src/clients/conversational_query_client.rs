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
use crate::clients::agent_provider::CouchbaseAgentProvider;
use crate::error;
use crate::error::{Error, ErrorKind, FeatureNotAvailableErrorKind};
use crate::options::conversational_query_options::{
    ConversationalAskOptions, ConversationalBeginChatOptions, ConversationalEndOptions,
    ConversationalModelCredential, ConversationalModelOptions, ConversationalOutput,
    ConversationalPauseOptions, ConversationalQueryOptions, ConversationalResumeOptions,
    ModelCredentialKind,
};
use crate::options::query_options::split_scan_consistency;
use crate::results::conversational_query_results::{ConversationalMetaData, ConversationalResult};
use crate::retry::RetryStrategy;
use couchbase_core::agent::Agent;
use couchbase_core::error::ErrorKind as CoreErrorKind;
use couchbase_core::options::conversational_query as core_cq;
use couchbase_core::options::conversational_query::{ChatState, CollectionKeyspace};
use couchbase_core::options::query::QueryOptions as CoreQueryOptions;
use couchbase_core::queryx;
use couchbase_core::queryx::conversational::{classify_error, ConversationalErrorClass};
use couchbase_core::queryx::query_options::NaturalLanguageOptions;
use serde_json::{Map, Value};
use std::sync::Arc;

const FEATURE_NAME: &str = "conversational query";

// The validated model and credential of a chat.
#[derive(Clone)]
pub(crate) struct ChatModel(NaturalLanguageOptions);

impl ChatModel {
    pub fn new(
        model: Option<&ConversationalModelOptions>,
        credential: Option<&ConversationalModelCredential>,
    ) -> error::Result<Self> {
        resolve_model(model, credential).map(Self)
    }
}

pub(crate) struct ConversationalQueryClient {
    backend: ConversationalQueryClientBackend,
}

impl ConversationalQueryClient {
    pub fn new(backend: ConversationalQueryClientBackend) -> Self {
        Self { backend }
    }

    pub async fn query(
        &self,
        prompt: String,
        keyspaces: Vec<CollectionKeyspace>,
        opts: ConversationalQueryOptions,
    ) -> error::Result<ConversationalResult> {
        match &self.backend {
            ConversationalQueryClientBackend::CouchbaseConversationalQueryClientBackend(
                backend,
            ) => backend.query(prompt, keyspaces, opts).await,
            ConversationalQueryClientBackend::Couchbase2ConversationalQueryClientBackend => {
                Err(couchbase2_unsupported())
            }
        }
    }

    pub async fn begin_chat(
        &self,
        keyspaces: Vec<CollectionKeyspace>,
        opts: ConversationalBeginChatOptions,
    ) -> error::Result<ChatState> {
        match &self.backend {
            ConversationalQueryClientBackend::CouchbaseConversationalQueryClientBackend(
                backend,
            ) => backend.begin_chat(keyspaces, opts).await,
            ConversationalQueryClientBackend::Couchbase2ConversationalQueryClientBackend => {
                Err(couchbase2_unsupported())
            }
        }
    }

    pub async fn ask(
        &self,
        chat: &ChatState,
        model: &ChatModel,
        prompt: String,
        opts: ConversationalAskOptions,
    ) -> error::Result<ConversationalResult> {
        match &self.backend {
            ConversationalQueryClientBackend::CouchbaseConversationalQueryClientBackend(
                backend,
            ) => backend.ask(chat, model, prompt, opts).await,
            ConversationalQueryClientBackend::Couchbase2ConversationalQueryClientBackend => {
                Err(couchbase2_unsupported())
            }
        }
    }

    pub async fn pause(
        &self,
        chat: &ChatState,
        model: &ChatModel,
        opts: ConversationalPauseOptions,
    ) -> error::Result<ConversationalMetaData> {
        match &self.backend {
            ConversationalQueryClientBackend::CouchbaseConversationalQueryClientBackend(
                backend,
            ) => backend.pause(chat, model, opts).await,
            ConversationalQueryClientBackend::Couchbase2ConversationalQueryClientBackend => {
                Err(couchbase2_unsupported())
            }
        }
    }

    pub async fn resume(
        &self,
        chat: &ChatState,
        opts: ConversationalResumeOptions,
    ) -> error::Result<ConversationalMetaData> {
        match &self.backend {
            ConversationalQueryClientBackend::CouchbaseConversationalQueryClientBackend(
                backend,
            ) => backend.resume(chat, opts).await,
            ConversationalQueryClientBackend::Couchbase2ConversationalQueryClientBackend => {
                Err(couchbase2_unsupported())
            }
        }
    }

    pub async fn end(
        &self,
        chat: &ChatState,
        opts: ConversationalEndOptions,
    ) -> error::Result<ConversationalMetaData> {
        match &self.backend {
            ConversationalQueryClientBackend::CouchbaseConversationalQueryClientBackend(
                backend,
            ) => backend.end(chat, opts).await,
            ConversationalQueryClientBackend::Couchbase2ConversationalQueryClientBackend => {
                Err(couchbase2_unsupported())
            }
        }
    }
}

pub(crate) enum ConversationalQueryClientBackend {
    CouchbaseConversationalQueryClientBackend(CouchbaseConversationalQueryClient),
    Couchbase2ConversationalQueryClientBackend,
}

pub(crate) struct CouchbaseConversationalQueryClient {
    agent_provider: CouchbaseAgentProvider,
    default_retry_strategy: Arc<dyn RetryStrategy>,
}

impl CouchbaseConversationalQueryClient {
    pub fn new(
        agent_provider: CouchbaseAgentProvider,
        default_retry_strategy: Arc<dyn RetryStrategy>,
    ) -> Self {
        Self {
            agent_provider,
            default_retry_strategy,
        }
    }

    async fn query(
        &self,
        prompt: String,
        keyspaces: Vec<CollectionKeyspace>,
        opts: ConversationalQueryOptions,
    ) -> error::Result<ConversationalResult> {
        let natural_language = resolve_model(opts.model.as_ref(), opts.credential.as_ref())?;
        let agent = self.agent().await?;

        let core_opts = core_cq::ConversationalQueryOptions::new(prompt, keyspaces)
            .hint(opts.hint.clone())
            .execute(opts.execute)
            .output(opts.output.map(Into::into))
            .use_knowledge(opts.use_knowledge)
            .natural_language(natural_language)
            .query_options(self.core_query_options(opts));

        agent
            .conversational_query(core_opts)
            .await
            .map(ConversationalResult::from)
            .map_err(map_error)
    }

    async fn begin_chat(
        &self,
        keyspaces: Vec<CollectionKeyspace>,
        opts: ConversationalBeginChatOptions,
    ) -> error::Result<ChatState> {
        let agent = self.agent().await?;

        let core_opts = core_cq::BeginChatOptions::new(keyspaces)
            .use_knowledge(opts.use_knowledge)
            .inactivity_timeout(opts.inactivity_timeout)
            .server_timeout(opts.server_timeout)
            .retry_strategy(self.retry_strategy(opts.retry_strategy));

        agent.begin_chat(core_opts).await.map_err(map_error)
    }

    async fn ask(
        &self,
        chat: &ChatState,
        model: &ChatModel,
        prompt: String,
        opts: ConversationalAskOptions,
    ) -> error::Result<ConversationalResult> {
        let agent = self.agent().await?;

        let core_opts = core_cq::AskChatOptions::new(prompt)
            .hint(opts.hint)
            .execute(opts.execute)
            .output(opts.output.map(Into::into))
            .natural_language(model.0.clone())
            .query_options(self.core_query_options(ConversationalQueryOptions {
                scan_consistency: opts.scan_consistency,
                scan_wait: opts.scan_wait,
                use_replica: opts.use_replica,
                client_context_id: opts.client_context_id,
                metrics: opts.metrics,
                profile: opts.profile,
                server_timeout: opts.server_timeout,
                retry_strategy: opts.retry_strategy,
                raw: opts.raw,
                ..Default::default()
            }));

        agent
            .ask_chat(chat, core_opts)
            .await
            .map(ConversationalResult::from)
            .map_err(map_error)
    }

    async fn pause(
        &self,
        chat: &ChatState,
        model: &ChatModel,
        opts: ConversationalPauseOptions,
    ) -> error::Result<ConversationalMetaData> {
        let agent = self.agent().await?;

        let core_opts = core_cq::PauseChatOptions::new()
            .summarize(opts.summarize)
            .natural_language(model.0.clone())
            .server_timeout(opts.server_timeout)
            .retry_strategy(self.retry_strategy(opts.retry_strategy));

        agent
            .pause_chat(chat, core_opts)
            .await
            .map(|meta| ConversationalMetaData::from(&meta))
            .map_err(map_error)
    }

    async fn resume(
        &self,
        chat: &ChatState,
        opts: ConversationalResumeOptions,
    ) -> error::Result<ConversationalMetaData> {
        let agent = self.agent().await?;

        let core_opts = core_cq::ResumeChatOptions::new()
            .server_timeout(opts.server_timeout)
            .retry_strategy(self.retry_strategy(opts.retry_strategy));

        agent
            .resume_chat(chat, core_opts)
            .await
            .map(|meta| ConversationalMetaData::from(&meta))
            .map_err(map_error)
    }

    async fn end(
        &self,
        chat: &ChatState,
        opts: ConversationalEndOptions,
    ) -> error::Result<ConversationalMetaData> {
        let agent = self.agent().await?;

        let core_opts = core_cq::EndChatOptions::new()
            .server_timeout(opts.server_timeout)
            .retry_strategy(self.retry_strategy(opts.retry_strategy));

        agent
            .end_chat(chat, core_opts)
            .await
            .map(|meta| ConversationalMetaData::from(&meta))
            .map_err(map_error)
    }

    async fn agent(&self) -> error::Result<Arc<Agent>> {
        let agent = self.agent_provider.get_agent().await;
        CouchbaseAgentProvider::upgrade_agent(agent)
    }

    fn retry_strategy(
        &self,
        retry_strategy: Option<Arc<dyn RetryStrategy>>,
    ) -> Arc<dyn RetryStrategy> {
        retry_strategy.unwrap_or_else(|| self.default_retry_strategy.clone())
    }

    fn core_query_options(&self, opts: ConversationalQueryOptions) -> CoreQueryOptions {
        let (scan_consistency, scan_vectors) = split_scan_consistency(opts.scan_consistency);
        CoreQueryOptions::new()
            // Generated once, so that retries of an operation share it, as for ordinary queries.
            .client_context_id(
                opts.client_context_id
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            )
            .metrics(opts.metrics.unwrap_or_default())
            .profile(opts.profile.map(Into::into))
            .scan_consistency(scan_consistency)
            .sparse_scan_vectors(scan_vectors)
            .scan_wait(opts.scan_wait)
            .timeout(opts.server_timeout)
            .use_replica(opts.use_replica.map(Into::into))
            .raw(opts.raw)
            .retry_strategy(self.retry_strategy(opts.retry_strategy))
    }
}

fn couchbase2_unsupported() -> Error {
    Error::new(ErrorKind::FeatureNotAvailable(
        FeatureNotAvailableErrorKind::new(
            FEATURE_NAME,
            "conversational query is not supported over couchbase2://".to_string(),
        ),
    ))
}

// Applies the Conversational Query error codes that map to SDK error kinds.
// ConversationalChatCurrentlyUnavailable is produced in core, which has the chat's routing.
pub(crate) fn map_error(err: couchbase_core::error::Error) -> Error {
    // The first Query error decides, rather than the first non-retryable one.
    let err = err.with_first_query_error_chosen();
    let class = classify_error(&err).and_then(|e| e.class);
    let mapped = Error::from(err);

    match class {
        Some(ConversationalErrorClass::RateLimited) => {
            mapped.with_kind(ErrorKind::RateLimitedFailure)
        }
        Some(ConversationalErrorClass::FeatureNotAvailable) => mapped.with_kind(
            ErrorKind::FeatureNotAvailable(FeatureNotAvailableErrorKind::new(
                FEATURE_NAME,
                "natural-language processing is disabled on the Query node".to_string(),
            )),
        ),
        Some(ConversationalErrorClass::AuthenticationFailure) => {
            mapped.with_kind(ErrorKind::AuthenticationFailure)
        }
        _ => mapped,
    }
}

fn resolve_model(
    model: Option<&ConversationalModelOptions>,
    credential: Option<&ConversationalModelCredential>,
) -> error::Result<NaturalLanguageOptions> {
    let default_model = ConversationalModelOptions::default();
    let model = model.unwrap_or(&default_model);
    let raw = model.raw.as_ref();

    if let Some(raw) = raw {
        for key in ["cred_id", "api_key"] {
            if raw.contains_key(key) {
                return Err(Error::invalid_argument(
                    "model.raw",
                    format!("{key} must be supplied through the credential"),
                ));
            }
        }
    }

    let mut natural_language = NaturalLanguageOptions::new();

    if let Some(ModelCredentialKind::Capella {
        email,
        password,
        organization_id,
    }) = credential.map(|c| &c.inner)
    {
        let unsupported = [
            ("endpoint", model.endpoint.is_some()),
            ("output_token_limit", model.output_token_limit.is_some()),
            ("moderation", model.moderation.is_some()),
            ("raw", raw.is_some_and(|raw| !raw.is_empty())),
        ];
        if let Some((option, _)) = unsupported.iter().find(|(_, set)| *set) {
            return Err(Error::invalid_argument(
                format!("model.{option}"),
                "not supported with a Capella credential",
            ));
        }

        return Ok(natural_language
            .cred(format!("{email}:{password}"))
            .org_id(organization_id.clone())
            .vendor(model.provider.clone())
            .model(model.name.clone()));
    }

    let mut config: Map<String, Value> = raw
        .map(|raw| raw.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();
    // Typed options take precedence over raw entries with the same key.
    let typed = [
        ("provider", model.provider.clone().map(Value::from)),
        ("model", model.name.clone().map(Value::from)),
        ("endpoint", model.endpoint.clone().map(Value::from)),
        (
            "output_token_limit",
            model.output_token_limit.map(Value::from),
        ),
        ("moderation", model.moderation.map(Value::from)),
    ];
    for (key, value) in typed {
        if let Some(value) = value {
            config.insert(key.to_string(), value);
        }
    }
    match credential.map(|c| &c.inner) {
        Some(ModelCredentialKind::CredentialStore { credential_name }) => {
            config.insert("cred_id".into(), Value::from(credential_name.clone()));
        }
        Some(ModelCredentialKind::ApiKey { key }) => {
            config.insert("api_key".into(), Value::from(key.clone()));
        }
        _ => {}
    }

    natural_language = natural_language.config(config);
    Ok(natural_language)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn natural_language_params(opts: NaturalLanguageOptions) -> Value {
        serde_json::to_value(queryx::query_options::QueryOptions::new().natural_language(opts))
            .unwrap()
    }

    #[test]
    fn capella_credential_maps_to_capella_parameters() {
        let model = ConversationalModelOptions::new()
            .provider("openai")
            .name("gpt");
        let credential =
            ConversationalModelCredential::capella("me@example.com", "hunter2", "org-1");

        let opts = resolve_model(Some(&model), Some(&credential)).unwrap();
        assert_eq!(
            json!({
                "natural_cred": "me@example.com:hunter2",
                "natural_orgid": "org-1",
                "natural_vendor": "openai",
                "natural_model": "gpt",
            }),
            natural_language_params(opts)
        );
    }

    #[test]
    fn capella_credential_rejects_direct_provider_options() {
        let credential =
            ConversationalModelCredential::capella("me@example.com", "hunter2", "org-1");
        for model in [
            ConversationalModelOptions::new().endpoint("https://example.com"),
            ConversationalModelOptions::new().output_token_limit(10),
            ConversationalModelOptions::new().moderation(true),
            ConversationalModelOptions::new()
                .add_raw("temperature", 0.5)
                .unwrap(),
        ] {
            let err = resolve_model(Some(&model), Some(&credential)).unwrap_err();
            assert!(matches!(err.kind(), ErrorKind::InvalidArgument(_)), "{err}");
        }

        // An empty raw map sets nothing.
        let mut model = ConversationalModelOptions::new();
        model.raw = Some(Default::default());
        assert!(resolve_model(Some(&model), Some(&credential)).is_ok());
    }

    #[test]
    fn direct_provider_options_map_to_natural_config() {
        let model = ConversationalModelOptions::new()
            .provider("openai")
            .name("gpt")
            .endpoint("https://example.com")
            .output_token_limit(100)
            .moderation(false)
            .add_raw("temperature", 0.5)
            .unwrap()
            // Typed options take precedence over raw entries.
            .add_raw("model", "ignored")
            .unwrap();

        let opts = resolve_model(
            Some(&model),
            Some(&ConversationalModelCredential::api_key("sk-1")),
        )
        .unwrap();
        assert_eq!(
            json!({"natural_config": {
                "provider": "openai",
                "model": "gpt",
                "endpoint": "https://example.com",
                "output_token_limit": 100,
                "moderation": false,
                "temperature": 0.5,
                "api_key": "sk-1",
            }}),
            natural_language_params(opts)
        );

        let opts = resolve_model(
            None,
            Some(&ConversationalModelCredential::credential_store("stored")),
        )
        .unwrap();
        assert_eq!(
            json!({"natural_config": {"cred_id": "stored"}}),
            natural_language_params(opts)
        );

        // An explicit endpoint can be used without a credential, and nothing is sent when
        // nothing is configured.
        let model = ConversationalModelOptions::new().endpoint("http://localhost:11434");
        let opts = resolve_model(Some(&model), None).unwrap();
        assert_eq!(
            json!({"natural_config": {"endpoint": "http://localhost:11434"}}),
            natural_language_params(opts)
        );
        assert_eq!(
            json!({}),
            natural_language_params(resolve_model(None, None).unwrap())
        );
    }

    #[test]
    fn raw_model_options_cannot_carry_credentials() {
        for key in ["cred_id", "api_key"] {
            let model = ConversationalModelOptions::new()
                .add_raw(key, "secret")
                .unwrap();
            for credential in [None, Some(ConversationalModelCredential::api_key("sk-1"))] {
                let err = resolve_model(Some(&model), credential.as_ref()).unwrap_err();
                assert!(matches!(err.kind(), ErrorKind::InvalidArgument(_)), "{err}");
            }
        }
    }
}
