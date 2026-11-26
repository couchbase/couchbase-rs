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

//! Conversational Query: natural-language prompts answered by the Query service.
//!
//! Get a [`ConversationalQuery`] from
//! [`Cluster::conversational_query`](crate::cluster::Cluster::conversational_query). Use
//! [`ConversationalQuery::query`] for independent one-shot prompts, or
//! [`ConversationalQuery::begin_chat`] for a multi-turn [`ConversationalQueryChat`].
//!
//! # Chats and handles
//!
//! A [`ConversationalQueryChatHandle`] is a serializable snapshot of a chat, so a chat can be
//! continued from another process or a later request with
//! [`ConversationalQuery::attach_chat`]. Save an updated handle after
//! [`begin_chat`](ConversationalQuery::begin_chat), [`pause`](ConversationalQueryChat::pause) and
//! [`resume`](ConversationalQueryChat::resume).
//!
//! A handle does not contain the model or credential. Supply them again when attaching,
//! otherwise a later ASK or summarizing PAUSE may fail. Neither `begin_chat` nor `attach_chat`
//! checks that the model or credential works; such errors surface on a later `ask` or `pause`.
//! On the Capella path, Query can reuse the chat's previous provider and model when both are
//! omitted from an ASK or summarizing PAUSE.
//!
//! ```rust,no_run
//! # use couchbase::cluster::Cluster;
//! use couchbase::conversational_query::ConversationalQueryChatHandle;
//! use couchbase::options::conversational_query_options::{
//!     ConversationalAttachChatOptions, ConversationalBeginChatOptions, ConversationalModelCredential, ConversationalModelOptions,
//! };
//! use futures::TryStreamExt;
//! use serde_json::Value;
//!
//! # async fn example(cluster: Cluster) -> couchbase::error::Result<()> {
//! let model = ConversationalModelOptions::new().provider("openai").name("gpt-4o");
//! let credential = ConversationalModelCredential::credential_store("openai-key");
//!
//! let hotels = cluster.bucket("travel-sample").scope("inventory").collection("hotel");
//! let cq = cluster.conversational_query();
//! let chat = cq
//!     .begin_chat(
//!         &[hotels],
//!         ConversationalBeginChatOptions::new()
//!             .model(model.clone())
//!             .credential(credential.clone()),
//!     )
//!     .await?;
//!
//! let mut result = chat.ask("Find hotels in Paris", None).await?;
//! println!("{:?}", result.generated_statement());
//! let rows: Vec<Value> = result.rows().try_collect().await?;
//!
//! // Save the handle, then continue the chat later, possibly in another process.
//! let saved = chat.handle()?.to_json();
//! let chat = cq.attach_chat(
//!     &ConversationalQueryChatHandle::from_json(&saved)?,
//!     ConversationalAttachChatOptions::new().model(model).credential(credential),
//! )?;
//! let result = chat.ask("Only those with a pool", None).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Ordering and recovery
//!
//! A [`ConversationalQueryChat`] may be used concurrently, but the order of concurrent operations is not
//! guaranteed. Applications must serialize PAUSE, RESUME and END on the same chat, including
//! across `ConversationalQueryChat` instances, and any other operations whose order matters.
//!
//! [`ErrorKind::ConversationalChatCurrentlyUnavailable`]
//! means the SDK could not reach the chat; an explicit [`ConversationalQueryChat::resume`] may recover it. An
//! ASK or lifecycle operation whose outcome is unknown must not be blindly replayed. END does
//! not delete a paused chat, so resume it first. After END succeeds, previously saved handles
//! can still be attached but cannot continue the chat.
//!
//! # Execution and Knowledge
//!
//! Setting `execute(true)` does not guarantee that the generated statement runs. Enable
//! metrics and inspect
//! [`generated_statement_executed`](crate::results::conversational_query_results::ConversationalMetaData::generated_statement_executed)
//! to find out. Generated statements run as the connected Couchbase user; the model provider
//! is called with the separate [`ConversationalModelCredential`](crate::options::conversational_query_options::ConversationalModelCredential). Prompts, chat content and Knowledge entries
//! may be sent to the model provider, so do not store secrets in Knowledge.
//!
//! # Unsupported configurations
//!
//! Chats fail with [`ErrorKind::FeatureNotAvailable`] if the cluster does not support them or the
//! connection goes through Capella Private Link.
//! PAUSE and RESUME have further server prerequisites, including edition and metadata storage;
//! see the Couchbase Server documentation.

use crate::clients::conversational_query_client::{ChatModel, ConversationalQueryClient};
use crate::clients::tracing_client::TracingClient;
use crate::collection::Collection;
use crate::error;
use crate::error::{Error, ErrorKind};
use crate::options::conversational_query_options::{
    ConversationalAskOptions, ConversationalAttachChatOptions, ConversationalBeginChatOptions,
    ConversationalEndOptions, ConversationalPauseOptions, ConversationalQueryOptions,
    ConversationalResumeOptions,
};
use crate::results::conversational_query_results::{ConversationalMetaData, ConversationalResult};
use crate::tracing::{Keyspace, SERVICE_VALUE_QUERY};
use couchbase_core::create_span;
use couchbase_core::options::conversational_query::{ChatState, CollectionKeyspace};
use serde_json::Value;
use std::future::Future;
use std::sync::Arc;
use tracing::Instrument;

/// Entry point for Conversational Query operations.
///
/// Obtained from [`Cluster::conversational_query`](crate::cluster::Cluster::conversational_query).
/// Cheap to clone.
#[derive(Clone)]
pub struct ConversationalQuery {
    client: Arc<ConversationalQueryClient>,
    tracing_client: Arc<TracingClient>,
}

impl ConversationalQuery {
    pub(crate) fn new(
        client: Arc<ConversationalQueryClient>,
        tracing_client: Arc<TracingClient>,
    ) -> Self {
        Self {
            client,
            tracing_client,
        }
    }

    /// Submits a single natural-language prompt about the given collections.
    ///
    /// Returns the generated statement, when Query returns one, together with any execution
    /// results. At least one collection is required.
    pub async fn query(
        &self,
        prompt: impl Into<String>,
        keyspaces: &[Collection],
        opts: impl Into<Option<ConversationalQueryOptions>>,
    ) -> error::Result<ConversationalResult> {
        let prompt = prompt.into();
        let opts = opts.into().unwrap_or_default();
        let ctx = self
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_QUERY),
                Keyspace::Cluster,
                create_span!("conversational_query"),
            )
            .await;
        let result = async {
            let keyspaces = collection_keyspaces(keyspaces)?;
            self.client.query(prompt, keyspaces, opts).await
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    /// Begins a new chat about the given collections.
    ///
    /// The model and credential in `opts` are kept by the returned [`ConversationalQueryChat`] and
    /// used for its `ask` and summarizing `pause` calls. At least one collection is required.
    pub async fn begin_chat(
        &self,
        keyspaces: &[Collection],
        opts: impl Into<Option<ConversationalBeginChatOptions>>,
    ) -> error::Result<ConversationalQueryChat> {
        let opts = opts.into().unwrap_or_default();
        let ctx = self
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_QUERY),
                Keyspace::Cluster,
                create_span!("conversational_query_begin_chat"),
            )
            .await;
        let result = async {
            let keyspaces = collection_keyspaces(keyspaces)?;
            let model = ChatModel::new(opts.model.as_ref(), opts.credential.as_ref())?;
            let state = self.client.begin_chat(keyspaces, opts).await?;
            Ok(ConversationalQueryChat::new(
                self.client.clone(),
                self.tracing_client.clone(),
                state,
                model,
            ))
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    /// Creates a [`ConversationalQueryChat`] for an existing chat from a [`ConversationalQueryChatHandle`]. This is a local
    /// operation and performs no server I/O.
    ///
    /// The returned chat uses only the model and credential in `opts`; nothing is inherited
    /// from any other `ConversationalQueryChat` for the same chat.
    pub fn attach_chat(
        &self,
        handle: &ConversationalQueryChatHandle,
        opts: impl Into<Option<ConversationalAttachChatOptions>>,
    ) -> error::Result<ConversationalQueryChat> {
        let opts = opts.into().unwrap_or_default();
        let model = ChatModel::new(opts.model.as_ref(), opts.credential.as_ref())?;
        Ok(ConversationalQueryChat::new(
            self.client.clone(),
            self.tracing_client.clone(),
            ChatState::new(handle.chat_id.clone(), handle.owner.clone()),
            model,
        ))
    }
}

/// A multi-turn Conversational Query chat.
///
/// Created by [`ConversationalQuery::begin_chat`] or [`ConversationalQuery::attach_chat`].
/// The model and credential are captured when the `ConversationalQueryChat` is created and cannot be changed;
/// attach a new `ConversationalQueryChat` from [`handle`](ConversationalQueryChat::handle) to use different ones.
pub struct ConversationalQueryChat {
    client: Arc<ConversationalQueryClient>,
    tracing_client: Arc<TracingClient>,
    state: ChatState,
    model: ChatModel,
}

impl ConversationalQueryChat {
    fn new(
        client: Arc<ConversationalQueryClient>,
        tracing_client: Arc<TracingClient>,
        state: ChatState,
        model: ChatModel,
    ) -> Self {
        Self {
            client,
            tracing_client,
            state,
            model,
        }
    }

    /// Sends a prompt to the chat.
    ///
    /// Fails with
    /// [`ErrorKind::ConversationalChatCurrentlyUnavailable`]
    /// if the chat is not currently live, for example because it is paused.
    pub async fn ask(
        &self,
        prompt: impl Into<String>,
        opts: impl Into<Option<ConversationalAskOptions>>,
    ) -> error::Result<ConversationalResult> {
        let ctx = self
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_QUERY),
                Keyspace::Cluster,
                create_span!("conversational_query_ask"),
            )
            .await;
        let result = self
            .client
            .ask(
                &self.state,
                &self.model,
                prompt.into(),
                opts.into().unwrap_or_default(),
            )
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    /// Pauses the chat, so that Query persists it.
    ///
    /// Unless [`ConversationalPauseOptions::summarize`] is `false`, Query may summarize the conversation
    /// with the chat's model before saving it. Call [`resume`](ConversationalQueryChat::resume)
    /// to continue the chat.
    pub async fn pause(
        &self,
        opts: impl Into<Option<ConversationalPauseOptions>>,
    ) -> error::Result<ConversationalMetaData> {
        let ctx = self
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_QUERY),
                Keyspace::Cluster,
                create_span!("conversational_query_pause"),
            )
            .await;
        let result = self
            .client
            .pause(&self.state, &self.model, opts.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    /// Resumes a paused chat.
    pub async fn resume(
        &self,
        opts: impl Into<Option<ConversationalResumeOptions>>,
    ) -> error::Result<ConversationalMetaData> {
        let ctx = self
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_QUERY),
                Keyspace::Cluster,
                create_span!("conversational_query_resume"),
            )
            .await;
        let result = self
            .client
            .resume(&self.state, opts.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    /// Ends the chat.
    ///
    /// After END succeeds this `ConversationalQueryChat` is closed: further operations and
    /// [`handle`](ConversationalQueryChat::handle) fail with
    /// [`ErrorKind::InvalidArgument`]. END does not
    /// delete a paused chat; resume it first.
    pub async fn end(
        &self,
        opts: impl Into<Option<ConversationalEndOptions>>,
    ) -> error::Result<ConversationalMetaData> {
        let ctx = self
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_QUERY),
                Keyspace::Cluster,
                create_span!("conversational_query_end"),
            )
            .await;
        let result = self
            .client
            .end(&self.state, opts.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    /// Returns a snapshot of the chat, for continuing it with
    /// [`ConversationalQuery::attach_chat`].
    pub fn handle(&self) -> error::Result<ConversationalQueryChatHandle> {
        Ok(ConversationalQueryChatHandle {
            chat_id: self.state.chat_id().to_string(),
            owner: self.state.owner()?,
        })
    }
}

impl std::fmt::Debug for ConversationalQueryChat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConversationalQueryChat")
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

/// A serializable snapshot of the state needed to continue a [`ConversationalQueryChat`].
///
/// Opaque: use [`to_json`](ConversationalQueryChatHandle::to_json) and [`from_json`](ConversationalQueryChatHandle::from_json) to
/// store and restore it. A handle must be used with the cluster where the chat was created;
/// the SDK does not check this. It does not contain the model or credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversationalQueryChatHandle {
    chat_id: String,
    owner: Option<String>,
}

impl ConversationalQueryChatHandle {
    /// Serializes the handle to JSON.
    pub fn to_json(&self) -> String {
        serde_json::json!({
            "chatId": self.chat_id,
            "owner": self.owner,
        })
        .to_string()
    }

    /// Parses a handle produced by [`to_json`](ConversationalQueryChatHandle::to_json). This is a local
    /// operation.
    ///
    /// Fails with [`ErrorKind::InvalidArgument`] if
    /// the JSON is malformed, a field is missing, or a field has the wrong type. Unrecognized
    /// fields are ignored.
    pub fn from_json(json: &str) -> error::Result<ConversationalQueryChatHandle> {
        let invalid = |msg: &str| Error::invalid_argument("handle", msg);

        let value: Value =
            serde_json::from_str(json).map_err(|e| invalid(&format!("malformed JSON: {e}")))?;
        let object = value
            .as_object()
            .ok_or_else(|| invalid("expected a JSON object"))?;

        let chat_id = match object.get("chatId") {
            Some(Value::String(chat_id)) => chat_id.clone(),
            Some(_) => return Err(invalid("chatId must be a string")),
            None => return Err(invalid("missing chatId")),
        };
        let owner = match object.get("owner") {
            Some(Value::String(owner)) => Some(owner.clone()),
            Some(Value::Null) => None,
            Some(_) => return Err(invalid("owner must be a string or null")),
            None => return Err(invalid("missing owner")),
        };

        Ok(ConversationalQueryChatHandle { chat_id, owner })
    }
}

fn collection_keyspaces(keyspaces: &[Collection]) -> error::Result<Vec<CollectionKeyspace>> {
    if keyspaces.is_empty() {
        return Err(Error::invalid_argument(
            "keyspaces",
            "at least one collection is required",
        ));
    }
    Ok(keyspaces
        .iter()
        .map(|c| CollectionKeyspace::new(c.bucket_name(), c.scope_name(), c.name()))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::conversational_query_options::{
        ConversationalModelCredential, ConversationalModelOptions,
    };
    use serde_json::json;

    #[test]
    fn empty_keyspaces_are_rejected() {
        let err = collection_keyspaces(&[]).unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::InvalidArgument(_)), "{err}");
    }

    #[test]
    fn chat_handle_round_trips() {
        for owner in [Some("node-1".to_string()), None] {
            let handle = ConversationalQueryChatHandle {
                chat_id: "chat-1".into(),
                owner: owner.clone(),
            };
            let json = handle.to_json();
            assert_eq!(
                json!({"chatId": "chat-1", "owner": owner}),
                serde_json::from_str::<Value>(&json).unwrap()
            );
            assert_eq!(
                handle,
                ConversationalQueryChatHandle::from_json(&json).unwrap()
            );
        }
    }

    #[test]
    fn chat_handle_ignores_unrecognized_fields() {
        let handle = ConversationalQueryChatHandle::from_json(
            r#"{"chatId": "c", "owner": null, "extra": 1}"#,
        )
        .unwrap();
        assert_eq!(
            ConversationalQueryChatHandle {
                chat_id: "c".into(),
                owner: None
            },
            handle
        );
    }

    #[test]
    fn chat_handle_rejects_invalid_json() {
        for json in [
            "not json",
            "[]",
            r#"{"owner": "n"}"#,
            r#"{"chatId": "c"}"#,
            r#"{"chatId": 1, "owner": "n"}"#,
            r#"{"chatId": "c", "owner": 1}"#,
        ] {
            let err = ConversationalQueryChatHandle::from_json(json).unwrap_err();
            assert!(
                matches!(err.kind(), ErrorKind::InvalidArgument(_)),
                "{json}: {err}"
            );
        }
    }

    #[test]
    fn debug_output_redacts_secrets() {
        let opts = ConversationalQueryOptions::new()
            .model(
                ConversationalModelOptions::new()
                    .add_raw("token", "raw-secret")
                    .unwrap(),
            )
            .credential(ConversationalModelCredential::capella(
                "me@example.com",
                "hunter2",
                "org",
            ))
            .add_raw("natural_cred", "param-secret")
            .unwrap();
        let debug = format!("{opts:?}");
        for secret in ["raw-secret", "hunter2", "param-secret"] {
            assert!(!debug.contains(secret), "{debug}");
        }
        assert!(debug.contains("token"), "{debug}");

        let debug = format!("{:?}", ConversationalModelCredential::api_key("sk-secret"));
        assert!(!debug.contains("sk-secret"), "{debug}");

        let debug = format!(
            "{:?}",
            ConversationalBeginChatOptions::new()
                .credential(ConversationalModelCredential::api_key("sk-secret"))
        );
        assert!(!debug.contains("sk-secret"), "{debug}");
    }
}
