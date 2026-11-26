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

use crate::error::{Error, ErrorKind, Result};
use crate::options::query::QueryOptions;
use crate::queryx::query_options::NaturalLanguageOptions;
use crate::retry::{RetryStrategy, DEFAULT_RETRY_STRATEGY};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct CollectionKeyspace {
    pub bucket_name: String,
    pub scope_name: String,
    pub collection_name: String,
}

impl CollectionKeyspace {
    pub fn new(
        bucket_name: impl Into<String>,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
    ) -> Self {
        Self {
            bucket_name: bucket_name.into(),
            scope_name: scope_name.into(),
            collection_name: collection_name.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConversationalQueryOutput {
    Sqlpp,
    FtsSqlpp,
    JsUdf,
}

impl ConversationalQueryOutput {
    pub(crate) fn wire_value(&self) -> &'static str {
        match self {
            ConversationalQueryOutput::Sqlpp => "SQL",
            ConversationalQueryOutput::FtsSqlpp => "FTSSQL",
            ConversationalQueryOutput::JsUdf => "JSUDF",
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ConversationalQueryOptions {
    pub prompt: String,
    pub keyspaces: Vec<CollectionKeyspace>,
    pub hint: Option<String>,
    pub execute: Option<bool>,
    pub output: Option<ConversationalQueryOutput>,
    pub use_knowledge: Option<bool>,
    pub natural_language: NaturalLanguageOptions,
    pub query_options: QueryOptions,
}

impl ConversationalQueryOptions {
    pub fn new(prompt: impl Into<String>, keyspaces: Vec<CollectionKeyspace>) -> Self {
        Self {
            prompt: prompt.into(),
            keyspaces,
            hint: None,
            execute: None,
            output: None,
            use_knowledge: None,
            natural_language: NaturalLanguageOptions::new(),
            query_options: QueryOptions::new(),
        }
    }

    pub fn hint(mut self, hint: impl Into<Option<String>>) -> Self {
        self.hint = hint.into();
        self
    }

    pub fn execute(mut self, execute: impl Into<Option<bool>>) -> Self {
        self.execute = execute.into();
        self
    }

    pub fn output(mut self, output: impl Into<Option<ConversationalQueryOutput>>) -> Self {
        self.output = output.into();
        self
    }

    pub fn use_knowledge(mut self, use_knowledge: impl Into<Option<bool>>) -> Self {
        self.use_knowledge = use_knowledge.into();
        self
    }

    pub fn natural_language(mut self, natural_language: NaturalLanguageOptions) -> Self {
        self.natural_language = natural_language;
        self
    }

    pub fn query_options(mut self, query_options: QueryOptions) -> Self {
        self.query_options = query_options;
        self
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct BeginChatOptions {
    pub keyspaces: Vec<CollectionKeyspace>,
    pub use_knowledge: Option<bool>,
    pub inactivity_timeout: Option<Duration>,
    pub server_timeout: Option<Duration>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl BeginChatOptions {
    pub fn new(keyspaces: Vec<CollectionKeyspace>) -> Self {
        Self {
            keyspaces,
            use_knowledge: None,
            inactivity_timeout: None,
            server_timeout: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn use_knowledge(mut self, use_knowledge: impl Into<Option<bool>>) -> Self {
        self.use_knowledge = use_knowledge.into();
        self
    }

    pub fn inactivity_timeout(mut self, inactivity_timeout: impl Into<Option<Duration>>) -> Self {
        self.inactivity_timeout = inactivity_timeout.into();
        self
    }

    pub fn server_timeout(mut self, server_timeout: impl Into<Option<Duration>>) -> Self {
        self.server_timeout = server_timeout.into();
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct AskChatOptions {
    pub prompt: String,
    pub hint: Option<String>,
    pub execute: Option<bool>,
    pub output: Option<ConversationalQueryOutput>,
    pub natural_language: NaturalLanguageOptions,
    pub query_options: QueryOptions,
}

impl AskChatOptions {
    pub fn new(prompt: impl Into<String>) -> Self {
        Self {
            prompt: prompt.into(),
            hint: None,
            execute: None,
            output: None,
            natural_language: NaturalLanguageOptions::new(),
            query_options: QueryOptions::new(),
        }
    }

    pub fn hint(mut self, hint: impl Into<Option<String>>) -> Self {
        self.hint = hint.into();
        self
    }

    pub fn execute(mut self, execute: impl Into<Option<bool>>) -> Self {
        self.execute = execute.into();
        self
    }

    pub fn output(mut self, output: impl Into<Option<ConversationalQueryOutput>>) -> Self {
        self.output = output.into();
        self
    }

    pub fn natural_language(mut self, natural_language: NaturalLanguageOptions) -> Self {
        self.natural_language = natural_language;
        self
    }

    pub fn query_options(mut self, query_options: QueryOptions) -> Self {
        self.query_options = query_options;
        self
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PauseChatOptions {
    pub summarize: Option<bool>,
    pub natural_language: NaturalLanguageOptions,
    pub server_timeout: Option<Duration>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl Default for PauseChatOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl PauseChatOptions {
    pub fn new() -> Self {
        Self {
            summarize: None,
            natural_language: NaturalLanguageOptions::new(),
            server_timeout: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn summarize(mut self, summarize: impl Into<Option<bool>>) -> Self {
        self.summarize = summarize.into();
        self
    }

    pub fn natural_language(mut self, natural_language: NaturalLanguageOptions) -> Self {
        self.natural_language = natural_language;
        self
    }

    pub fn server_timeout(mut self, server_timeout: impl Into<Option<Duration>>) -> Self {
        self.server_timeout = server_timeout.into();
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ResumeChatOptions {
    pub server_timeout: Option<Duration>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl Default for ResumeChatOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl ResumeChatOptions {
    pub fn new() -> Self {
        Self {
            server_timeout: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn server_timeout(mut self, server_timeout: impl Into<Option<Duration>>) -> Self {
        self.server_timeout = server_timeout.into();
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct EndChatOptions {
    pub server_timeout: Option<Duration>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl Default for EndChatOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl EndChatOptions {
    pub fn new() -> Self {
        Self {
            server_timeout: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn server_timeout(mut self, server_timeout: impl Into<Option<Duration>>) -> Self {
        self.server_timeout = server_timeout.into();
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

pub struct ChatState {
    chat_id: String,
    routing: Mutex<Routing>,
}

#[derive(Debug, Default)]
struct Routing {
    owner: Option<String>,
    closed: bool, // Set once END succeeds.
}

impl ChatState {
    pub fn new(chat_id: impl Into<String>, owner: Option<String>) -> Self {
        Self {
            chat_id: chat_id.into(),
            routing: Mutex::new(Routing {
                owner,
                closed: false,
            }),
        }
    }

    pub fn chat_id(&self) -> &str {
        &self.chat_id
    }

    // The current owner, or an error once the chat has ended.
    pub fn owner(&self) -> Result<Option<String>> {
        let routing = self.routing.lock().unwrap();
        if routing.closed {
            return Err(Error::new(ErrorKind::InvalidArgument {
                msg: "the chat has ended".to_string(),
                arg: Some("chat".to_string()),
            }));
        }
        Ok(routing.owner.clone())
    }

    pub(crate) fn set_owner(&self, owner: Option<String>) {
        self.routing.lock().unwrap().owner = owner;
    }

    pub(crate) fn close(&self) {
        self.routing.lock().unwrap().closed = true;
    }

    // Sets the owner to `owner` unless another operation changed it after `expected` was set.
    pub(crate) fn replace_owner(&self, expected: &str, owner: Option<String>) {
        let mut routing = self.routing.lock().unwrap();
        if routing.owner.as_deref() == Some(expected) {
            routing.owner = owner;
        }
    }
}

impl std::fmt::Debug for ChatState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let routing = self.routing.lock().unwrap();
        f.debug_struct("ChatState")
            .field("chat_id", &self.chat_id)
            .field("owner", &routing.owner)
            .field("closed", &routing.closed)
            .finish()
    }
}
