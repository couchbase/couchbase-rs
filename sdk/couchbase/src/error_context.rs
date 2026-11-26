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

use crate::error::ErrorKind;
use http::Method;
use serde::ser::SerializeStruct;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

/// Additional context information attached to an [`Error`](crate::error::Error).
///
/// Provides diagnostic details such as the last dispatched-to address, dispatched-from address,
/// and extended context specific to the service type (KV, query, search, HTTP).
///
/// For diagnostic purposes only, not programmatically accessible.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct ErrorContext {
    // retry_reasons: Vec<RetryReason>,
    // retry_attempts: u32,
    last_dispatched_from: Option<String>,
    last_dispatched_to: Option<String>,
    extended_context: Option<ExtendedErrorContext>,
}

impl ErrorContext {
    pub(crate) fn new(// retry_reasons: Vec<RetryReason>,
                      // retry_attempts: u32,
    ) -> Self {
        Self {
            // retry_reasons,
            // retry_attempts,
            last_dispatched_from: None,
            last_dispatched_to: None,
            extended_context: None,
        }
    }

    pub(crate) fn with_dispatched_from(mut self, dispatched_from: String) -> Self {
        self.last_dispatched_from = Some(dispatched_from);
        self
    }

    pub(crate) fn with_dispatched_to(mut self, dispatched_to: String) -> Self {
        self.last_dispatched_to = Some(dispatched_to);
        self
    }

    pub(crate) fn with_extended_context(mut self, context: ExtendedErrorContext) -> Self {
        self.extended_context = Some(context);
        self
    }

    pub(crate) fn extended_context(&self) -> Option<&ExtendedErrorContext> {
        self.extended_context.as_ref()
    }

    /// Returns the last address that the operation was dispatched to, if available.
    pub fn last_dispatched_to(&self) -> Option<&String> {
        self.last_dispatched_to.as_ref()
    }

    /// Returns the local address that the operation was dispatched from, if available.
    pub fn last_dispatched_from(&self) -> Option<&String> {
        self.last_dispatched_from.as_ref()
    }

    // pub fn retry_attempts(&self) -> u32 {
    //     self.retry_attempts
    // }
    //
    // pub fn retry_reasons(&self) -> &[RetryReason] {
    //     &self.retry_reasons
    // }
}

impl Serialize for ErrorContext {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("context", 0)?;
        // state.serialize_field("retry_attempts", &self.retry_attempts)?;
        // state.serialize_field(
        //     "retry_reasons",
        //     &self
        //         .retry_reasons
        //         .iter()
        //         .map(|rr| rr.to_string())
        //         .collect::<String>(),
        // )?;

        if let Some(ref last_dispatched_from) = self.last_dispatched_from {
            state.serialize_field("last_dispatched_from", last_dispatched_from)?;
        }

        if let Some(ref last_dispatched_to) = self.last_dispatched_to {
            state.serialize_field("last_dispatched_to", last_dispatched_to)?;
        }

        if let Some(ref extended_context) = self.extended_context {
            state.serialize_field("extended_context", extended_context)?;
        }

        state.end()
    }
}

impl Display for ErrorContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(&self))
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum ExtendedErrorContext {
    KeyValue(KeyValueErrorContext),
    Query(QueryErrorContext),
    Search(SearchErrorContext),
    Http(HttpErrorContext),
    Chat(ChatErrorContext),
}

impl Serialize for ExtendedErrorContext {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            ExtendedErrorContext::KeyValue(context) => context.serialize(serializer),
            ExtendedErrorContext::Query(context) => context.serialize(serializer),
            ExtendedErrorContext::Search(context) => context.serialize(serializer),
            ExtendedErrorContext::Http(context) => context.serialize(serializer),
            ExtendedErrorContext::Chat(context) => context.serialize(serializer),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(crate) struct KeyValueErrorContext {
    pub(crate) key: String,
    pub(crate) opaque: u32,
    pub(crate) op_code: u8,
    pub(crate) status: u16,
    pub(crate) bucket_name: String,
    pub(crate) scope_name: String,
    pub(crate) collection_name: String,
    pub(crate) error_name: Option<String>,
    pub(crate) error_desc: Option<String>,
    pub(crate) xcontent: Option<String>,
    pub(crate) xref: Option<String>,
    pub(crate) source_message: Option<String>,
}

impl KeyValueErrorContext {
    pub fn new(
        key: String,
        opaque: u32,
        op_code: u8,
        status: u16,
        bucket_name: String,
        scope_name: String,
        collection_name: String,
    ) -> Self {
        Self {
            key,
            opaque,
            op_code,
            status,
            bucket_name,
            scope_name,
            collection_name,
            error_name: None,
            error_desc: None,
            xcontent: None,
            xref: None,
            source_message: None,
        }
    }

    pub fn with_error_name(mut self, error_name: String) -> Self {
        self.error_name = Some(error_name);
        self
    }

    pub fn with_error_desc(mut self, error_desc: String) -> Self {
        self.error_desc = Some(error_desc);
        self
    }

    pub fn with_xcontent(mut self, xcontent: String) -> Self {
        self.xcontent = Some(xcontent);
        self
    }

    pub fn with_xref(mut self, xref: String) -> Self {
        self.xref = Some(xref);
        self
    }

    pub fn with_source_message(mut self, source_message: String) -> Self {
        self.source_message = Some(source_message);
        self
    }
}

impl Serialize for KeyValueErrorContext {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("context", 7)?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("opaque", &self.opaque)?;
        state.serialize_field("opCode", &self.op_code)?;
        state.serialize_field("status", &self.status)?;
        state.serialize_field("bucketName", &self.bucket_name)?;
        state.serialize_field("scopeName", &self.scope_name)?;
        state.serialize_field("collectionName", &self.collection_name)?;

        if let Some(ref error_name) = self.error_name {
            state.serialize_field("errorName", error_name)?;
        }

        if let Some(ref error_desc) = self.error_desc {
            state.serialize_field("errorDesc", error_desc)?;
        }

        if let Some(ref xcontent) = self.xcontent {
            state.serialize_field("xcontent", xcontent)?;
        }

        if let Some(ref xref) = self.xref {
            state.serialize_field("xref", xref)?;
        }

        if let Some(ref source_message) = self.source_message {
            state.serialize_field("sourceMessage", source_message)?;
        }

        state.end()
    }
}

impl Display for KeyValueErrorContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(&self))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct QueryErrorDesc {
    pub(crate) kind: ErrorKind,

    pub(crate) code: u32,
    pub(crate) message: String,
    pub(crate) retry: bool,
    pub(crate) reason: Value,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct QueryErrorContext {
    pub(crate) statement: String,
    pub(crate) code: Option<u32>,
    pub(crate) message: Option<String>,
    pub(crate) client_context_id: String,
    pub(crate) http_status_code: Option<http::StatusCode>,
    pub(crate) descs: Vec<QueryErrorDesc>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SearchErrorContext {
    pub(crate) index_name: String,
    pub(crate) error_text: Option<String>,
    pub(crate) http_status_code: Option<http::StatusCode>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct HttpErrorContext {
    pub(crate) status_code: http::StatusCode,
    pub(crate) path: String,
    pub(crate) method: Method,
    pub(crate) error_text: Option<String>,
}

impl Serialize for QueryErrorDesc {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("context", 4)?;
        state.serialize_field("code", &self.code)?;
        state.serialize_field("message", &self.message)?;
        state.serialize_field("retry", &self.retry)?;
        state.serialize_field("reason", &self.reason)?;

        state.end()
    }
}

impl Serialize for QueryErrorContext {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("context", 2)?;
        state.serialize_field("statement", &self.statement)?;
        state.serialize_field("clientContextId", &self.client_context_id)?;

        if let Some(ref code) = self.code {
            state.serialize_field("code", &code)?;
        }

        if let Some(ref message) = self.message {
            state.serialize_field("message", message)?;
        }

        if let Some(ref http_status_code) = self.http_status_code {
            state.serialize_field("httpStatusCode", &http_status_code.as_u16())?;
        }

        state.serialize_field("descs", &self.descs)?;

        state.end()
    }
}

/// Context for a Conversational Query chat operation that could not reach the chat.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct ChatErrorContext {
    pub(crate) chat_id: String,
    pub(crate) owner: Option<String>,
    pub(crate) operation: &'static str,
    // The server error, when Query reported that the chat is not live.
    pub(crate) query: Option<QueryErrorContext>,
}

impl Serialize for ChatErrorContext {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("context", 4)?;
        state.serialize_field("chatId", &self.chat_id)?;
        state.serialize_field("owner", &self.owner)?;
        state.serialize_field("operation", self.operation)?;
        if let Some(ref query) = self.query {
            state.serialize_field("query", query)?;
        }
        state.end()
    }
}

impl Display for QueryErrorContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(&self))
    }
}

impl Serialize for SearchErrorContext {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("context", 1)?;
        state.serialize_field("indexName", &self.index_name)?;

        if let Some(ref error_text) = self.error_text {
            state.serialize_field("errorText", error_text)?;
        }

        if let Some(ref http_status_code) = self.http_status_code {
            state.serialize_field("httpStatusCode", &http_status_code.as_u16())?;
        }

        state.end()
    }
}

impl Serialize for HttpErrorContext {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("context", 3)?;
        state.serialize_field("statusCode", &self.status_code.as_u16())?;
        state.serialize_field("path", &self.path)?;
        state.serialize_field("method", &self.method.to_string())?;

        if let Some(ref error_text) = self.error_text {
            state.serialize_field("errorText", error_text)?;
        }

        state.end()
    }
}
