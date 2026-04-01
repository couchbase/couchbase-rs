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

use crate::httpx;
use crate::tracingcomponent::MetricsName;
use http::StatusCode;
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::{Display, Formatter};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq)]
pub struct Error {
    inner: ErrorImpl,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner.kind)
    }
}

impl StdError for Error {}

impl Error {
    pub(crate) fn new_server_error(e: ServerError) -> Error {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::Server(e)),
            },
        }
    }

    pub(crate) fn new_resource_error(e: ResourceError) -> Error {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::Resource(e)),
            },
        }
    }

    pub(crate) fn new_message_error(
        msg: impl Into<String>,
        endpoint: impl Into<Option<String>>,
        statement: impl Into<Option<String>>,
        client_context_id: impl Into<Option<String>>,
    ) -> Error {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::Message {
                    msg: msg.into(),
                    endpoint: endpoint.into(),
                    statement: statement.into(),
                    client_context_id: client_context_id.into(),
                }),
            },
        }
    }

    pub(crate) fn new_encoding_error(msg: impl Into<String>) -> Error {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::Encoding { msg: msg.into() }),
            },
        }
    }

    pub(crate) fn new_invalid_argument_error(
        msg: impl Into<String>,
        arg: impl Into<Option<String>>,
    ) -> Self {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::InvalidArgument {
                    msg: msg.into(),
                    arg: arg.into(),
                }),
            },
        }
    }

    pub(crate) fn new_http_error(
        error: httpx::error::Error,
        endpoint: impl Into<String>,
        statement: impl Into<Option<String>>,
        client_context_id: impl Into<Option<String>>,
    ) -> Self {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::Http {
                    error,
                    endpoint: endpoint.into(),
                    statement: statement.into(),
                    client_context_id: client_context_id.into(),
                }),
            },
        }
    }

    pub fn kind(&self) -> &ErrorKind {
        &self.inner.kind
    }
}

#[derive(Debug, Clone)]
struct ErrorImpl {
    kind: Box<ErrorKind>,
}

impl PartialEq for ErrorImpl {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    Server(ServerError),
    #[non_exhaustive]
    Http {
        error: httpx::error::Error,
        endpoint: String,
        statement: Option<String>,
        client_context_id: Option<String>,
    },
    Resource(ResourceError),
    #[non_exhaustive]
    Message {
        msg: String,
        endpoint: Option<String>,
        statement: Option<String>,
        client_context_id: Option<String>,
    },
    #[non_exhaustive]
    InvalidArgument {
        msg: String,
        arg: Option<String>,
    },
    #[non_exhaustive]
    Encoding {
        msg: String,
    },
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::Server(e) => write!(f, "{e}"),
            ErrorKind::Resource(e) => write!(f, "{e}"),
            ErrorKind::InvalidArgument { msg, arg } => {
                let base_msg = format!("invalid argument: {msg}");
                if let Some(arg) = arg {
                    write!(f, "{base_msg}, arg: {arg}")
                } else {
                    write!(f, "{base_msg}")
                }
            }
            ErrorKind::Encoding { msg } => write!(f, "encoding error: {msg}"),
            ErrorKind::Http {
                error,
                endpoint,
                statement,
                client_context_id,
            } => {
                write!(f, "http error {error}: endpoint: {endpoint}")?;
                if let Some(statement) = statement {
                    write!(f, ", statement: {statement}")?;
                }
                if let Some(client_context_id) = client_context_id {
                    write!(f, ", client context id: {client_context_id}")?;
                }
                Ok(())
            }
            ErrorKind::Message {
                msg,
                endpoint,
                statement,
                client_context_id,
            } => {
                write!(f, "{msg}")?;
                if let Some(endpoint) = endpoint {
                    write!(f, ", endpoint: {endpoint}")?;
                }
                if let Some(statement) = statement {
                    write!(f, ", statement: {statement}")?;
                }
                if let Some(client_context_id) = client_context_id {
                    write!(f, ", client context id: {client_context_id}")?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerError {
    kind: ServerErrorKind,

    endpoint: String,
    status_code: StatusCode,
    code: u32,
    msg: String,
    retry: bool,

    statement: Option<String>,
    client_context_id: Option<String>,

    all_error_descs: Vec<ErrorDesc>,
}

impl ServerError {
    pub(crate) fn new(
        kind: ServerErrorKind,
        endpoint: impl Into<String>,
        status_code: StatusCode,
        code: u32,
        retry: bool,
        msg: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            endpoint: endpoint.into(),
            status_code,
            code,
            msg: msg.into(),
            retry,
            statement: None,
            client_context_id: None,
            all_error_descs: vec![],
        }
    }

    pub fn kind(&self) -> &ServerErrorKind {
        &self.kind
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn statement(&self) -> Option<&str> {
        self.statement.as_deref()
    }

    pub fn status_code(&self) -> StatusCode {
        self.status_code
    }

    pub fn client_context_id(&self) -> Option<&str> {
        self.client_context_id.as_deref()
    }

    pub fn code(&self) -> u32 {
        self.code
    }

    pub fn msg(&self) -> &str {
        &self.msg
    }

    pub fn retry(&self) -> bool {
        self.retry
    }

    pub fn all_error_descs(&self) -> &[ErrorDesc] {
        &self.all_error_descs
    }

    pub(crate) fn with_statement(mut self, statement: impl Into<String>) -> Self {
        self.statement = Some(statement.into());
        self
    }

    pub(crate) fn with_client_context_id(mut self, client_context_id: impl Into<String>) -> Self {
        self.client_context_id = Some(client_context_id.into());
        self
    }

    pub(crate) fn with_error_descs(mut self, error_descs: Vec<ErrorDesc>) -> Self {
        self.all_error_descs = error_descs;
        self
    }
}

impl Display for ServerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "server error of kind: {} code: {}, msg: {}",
            self.kind, self.code, self.msg
        )?;

        if let Some(client_context_id) = &self.client_context_id {
            write!(f, ", client context id: {client_context_id}")?;
        }
        if let Some(statement) = &self.statement {
            write!(f, ", statement: {statement}")?;
        }

        write!(
            f,
            ", endpoint: {},  status code: {}",
            self.endpoint, self.status_code
        )?;

        if !self.all_error_descs.is_empty() {
            write!(f, ", all error descriptions: {:?}", self.all_error_descs)?;
        }

        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ResourceError {
    cause: ServerError,
    bucket_name: Option<String>,
    scope_name: Option<String>,
    collection_name: Option<String>,
    index_name: Option<String>,
}

impl ResourceError {
    pub(crate) fn new(cause: ServerError) -> Self {
        match cause.kind {
            ServerErrorKind::CollectionNotFound => Self::parse_resource_not_found(cause),
            ServerErrorKind::ScopeNotFound => Self::parse_resource_not_found(cause),
            ServerErrorKind::AuthenticationFailure => Self::parse_auth_failure(cause),
            ServerErrorKind::IndexNotFound => Self::parse_index_not_found_or_exists(cause),
            ServerErrorKind::IndexExists => Self::parse_index_not_found_or_exists(cause),
            _ => Self {
                cause,
                bucket_name: None,
                scope_name: None,
                collection_name: None,
                index_name: None,
            },
        }
    }

    pub fn cause(&self) -> &ServerError {
        &self.cause
    }

    pub fn bucket_name(&self) -> Option<&str> {
        self.bucket_name.as_deref()
    }

    pub fn scope_name(&self) -> Option<&str> {
        self.scope_name.as_deref()
    }

    pub fn collection_name(&self) -> Option<&str> {
        self.collection_name.as_deref()
    }

    pub fn index_name(&self) -> Option<&str> {
        self.index_name.as_deref()
    }

    fn parse_index_not_found_or_exists(cause: ServerError) -> ResourceError {
        let msg = cause.msg.clone();
        let mut fields = msg.split_whitespace();

        // msg for not found is of the form - "Index Not Found - cause: GSI index testingIndex not found."
        // msg for index exists is of the form - "The index NewIndex already exists."
        while let Some(field) = fields.next() {
            if field == "index" {
                return ResourceError {
                    cause,
                    bucket_name: None,
                    scope_name: None,
                    collection_name: None,
                    index_name: Some(fields.next().unwrap().to_string()),
                };
            }
        }

        ResourceError {
            cause,
            bucket_name: None,
            scope_name: None,
            collection_name: None,
            index_name: None,
        }
    }

    fn parse_resource_not_found(cause: ServerError) -> ResourceError {
        let msg = cause.msg.clone();
        let mut fields = msg.split_whitespace();
        // Resource path is of the form bucket:bucket.scope.collection
        let path = fields.find(|f| f.contains('.') && f.contains(':'));

        if let Some(p) = path {
            if let Some(trimmed_path) = p.split(':').nth(1) {
                let fields: Vec<&str> = trimmed_path.split('.').collect();

                if cause.kind == ServerErrorKind::ScopeNotFound {
                    // Bucket names are the only one that can contain `.`, which is why we need to reconstruct the name if split
                    let bucket_name = fields[0..fields.len() - 1].join(".");
                    let scope_name = fields[fields.len() - 1];

                    return ResourceError {
                        cause,
                        bucket_name: Some(bucket_name),
                        scope_name: Some(scope_name.to_string()),
                        collection_name: None,
                        index_name: None,
                    };
                } else if cause.kind == ServerErrorKind::CollectionNotFound {
                    // Bucket names are the only one that can contain `.`, which is why we need to reconstruct the name if split
                    let bucket_name = fields[0..fields.len() - 2].join(".");
                    let scope_name = fields[fields.len() - 2];
                    let collection_name = fields[fields.len() - 1];

                    return ResourceError {
                        cause,
                        bucket_name: Some(bucket_name),
                        scope_name: Some(scope_name.to_string()),
                        collection_name: Some(collection_name.to_string()),
                        index_name: None,
                    };
                }
            }
        }

        ResourceError {
            cause,
            bucket_name: None,
            scope_name: None,
            collection_name: None,
            index_name: None,
        }
    }

    fn parse_auth_failure(cause: ServerError) -> Self {
        let msg = &cause.msg;
        let mut fields = msg.split_whitespace();
        let path = fields.find(|f| f.contains(':'));

        if let Some(p) = path {
            if let Some(trimmed_path) = p.split(':').nth(1) {
                let (bucket_name, scope_name, collection_name) = if trimmed_path.contains('`') {
                    // trimmedPath will have the form "`bucket.name`" or "`bucket.name`.scope.collection" so the first element of fields
                    // will be the empty string
                    let fields: Vec<&str> = trimmed_path.split('`').collect();

                    let bucket_name = fields[1];

                    let (scope_name, collection_name) = if fields[2].is_empty() {
                        (None, None)
                    } else {
                        // scope_and_col is of the form ".scope.collection" meaning fields[1] is empty and the names are in fields[1] and
                        // fields[2]
                        let scope_and_col = fields[2];
                        let fields: Vec<&str> = scope_and_col.split('.').collect();
                        let (scope, collection) = if fields.len() >= 3 {
                            (Some(fields[1].to_string()), Some(fields[2].to_string()))
                        } else {
                            (None, None)
                        };

                        (scope, collection)
                    };

                    (Some(bucket_name.to_string()), scope_name, collection_name)
                } else {
                    let fields: Vec<&str> = trimmed_path.split('.').collect();

                    let bucket_name = fields[0];

                    let (scope_name, collection_name) = if fields.len() >= 3 {
                        (Some(fields[1].to_string()), Some(fields[2].to_string()))
                    } else {
                        (None, None)
                    };

                    (Some(bucket_name.to_string()), scope_name, collection_name)
                };

                return ResourceError {
                    cause,
                    bucket_name,
                    scope_name,
                    collection_name,
                    index_name: None,
                };
            }
        }

        ResourceError {
            cause,
            bucket_name: None,
            scope_name: None,
            collection_name: None,
            index_name: None,
        }
    }
}

impl Display for ResourceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "resource error caused by: {}", self.cause)?;

        if let Some(bucket_name) = &self.bucket_name {
            write!(f, ", bucket: {bucket_name}")?;
        }
        if let Some(scope_name) = &self.scope_name {
            write!(f, ", scope: {scope_name}")?;
        }
        if let Some(collection_name) = &self.collection_name {
            write!(f, ", collection: {collection_name}")?;
        }
        if let Some(index_name) = &self.index_name {
            write!(f, ", index: {index_name}")?;
        }

        Ok(())
    }
}

impl StdError for ResourceError {}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ErrorDesc {
    kind: ServerErrorKind,

    code: u32,
    message: String,
    retry: bool,
    reason: HashMap<String, Value>,
}

impl ErrorDesc {
    pub fn new(
        kind: ServerErrorKind,
        code: u32,
        message: String,
        retry: bool,
        reason: HashMap<String, Value>,
    ) -> Self {
        Self {
            kind,
            code,
            message,
            retry,
            reason,
        }
    }

    pub fn kind(&self) -> &ServerErrorKind {
        &self.kind
    }

    pub fn code(&self) -> u32 {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn retry(&self) -> bool {
        self.retry
    }

    pub fn reason(&self) -> &HashMap<String, Value> {
        &self.reason
    }
}

impl Display for ErrorDesc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "error description of kind: {}, code: {}, message: {}, retry: {}, reason: {:?}",
            self.kind, self.code, self.message, self.retry, self.reason
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ServerErrorKind {
    ParsingFailure,
    Internal,
    AuthenticationFailure,
    CasMismatch,
    DocNotFound,
    DocExists,
    PlanningFailure,
    IndexFailure,
    PreparedStatementFailure,
    DMLFailure,
    Timeout,
    IndexExists,
    IndexNotFound,
    WriteInReadOnlyMode,
    ScopeNotFound,
    CollectionNotFound,
    InvalidArgument { argument: String, reason: String },
    BuildAlreadyInProgress,
    Unknown,
}

impl Display for ServerErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerErrorKind::ParsingFailure => write!(f, "parsing failure"),
            ServerErrorKind::Internal => write!(f, "internal server error"),
            ServerErrorKind::AuthenticationFailure => write!(f, "authentication failure"),
            ServerErrorKind::CasMismatch => write!(f, "cas mismatch"),
            ServerErrorKind::DocNotFound => write!(f, "doc not found"),
            ServerErrorKind::DocExists => write!(f, "doc exists"),
            ServerErrorKind::PlanningFailure => write!(f, "planning failure"),
            ServerErrorKind::IndexFailure => write!(f, "index failure"),
            ServerErrorKind::PreparedStatementFailure => write!(f, "prepared statement failure"),
            ServerErrorKind::DMLFailure => write!(
                f,
                "data service returned an error during execution of DML statement"
            ),
            ServerErrorKind::Timeout => write!(f, "server timeout"),
            ServerErrorKind::IndexExists => write!(f, "index exists"),
            ServerErrorKind::IndexNotFound => write!(f, "index not found"),
            ServerErrorKind::WriteInReadOnlyMode => {
                write!(f, "write statement used in a read-only query")
            }
            ServerErrorKind::ScopeNotFound => write!(f, "scope not found"),
            ServerErrorKind::CollectionNotFound => write!(f, "collection not found"),
            ServerErrorKind::InvalidArgument { argument, reason } => write!(
                f,
                "server invalid argument: (argument: {argument}, reason: {reason})"
            ),
            ServerErrorKind::BuildAlreadyInProgress => write!(f, "build already in progress"),
            ServerErrorKind::Unknown => write!(f, "unknown query error"),
        }
    }
}

impl Error {
    pub fn is_parsing_failure(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::ParsingFailure,
                ..
            })
        )
    }

    pub fn is_internal(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::Internal,
                ..
            })
        )
    }

    pub fn is_authentication_failure(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::AuthenticationFailure,
                ..
            })
        )
    }

    pub fn is_cas_mismatch(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::CasMismatch,
                ..
            })
        )
    }

    pub fn is_doc_not_found(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::DocNotFound,
                ..
            })
        )
    }

    pub fn is_doc_exists(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::DocExists,
                ..
            })
        )
    }

    pub fn is_planning_failure(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::PlanningFailure,
                ..
            })
        )
    }

    pub fn is_index_failure(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::IndexFailure,
                ..
            })
        )
    }

    pub fn is_prepared_statement_failure(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::PreparedStatementFailure,
                ..
            })
        )
    }

    pub fn is_dml_failure(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::DMLFailure,
                ..
            })
        )
    }

    pub fn is_server_timeout(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::Timeout,
                ..
            })
        )
    }

    pub fn is_write_in_read_only_mode(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::WriteInReadOnlyMode,
                ..
            })
        )
    }

    pub fn is_invalid_argument(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::InvalidArgument { .. },
                ..
            })
        )
    }

    pub fn is_build_already_in_progress(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::BuildAlreadyInProgress,
                ..
            })
        )
    }

    pub fn is_scope_not_found(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Resource(ResourceError {
                cause: ServerError {
                    kind: ServerErrorKind::ScopeNotFound,
                    ..
                },
                ..
            })
        ) || matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::ScopeNotFound,
                ..
            })
        )
    }

    pub fn is_collection_not_found(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Resource(ResourceError {
                cause: ServerError {
                    kind: ServerErrorKind::CollectionNotFound,
                    ..
                },
                ..
            })
        ) || matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::CollectionNotFound,
                ..
            })
        )
    }

    pub fn is_index_not_found(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Resource(ResourceError {
                cause: ServerError {
                    kind: ServerErrorKind::IndexNotFound,
                    ..
                },
                ..
            })
        ) || matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::IndexNotFound,
                ..
            })
        )
    }

    pub fn is_index_exists(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Resource(ResourceError {
                cause: ServerError {
                    kind: ServerErrorKind::IndexExists,
                    ..
                },
                ..
            })
        ) || matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::IndexExists,
                ..
            })
        )
    }
}

impl MetricsName for Error {
    fn metrics_name(&self) -> &'static str {
        match self.kind() {
            ErrorKind::Server(e) => e.kind().metrics_name(),
            ErrorKind::Resource(e) => e.cause().kind().metrics_name(),
            ErrorKind::Http { error, .. } => error.metrics_name(),
            ErrorKind::Message { .. } => "queryx._OTHER",
            ErrorKind::InvalidArgument { .. } => "queryx.InvalidArgument",
            ErrorKind::Encoding { .. } => "queryx.Encoding",
        }
    }
}

impl MetricsName for ServerErrorKind {
    fn metrics_name(&self) -> &'static str {
        match self {
            ServerErrorKind::ParsingFailure => "queryx.ParsingFailure",
            ServerErrorKind::Internal => "queryx.Internal",
            ServerErrorKind::AuthenticationFailure => "queryx.AuthenticationFailure",
            ServerErrorKind::CasMismatch => "queryx.CasMismatch",
            ServerErrorKind::DocNotFound => "queryx.DocNotFound",
            ServerErrorKind::DocExists => "queryx.DocExists",
            ServerErrorKind::PlanningFailure => "queryx.PlanningFailure",
            ServerErrorKind::IndexFailure => "queryx.IndexFailure",
            ServerErrorKind::PreparedStatementFailure => "queryx.PreparedStatementFailure",
            ServerErrorKind::DMLFailure => "queryx.DMLFailure",
            ServerErrorKind::Timeout => "queryx.Timeout",
            ServerErrorKind::IndexExists => "queryx.IndexExists",
            ServerErrorKind::IndexNotFound => "queryx.IndexNotFound",
            ServerErrorKind::WriteInReadOnlyMode => "queryx.WriteInReadOnlyMode",
            ServerErrorKind::ScopeNotFound => "queryx.ScopeNotFound",
            ServerErrorKind::CollectionNotFound => "queryx.CollectionNotFound",
            ServerErrorKind::InvalidArgument { .. } => "queryx.InvalidArgument",
            ServerErrorKind::BuildAlreadyInProgress => "queryx.BuildAlreadyInProgress",
            ServerErrorKind::Unknown => "queryx._OTHER",
        }
    }
}
