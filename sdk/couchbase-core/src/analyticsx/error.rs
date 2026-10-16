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
        msg: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            endpoint: endpoint.into(),
            status_code,
            code,
            msg: msg.into(),
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
pub struct ErrorDesc {
    kind: ServerErrorKind,

    code: u32,
    message: String,
}

impl ErrorDesc {
    pub fn new(kind: ServerErrorKind, code: u32, message: String) -> Self {
        Self {
            kind,
            code,
            message,
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
}

impl Display for ErrorDesc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "error description of kind: {}, code: {}, message: {}",
            self.kind, self.code, self.message
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ServerErrorKind {
    CompilationFailure,
    Internal,
    AuthenticationFailure,
    ParsingFailure,
    ScanWaitTimeout,
    InvalidArgument {
        argument: Option<String>,
        reason: String,
    },
    TemporaryFailure,
    JobQueueFull,
    IndexNotFound,
    IndexExists,
    DatasetNotFound,
    DatasetExists,
    DataverseNotFound,
    DataverseExists,
    LinkNotFound,
    LinkExists,
    Unknown,
}

impl Display for ServerErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerErrorKind::Internal => write!(f, "internal server error"),
            ServerErrorKind::AuthenticationFailure => write!(f, "authentication failure"),
            ServerErrorKind::InvalidArgument { argument, reason } => {
                if let Some(argument) = argument {
                    write!(
                        f,
                        "server invalid argument: (argument: {argument}, reason: {reason})"
                    )
                } else {
                    write!(f, "server invalid argument: (reason: {reason})")
                }
            }
            ServerErrorKind::Unknown => write!(f, "unknown query error"),
            ServerErrorKind::CompilationFailure => write!(f, "compilation failure"),
            ServerErrorKind::ParsingFailure => write!(f, "parsing failure"),
            ServerErrorKind::ScanWaitTimeout => write!(f, "scan wait timeout"),
            ServerErrorKind::TemporaryFailure => write!(f, "temporary failure"),
            ServerErrorKind::DatasetExists => write!(f, "dataset exists"),
            ServerErrorKind::DatasetNotFound => write!(f, "dataset not found"),
            ServerErrorKind::DataverseExists => write!(f, "dataverse exists"),
            ServerErrorKind::DataverseNotFound => write!(f, "dataverse not found"),
            ServerErrorKind::IndexExists => write!(f, "index exists"),
            ServerErrorKind::IndexNotFound => write!(f, "index not found"),
            ServerErrorKind::LinkExists => write!(f, "link exists"),
            ServerErrorKind::LinkNotFound => write!(f, "link not found"),
            ServerErrorKind::JobQueueFull => write!(f, "job queue full"),
        }
    }
}

impl MetricsName for Error {
    fn metrics_name(&self) -> &'static str {
        match self.kind() {
            ErrorKind::Server(e) => e.kind().metrics_name(),
            ErrorKind::Http { error, .. } => error.metrics_name(),
            ErrorKind::Message { .. } => "analyticsx._OTHER",
            ErrorKind::InvalidArgument { .. } => "analyticsx.InvalidArgument",
            ErrorKind::Encoding { .. } => "analyticsx.Encoding",
        }
    }
}

impl MetricsName for ServerErrorKind {
    fn metrics_name(&self) -> &'static str {
        match self {
            ServerErrorKind::CompilationFailure => "analyticsx.CompilationFailure",
            ServerErrorKind::Internal => "analyticsx.Internal",
            ServerErrorKind::AuthenticationFailure => "analyticsx.AuthenticationFailure",
            ServerErrorKind::ParsingFailure => "analyticsx.ParsingFailure",
            ServerErrorKind::ScanWaitTimeout => "analyticsx.ScanWaitTimeout",
            ServerErrorKind::InvalidArgument { .. } => "analyticsx.InvalidArgument",
            ServerErrorKind::TemporaryFailure => "analyticsx.TemporaryFailure",
            ServerErrorKind::JobQueueFull => "analyticsx.JobQueueFull",
            ServerErrorKind::IndexNotFound => "analyticsx.IndexNotFound",
            ServerErrorKind::IndexExists => "analyticsx.IndexExists",
            ServerErrorKind::DatasetNotFound => "analyticsx.DatasetNotFound",
            ServerErrorKind::DatasetExists => "analyticsx.DatasetExists",
            ServerErrorKind::DataverseNotFound => "analyticsx.DataverseNotFound",
            ServerErrorKind::DataverseExists => "analyticsx.DataverseExists",
            ServerErrorKind::LinkNotFound => "analyticsx.LinkNotFound",
            ServerErrorKind::LinkExists => "analyticsx.LinkExists",
            ServerErrorKind::Unknown => "analyticsx._OTHER",
        }
    }
}
