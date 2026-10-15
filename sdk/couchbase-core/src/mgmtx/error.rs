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
use http::{Method, StatusCode};
use std::error::Error as StdError;
use std::fmt::{Display, Formatter};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, PartialEq)]
pub struct Error {
    inner: Box<ErrorImpl>,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner.kind)
    }
}

impl StdError for Error {}

impl Error {
    pub(crate) fn new_message_error(msg: impl Into<String>) -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind: ErrorKind::Message(msg.into()),
            }),
        }
    }

    pub(crate) fn new_invalid_argument_error(
        msg: impl Into<String>,
        arg: impl Into<Option<String>>,
    ) -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind: ErrorKind::InvalidArgument {
                    msg: msg.into(),
                    arg: arg.into(),
                },
            }),
        }
    }

    pub fn kind(&self) -> &ErrorKind {
        &self.inner.kind
    }
}

#[derive(Debug)]
pub struct ErrorImpl {
    pub kind: ErrorKind,
}

impl PartialEq for ErrorImpl {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    Server(ServerError),
    Resource(ResourceError),
    #[non_exhaustive]
    InvalidArgument {
        msg: String,
        arg: Option<String>,
    },
    Message(String),
    Http(httpx::error::Error),
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::Server(e) => write!(f, "server error: {e}"),
            ErrorKind::Resource(e) => write!(f, "resource error: {e}"),
            ErrorKind::InvalidArgument { msg, arg } => {
                if let Some(arg) = arg {
                    write!(f, "invalid argument: {msg}: {arg}")
                } else {
                    write!(f, "invalid argument: {msg}")
                }
            }
            ErrorKind::Message(msg) => write!(f, "{msg}"),
            ErrorKind::Http(e) => write!(f, "http error: {e}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerError {
    status_code: StatusCode,
    url: String,
    body: String,
    method: Method,
    path: String,
    kind: ServerErrorKind,
}

impl StdError for ServerError {}

impl Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "server error: method: {}, path: {} status code: {}, body: {}, kind: {}",
            self.method, self.path, self.status_code, self.body, self.kind
        )
    }
}

impl ServerError {
    pub(crate) fn new(
        status_code: StatusCode,
        url: String,
        method: Method,
        path: String,
        body: String,
        kind: ServerErrorKind,
    ) -> Self {
        Self {
            status_code,
            url,
            method,
            path,
            body,
            kind,
        }
    }

    pub fn kind(&self) -> &ServerErrorKind {
        &self.kind
    }

    pub fn status_code(&self) -> StatusCode {
        self.status_code
    }

    pub fn body(&self) -> &str {
        &self.body
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn method(&self) -> &Method {
        &self.method
    }

    pub fn url(&self) -> &str {
        &self.url
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
#[non_exhaustive]
pub enum ServerErrorKind {
    AccessDenied,
    UnsupportedFeature { feature: String },
    ScopeExists,
    ScopeNotFound,
    CollectionExists,
    CollectionNotFound,
    BucketExists,
    BucketNotFound,
    FlushDisabled,
    ServerInvalidArg { arg: Option<String>, reason: String },
    SampleAlreadyLoaded,
    InvalidSampleBucket,
    BucketUuidMismatch,
    UserNotFound,
    GroupNotFound,
    OperationDelayed,
    Unknown,
}

impl Display for ServerErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerErrorKind::AccessDenied => write!(f, "access denied"),
            ServerErrorKind::UnsupportedFeature { feature } => {
                write!(f, "unsupported feature {feature}")
            }
            ServerErrorKind::ScopeExists => write!(f, "scope exists"),
            ServerErrorKind::ScopeNotFound => write!(f, "scope not found"),
            ServerErrorKind::CollectionExists => write!(f, "collection exists"),
            ServerErrorKind::CollectionNotFound => write!(f, "collection not found"),
            ServerErrorKind::BucketExists => write!(f, "bucket exists"),
            ServerErrorKind::BucketNotFound => write!(f, "bucket not found"),
            ServerErrorKind::FlushDisabled => write!(f, "flush disabled"),
            ServerErrorKind::ServerInvalidArg { arg, reason } => {
                if let Some(arg) = arg {
                    write!(f, "server invalid argument: {arg} - {reason}")
                } else {
                    write!(f, "server invalid argument: {reason}")
                }
            }
            ServerErrorKind::BucketUuidMismatch => write!(f, "bucket uuid mismatch"),
            ServerErrorKind::UserNotFound => write!(f, "user not found"),
            ServerErrorKind::GroupNotFound => write!(f, "group not found"),
            ServerErrorKind::OperationDelayed => {
                write!(f, "operation was delayed, but will continue")
            }
            ServerErrorKind::SampleAlreadyLoaded => write!(f, "sample already loaded"),
            ServerErrorKind::InvalidSampleBucket => write!(f, "invalid sample bucket"),
            ServerErrorKind::Unknown => write!(f, "unknown error"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceError {
    cause: ServerError,
    scope_name: String,
    collection_name: String,
    bucket_name: String,
}

impl StdError for ResourceError {}

impl Display for ResourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "resource error: scope: {}, collection: {}, bucket: {}, cause: {}",
            self.scope_name, self.collection_name, self.bucket_name, self.cause
        )
    }
}

impl ResourceError {
    pub(crate) fn new(
        cause: ServerError,
        bucket_name: impl Into<String>,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
    ) -> Self {
        Self {
            cause,
            bucket_name: bucket_name.into(),
            scope_name: scope_name.into(),
            collection_name: collection_name.into(),
        }
    }

    pub fn cause(&self) -> &ServerError {
        &self.cause
    }

    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }

    pub fn scope_name(&self) -> &str {
        &self.scope_name
    }

    pub fn collection_name(&self) -> &str {
        &self.collection_name
    }
}

impl<E> From<E> for Error
where
    ErrorKind: From<E>,
{
    fn from(err: E) -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind: ErrorKind::from(err),
            }),
        }
    }
}

impl From<ServerError> for Error {
    fn from(value: ServerError) -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind: ErrorKind::Server(value),
            }),
        }
    }
}

impl From<ResourceError> for Error {
    fn from(value: ResourceError) -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind: ErrorKind::Resource(value),
            }),
        }
    }
}

impl From<httpx::error::Error> for Error {
    fn from(value: httpx::error::Error) -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind: ErrorKind::Http(value),
            }),
        }
    }
}

impl MetricsName for Error {
    fn metrics_name(&self) -> &'static str {
        match self.kind() {
            ErrorKind::Server(e) => e.kind().metrics_name(),
            ErrorKind::Resource(e) => e.cause().kind().metrics_name(),
            ErrorKind::InvalidArgument { .. } => "mgmtx.InvalidArgument",
            ErrorKind::Message(_) => "mgmtx._OTHER",
            ErrorKind::Http(e) => e.metrics_name(),
        }
    }
}

impl MetricsName for ServerErrorKind {
    fn metrics_name(&self) -> &'static str {
        match self {
            ServerErrorKind::AccessDenied => "mgmtx.AccessDenied",
            ServerErrorKind::UnsupportedFeature { .. } => "mgmtx.UnsupportedFeature",
            ServerErrorKind::ScopeExists => "mgmtx.ScopeExists",
            ServerErrorKind::ScopeNotFound => "mgmtx.ScopeNotFound",
            ServerErrorKind::CollectionExists => "mgmtx.CollectionExists",
            ServerErrorKind::CollectionNotFound => "mgmtx.CollectionNotFound",
            ServerErrorKind::BucketExists => "mgmtx.BucketExists",
            ServerErrorKind::BucketNotFound => "mgmtx.BucketNotFound",
            ServerErrorKind::FlushDisabled => "mgmtx.FlushDisabled",
            ServerErrorKind::ServerInvalidArg { .. } => "mgmtx.ServerInvalidArg",
            ServerErrorKind::SampleAlreadyLoaded => "mgmtx.SampleAlreadyLoaded",
            ServerErrorKind::InvalidSampleBucket => "mgmtx.InvalidSampleBucket",
            ServerErrorKind::BucketUuidMismatch => "mgmtx.BucketUuidMismatch",
            ServerErrorKind::UserNotFound => "mgmtx.UserNotFound",
            ServerErrorKind::GroupNotFound => "mgmtx.GroupNotFound",
            ServerErrorKind::OperationDelayed => "mgmtx.OperationDelayed",
            ServerErrorKind::Unknown => "mgmtx._OTHER",
        }
    }
}
