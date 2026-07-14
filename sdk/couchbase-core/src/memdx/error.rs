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

use serde::Deserialize;
use std::error::Error as StdError;
use std::fmt::{Display, Formatter, Pointer};
use std::io;

use crate::memdx::opcode::OpCode;
use crate::memdx::status::Status;
use crate::tracingcomponent::MetricsName;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, PartialEq)]
pub struct Error {
    inner: ErrorImpl,
}

impl Error {
    pub(crate) fn new_protocol_error(msg: impl Into<String>) -> Self {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::Protocol { msg: msg.into() }),
                source: None,
            },
        }
    }

    pub(crate) fn new_decompression_error() -> Self {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::Decompression {}),
                source: None,
            },
        }
    }

    pub(crate) fn new_message_error(msg: impl Into<String>) -> Self {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::Message(msg.into())),
                source: None,
            },
        }
    }

    pub(crate) fn new_cancelled_error(cancellation_kind: CancellationErrorKind) -> Self {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::Cancelled(cancellation_kind)),
                source: None,
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
                source: None,
            },
        }
    }

    pub(crate) fn new_connection_failed_error(
        reason: impl Into<String>,
        source: Box<io::Error>,
    ) -> Self {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::ConnectionFailed { msg: reason.into() }),
                source: Some(source),
            },
        }
    }

    pub(crate) fn new_dispatch_error(opaque: u32, op_code: OpCode, source: Box<Error>) -> Self {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::Dispatch { opaque, op_code }),
                source: Some(source),
            },
        }
    }

    pub(crate) fn new_close_error(msg: String, source: Box<Error>) -> Self {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::Close { msg }),
                source: Some(source),
            },
        }
    }

    pub fn has_server_config(&self) -> Option<&Vec<u8>> {
        if let ErrorKind::Server(ServerError { config, .. }) = self.inner.kind.as_ref() {
            config.as_ref()
        } else {
            None
        }
    }

    pub fn has_server_error_context(&self) -> Option<&Vec<u8>> {
        if let ErrorKind::Server(ServerError { context, .. }) = self.inner.kind.as_ref() {
            context.as_ref()
        } else if let ErrorKind::Resource(ResourceError { cause, .. }) = self.inner.kind.as_ref() {
            cause.context.as_ref()
        } else {
            None
        }
    }

    pub fn has_opaque(&self) -> Option<u32> {
        let inner_kind = self.inner.kind.as_ref();
        if let ErrorKind::Server(ServerError { opaque, .. }) = inner_kind {
            Some(*opaque)
        } else if let ErrorKind::Resource(e) = inner_kind {
            Some(e.cause.opaque)
        } else if let ErrorKind::Dispatch { opaque, .. } = inner_kind {
            Some(*opaque)
        } else {
            None
        }
    }

    pub fn is_cancellation_error(&self) -> bool {
        matches!(self.inner.kind.as_ref(), ErrorKind::Cancelled { .. })
    }

    pub fn is_dispatch_error(&self) -> bool {
        matches!(self.inner.kind.as_ref(), ErrorKind::Dispatch { .. })
    }

    pub fn is_server_error_kind(&self, kind: ServerErrorKind) -> bool {
        match self.inner.kind.as_ref() {
            ErrorKind::Server(e) => e.kind == kind,
            ErrorKind::Resource(e) => e.cause.kind == kind,
            _ => false,
        }
    }

    pub fn kind(&self) -> &ErrorKind {
        &self.inner.kind
    }

    pub(crate) fn with<C: Into<Source>>(mut self, source: C) -> Error {
        self.inner.source = Some(source.into());
        self
    }
}

type Source = Box<dyn StdError + Send + Sync>;

#[derive(Debug)]
struct ErrorImpl {
    kind: Box<ErrorKind>,
    source: Option<Source>,
}

impl PartialEq for ErrorImpl {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner.kind)?;
        if let Some(source) = &self.inner.source {
            write!(f, ": {source}")?;
        }
        Ok(())
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.inner
            .source
            .as_ref()
            .map(|cause| &**cause as &(dyn StdError + 'static))
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum ErrorKind {
    Server(ServerError),
    Resource(ResourceError),
    #[non_exhaustive]
    Dispatch {
        opaque: u32,
        op_code: OpCode,
    },
    #[non_exhaustive]
    Close {
        msg: String,
    },
    #[non_exhaustive]
    Protocol {
        msg: String,
    },
    Cancelled(CancellationErrorKind),
    #[non_exhaustive]
    ConnectionFailed {
        msg: String,
    },
    Io,
    #[non_exhaustive]
    InvalidArgument {
        msg: String,
        arg: Option<String>,
    },
    Decompression,
    Message(String),
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::Server(e) => write!(f, "{e}"),
            ErrorKind::Resource(e) => write!(f, "{e}"),
            ErrorKind::Dispatch { opaque, op_code } => {
                write!(f, "dispatch failed: opaque: {opaque}, op_code: {op_code}")
            }
            ErrorKind::Close { msg } => {
                write!(f, "close error {msg}")
            }
            ErrorKind::Protocol { msg } => {
                write!(f, "{msg}")
            }
            ErrorKind::Cancelled(kind) => {
                write!(f, "request cancelled: {kind}")
            }
            ErrorKind::ConnectionFailed { msg } => {
                write!(f, "connection failed {msg}")
            }
            ErrorKind::Io => {
                write!(f, "connection error")
            }
            ErrorKind::InvalidArgument { msg, arg } => {
                let base_msg = format!("invalid argument: {msg}");
                if let Some(arg) = arg {
                    write!(f, "{base_msg}, arg: {arg}")
                } else {
                    write!(f, "{base_msg}")
                }
            }
            ErrorKind::Decompression => write!(f, "decompression error"),
            ErrorKind::Message(msg) => write!(f, "{msg}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ResourceError {
    cause: ServerError,
    scope_name: String,
    collection_name: String,
}

impl StdError for ResourceError {}

impl ResourceError {
    pub(crate) fn new(
        cause: ServerError,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
    ) -> Self {
        Self {
            cause,
            scope_name: scope_name.into(),
            collection_name: collection_name.into(),
        }
    }

    pub fn cause(&self) -> &ServerError {
        &self.cause
    }

    pub fn scope_name(&self) -> &str {
        &self.scope_name
    }

    pub fn collection_name(&self) -> &str {
        &self.collection_name
    }
}

impl Display for ResourceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Resource error: {}, scope: {}, collection: {}",
            self.cause, self.scope_name, self.collection_name
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ServerError {
    kind: ServerErrorKind,
    config: Option<Vec<u8>>,
    context: Option<Vec<u8>>,
    op_code: OpCode,
    status: Status,
    opaque: u32,
}

impl StdError for ServerError {}

impl Display for ServerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut base_msg = format!(
            "Server error: {}, status: 0x{:02x}, opcode: {}, opaque: {}",
            self.kind,
            u16::from(self.status),
            self.op_code,
            self.opaque
        );

        if let Some(context) = &self.context {
            if let Some(parsed) = Self::parse_context(context) {
                base_msg.push_str(" (");
                if let Some(text) = &parsed.text {
                    base_msg.push_str(&format!("context: {text}, "));
                }

                if let Some(error_ref) = &parsed.error_ref {
                    base_msg.push_str(&format!("error_ref: {error_ref}"));
                }
                base_msg.push(')');
            }
        }

        write!(f, "{base_msg}")
    }
}

impl ServerError {
    pub(crate) fn new(kind: ServerErrorKind, op_code: OpCode, status: Status, opaque: u32) -> Self {
        Self {
            kind,
            config: None,
            context: None,
            op_code,
            status,
            opaque,
        }
    }

    pub(crate) fn with_context(mut self, context: Vec<u8>) -> Self {
        self.context = Some(context);
        self
    }

    pub(crate) fn with_config(mut self, config: Vec<u8>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn kind(&self) -> &ServerErrorKind {
        &self.kind
    }

    pub fn config(&self) -> Option<&Vec<u8>> {
        self.config.as_ref()
    }

    pub fn context(&self) -> Option<&Vec<u8>> {
        self.context.as_ref()
    }

    pub fn op_code(&self) -> OpCode {
        self.op_code
    }

    pub fn status(&self) -> Status {
        self.status
    }

    pub fn opaque(&self) -> u32 {
        self.opaque
    }

    pub fn parse_context(context: &[u8]) -> Option<ServerErrorContext> {
        if context.is_empty() {
            return None;
        }

        let context_json: ServerErrorContextJson = match serde_json::from_slice(context) {
            Ok(c) => c,
            Err(_) => {
                return None;
            }
        };

        let text = context_json.error.context;

        let error_ref = context_json.error.error_ref;

        let manifest_rev = context_json
            .manifest_rev
            .map(|manifest_rev| u64::from_str_radix(&manifest_rev, 16).unwrap_or_default());

        Some(ServerErrorContext {
            text,
            error_ref,
            manifest_rev,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ServerErrorContext {
    pub text: Option<String>,
    pub error_ref: Option<String>,
    pub manifest_rev: Option<u64>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ServerErrorKind {
    KeyNotFound,
    KeyExists,
    TooBig,
    NotStored,
    BadDelta,
    NotMyVbucket,
    NoBucket,
    Locked,
    NotLocked,
    Auth { msg: String },
    RangeError,
    Access,
    RateLimitedNetworkIngress,
    RateLimitedNetworkEgress,
    RateLimitedMaxConnections,
    RateLimitedMaxCommands,
    RateLimitedScopeSizeLimitExceeded,
    UnknownCommand,
    NotSupported,
    InternalError,
    Busy,
    TmpFail,
    UnknownCollectionID,
    UnknownScopeName,
    UnknownCollectionName,
    DurabilityInvalid,
    DurabilityImpossible,
    SyncWriteInProgress,
    SyncWriteAmbiguous,
    SyncWriteRecommitInProgress,
    RangeScanCancelled,
    RangeScanVBUUIDNotEqual,
    InvalidArgs,
    AuthStale,

    ConfigNotSet,
    UnknownBucketName,
    CasMismatch,

    Subdoc { error: SubdocError },

    UnknownStatus { status: Status },
}

impl Display for ServerErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerErrorKind::NotMyVbucket => write!(f, "not my vbucket"),
            ServerErrorKind::KeyExists => write!(f, "key exists"),
            ServerErrorKind::NotStored => write!(f, "key not stored"),
            ServerErrorKind::KeyNotFound => write!(f, "key not found"),
            ServerErrorKind::TmpFail => write!(f, "temporary failure"),
            ServerErrorKind::CasMismatch => write!(f, "cas mismatch"),
            ServerErrorKind::Locked => write!(f, "locked"),
            ServerErrorKind::NotLocked => write!(f, "not locked"),
            ServerErrorKind::TooBig => write!(f, "too big"),
            ServerErrorKind::UnknownCollectionID => write!(f, "unknown collection id"),
            ServerErrorKind::NoBucket => write!(f, "no bucket selected"),
            ServerErrorKind::UnknownBucketName => write!(f, "unknown bucket name"),
            ServerErrorKind::Access => write!(f, "access error"),
            ServerErrorKind::Auth { msg } => write!(f, "auth error {msg}"),
            ServerErrorKind::ConfigNotSet => write!(f, "config not set"),
            ServerErrorKind::UnknownScopeName => write!(f, "scope name unknown"),
            ServerErrorKind::UnknownCollectionName => write!(f, "collection name unknown"),
            ServerErrorKind::Subdoc { error } => write!(f, "{error}"),
            ServerErrorKind::UnknownStatus { status } => {
                write!(f, "server status unexpected for operation: {status}")
            }
            ServerErrorKind::BadDelta => write!(f, "bad delta"),
            ServerErrorKind::UnknownCommand => write!(f, "unknown command"),
            ServerErrorKind::RangeError => write!(f, "range error"),
            ServerErrorKind::RateLimitedNetworkIngress => {
                write!(f, "rate limited: network ingress")
            }
            ServerErrorKind::RateLimitedNetworkEgress => write!(f, "rate limited: network egress"),
            ServerErrorKind::RateLimitedMaxConnections => {
                write!(f, "rate limited: max connections")
            }
            ServerErrorKind::RateLimitedMaxCommands => write!(f, "rate limited: max commands"),
            ServerErrorKind::RateLimitedScopeSizeLimitExceeded => {
                write!(f, "rate limited: scope size limit exceeded")
            }
            ServerErrorKind::NotSupported => write!(f, "not supported"),
            ServerErrorKind::InternalError => write!(f, "internal error"),
            ServerErrorKind::Busy => write!(f, "busy"),
            ServerErrorKind::DurabilityInvalid => write!(f, "durability invalid"),
            ServerErrorKind::DurabilityImpossible => write!(f, "durability impossible"),
            ServerErrorKind::SyncWriteInProgress => write!(f, "sync write in progress"),
            ServerErrorKind::SyncWriteAmbiguous => write!(f, "sync write ambiguous"),
            ServerErrorKind::SyncWriteRecommitInProgress => {
                write!(f, "sync write recommit in progress")
            }
            ServerErrorKind::RangeScanCancelled => write!(f, "range scan cancelled"),
            ServerErrorKind::RangeScanVBUUIDNotEqual => write!(f, "range scan vbUUID not equal"),
            ServerErrorKind::InvalidArgs => write!(f, "invalid args"),
            ServerErrorKind::AuthStale => write!(f, "auth stale"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct SubdocError {
    kind: SubdocErrorKind,
    op_index: Option<u8>,
}

impl StdError for SubdocError {}

impl SubdocError {
    pub(crate) fn new(kind: SubdocErrorKind, op_index: impl Into<Option<u8>>) -> Self {
        Self {
            kind,
            op_index: op_index.into(),
        }
    }

    pub fn is_error_kind(&self, kind: SubdocErrorKind) -> bool {
        self.kind == kind
    }

    pub fn kind(&self) -> &SubdocErrorKind {
        &self.kind
    }

    pub fn op_index(&self) -> Option<u8> {
        self.op_index
    }
}

impl Display for SubdocError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(op_index) = self.op_index {
            let base_msg = format!("Subdoc error: {}, op_index: {}", self.kind, op_index);
            write!(f, "{base_msg}")
        } else {
            let base_msg = format!("Subdoc error: {}", self.kind);
            write!(f, "{base_msg}")
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum SubdocErrorKind {
    PathNotFound,
    PathMismatch,
    PathInvalid,
    PathTooBig,
    DocTooDeep,
    CantInsert,
    NotJSON,
    BadRange,
    BadDelta,
    PathExists,
    ValueTooDeep,
    InvalidCombo,
    XattrInvalidFlagCombo,
    XattrInvalidKeyCombo,
    XattrUnknownMacro,
    XattrUnknownVAttr,
    XattrCannotModifyVAttr,
    InvalidXattrOrder,
    XattrUnknownVattrMacro,
    CanOnlyReviveDeletedDocuments,
    DeletedDocumentCantHaveValue,
    UnknownStatus { status: Status },
}

impl Display for SubdocErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SubdocErrorKind::PathNotFound => write!(f, "subdoc path not found"),
            SubdocErrorKind::PathMismatch => write!(f, "subdoc path mismatch"),
            SubdocErrorKind::PathInvalid => write!(f, "subdoc path invalid"),
            SubdocErrorKind::PathTooBig => write!(f, "subdoc path too big"),
            SubdocErrorKind::DocTooDeep => write!(f, "subdoc doc too deep"),
            SubdocErrorKind::CantInsert => write!(f, "subdoc can't insert"),
            SubdocErrorKind::NotJSON => write!(f, "subdoc not JSON"),
            SubdocErrorKind::BadRange => write!(f, "subdoc bad range"),
            SubdocErrorKind::BadDelta => write!(f, "subdoc bad delta"),
            SubdocErrorKind::PathExists => write!(f, "subdoc path exists"),
            SubdocErrorKind::ValueTooDeep => write!(f, "subdoc value too deep"),
            SubdocErrorKind::InvalidCombo => write!(f, "subdoc invalid combo"),
            SubdocErrorKind::XattrInvalidFlagCombo => write!(f, "subdoc xattr invalid flag combo"),
            SubdocErrorKind::XattrInvalidKeyCombo => write!(f, "subdoc xattr invalid key combo"),
            SubdocErrorKind::XattrUnknownMacro => write!(f, "subdoc xattr unknown macro"),
            SubdocErrorKind::XattrUnknownVAttr => write!(f, "subdoc xattr unknown vattr"),
            SubdocErrorKind::XattrCannotModifyVAttr => {
                write!(f, "subdoc xattr cannot modify vattr")
            }
            SubdocErrorKind::InvalidXattrOrder => write!(f, "subdoc invalid xattr order"),
            SubdocErrorKind::XattrUnknownVattrMacro => {
                write!(f, "subdoc xattr unknown vattr macro")
            }
            SubdocErrorKind::CanOnlyReviveDeletedDocuments => {
                write!(f, "subdoc can only revive deleted documents")
            }
            SubdocErrorKind::DeletedDocumentCantHaveValue => {
                write!(f, "subdoc deleted document can't have value")
            }
            SubdocErrorKind::UnknownStatus { status } => write!(
                f,
                "subdoc unknown status unexpected for operation: {status}"
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum CancellationErrorKind {
    Timeout,
    RequestCancelled,
    ClosedInFlight,
}

impl Display for CancellationErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let txt = match self {
            CancellationErrorKind::Timeout => "Timeout",
            CancellationErrorKind::RequestCancelled => "Request cancelled",
            CancellationErrorKind::ClosedInFlight => "Closed in flight",
        };

        write!(f, "{txt}")
    }
}

impl<E> From<E> for Error
where
    ErrorKind: From<E>,
{
    fn from(err: E) -> Self {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::from(err)),
                source: None,
            },
        }
    }
}

impl From<ServerError> for Error {
    fn from(value: ServerError) -> Self {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::Server(value)),
                source: None,
            },
        }
    }
}

impl From<ResourceError> for Error {
    fn from(value: ResourceError) -> Self {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::Resource(value)),
                source: None,
            },
        }
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::Io),
                source: Some(Box::new(value)),
            },
        }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq, Eq, Default)]
struct ServerErrorContextJsonContext {
    #[serde(alias = "context")]
    context: Option<String>,
    #[serde(alias = "ref")]
    pub error_ref: Option<String>,
}

#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
struct ServerErrorContextJson {
    #[serde(alias = "error", default)]
    error: ServerErrorContextJsonContext,
    #[serde(alias = "manifest_uid")]
    pub manifest_rev: Option<String>,
}

impl MetricsName for Error {
    fn metrics_name(&self) -> &'static str {
        self.kind().metrics_name()
    }
}

impl MetricsName for ErrorKind {
    fn metrics_name(&self) -> &'static str {
        match self {
            ErrorKind::Server(err) => err.kind().metrics_name(),
            ErrorKind::Resource(err) => err.cause().kind().metrics_name(),
            ErrorKind::Dispatch { .. } => "memdx.Dispatch",
            ErrorKind::Close { .. } => "memdx.Close",
            ErrorKind::Protocol { .. } => "memdx.Protocol",
            ErrorKind::Cancelled(_) => "memdx.Cancelled",
            ErrorKind::ConnectionFailed { .. } => "memdx.ConnectionFailed",
            ErrorKind::Io => "memdx.Io",
            ErrorKind::InvalidArgument { .. } => "memdx.InvalidArgument",
            ErrorKind::Decompression => "memdx.Decompression",
            ErrorKind::Message(_) => "memdx._OTHER",
        }
    }
}

impl MetricsName for ServerErrorKind {
    fn metrics_name(&self) -> &'static str {
        match self {
            ServerErrorKind::KeyNotFound => "memdx.KeyNotFound",
            ServerErrorKind::KeyExists => "memdx.KeyExists",
            ServerErrorKind::TooBig => "memdx.TooBig",
            ServerErrorKind::NotStored => "memdx.NotStored",
            ServerErrorKind::BadDelta => "memdx.BadDelta",
            ServerErrorKind::NotMyVbucket => "memdx.NotMyVbucket",
            ServerErrorKind::NoBucket => "memdx.NoBucket",
            ServerErrorKind::Locked => "memdx.Locked",
            ServerErrorKind::NotLocked => "memdx.NotLocked",
            ServerErrorKind::Auth { .. } => "memdx.Auth",
            ServerErrorKind::RangeError => "memdx.RangeError",
            ServerErrorKind::Access => "memdx.Access",
            ServerErrorKind::RateLimitedNetworkIngress => "memdx.RateLimitedNetworkIngress",
            ServerErrorKind::RateLimitedNetworkEgress => "memdx.RateLimitedNetworkEgress",
            ServerErrorKind::RateLimitedMaxConnections => "memdx.RateLimitedMaxConnections",
            ServerErrorKind::RateLimitedMaxCommands => "memdx.RateLimitedMaxCommands",
            ServerErrorKind::RateLimitedScopeSizeLimitExceeded => {
                "memdx.RateLimitedScopeSizeLimitExceeded"
            }
            ServerErrorKind::UnknownCommand => "memdx.UnknownCommand",
            ServerErrorKind::NotSupported => "memdx.NotSupported",
            ServerErrorKind::InternalError => "memdx.InternalError",
            ServerErrorKind::Busy => "memdx.Busy",
            ServerErrorKind::TmpFail => "memdx.TmpFail",
            ServerErrorKind::UnknownCollectionID => "memdx.UnknownCollectionID",
            ServerErrorKind::UnknownScopeName => "memdx.UnknownScopeName",
            ServerErrorKind::UnknownCollectionName => "memdx.UnknownCollectionName",
            ServerErrorKind::DurabilityInvalid => "memdx.DurabilityInvalid",
            ServerErrorKind::DurabilityImpossible => "memdx.DurabilityImpossible",
            ServerErrorKind::SyncWriteInProgress => "memdx.SyncWriteInProgress",
            ServerErrorKind::SyncWriteAmbiguous => "memdx.SyncWriteAmbiguous",
            ServerErrorKind::SyncWriteRecommitInProgress => "memdx.SyncWriteRecommitInProgress",
            ServerErrorKind::RangeScanCancelled => "memdx.RangeScanCancelled",
            ServerErrorKind::RangeScanVBUUIDNotEqual => "memdx.RangeScanVBUUIDNotEqual",
            ServerErrorKind::InvalidArgs => "memdx.InvalidArgs",
            ServerErrorKind::AuthStale => "memdx.AuthStale",
            ServerErrorKind::ConfigNotSet => "memdx.ConfigNotSet",
            ServerErrorKind::UnknownBucketName => "memdx.UnknownBucketName",
            ServerErrorKind::CasMismatch => "memdx.CasMismatch",
            ServerErrorKind::Subdoc { error } => error.kind().metrics_name(),
            ServerErrorKind::UnknownStatus { .. } => "memdx._OTHER",
        }
    }
}

impl MetricsName for SubdocErrorKind {
    fn metrics_name(&self) -> &'static str {
        match self {
            SubdocErrorKind::PathNotFound => "memdx.subdoc.PathNotFound",
            SubdocErrorKind::PathMismatch => "memdx.subdoc.PathMismatch",
            SubdocErrorKind::PathInvalid => "memdx.subdoc.PathInvalid",
            SubdocErrorKind::PathTooBig => "memdx.subdoc.PathTooBig",
            SubdocErrorKind::DocTooDeep => "memdx.subdoc.DocTooDeep",
            SubdocErrorKind::CantInsert => "memdx.subdoc.CantInsert",
            SubdocErrorKind::NotJSON => "memdx.subdoc.NotJSON",
            SubdocErrorKind::BadRange => "memdx.subdoc.BadRange",
            SubdocErrorKind::BadDelta => "memdx.subdoc.BadDelta",
            SubdocErrorKind::PathExists => "memdx.subdoc.PathExists",
            SubdocErrorKind::ValueTooDeep => "memdx.subdoc.ValueTooDeep",
            SubdocErrorKind::InvalidCombo => "memdx.subdoc.InvalidCombo",
            SubdocErrorKind::XattrInvalidFlagCombo => "memdx.subdoc.XattrInvalidFlagCombo",
            SubdocErrorKind::XattrInvalidKeyCombo => "memdx.subdoc.XattrInvalidKeyCombo",
            SubdocErrorKind::XattrUnknownMacro => "memdx.subdoc.XattrUnknownMacro",
            SubdocErrorKind::XattrUnknownVAttr => "memdx.subdoc.XattrUnknownVAttr",
            SubdocErrorKind::XattrCannotModifyVAttr => "memdx.subdoc.XattrCannotModifyVAttr",
            SubdocErrorKind::InvalidXattrOrder => "memdx.subdoc.InvalidXattrOrder",
            SubdocErrorKind::XattrUnknownVattrMacro => "memdx.subdoc.XattrUnknownVattrMacro",
            SubdocErrorKind::CanOnlyReviveDeletedDocuments => {
                "memdx.subdoc.CanOnlyReviveDeletedDocuments"
            }
            SubdocErrorKind::DeletedDocumentCantHaveValue => {
                "memdx.subdoc.DeletedDocumentCantHaveValue"
            }
            SubdocErrorKind::UnknownStatus { .. } => "memdx.subdoc._OTHER",
        }
    }
}
