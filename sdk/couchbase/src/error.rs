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

//! Error types for all Couchbase SDK operations.
//!
//! Every fallible operation returns [`Result<T>`], which wraps [`Error`].
//! Use [`Error::kind()`] to inspect the [`ErrorKind`] and determine the failure category.

use crate::error_context::{
    ErrorContext, ExtendedErrorContext, HttpErrorContext, KeyValueErrorContext, QueryErrorContext,
    QueryErrorDesc, SearchErrorContext,
};
use crate::service_type::ServiceType;
use couchbase_core::memdx::error::{ServerError, ServerErrorKind, SubdocError, SubdocErrorKind};
use couchbase_core::tracingcomponent::MetricsName;
use serde::ser::SerializeStruct;
use std::error::Error as StdError;
use std::fmt::{Display, Formatter};
use std::ops::Deref;

/// A type alias for `std::result::Result<T, Error>` used throughout the SDK.
pub type Result<T> = std::result::Result<T, Error>;

/// The error type for all Couchbase SDK operations.
///
/// An `Error` contains an [`ErrorKind`] that describes what went wrong, and an optional
/// error context with additional diagnostic information
/// (e.g. document ID, opcode, server status code, dispatched-to address).
///
/// # Inspecting Errors
///
/// Use [`kind()`](Error::kind) to determine the category of error:
///
/// ```rust,no_run
/// use couchbase::error::{Error, ErrorKind};
///
/// fn handle_error(err: &Error) {
///     match err.kind() {
///         ErrorKind::DocumentNotFound => println!("Not found!"),
///         ErrorKind::DocumentExists => println!("Already exists!"),
///         ErrorKind::CasMismatch => println!("Concurrent modification!"),
///         ErrorKind::ServerTimeout => println!("Timed out!"),
///         _ => println!("Other error: {err}"),
///     }
/// }
/// ```
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Error {
    kind: Box<ErrorKind>,
    context: Box<Option<ErrorContext>>,
}

impl Error {
    /// Allows explicit construction of the Error type e.g. for mocking purposes.
    /// **Uncommitted: This feature may change in the future**.
    pub fn new(kind: ErrorKind) -> Self {
        Self::new_internal(kind)
    }

    /// Returns the [`ErrorKind`] describing the category of this error.
    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }

    pub(crate) fn other_failure(msg: impl Into<String>) -> Self {
        Self::new(ErrorKind::OtherFailure(msg.into()))
    }

    // We don't use a From impl as it'd be a blanket coverage and we want to
    // distinguish encoding from decoding.
    pub(crate) fn encoding_failure_from_serde(e: serde_json::Error) -> Self {
        Self::new(ErrorKind::EncodingFailure(format!("encoding failed: {e}")))
    }

    // We don't use a From impl as it'd be a blanket coverage and we want to
    // distinguish encoding from decoding.
    pub(crate) fn decoding_failure_from_serde(e: serde_json::Error) -> Self {
        Self::new(ErrorKind::DecodingFailure(format!("decoding failed: {e}")))
    }

    pub(crate) fn invalid_argument(arg: impl Into<String>, msg: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidArgument(InvalidArgumentErrorKind {
            msg: msg.into(),
            arg: Some(arg.into()),
        }))
    }

    pub(crate) fn with_context(mut self, context: ErrorContext) -> Self {
        self.context = Box::new(Some(context));
        self
    }

    fn new_internal(kind: ErrorKind) -> Self {
        Self {
            kind: Box::new(kind),
            context: Box::new(None),
        }
    }

    fn parse_kv_server_error(
        e: &ServerError,
        doc_id: &[u8],
        bucket_name: String,
        scope_name: String,
        collection_name: String,
        is_replica: bool,
    ) -> (ErrorKind, KeyValueErrorContext) {
        let doc_id = String::from_utf8_lossy(doc_id).to_string();

        let mut extended_context = KeyValueErrorContext::new(
            doc_id,
            e.opaque(),
            e.op_code().into(),
            e.status().into(),
            bucket_name,
            scope_name,
            collection_name,
        );

        if let Some(source) = e.source() {
            extended_context = extended_context.with_source_message(source.to_string());
        }

        if let Some(xerror) = e.context() {
            if let Some(parsed) = ServerError::parse_context(xerror) {
                if let Some(text) = parsed.text {
                    extended_context = extended_context.with_xcontent(text);
                }

                if let Some(error_ref) = parsed.error_ref {
                    extended_context = extended_context.with_xref(error_ref);
                }
            }
        }

        let kind = if is_replica && *e.kind() == ServerErrorKind::KeyNotFound {
            ErrorKind::DocumentNotFoundOnReplica
        } else {
            e.kind().into()
        };

        (kind, extended_context)
    }

    fn parse_query_server_error(
        e: &couchbase_core::queryx::error::ServerError,
    ) -> (ErrorKind, QueryErrorContext) {
        let kind = ErrorKind::from(e.kind());

        let extended_context = QueryErrorContext {
            // This is never going to be missing from the public API.
            statement: e.statement().unwrap_or_default().to_string(),
            code: Some(e.code()),
            message: Some(e.msg().to_string()),
            client_context_id: e.client_context_id().unwrap_or_default().to_string(),
            http_status_code: Some(e.status_code()),
            descs: e
                .all_error_descs()
                .iter()
                .map(|ed| QueryErrorDesc {
                    kind: ErrorKind::from(ed.kind()),
                    code: ed.code(),
                    message: ed.message().to_string(),
                    retry: ed.retry(),
                    reason: ed.reason().clone(),
                })
                .collect(),
        };

        (kind, extended_context)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let msg = self.kind.to_string();

        if let Some(context) = self.context.deref() {
            write!(f, "{msg}: {context}")
        } else {
            write!(f, "{msg}")
        }
    }
}

impl StdError for Error {}

/// Categorizes the type of error that occurred.
///
/// Match on `ErrorKind` variants to handle specific error conditions. The variants
/// cover shared errors (timeouts, authentication, argument validation), key-value errors
/// (document not found, CAS mismatch, durability), query errors, and management errors.
///
/// # Common Variants
///
/// | Variant | When it occurs |
/// |---------|---------------|
/// | [`DocumentNotFound`](ErrorKind::DocumentNotFound) | `get`, `replace`, `remove` on a missing document |
/// | [`DocumentExists`](ErrorKind::DocumentExists) | `insert` when the document already exists |
/// | [`CasMismatch`](ErrorKind::CasMismatch) | CAS-guarded operation with a stale CAS value |
/// | [`ServerTimeout`](ErrorKind::ServerTimeout) | Operation timed out on the server |
/// | [`InvalidArgument`](ErrorKind::InvalidArgument) | Bad argument passed to an operation |
/// | [`DocumentLocked`](ErrorKind::DocumentLocked) | Attempting to mutate a locked document |
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// An unclassified error with a descriptive message.
    OtherFailure(String),
    /// The operation timed out on the server side.
    ServerTimeout,
    /// The [`Cluster`](crate::cluster::Cluster) has been dropped and is no longer usable.
    ClusterDropped,

    // Shared Error Definitions RFC#58@16
    /// An invalid argument was passed to an operation.
    InvalidArgument(InvalidArgumentErrorKind),
    /// The requested service is not available on the cluster.
    ServiceNotAvailable(ServiceType),
    /// A feature required by this operation is not available.
    FeatureNotAvailable(FeatureNotAvailableErrorKind),
    /// An internal server error occurred.
    InternalServerFailure,
    /// Authentication failed (invalid credentials).
    AuthenticationFailure,
    /// A temporary/transient failure occurred; the operation may be retried.
    TemporaryFailure,
    /// A failure occurred on the server while parsing a request.
    ParsingFailure,
    /// The CAS value provided does not match the current document CAS.
    CasMismatch,
    /// The requested bucket was not found on the cluster.
    BucketNotFound,
    /// The requested collection was not found.
    CollectionNotFound,
    /// The requested scope was not found.
    ScopeNotFound,
    /// Failed to encode a value (e.g. JSON serialization failure).
    EncodingFailure(String),
    /// Failed to decode a value (e.g. JSON deserialization failure).
    DecodingFailure(String),
    /// The operation is not supported by the server.
    UnsupportedOperation,
    /// The requested index was not found.
    IndexNotFound,
    /// An index with the given name already exists.
    IndexExists,
    /// The operation was rejected because the rate limit was exceeded.
    RateLimitedFailure,
    /// The operation was rejected because the quota limit was exceeded.
    QuotaLimitedFailure,
    /// The request was canceled before completion.
    RequestCanceled,

    // Key Value Error Definitions RFC#58@16
    /// The document was not found in the collection.
    DocumentNotFound,
    /// A [`Collection::get_replica`](crate::collection::Collection::get_replica) found
    /// no document for this key on the replica it read from.
    DocumentNotFoundOnReplica,
    /// The document exists but could not be retrieved (e.g. all replicas failed).
    DocumentUnretrievable,
    /// A [`GetReplicaStrategy`](crate::get_replica_strategy::GetReplicaStrategy) requested
    /// a replica index that the bucket does not have (and `wrap` was not set).
    ReplicaIndexOutOfBounds,
    /// The requested replica exists in the bucket's topology, but no node is currently
    /// assigned to it (e.g. mid-rebalance or during a failover).
    ReplicaIndexCurrentlyUnavailable,
    /// The document is locked by another operation.
    DocumentLocked,
    /// The value is too large for the server to store.
    ValueTooLarge,
    /// A document with the same key already exists (insert conflict).
    DocumentExists,
    /// The requested durability level is not available on this cluster.
    DurabilityLevelNotAvailable,
    /// The requested durability requirements are impossible to satisfy.
    DurabilityImpossible,
    /// The durability outcome is ambiguous (the write may or may not have been durable).
    DurabilityAmbiguous,
    /// A durable write is already in progress for this document.
    DurabilityWriteInProgress,
    /// A previous durable write is being recommitted.
    DurableWriteRecommitInProgress,
    /// A sub-document path was not found in the document.
    PathNotFound,
    /// A sub-document path type mismatch (e.g. accessing an array element on an object).
    PathMismatch,
    /// A sub-document path is syntactically invalid.
    PathInvalid,
    /// A sub-document path is too long.
    PathTooBig,
    /// A sub-document path has too many levels of nesting.
    PathTooDeep,
    /// The value being inserted has too many levels of nesting.
    ValueTooDeep,
    /// The value for a sub-document operation is invalid.
    ValueInvalid,
    /// The existing document is not valid JSON.
    DocumentNotJSON,
    /// A numeric value in the document is too large.
    NumberTooBig,
    /// The delta value for an increment/decrement is invalid.
    DeltaInvalid,
    /// The sub-document path already exists (insert conflict).
    PathExists,
    /// An unknown macro was referenced in a sub-document operation.
    XattrUnknownMacro,
    /// An invalid combination of xattr keys was used.
    XattrInvalidKeyCombo,
    /// An unknown virtual attribute was referenced.
    XattrUnknownVirtualAttribute,
    /// Cannot modify a virtual attribute.
    XattrCannotModifyVirtualAttribute,
    /// Access denied to extended attributes.
    XattrNoAccess,
    /// Extended attributes must be accessed before regular attributes.
    XattrInvalidOrder,
    /// An invalid combination of xattr flags was used.
    XattrInvalidFlagCombo,
    /// The mutation token is outdated and cannot be used for consistency.
    MutationTokenOutdated,
    /// The document is not locked (unlock was called without a prior lock).
    DocumentNotLocked,

    // Query Error Definitions RFC#58@16
    /// A query planning failure occurred.
    PlanningFailure,
    /// A query index failure occurred.
    IndexFailure,
    /// A prepared statement failure occurred.
    PreparedStatementFailure,
    /// A DML (Data Manipulation Language) failure occurred.
    DMLFailure,

    // Management Error Definitions RFC#58@16
    /// A collection with the given name already exists.
    CollectionExists,
    /// A scope with the given name already exists.
    ScopeExists,
    /// The requested user was not found.
    UserNotFound,
    /// The requested group was not found.
    GroupNotFound,
    /// A bucket with the given name already exists.
    BucketExists,
    /// The user already exists.
    UserExists,
    /// The group already exists.
    GroupExists,
    /// The bucket does not have flush enabled.
    BucketNotFlushable,
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let msg = match self {
            ErrorKind::OtherFailure(msg) => {
                return write!(f, "{msg}");
            }
            ErrorKind::InvalidArgument(invalid_arg_kind) => {
                let arg = &invalid_arg_kind.arg;
                let msg = &invalid_arg_kind.msg;

                return if let Some(arg) = arg {
                    write!(f, "invalid argument for {arg}: {msg}")
                } else {
                    write!(f, "invalid argument: {msg}")
                };
            }
            ErrorKind::ClusterDropped => {
                "the client is disconnected (the cluster resource has been dropped)"
            }
            ErrorKind::ServiceNotAvailable(service) => {
                return write!(f, "service not available: {service}");
            }
            ErrorKind::FeatureNotAvailable(feature_not_available_kind) => {
                let msg = &feature_not_available_kind.msg;
                let feature = &feature_not_available_kind.feature;

                return if let Some(msg) = msg {
                    write!(f, "feature not available: {feature} ({msg})")
                } else {
                    write!(f, "feature not available: {feature}")
                };
            }
            ErrorKind::EncodingFailure(msg) => return write!(f, "encoding failure: {msg}"),
            ErrorKind::DecodingFailure(msg) => return write!(f, "decoding failure: {msg}"),
            ErrorKind::InternalServerFailure => "internal server failure",
            ErrorKind::AuthenticationFailure => "authentication failure",
            ErrorKind::TemporaryFailure => "temporary failure",
            ErrorKind::ParsingFailure => "parsing failure",
            ErrorKind::CasMismatch => "cas mismatch",
            ErrorKind::BucketNotFound => "bucket not found",
            ErrorKind::CollectionNotFound => "collection not found",
            ErrorKind::ScopeNotFound => "scope not found",
            ErrorKind::UnsupportedOperation => "unsupported operation",
            ErrorKind::IndexNotFound => "index not found",
            ErrorKind::IndexExists => "index exists",
            ErrorKind::RateLimitedFailure => "rate limited failure",
            ErrorKind::QuotaLimitedFailure => "quota limited failure",
            ErrorKind::RequestCanceled => "request canceled",
            ErrorKind::DocumentNotFound => "document not found",
            ErrorKind::DocumentNotFoundOnReplica => "document not found on replica",
            ErrorKind::DocumentUnretrievable => "document unretrievable",
            ErrorKind::ReplicaIndexOutOfBounds => "replica index out of bounds",
            ErrorKind::ReplicaIndexCurrentlyUnavailable => "replica index currently unavailable",
            ErrorKind::DocumentLocked => "document locked",
            ErrorKind::ValueTooLarge => "value too large",
            ErrorKind::DocumentExists => "document exists",
            ErrorKind::DurabilityLevelNotAvailable => "durability level not available",
            ErrorKind::DurabilityImpossible => "durability impossible",
            ErrorKind::DurabilityAmbiguous => "durability ambiguous",
            ErrorKind::DurabilityWriteInProgress => "durability write in progress",
            ErrorKind::DurableWriteRecommitInProgress => "durable write recommit in progress",
            ErrorKind::PathNotFound => "path not found",
            ErrorKind::PathMismatch => "path mismatch",
            ErrorKind::PathInvalid => "path invalid",
            ErrorKind::PathTooBig => "path too big",
            ErrorKind::PathTooDeep => "path too deep",
            ErrorKind::ValueTooDeep => "value too deep",
            ErrorKind::ValueInvalid => "value invalid",
            ErrorKind::DocumentNotJSON => "document not json",
            ErrorKind::NumberTooBig => "number too big",
            ErrorKind::DeltaInvalid => "delta invalid",
            ErrorKind::PathExists => "path exists",
            ErrorKind::XattrUnknownMacro => "xattr unknown macro",
            ErrorKind::XattrInvalidKeyCombo => "xattr invalid key combo",
            ErrorKind::XattrUnknownVirtualAttribute => "xattr unknown virtual attribute",
            ErrorKind::XattrCannotModifyVirtualAttribute => "xattr cannot modify virtual attribute",
            ErrorKind::XattrNoAccess => "xattr no access",
            ErrorKind::XattrInvalidOrder => "xattr invalid order",
            ErrorKind::XattrInvalidFlagCombo => "xattr invalid flag combo",
            ErrorKind::MutationTokenOutdated => "mutation token outdated",
            ErrorKind::DocumentNotLocked => "document not locked",
            ErrorKind::PlanningFailure => "planning failure",
            ErrorKind::IndexFailure => "index failure",
            ErrorKind::PreparedStatementFailure => "prepared statement failure",
            ErrorKind::DMLFailure => "dml failure",
            ErrorKind::CollectionExists => "collection exists",
            ErrorKind::ScopeExists => "scope exists",
            ErrorKind::UserNotFound => "user not found",
            ErrorKind::GroupNotFound => "group not found",
            ErrorKind::BucketExists => "bucket exists",
            ErrorKind::UserExists => "user exists",
            ErrorKind::GroupExists => "group exists",
            ErrorKind::BucketNotFlushable => "bucket not flushable",
            ErrorKind::ServerTimeout => "server timeout",
        };

        write!(f, "{msg}")
    }
}

impl MetricsName for ErrorKind {
    fn metrics_name(&self) -> &'static str {
        match self {
            ErrorKind::ServerTimeout => "ServerTimeout",
            ErrorKind::ClusterDropped => "ClusterDropped",
            ErrorKind::InvalidArgument(_) => "InvalidArgument",
            ErrorKind::ServiceNotAvailable(_) => "ServiceNotAvailable",
            ErrorKind::FeatureNotAvailable(_) => "FeatureNotAvailable",
            ErrorKind::InternalServerFailure => "InternalServerFailure",
            ErrorKind::AuthenticationFailure => "AuthenticationFailure",
            ErrorKind::TemporaryFailure => "TemporaryFailure",
            ErrorKind::ParsingFailure => "ParsingFailure",
            ErrorKind::CasMismatch => "CasMismatch",
            ErrorKind::BucketNotFound => "BucketNotFound",
            ErrorKind::CollectionNotFound => "CollectionNotFound",
            ErrorKind::ScopeNotFound => "ScopeNotFound",
            ErrorKind::EncodingFailure(_) => "EncodingFailure",
            ErrorKind::DecodingFailure(_) => "DecodingFailure",
            ErrorKind::UnsupportedOperation => "UnsupportedOperation",
            ErrorKind::IndexNotFound => "IndexNotFound",
            ErrorKind::IndexExists => "IndexExists",
            ErrorKind::RateLimitedFailure => "RateLimited",
            ErrorKind::QuotaLimitedFailure => "QuotaLimited",
            ErrorKind::RequestCanceled => "RequestCanceled",
            ErrorKind::DocumentNotFound => "DocumentNotFound",
            ErrorKind::DocumentNotFoundOnReplica => "DocumentNotFoundOnReplica",
            ErrorKind::DocumentUnretrievable => "DocumentUnretrievable",
            ErrorKind::ReplicaIndexOutOfBounds => "ReplicaIndexOutOfBounds",
            ErrorKind::ReplicaIndexCurrentlyUnavailable => "ReplicaIndexCurrentlyUnavailable",
            ErrorKind::DocumentLocked => "DocumentLocked",
            ErrorKind::ValueTooLarge => "ValueTooLarge",
            ErrorKind::DocumentExists => "DocumentExists",
            ErrorKind::DurabilityLevelNotAvailable => "DurabilityLevelNotAvailable",
            ErrorKind::DurabilityImpossible => "DurabilityImpossible",
            ErrorKind::DurabilityAmbiguous => "DurabilityAmbiguous",
            ErrorKind::DurabilityWriteInProgress => "DurableWriteInProgress",
            ErrorKind::DurableWriteRecommitInProgress => "DurableWriteRecommitInProgress",
            ErrorKind::PathNotFound => "PathNotFound",
            ErrorKind::PathMismatch => "PathMismatch",
            ErrorKind::PathInvalid => "PathInvalid",
            ErrorKind::PathTooBig => "PathTooBig",
            ErrorKind::PathTooDeep => "PathTooDeep",
            ErrorKind::ValueTooDeep => "ValueTooDeep",
            ErrorKind::ValueInvalid => "ValueInvalid",
            ErrorKind::DocumentNotJSON => "DocumentNotJson",
            ErrorKind::NumberTooBig => "NumberTooBig",
            ErrorKind::DeltaInvalid => "DeltaInvalid",
            ErrorKind::PathExists => "PathExists",
            ErrorKind::XattrUnknownMacro => "XattrUnknownMacro",
            ErrorKind::XattrInvalidKeyCombo => "XattrInvalidKeyCombo",
            ErrorKind::XattrUnknownVirtualAttribute => "XattrUnknownVirtualAttribute",
            ErrorKind::XattrCannotModifyVirtualAttribute => "XattrCannotModifyVirtualAttribute",
            ErrorKind::XattrNoAccess => "XattrNoAccess",
            ErrorKind::XattrInvalidOrder => "XattrInvalidOrder",
            ErrorKind::XattrInvalidFlagCombo => "XattrInvalidFlagCombo",
            ErrorKind::MutationTokenOutdated => "MutationTokenOutdated",
            ErrorKind::DocumentNotLocked => "DocumentNotLocked",
            ErrorKind::PlanningFailure => "PlanningFailure",
            ErrorKind::IndexFailure => "IndexFailure",
            ErrorKind::PreparedStatementFailure => "PreparedStatementFailure",
            ErrorKind::DMLFailure => "DmlFailure",
            ErrorKind::CollectionExists => "CollectionExists",
            ErrorKind::ScopeExists => "ScopeExists",
            ErrorKind::UserNotFound => "UserNotFound",
            ErrorKind::GroupNotFound => "GroupNotFound",
            ErrorKind::BucketExists => "BucketExists",
            ErrorKind::UserExists => "UserExists",
            ErrorKind::GroupExists => "GroupExists",
            ErrorKind::BucketNotFlushable => "BucketNotFlushable",
            ErrorKind::OtherFailure(_) => "_OTHER",
            _ => "_OTHER",
        }
    }
}

impl MetricsName for Error {
    fn metrics_name(&self) -> &'static str {
        self.kind().metrics_name()
    }
}

/// Details about an [`ErrorKind::InvalidArgument`] error.
///
/// Contains the name of the offending argument (when known) and a message
/// describing why the value was rejected.
///
/// Use [`arg()`](InvalidArgumentErrorKind::arg) to get the argument name and
/// [`msg()`](InvalidArgumentErrorKind::msg) for the human-readable description.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct InvalidArgumentErrorKind {
    pub(crate) msg: String,
    pub(crate) arg: Option<String>,
}

impl InvalidArgumentErrorKind {
    /// Returns the name of the argument that was invalid, when available.
    pub fn arg(&self) -> Option<&str> {
        self.arg.as_deref()
    }

    /// Returns the human-readable description of why the argument was invalid.
    pub fn msg(&self) -> &str {
        &self.msg
    }

    /// Creates a new `InvalidArgumentErrorKind`. **Uncommitted: This feature may change in the future**.
    pub fn new(arg: impl Into<Option<String>>, msg: impl Into<String>) -> Self {
        Self {
            msg: msg.into(),
            arg: arg.into(),
        }
    }
}

/// Details about an [`ErrorKind::FeatureNotAvailable`] error.
///
/// Identifies which feature is missing and optionally provides an explanatory
/// message from the server or SDK.
///
/// Use [`feature()`](FeatureNotAvailableErrorKind::feature) to get the feature name
/// and [`msg()`](FeatureNotAvailableErrorKind::msg) for the optional description.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct FeatureNotAvailableErrorKind {
    pub(crate) feature: String,
    pub(crate) msg: Option<String>,
}

impl FeatureNotAvailableErrorKind {
    /// Returns the name of the feature that is not available.
    pub fn feature(&self) -> &str {
        &self.feature
    }

    /// Returns the optional explanatory message from the server or SDK.
    pub fn msg(&self) -> Option<&str> {
        self.msg.as_deref()
    }

    /// Creates a new `FeatureNotAvailableErrorKind`. **Uncommitted: This feature may change in the future**.
    pub fn new(feature: impl Into<String>, msg: impl Into<Option<String>>) -> Self {
        Self {
            feature: feature.into(),
            msg: msg.into(),
        }
    }
}

impl From<couchbase_core::error::Error> for Error {
    fn from(value: couchbase_core::error::Error) -> Self {
        match value.kind() {
            couchbase_core::error::ErrorKind::Memdx(e) => e.into(),
            couchbase_core::error::ErrorKind::Query(e) => e.into(),
            couchbase_core::error::ErrorKind::Search(e) => e.into(),
            couchbase_core::error::ErrorKind::Http(e) => {
                let kind = match e.kind() {
                    couchbase_core::httpx::error::ErrorKind::Connect { msg, .. } => {
                        ErrorKind::OtherFailure(msg.clone())
                    }
                    couchbase_core::httpx::error::ErrorKind::Decoding(msg) => {
                        ErrorKind::DecodingFailure(msg.clone())
                    }
                    couchbase_core::httpx::error::ErrorKind::Message(msg) => {
                        ErrorKind::OtherFailure(msg.clone())
                    }
                    _ => ErrorKind::OtherFailure(value.to_string()),
                };

                Error {
                    kind: Box::new(kind),
                    context: Box::new(None),
                }
            }
            couchbase_core::error::ErrorKind::Mgmt(e) => e.into(),
            couchbase_core::error::ErrorKind::VbucketMapOutdated => Error {
                kind: Box::new(ErrorKind::OtherFailure(value.to_string())),
                context: Box::new(None),
            },
            couchbase_core::error::ErrorKind::InvalidArgument { msg, arg, .. } => Error {
                kind: Box::new(ErrorKind::InvalidArgument(InvalidArgumentErrorKind {
                    msg: msg.clone(),
                    arg: arg.clone(),
                })),
                context: Box::new(None),
            },
            couchbase_core::error::ErrorKind::EndpointNotKnown { .. } => Error {
                kind: Box::new(ErrorKind::OtherFailure(value.to_string())),
                context: Box::new(None),
            },
            couchbase_core::error::ErrorKind::InvalidVbucket { .. } => Error {
                kind: Box::new(ErrorKind::OtherFailure(value.to_string())),
                context: Box::new(None),
            },
            couchbase_core::error::ErrorKind::InvalidReplica { .. } => Error {
                kind: Box::new(ErrorKind::ReplicaIndexOutOfBounds),
                context: Box::new(None),
            },
            couchbase_core::error::ErrorKind::NoEndpointsAvailable => Error {
                kind: Box::new(ErrorKind::OtherFailure(value.to_string())),
                context: Box::new(None),
            },
            couchbase_core::error::ErrorKind::Shutdown => Error {
                kind: Box::new(ErrorKind::OtherFailure(value.to_string())),
                context: Box::new(None),
            },
            couchbase_core::error::ErrorKind::NoBucket => Error {
                kind: Box::new(ErrorKind::OtherFailure(value.to_string())),
                context: Box::new(None),
            },
            couchbase_core::error::ErrorKind::IllegalState { msg, .. } => Error {
                kind: Box::new(ErrorKind::OtherFailure(msg.to_string())),
                context: Box::new(None),
            },
            couchbase_core::error::ErrorKind::NoVbucketMap => Error {
                kind: Box::new(ErrorKind::OtherFailure(value.to_string())),
                context: Box::new(None),
            },
            couchbase_core::error::ErrorKind::NoServerAssigned { vb_server_idx, .. } => Error {
                kind: Box::new(if *vb_server_idx > 0 {
                    ErrorKind::ReplicaIndexCurrentlyUnavailable
                } else {
                    ErrorKind::OtherFailure(value.to_string())
                }),
                context: Box::new(None),
            },
            couchbase_core::error::ErrorKind::CollectionManifestOutdated { .. } => Error {
                kind: Box::new(ErrorKind::OtherFailure(value.to_string())),
                context: Box::new(None),
            },
            couchbase_core::error::ErrorKind::Message { msg, .. } => Error {
                kind: Box::new(ErrorKind::OtherFailure(msg.to_string())),
                context: Box::new(None),
            },
            couchbase_core::error::ErrorKind::ServiceNotAvailable { service, .. } => Error {
                kind: Box::new(ErrorKind::ServiceNotAvailable(ServiceType::from(service))),
                context: Box::new(None),
            },
            couchbase_core::error::ErrorKind::FeatureNotAvailable { feature, msg, .. } => Error {
                kind: Box::new(ErrorKind::FeatureNotAvailable(
                    FeatureNotAvailableErrorKind {
                        feature: feature.clone(),
                        msg: Some(msg.clone()),
                    },
                )),
                context: Box::new(None),
            },
            couchbase_core::error::ErrorKind::Compression { msg, .. } => Error {
                kind: Box::new(ErrorKind::OtherFailure(msg.to_string())),
                context: Box::new(None),
            },
            couchbase_core::error::ErrorKind::Internal { msg, .. } => Error {
                kind: Box::new(ErrorKind::OtherFailure(msg.to_string())),
                context: Box::new(None),
            },
            _ => Error {
                kind: Box::new(ErrorKind::OtherFailure(value.to_string())),
                context: Box::new(None),
            },
        }
    }
}

impl From<&couchbase_core::error::MemdxError> for Error {
    fn from(value: &couchbase_core::error::MemdxError) -> Self {
        let (kind, extended_context) = match value.kind() {
            couchbase_core::memdx::error::ErrorKind::Server(e) => {
                let (kind, extended_context) = Error::parse_kv_server_error(
                    e,
                    value.doc_id().unwrap_or_default(),
                    value.bucket_name().cloned().unwrap_or_default().to_string(),
                    value.scope_name().cloned().unwrap_or_default().to_string(),
                    value
                        .collection_name()
                        .cloned()
                        .unwrap_or_default()
                        .to_string(),
                    value.is_replica(),
                );

                (kind, Some(ExtendedErrorContext::KeyValue(extended_context)))
            }
            couchbase_core::memdx::error::ErrorKind::Resource(e) => {
                let server_error = e.cause();

                let (kind, extended_context) = Error::parse_kv_server_error(
                    server_error,
                    value.doc_id().unwrap_or_default(),
                    value.bucket_name().cloned().unwrap_or_default().to_string(),
                    e.scope_name().to_string(),
                    e.collection_name().to_string(),
                    value.is_replica(),
                );

                (kind, Some(ExtendedErrorContext::KeyValue(extended_context)))
            }
            couchbase_core::memdx::error::ErrorKind::Dispatch { .. } => {
                let kind = ErrorKind::OtherFailure(value.to_string());
                (kind, None)
            }
            couchbase_core::memdx::error::ErrorKind::Close { .. } => {
                let kind = ErrorKind::OtherFailure(value.to_string());
                (kind, None)
            }
            couchbase_core::memdx::error::ErrorKind::Protocol { .. } => {
                let kind = ErrorKind::OtherFailure(value.to_string());
                (kind, None)
            }
            couchbase_core::memdx::error::ErrorKind::Cancelled(_) => {
                let kind = ErrorKind::OtherFailure(value.to_string());
                (kind, None)
            }
            couchbase_core::memdx::error::ErrorKind::ConnectionFailed { .. } => {
                let kind = ErrorKind::OtherFailure(value.to_string());
                (kind, None)
            }
            couchbase_core::memdx::error::ErrorKind::Io => {
                let msg = if let Some(source) = value.source() {
                    source.to_string()
                } else {
                    value.to_string()
                };

                let kind = ErrorKind::OtherFailure(msg);
                (kind, None)
            }
            couchbase_core::memdx::error::ErrorKind::InvalidArgument { arg, msg, .. } => {
                let msg = if let Some(source) = value.source() {
                    source.to_string()
                } else {
                    msg.clone()
                };
                let kind = ErrorKind::InvalidArgument(InvalidArgumentErrorKind {
                    msg,
                    arg: arg.clone(),
                });
                (kind, None)
            }
            couchbase_core::memdx::error::ErrorKind::Decompression => {
                let kind = ErrorKind::OtherFailure(value.to_string());
                (kind, None)
            }
            couchbase_core::memdx::error::ErrorKind::Message(_) => {
                let kind = ErrorKind::OtherFailure(value.to_string());
                (kind, None)
            }
            _ => {
                let kind = ErrorKind::OtherFailure(value.to_string());
                (kind, None)
            }
        };
        let mut context = ErrorContext::new();

        let d_from = value.dispatched_from();
        let d_to = value.dispatched_to();

        if let Some(d_from) = d_from {
            context = context.with_dispatched_from(d_from.to_string());
        }
        if let Some(d_to) = d_to {
            context = context.with_dispatched_to(d_to.to_string());
        }

        if let Some(extended) = extended_context {
            context = context.with_extended_context(extended);
        }

        Error {
            kind: Box::new(kind),
            context: Box::new(Some(context)),
        }
    }
}

impl From<&ServerErrorKind> for ErrorKind {
    fn from(value: &ServerErrorKind) -> Self {
        match value {
            ServerErrorKind::NotMyVbucket => ErrorKind::OtherFailure(value.to_string()),
            ServerErrorKind::KeyExists => ErrorKind::DocumentExists,
            ServerErrorKind::NotStored => ErrorKind::DocumentNotFound,
            ServerErrorKind::KeyNotFound => ErrorKind::DocumentNotFound,
            ServerErrorKind::TmpFail => ErrorKind::TemporaryFailure,
            ServerErrorKind::CasMismatch => ErrorKind::CasMismatch,
            ServerErrorKind::Locked => ErrorKind::DocumentLocked,
            ServerErrorKind::NotLocked => ErrorKind::DocumentNotLocked,
            ServerErrorKind::TooBig => ErrorKind::ValueTooLarge,
            ServerErrorKind::UnknownCollectionID => ErrorKind::CollectionNotFound,
            ServerErrorKind::NoBucket => ErrorKind::OtherFailure(value.to_string()),
            ServerErrorKind::UnknownBucketName => ErrorKind::BucketNotFound,
            ServerErrorKind::Access => ErrorKind::AuthenticationFailure,
            ServerErrorKind::Auth { .. } => ErrorKind::AuthenticationFailure,
            ServerErrorKind::ConfigNotSet => ErrorKind::OtherFailure(value.to_string()),
            ServerErrorKind::UnknownScopeName => ErrorKind::ScopeNotFound,
            ServerErrorKind::UnknownCollectionName => ErrorKind::CollectionNotFound,
            ServerErrorKind::Subdoc { error } => error.into(),
            ServerErrorKind::UnknownStatus { .. } => ErrorKind::OtherFailure(value.to_string()),
            ServerErrorKind::RateLimitedScopeSizeLimitExceeded => ErrorKind::QuotaLimitedFailure,
            ServerErrorKind::RateLimitedNetworkEgress => ErrorKind::RateLimitedFailure,
            ServerErrorKind::RateLimitedNetworkIngress => ErrorKind::RateLimitedFailure,
            ServerErrorKind::RateLimitedMaxCommands => ErrorKind::RateLimitedFailure,
            ServerErrorKind::RateLimitedMaxConnections => ErrorKind::RateLimitedFailure,
            ServerErrorKind::DurabilityImpossible => ErrorKind::DurabilityImpossible,
            ServerErrorKind::SyncWriteRecommitInProgress => {
                ErrorKind::DurableWriteRecommitInProgress
            }
            ServerErrorKind::SyncWriteInProgress => ErrorKind::DurabilityWriteInProgress,
            ServerErrorKind::SyncWriteAmbiguous => ErrorKind::DurabilityAmbiguous,
            ServerErrorKind::DurabilityInvalid => ErrorKind::DurabilityLevelNotAvailable,
            ServerErrorKind::BadDelta => ErrorKind::DeltaInvalid,
            ServerErrorKind::RangeError => ErrorKind::NumberTooBig,
            ServerErrorKind::InternalError => ErrorKind::InternalServerFailure,
            ServerErrorKind::Busy => ErrorKind::TemporaryFailure,
            ServerErrorKind::UnknownCommand => ErrorKind::UnsupportedOperation,
            ServerErrorKind::NotSupported => ErrorKind::UnsupportedOperation,
            // These can't happen until we support range scan.
            ServerErrorKind::RangeScanCancelled => ErrorKind::OtherFailure(value.to_string()),
            ServerErrorKind::RangeScanVBUUIDNotEqual => ErrorKind::OtherFailure(value.to_string()),
            _ => ErrorKind::OtherFailure(value.to_string()),
        }
    }
}

impl From<&SubdocError> for ErrorKind {
    fn from(value: &SubdocError) -> Self {
        match value.kind() {
            SubdocErrorKind::PathNotFound => ErrorKind::PathNotFound,
            SubdocErrorKind::PathMismatch => ErrorKind::PathMismatch,
            SubdocErrorKind::PathInvalid => ErrorKind::PathInvalid,
            SubdocErrorKind::PathTooBig => ErrorKind::PathTooBig,
            SubdocErrorKind::DocTooDeep => ErrorKind::PathTooDeep,
            SubdocErrorKind::CantInsert => ErrorKind::ValueInvalid,
            SubdocErrorKind::NotJSON => ErrorKind::DocumentNotJSON,
            SubdocErrorKind::BadRange => ErrorKind::NumberTooBig,
            SubdocErrorKind::BadDelta => ErrorKind::DeltaInvalid,
            SubdocErrorKind::PathExists => ErrorKind::PathExists,
            SubdocErrorKind::ValueTooDeep => ErrorKind::ValueTooDeep,
            SubdocErrorKind::InvalidCombo => ErrorKind::InvalidArgument(InvalidArgumentErrorKind {
                msg: value.to_string(),
                arg: None,
            }),
            SubdocErrorKind::XattrInvalidFlagCombo => ErrorKind::XattrInvalidFlagCombo,
            SubdocErrorKind::XattrInvalidKeyCombo => ErrorKind::XattrInvalidKeyCombo,
            SubdocErrorKind::XattrUnknownMacro => ErrorKind::XattrUnknownMacro,
            SubdocErrorKind::XattrUnknownVAttr => ErrorKind::XattrUnknownVirtualAttribute,
            SubdocErrorKind::XattrCannotModifyVAttr => ErrorKind::XattrCannotModifyVirtualAttribute,
            SubdocErrorKind::InvalidXattrOrder => ErrorKind::XattrInvalidOrder,
            SubdocErrorKind::XattrUnknownVattrMacro => ErrorKind::XattrUnknownVirtualAttribute,
            SubdocErrorKind::CanOnlyReviveDeletedDocuments => {
                ErrorKind::OtherFailure(value.to_string())
            }
            SubdocErrorKind::DeletedDocumentCantHaveValue => {
                ErrorKind::OtherFailure(value.to_string())
            }
            _ => ErrorKind::OtherFailure(value.to_string()),
        }
    }
}

impl From<&couchbase_core::queryx::error::Error> for Error {
    fn from(value: &couchbase_core::queryx::error::Error) -> Self {
        match value.kind() {
            couchbase_core::queryx::error::ErrorKind::Server(e) => {
                let (kind, extended_context) = Error::parse_query_server_error(e);

                let context = ErrorContext::new()
                    .with_extended_context(ExtendedErrorContext::Query(extended_context))
                    .with_dispatched_to(e.endpoint().to_string());

                Error {
                    kind: Box::new(kind),
                    context: Box::new(Some(context)),
                }
            }
            couchbase_core::queryx::error::ErrorKind::Http {
                endpoint,
                statement,
                client_context_id,
                error,
                ..
            } => {
                let context = ErrorContext::new()
                    .with_extended_context(ExtendedErrorContext::Query(QueryErrorContext {
                        statement: statement.clone().unwrap_or_default(),
                        code: None,
                        message: None,
                        client_context_id: client_context_id.clone().unwrap_or_default(),
                        http_status_code: None,
                        descs: vec![],
                    }))
                    .with_dispatched_to(endpoint.to_string());

                let msg = if let Some(source) = value.source() {
                    source.to_string()
                } else {
                    error.to_string()
                };

                Error {
                    kind: Box::new(ErrorKind::OtherFailure(msg)),
                    context: Box::new(Some(context)),
                }
            }
            couchbase_core::queryx::error::ErrorKind::Resource(e) => {
                let cause = e.cause();
                let (kind, extended_context) = Error::parse_query_server_error(cause);

                let context = ErrorContext::new()
                    .with_extended_context(ExtendedErrorContext::Query(extended_context))
                    .with_dispatched_to(cause.endpoint().to_string());

                Error {
                    kind: Box::new(kind),
                    context: Box::new(Some(context)),
                }
            }
            couchbase_core::queryx::error::ErrorKind::Message {
                msg,
                endpoint,
                statement,
                client_context_id,
                ..
            } => {
                let mut context = ErrorContext::new().with_extended_context(
                    ExtendedErrorContext::Query(QueryErrorContext {
                        statement: statement.clone().unwrap_or_default(),
                        code: None,
                        message: None,
                        client_context_id: client_context_id.clone().unwrap_or_default(),
                        http_status_code: None,
                        descs: vec![],
                    }),
                );

                if let Some(endpoint) = endpoint {
                    context = context.with_dispatched_to(endpoint.to_string())
                };

                Error {
                    kind: Box::new(ErrorKind::OtherFailure(msg.clone())),
                    context: Box::new(Some(context)),
                }
            }
            couchbase_core::queryx::error::ErrorKind::InvalidArgument { msg, arg, .. } => Error {
                kind: Box::new(ErrorKind::InvalidArgument(InvalidArgumentErrorKind {
                    msg: msg.clone(),
                    arg: arg.clone(),
                })),
                context: Box::new(None),
            },
            couchbase_core::queryx::error::ErrorKind::Encoding { msg, .. } => Error {
                kind: Box::new(ErrorKind::OtherFailure(msg.clone())),
                context: Box::new(None),
            },
            _ => Error {
                kind: Box::new(ErrorKind::OtherFailure(value.to_string())),
                context: Box::new(None),
            },
        }
    }
}

impl From<&couchbase_core::queryx::error::ServerErrorKind> for ErrorKind {
    fn from(value: &couchbase_core::queryx::error::ServerErrorKind) -> Self {
        match value {
            couchbase_core::queryx::error::ServerErrorKind::ParsingFailure => {
                ErrorKind::ParsingFailure
            }
            couchbase_core::queryx::error::ServerErrorKind::Internal => {
                ErrorKind::InternalServerFailure
            }
            couchbase_core::queryx::error::ServerErrorKind::AuthenticationFailure => {
                ErrorKind::AuthenticationFailure
            }
            couchbase_core::queryx::error::ServerErrorKind::CasMismatch => ErrorKind::CasMismatch,
            couchbase_core::queryx::error::ServerErrorKind::DocNotFound => {
                ErrorKind::DocumentNotFound
            }
            couchbase_core::queryx::error::ServerErrorKind::DocExists => ErrorKind::DocumentExists,
            couchbase_core::queryx::error::ServerErrorKind::PlanningFailure => {
                ErrorKind::PlanningFailure
            }
            couchbase_core::queryx::error::ServerErrorKind::IndexFailure => ErrorKind::IndexFailure,
            couchbase_core::queryx::error::ServerErrorKind::PreparedStatementFailure => {
                ErrorKind::PreparedStatementFailure
            }
            couchbase_core::queryx::error::ServerErrorKind::DMLFailure => ErrorKind::DMLFailure,
            couchbase_core::queryx::error::ServerErrorKind::Timeout => ErrorKind::ServerTimeout,
            couchbase_core::queryx::error::ServerErrorKind::IndexExists => ErrorKind::IndexExists,
            couchbase_core::queryx::error::ServerErrorKind::IndexNotFound => {
                ErrorKind::IndexNotFound
            }
            couchbase_core::queryx::error::ServerErrorKind::WriteInReadOnlyMode => {
                ErrorKind::OtherFailure(value.to_string())
            }
            couchbase_core::queryx::error::ServerErrorKind::ScopeNotFound => {
                ErrorKind::ScopeNotFound
            }
            couchbase_core::queryx::error::ServerErrorKind::CollectionNotFound => {
                ErrorKind::CollectionNotFound
            }
            couchbase_core::queryx::error::ServerErrorKind::InvalidArgument {
                argument,
                reason,
                ..
            } => ErrorKind::InvalidArgument(InvalidArgumentErrorKind {
                msg: reason.clone(),
                arg: Some(argument.clone()),
            }),
            couchbase_core::queryx::error::ServerErrorKind::BuildAlreadyInProgress => {
                ErrorKind::OtherFailure(value.to_string())
            }
            couchbase_core::queryx::error::ServerErrorKind::Unknown => {
                ErrorKind::OtherFailure(value.to_string())
            }
            _ => ErrorKind::OtherFailure(value.to_string()),
        }
    }
}

impl From<&couchbase_core::searchx::error::Error> for Error {
    fn from(value: &couchbase_core::searchx::error::Error) -> Self {
        match value.kind() {
            couchbase_core::searchx::error::ErrorKind::Server(e) => {
                let kind = e.kind().into();

                let context = ErrorContext::new()
                    .with_extended_context(ExtendedErrorContext::Search(SearchErrorContext {
                        index_name: e.index_name().to_string(),
                        error_text: Some(e.error_text().to_string()),
                        http_status_code: Some(e.status_code()),
                    }))
                    .with_dispatched_to(e.endpoint().to_string());

                Error {
                    kind: Box::new(kind),
                    context: Box::new(Some(context)),
                }
            }
            couchbase_core::searchx::error::ErrorKind::Http {
                endpoint, error, ..
            } => {
                let context = ErrorContext::new().with_dispatched_to(endpoint.clone());

                let msg = if let Some(source) = value.source() {
                    source.to_string()
                } else {
                    error.to_string()
                };

                Error {
                    kind: Box::new(ErrorKind::OtherFailure(msg)),
                    context: Box::new(Some(context)),
                }
            }
            couchbase_core::searchx::error::ErrorKind::Message { msg, endpoint, .. } => {
                let context = endpoint
                    .as_ref()
                    .map(|endpoint| ErrorContext::new().with_dispatched_to(endpoint.clone()));

                Error {
                    kind: Box::new(ErrorKind::OtherFailure(msg.clone())),
                    context: Box::new(context),
                }
            }
            couchbase_core::searchx::error::ErrorKind::InvalidArgument { msg, arg, .. } => Error {
                kind: Box::new(ErrorKind::InvalidArgument(InvalidArgumentErrorKind {
                    msg: msg.clone(),
                    arg: arg.clone(),
                })),
                context: Box::new(None),
            },
            couchbase_core::searchx::error::ErrorKind::Encoding { msg, .. } => Error {
                kind: Box::new(ErrorKind::EncodingFailure(msg.clone())),
                context: Box::new(None),
            },
            couchbase_core::searchx::error::ErrorKind::UnsupportedFeature { feature, .. } => {
                Error {
                    kind: Box::new(ErrorKind::FeatureNotAvailable(
                        FeatureNotAvailableErrorKind {
                            feature: feature.clone(),
                            msg: None,
                        },
                    )),
                    context: Box::new(None),
                }
            }
            _ => Error {
                kind: Box::new(ErrorKind::OtherFailure(value.to_string())),
                context: Box::new(None),
            },
        }
    }
}

impl From<&couchbase_core::searchx::error::ServerErrorKind> for ErrorKind {
    fn from(value: &couchbase_core::searchx::error::ServerErrorKind) -> Self {
        match value {
            couchbase_core::searchx::error::ServerErrorKind::Internal => {
                ErrorKind::InternalServerFailure
            }
            couchbase_core::searchx::error::ServerErrorKind::AuthenticationFailure => {
                ErrorKind::AuthenticationFailure
            }
            couchbase_core::searchx::error::ServerErrorKind::IndexExists => {
                ErrorKind::IndexNotFound
            }
            couchbase_core::searchx::error::ServerErrorKind::IndexNotFound => {
                ErrorKind::IndexNotFound
            }
            couchbase_core::searchx::error::ServerErrorKind::UnknownIndexType => {
                ErrorKind::OtherFailure(value.to_string())
            }
            couchbase_core::searchx::error::ServerErrorKind::SourceTypeIncorrect => {
                ErrorKind::OtherFailure(value.to_string())
            }
            couchbase_core::searchx::error::ServerErrorKind::SourceNotFound => {
                ErrorKind::OtherFailure(value.to_string())
            }
            couchbase_core::searchx::error::ServerErrorKind::NoIndexPartitionsPlanned => {
                ErrorKind::OtherFailure(value.to_string())
            }
            couchbase_core::searchx::error::ServerErrorKind::NoIndexPartitionsFound => {
                ErrorKind::OtherFailure(value.to_string())
            }
            couchbase_core::searchx::error::ServerErrorKind::UnsupportedFeature => {
                ErrorKind::OtherFailure(value.to_string())
            }
            couchbase_core::searchx::error::ServerErrorKind::RateLimitedFailure => {
                ErrorKind::RateLimitedFailure
            }
            couchbase_core::searchx::error::ServerErrorKind::Unknown => {
                ErrorKind::OtherFailure(value.to_string())
            }
            _ => ErrorKind::OtherFailure(value.to_string()),
        }
    }
}

impl From<&couchbase_core::mgmtx::error::Error> for Error {
    fn from(value: &couchbase_core::mgmtx::error::Error) -> Self {
        match value.kind() {
            couchbase_core::mgmtx::error::ErrorKind::Server(e) => {
                let kind = e.kind().into();

                let context = ErrorContext::new()
                    .with_extended_context(ExtendedErrorContext::Http(HttpErrorContext {
                        status_code: e.status_code(),
                        path: e.path().to_string(),
                        method: e.method().clone(),
                        error_text: Some(e.body().to_string()),
                    }))
                    .with_dispatched_to(e.url().to_string());

                Error {
                    kind: Box::new(kind),
                    context: Box::new(Some(context)),
                }
            }
            couchbase_core::mgmtx::error::ErrorKind::Resource(e) => {
                let e = e.cause();
                let kind = e.kind().into();

                let context = ErrorContext::new()
                    .with_extended_context(ExtendedErrorContext::Http(HttpErrorContext {
                        status_code: e.status_code(),
                        path: e.path().to_string(),
                        method: e.method().clone(),
                        error_text: Some(e.body().to_string()),
                    }))
                    .with_dispatched_to(e.url().to_string());

                Error {
                    kind: Box::new(kind),
                    context: Box::new(Some(context)),
                }
            }
            couchbase_core::mgmtx::error::ErrorKind::InvalidArgument { msg, arg, .. } => Error {
                kind: Box::new(ErrorKind::InvalidArgument(InvalidArgumentErrorKind {
                    msg: msg.clone(),
                    arg: arg.clone(),
                })),
                context: Box::new(None),
            },
            couchbase_core::mgmtx::error::ErrorKind::Message(msg) => Error {
                kind: Box::new(ErrorKind::OtherFailure(msg.clone())),
                context: Box::new(None),
            },
            _ => Error {
                kind: Box::new(ErrorKind::OtherFailure(value.to_string())),
                context: Box::new(None),
            },
        }
    }
}

impl From<&couchbase_core::mgmtx::error::ServerErrorKind> for ErrorKind {
    fn from(value: &couchbase_core::mgmtx::error::ServerErrorKind) -> Self {
        match value {
            couchbase_core::mgmtx::error::ServerErrorKind::AccessDenied => {
                ErrorKind::AuthenticationFailure
            }
            couchbase_core::mgmtx::error::ServerErrorKind::UnsupportedFeature { feature } => {
                ErrorKind::FeatureNotAvailable(FeatureNotAvailableErrorKind {
                    feature: feature.clone(),
                    msg: None,
                })
            }
            couchbase_core::mgmtx::error::ServerErrorKind::ScopeExists => ErrorKind::ScopeExists,
            couchbase_core::mgmtx::error::ServerErrorKind::ScopeNotFound => {
                ErrorKind::ScopeNotFound
            }
            couchbase_core::mgmtx::error::ServerErrorKind::CollectionExists => {
                ErrorKind::CollectionExists
            }
            couchbase_core::mgmtx::error::ServerErrorKind::CollectionNotFound => {
                ErrorKind::CollectionNotFound
            }
            couchbase_core::mgmtx::error::ServerErrorKind::BucketExists => ErrorKind::BucketExists,
            couchbase_core::mgmtx::error::ServerErrorKind::BucketNotFound => {
                ErrorKind::BucketNotFound
            }
            couchbase_core::mgmtx::error::ServerErrorKind::FlushDisabled => {
                ErrorKind::BucketNotFlushable
            }
            couchbase_core::mgmtx::error::ServerErrorKind::ServerInvalidArg { arg, reason } => {
                ErrorKind::InvalidArgument(InvalidArgumentErrorKind {
                    arg: Some(arg.clone()),
                    msg: reason.clone(),
                })
            }
            couchbase_core::mgmtx::error::ServerErrorKind::BucketUuidMismatch => {
                ErrorKind::OtherFailure(value.to_string())
            }
            couchbase_core::mgmtx::error::ServerErrorKind::UserNotFound => ErrorKind::UserNotFound,
            couchbase_core::mgmtx::error::ServerErrorKind::GroupNotFound => {
                ErrorKind::GroupNotFound
            }
            couchbase_core::mgmtx::error::ServerErrorKind::OperationDelayed => {
                ErrorKind::OtherFailure(value.to_string())
            }
            couchbase_core::mgmtx::error::ServerErrorKind::Unknown => {
                ErrorKind::OtherFailure(value.to_string())
            }
            _ => ErrorKind::OtherFailure(value.to_string()),
        }
    }
}

impl From<couchbase_connstr::error::Error> for Error {
    fn from(value: couchbase_connstr::error::Error) -> Self {
        let kind = match value.kind() {
            couchbase_connstr::error::ErrorKind::InvalidArgument { msg, arg } => {
                ErrorKind::InvalidArgument(InvalidArgumentErrorKind {
                    msg: msg.clone(),
                    arg: Some(arg.clone()),
                })
            }
            _ => ErrorKind::OtherFailure(value.to_string()),
        };

        Error {
            kind: Box::new(kind),
            context: Box::new(None),
        }
    }
}
