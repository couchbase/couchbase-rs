use crate::errors::error::{Error, Result};
use crate::proto::protocol::sdk::result::Result::Exception;
use crate::proto::protocol::shared::expiry::ExpiryType;
use crate::proto::protocol::shared::{
    exception, CouchbaseExceptionEx, CouchbaseExceptionType, Durability, DurabilityType, Expiry,
    MutationState,
};
use crate::proto::protocol::{run, sdk, shared};
use couchbase::durability_level::DurabilityLevel;
use couchbase::error::ErrorKind;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn sdk_error_to_proto_run_result(error: couchbase::error::Error) -> run::result::Result {
    run::result::Result::Sdk(sdk_error_to_proto_sdk_result(error))
}

pub fn sdk_error_to_proto_sdk_result(error: couchbase::error::Error) -> sdk::Result {
    sdk::Result {
        result: Some(Exception(shared::Exception {
            exception: Some(sdk_error_to_proto_exception(error)),
        })),
    }
}

pub fn sdk_error_to_proto_exception(error: couchbase::error::Error) -> exception::Exception {
    exception::Exception::Couchbase(Box::new(CouchbaseExceptionEx {
        name: error.kind().to_string(),
        r#type: map_cb_error_to_proto(error.kind()) as i32,
        cause: None,
        serialized: error.to_string(),
    }))
}

fn map_cb_error_to_proto(kind: &ErrorKind) -> CouchbaseExceptionType {
    match kind {
        ErrorKind::OtherFailure(_) => CouchbaseExceptionType::SdkCouchbaseException,
        ErrorKind::ServerTimeout => CouchbaseExceptionType::SdkTimeoutException,
        ErrorKind::InvalidArgument(_) => CouchbaseExceptionType::SdkInvalidArgumentException,
        ErrorKind::ServiceNotAvailable(_) => {
            CouchbaseExceptionType::SdkServiceNotAvailableException
        }
        ErrorKind::FeatureNotAvailable(_) => {
            CouchbaseExceptionType::SdkFeatureNotAvailableException
        }
        ErrorKind::InternalServerFailure => {
            CouchbaseExceptionType::SdkInternalServerFailureException
        }
        ErrorKind::AuthenticationFailure => {
            CouchbaseExceptionType::SdkAuthenticationFailureException
        }
        ErrorKind::TemporaryFailure => CouchbaseExceptionType::SdkTemporaryFailureException,
        ErrorKind::ParsingFailure => CouchbaseExceptionType::SdkParsingFailureException,
        ErrorKind::CasMismatch => CouchbaseExceptionType::SdkCasMismatchException,
        ErrorKind::BucketNotFound => CouchbaseExceptionType::SdkBucketNotFoundException,
        ErrorKind::CollectionNotFound => CouchbaseExceptionType::SdkCollectionNotFoundException,
        ErrorKind::ScopeNotFound => CouchbaseExceptionType::SdkScopeNotFoundException,
        ErrorKind::EncodingFailure(_) => CouchbaseExceptionType::SdkEncodingFailureException,
        ErrorKind::DecodingFailure(_) => CouchbaseExceptionType::SdkDecodingFailureException,
        ErrorKind::UnsupportedOperation => CouchbaseExceptionType::SdkUnsupportedOperationException,
        ErrorKind::IndexNotFound => CouchbaseExceptionType::SdkIndexNotFoundException,
        ErrorKind::IndexExists => CouchbaseExceptionType::SdkIndexExistsException,
        ErrorKind::RateLimitedFailure => CouchbaseExceptionType::SdkRateLimitedException,
        ErrorKind::QuotaLimitedFailure => CouchbaseExceptionType::SdkQuotaLimitedException,
        ErrorKind::DocumentNotFound => CouchbaseExceptionType::SdkDocumentNotFoundException,
        ErrorKind::DocumentUnretrievable => {
            CouchbaseExceptionType::SdkDocumentUnretrievableException
        }
        ErrorKind::DocumentLocked => CouchbaseExceptionType::SdkDocumentLockedException,
        ErrorKind::ValueTooLarge => CouchbaseExceptionType::SdkValueTooLargeException,
        ErrorKind::DocumentExists => CouchbaseExceptionType::SdkDocumentExistsException,
        ErrorKind::DurabilityLevelNotAvailable => {
            CouchbaseExceptionType::SdkDurabilityLevelNotAvailableException
        }
        ErrorKind::DurabilityImpossible => CouchbaseExceptionType::SdkDurabilityImpossibleException,
        ErrorKind::DurabilityAmbiguous => CouchbaseExceptionType::SdkDurabilityAmbiguousException,
        ErrorKind::DurabilityWriteInProgress => {
            CouchbaseExceptionType::SdkDurableWriteInProgressException
        }
        ErrorKind::DurableWriteRecommitInProgress => {
            CouchbaseExceptionType::SdkDurableWriteRecommitInProgressException
        }
        ErrorKind::PathNotFound => CouchbaseExceptionType::SdkPathNotFoundException,
        ErrorKind::PathMismatch => CouchbaseExceptionType::SdkPathMismatchException,
        ErrorKind::PathInvalid => CouchbaseExceptionType::SdkPathInvalidException,
        ErrorKind::PathTooBig => CouchbaseExceptionType::SdkPathTooBigException,
        ErrorKind::PathTooDeep => CouchbaseExceptionType::SdkPathTooDeepException,
        ErrorKind::ValueTooDeep => CouchbaseExceptionType::SdkValueTooDeepException,
        ErrorKind::ValueInvalid => CouchbaseExceptionType::SdkValueInvalidException,
        ErrorKind::DocumentNotJSON => CouchbaseExceptionType::SdkDocumentNotJsonException,
        ErrorKind::NumberTooBig => CouchbaseExceptionType::SdkNumberTooBigException,
        ErrorKind::DeltaInvalid => CouchbaseExceptionType::SdkDeltaInvalidException,
        ErrorKind::PathExists => CouchbaseExceptionType::SdkPathExistsException,
        ErrorKind::XattrUnknownMacro => CouchbaseExceptionType::SdkXattrUnknownMacroException,
        ErrorKind::XattrInvalidKeyCombo => CouchbaseExceptionType::SdkXattrInvalidKeyComboException,
        ErrorKind::XattrUnknownVirtualAttribute => {
            CouchbaseExceptionType::SdkXattrUnknownVirtualAttributeException
        }
        ErrorKind::XattrCannotModifyVirtualAttribute => {
            CouchbaseExceptionType::SdkXattrCannotModifyVirtualAttributeException
        }
        ErrorKind::XattrNoAccess => CouchbaseExceptionType::SdkXattrNoAccessException,
        ErrorKind::DocumentNotLocked => CouchbaseExceptionType::SdkDocumentNotLockedException,
        ErrorKind::PlanningFailure => CouchbaseExceptionType::SdkPlanningFailureException,
        ErrorKind::IndexFailure => CouchbaseExceptionType::SdkIndexFailureException,
        ErrorKind::PreparedStatementFailure => {
            CouchbaseExceptionType::SdkPreparedStatementFailureException
        }
        ErrorKind::DMLFailure => CouchbaseExceptionType::SdkDmlFailureException,
        ErrorKind::CollectionExists => CouchbaseExceptionType::SdkCollectionExistsException,
        ErrorKind::ScopeExists => CouchbaseExceptionType::SdkScopeExistsException,
        ErrorKind::UserNotFound => CouchbaseExceptionType::SdkUserNotFoundException,
        ErrorKind::GroupNotFound => CouchbaseExceptionType::SdkGroupNotFoundException,
        ErrorKind::BucketExists => CouchbaseExceptionType::SdkBucketExistsException,
        ErrorKind::UserExists => CouchbaseExceptionType::SdkUserExistsException,
        ErrorKind::BucketNotFlushable => CouchbaseExceptionType::SdkBucketNotFlushableException,

        // These don't have a direct mapping to the proto exceptions
        ErrorKind::XattrInvalidOrder => CouchbaseExceptionType::SdkCouchbaseException,
        ErrorKind::XattrInvalidFlagCombo => CouchbaseExceptionType::SdkCouchbaseException,
        ErrorKind::MutationTokenOutdated => CouchbaseExceptionType::SdkCouchbaseException,
        ErrorKind::GroupExists => CouchbaseExceptionType::SdkCouchbaseException,
        &_ => CouchbaseExceptionType::SdkCouchbaseException,
    }
}

impl TryFrom<Expiry> for Duration {
    type Error = Box<Error>;

    fn try_from(value: Expiry) -> Result<Self> {
        match value.expiry_type {
            Some(ExpiryType::RelativeSecs(s)) => Ok(Duration::from_secs(s as u64)),
            Some(ExpiryType::AbsoluteEpochSecs(s)) => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|_| Error::internal("System time before epoch"))?;
                let expiry = Duration::from_secs(s as u64);
                Ok(expiry - now)
            }
            None => Err(Error::invalid_argument("Expiry must have a value")),
        }
    }
}

impl TryFrom<DurabilityType> for DurabilityLevel {
    type Error = Box<Error>;

    fn try_from(value: DurabilityType) -> Result<Self> {
        match value.durability {
            Some(shared::durability_type::Durability::DurabilityLevel(d)) => {
                match Durability::try_from(d) {
                    Ok(Durability::None) => Ok(DurabilityLevel::NONE),
                    Ok(Durability::Majority) => Ok(DurabilityLevel::MAJORITY),
                    Ok(Durability::MajorityAndPersistToActive) => {
                        Ok(DurabilityLevel::MAJORITY_AND_PERSIST_ACTIVE)
                    }
                    Ok(Durability::PersistToMajority) => Ok(DurabilityLevel::PERSIST_TO_MAJORITY),
                    Err(_) => Err(Error::invalid_argument(
                        "Invalid durability level specified",
                    )),
                }
            }
            Some(shared::durability_type::Durability::Observe(_)) => Err(Error::unimplemented(
                "Observe based durability is unimplemented",
            )),
            None => Err(Error::invalid_argument("Durability must have a value")),
        }
    }
}

impl From<MutationState> for couchbase::mutation_state::MutationState {
    fn from(value: MutationState) -> Self {
        let mut state = couchbase::mutation_state::MutationState::new();
        for token in value.tokens {
            let tok = couchbase::mutation_state::MutationToken::from_parts(
                token.partition_id as u16,
                token.partition_uuid as u64,
                token.sequence_number as u64,
                token.bucket_name,
            );
            state = state.push_token(tok);
        }
        state
    }
}
