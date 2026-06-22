use crate::proto::protocol::run;
use crate::translations::common::sdk_error_to_proto_run_result;
use std::fmt;

pub type Result<T> = std::result::Result<T, Box<Error>>;

pub enum Error {
    Status(tonic::Status),
    // This represents an SDK error that occurred e.g. during command creation that we want to propagate to the driver
    Sdk(Box<run::Result>),
}

impl Error {
    pub fn internal(msg: impl Into<String>) -> Box<Self> {
        Box::new(Error::Status(tonic::Status::internal(msg.into())))
    }

    pub fn invalid_argument(msg: impl Into<String>) -> Box<Self> {
        Box::new(Error::Status(tonic::Status::invalid_argument(msg.into())))
    }

    pub fn unimplemented(msg: impl Into<String>) -> Box<Self> {
        Box::new(Error::Status(tonic::Status::unimplemented(msg.into())))
    }

    pub fn sdk(err: couchbase::error::Error) -> Box<Self> {
        Box::new(Error::Sdk(Box::new(run::Result {
            elapsed_nanos: 0,
            initiated: None,
            result: Some(sdk_error_to_proto_run_result(err)),
        })))
    }

    pub fn status(self) -> tonic::Status {
        match self {
            Error::Status(status) => status,
            _ => tonic::Status::internal("Tried to extract GRPC status from non-status error, this is probably a performer bug"),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Status(status) => write!(f, "{status}"),
            Error::Sdk(sdk_error) => write!(f, "{sdk_error:?}"),
        }
    }
}

impl From<couchbase::error::Error> for Box<Error> {
    fn from(err: couchbase::error::Error) -> Self {
        Error::sdk(err)
    }
}
