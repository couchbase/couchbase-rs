use crate::errors::error::{Error, Result};
use crate::proto::protocol::sdk::cluster::bucket_manager;
use couchbase::options::bucket_mgmt_options::*;

impl TryFrom<bucket_manager::GetBucketOptions> for GetBucketOptions {
    type Error = Box<Error>;

    fn try_from(options: bucket_manager::GetBucketOptions) -> Result<Self> {
        let opts = GetBucketOptions::default();

        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}

impl TryFrom<bucket_manager::GetAllBucketsOptions> for GetAllBucketsOptions {
    type Error = Box<Error>;

    fn try_from(options: bucket_manager::GetAllBucketsOptions) -> Result<Self> {
        let opts = GetAllBucketsOptions::default();

        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}

impl TryFrom<bucket_manager::CreateBucketOptions> for CreateBucketOptions {
    type Error = Box<Error>;

    fn try_from(options: bucket_manager::CreateBucketOptions) -> Result<Self> {
        let opts = CreateBucketOptions::default();

        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}

impl TryFrom<bucket_manager::DropBucketOptions> for DropBucketOptions {
    type Error = Box<Error>;

    fn try_from(options: bucket_manager::DropBucketOptions) -> Result<Self> {
        let opts = DropBucketOptions::default();

        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}

impl TryFrom<bucket_manager::FlushBucketOptions> for FlushBucketOptions {
    type Error = Box<Error>;

    fn try_from(options: bucket_manager::FlushBucketOptions) -> Result<Self> {
        let opts = FlushBucketOptions::default();

        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}

impl TryFrom<bucket_manager::UpdateBucketOptions> for UpdateBucketOptions {
    type Error = Box<Error>;

    fn try_from(options: bucket_manager::UpdateBucketOptions) -> Result<Self> {
        let opts = UpdateBucketOptions::default();

        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}
