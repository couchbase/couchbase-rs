use crate::errors::error::{Error, Result};
use crate::proto::protocol::sdk::cluster::bucket_manager;
use couchbase::options::bucket_mgmt_options::*;

impl TryFrom<bucket_manager::GetBucketOptions> for GetBucketOptions {
    type Error = Box<Error>;

    fn try_from(_options: bucket_manager::GetBucketOptions) -> Result<Self> {
        let opts = GetBucketOptions::default();

        Ok(opts)
    }
}

impl TryFrom<bucket_manager::GetAllBucketsOptions> for GetAllBucketsOptions {
    type Error = Box<Error>;

    fn try_from(_options: bucket_manager::GetAllBucketsOptions) -> Result<Self> {
        let opts = GetAllBucketsOptions::default();

        Ok(opts)
    }
}

impl TryFrom<bucket_manager::CreateBucketOptions> for CreateBucketOptions {
    type Error = Box<Error>;

    fn try_from(_options: bucket_manager::CreateBucketOptions) -> Result<Self> {
        let opts = CreateBucketOptions::default();

        Ok(opts)
    }
}

impl TryFrom<bucket_manager::DropBucketOptions> for DropBucketOptions {
    type Error = Box<Error>;

    fn try_from(_options: bucket_manager::DropBucketOptions) -> Result<Self> {
        let opts = DropBucketOptions::default();

        Ok(opts)
    }
}

impl TryFrom<bucket_manager::FlushBucketOptions> for FlushBucketOptions {
    type Error = Box<Error>;

    fn try_from(_options: bucket_manager::FlushBucketOptions) -> Result<Self> {
        let opts = FlushBucketOptions::default();

        Ok(opts)
    }
}

impl TryFrom<bucket_manager::UpdateBucketOptions> for UpdateBucketOptions {
    type Error = Box<Error>;

    fn try_from(_options: bucket_manager::UpdateBucketOptions) -> Result<Self> {
        let opts = UpdateBucketOptions::default();

        Ok(opts)
    }
}
