use crate::errors::error::Error;
use crate::proto::protocol::sdk::bucket::collection_manager::{
    CreateCollectionOptions, CreateScopeOptions, DropCollectionOptions, DropScopeOptions,
    GetAllScopesOptions, UpdateCollectionOptions,
};

impl TryFrom<GetAllScopesOptions>
    for couchbase::options::collection_mgmt_options::GetAllScopesOptions
{
    type Error = Box<Error>;

    fn try_from(options: GetAllScopesOptions) -> Result<Self, Self::Error> {
        let opts = couchbase::options::collection_mgmt_options::GetAllScopesOptions::default();

        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}

impl TryFrom<CreateScopeOptions>
    for couchbase::options::collection_mgmt_options::CreateScopeOptions
{
    type Error = Box<Error>;

    fn try_from(options: CreateScopeOptions) -> Result<Self, Self::Error> {
        let opts = couchbase::options::collection_mgmt_options::CreateScopeOptions::default();

        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}

impl TryFrom<DropScopeOptions> for couchbase::options::collection_mgmt_options::DropScopeOptions {
    type Error = Box<Error>;

    fn try_from(options: DropScopeOptions) -> Result<Self, Self::Error> {
        let opts = couchbase::options::collection_mgmt_options::DropScopeOptions::default();

        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}

impl TryFrom<CreateCollectionOptions>
    for couchbase::options::collection_mgmt_options::CreateCollectionOptions
{
    type Error = Box<Error>;

    fn try_from(options: CreateCollectionOptions) -> Result<Self, Self::Error> {
        let opts = couchbase::options::collection_mgmt_options::CreateCollectionOptions::default();

        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}

impl TryFrom<UpdateCollectionOptions>
    for couchbase::options::collection_mgmt_options::UpdateCollectionOptions
{
    type Error = Box<Error>;

    fn try_from(options: UpdateCollectionOptions) -> Result<Self, Self::Error> {
        let opts = couchbase::options::collection_mgmt_options::UpdateCollectionOptions::default();

        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}

impl TryFrom<DropCollectionOptions>
    for couchbase::options::collection_mgmt_options::DropCollectionOptions
{
    type Error = Box<Error>;

    fn try_from(options: DropCollectionOptions) -> Result<Self, Self::Error> {
        let opts = couchbase::options::collection_mgmt_options::DropCollectionOptions::default();

        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}
