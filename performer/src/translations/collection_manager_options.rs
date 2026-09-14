use crate::errors::error::Error;
use crate::proto::protocol::sdk::bucket::collection_manager::{
    CreateCollectionOptions, CreateScopeOptions, DropCollectionOptions, DropScopeOptions,
    GetAllScopesOptions, UpdateCollectionOptions,
};

impl TryFrom<GetAllScopesOptions>
    for couchbase::options::collection_mgmt_options::GetAllScopesOptions
{
    type Error = Box<Error>;

    fn try_from(_options: GetAllScopesOptions) -> Result<Self, Self::Error> {
        let opts = couchbase::options::collection_mgmt_options::GetAllScopesOptions::default();

        Ok(opts)
    }
}

impl TryFrom<CreateScopeOptions>
    for couchbase::options::collection_mgmt_options::CreateScopeOptions
{
    type Error = Box<Error>;

    fn try_from(_options: CreateScopeOptions) -> Result<Self, Self::Error> {
        let opts = couchbase::options::collection_mgmt_options::CreateScopeOptions::default();

        Ok(opts)
    }
}

impl TryFrom<DropScopeOptions> for couchbase::options::collection_mgmt_options::DropScopeOptions {
    type Error = Box<Error>;

    fn try_from(_options: DropScopeOptions) -> Result<Self, Self::Error> {
        let opts = couchbase::options::collection_mgmt_options::DropScopeOptions::default();

        Ok(opts)
    }
}

impl TryFrom<CreateCollectionOptions>
    for couchbase::options::collection_mgmt_options::CreateCollectionOptions
{
    type Error = Box<Error>;

    fn try_from(_options: CreateCollectionOptions) -> Result<Self, Self::Error> {
        let opts = couchbase::options::collection_mgmt_options::CreateCollectionOptions::default();

        Ok(opts)
    }
}

impl TryFrom<UpdateCollectionOptions>
    for couchbase::options::collection_mgmt_options::UpdateCollectionOptions
{
    type Error = Box<Error>;

    fn try_from(_options: UpdateCollectionOptions) -> Result<Self, Self::Error> {
        let opts = couchbase::options::collection_mgmt_options::UpdateCollectionOptions::default();

        Ok(opts)
    }
}

impl TryFrom<DropCollectionOptions>
    for couchbase::options::collection_mgmt_options::DropCollectionOptions
{
    type Error = Box<Error>;

    fn try_from(_options: DropCollectionOptions) -> Result<Self, Self::Error> {
        let opts = couchbase::options::collection_mgmt_options::DropCollectionOptions::default();

        Ok(opts)
    }
}
