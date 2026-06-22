use crate::errors::error::Error;
use crate::proto::protocol::sdk::query::index_manager::{
    BuildDeferredIndexesOptions, CreatePrimaryQueryIndexOptions, CreateQueryIndexOptions,
    DropIndexOptions, DropPrimaryIndexOptions, GetAllQueryIndexOptions, WatchIndexesOptions,
};

impl TryFrom<GetAllQueryIndexOptions>
    for couchbase::options::query_index_mgmt_options::GetAllQueryIndexesOptions
{
    type Error = Box<Error>;

    fn try_from(options: GetAllQueryIndexOptions) -> Result<Self, Self::Error> {
        let opts =
            couchbase::options::query_index_mgmt_options::GetAllQueryIndexesOptions::default();

        if let Some(_scope_name) = options.scope_name {
            return Err(Error::unimplemented("scope_name is unimplemented"));
        }
        if let Some(_collection_name) = options.collection_name {
            return Err(Error::unimplemented("collection_name is unimplemented"));
        }
        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}

impl TryFrom<CreatePrimaryQueryIndexOptions>
    for couchbase::options::query_index_mgmt_options::CreatePrimaryQueryIndexOptions
{
    type Error = Box<Error>;

    fn try_from(options: CreatePrimaryQueryIndexOptions) -> Result<Self, Self::Error> {
        let mut opts =
            couchbase::options::query_index_mgmt_options::CreatePrimaryQueryIndexOptions::default();

        if let Some(ignore_if_exists) = options.ignore_if_exists {
            opts = opts.ignore_if_exists(ignore_if_exists);
        }
        if let Some(num_replicas) = options.num_replicas {
            opts = opts.num_replicas(num_replicas as u32);
        }
        if let Some(deferred) = options.deferred {
            opts = opts.deferred(deferred);
        }
        if let Some(index_name) = options.index_name {
            opts = opts.index_name(index_name);
        }
        if let Some(_scope_name) = options.scope_name {
            return Err(Error::unimplemented("scope_name is unimplemented"));
        }
        if let Some(_collection_name) = options.collection_name {
            return Err(Error::unimplemented("collection_name is unimplemented"));
        }
        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}

impl TryFrom<CreateQueryIndexOptions>
    for couchbase::options::query_index_mgmt_options::CreateQueryIndexOptions
{
    type Error = Box<Error>;

    fn try_from(options: CreateQueryIndexOptions) -> Result<Self, Self::Error> {
        let mut opts =
            couchbase::options::query_index_mgmt_options::CreateQueryIndexOptions::default();

        if let Some(ignore_if_exists) = options.ignore_if_exists {
            opts = opts.ignore_if_exists(ignore_if_exists);
        }
        if let Some(num_replicas) = options.num_replicas {
            opts = opts.num_replicas(num_replicas as u32);
        }
        if let Some(deferred) = options.deferred {
            opts = opts.deferred(deferred);
        }
        if let Some(_scope_name) = options.scope_name {
            return Err(Error::unimplemented("scope_name is unimplemented"));
        }
        if let Some(_collection_name) = options.collection_name {
            return Err(Error::unimplemented("collection_name is unimplemented"));
        }
        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}

impl TryFrom<DropPrimaryIndexOptions>
    for couchbase::options::query_index_mgmt_options::DropPrimaryQueryIndexOptions
{
    type Error = Box<Error>;

    fn try_from(options: DropPrimaryIndexOptions) -> Result<Self, Self::Error> {
        let mut opts =
            couchbase::options::query_index_mgmt_options::DropPrimaryQueryIndexOptions::default();

        if let Some(ignore_if_not_exists) = options.ignore_if_not_exists {
            opts = opts.ignore_if_not_exists(ignore_if_not_exists);
        }
        if let Some(_scope_name) = options.scope_name {
            return Err(Error::unimplemented("scope_name is unimplemented"));
        }
        if let Some(_collection_name) = options.collection_name {
            return Err(Error::unimplemented("collection_name is unimplemented"));
        }
        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}

impl TryFrom<DropIndexOptions>
    for couchbase::options::query_index_mgmt_options::DropQueryIndexOptions
{
    type Error = Box<Error>;

    fn try_from(options: DropIndexOptions) -> Result<Self, Self::Error> {
        let mut opts =
            couchbase::options::query_index_mgmt_options::DropQueryIndexOptions::default();

        if let Some(ignore_if_not_exists) = options.ignore_if_not_exists {
            opts = opts.ignore_if_not_exists(ignore_if_not_exists);
        }
        if let Some(_scope_name) = options.scope_name {
            return Err(Error::unimplemented("scope_name is unimplemented"));
        }
        if let Some(_collection_name) = options.collection_name {
            return Err(Error::unimplemented("collection_name is unimplemented"));
        }
        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}

impl TryFrom<WatchIndexesOptions>
    for couchbase::options::query_index_mgmt_options::WatchQueryIndexOptions
{
    type Error = Box<Error>;

    fn try_from(options: WatchIndexesOptions) -> Result<Self, Self::Error> {
        let mut opts =
            couchbase::options::query_index_mgmt_options::WatchQueryIndexOptions::default();

        if let Some(watch_primary) = options.watch_primary {
            opts = opts.watch_primary(watch_primary);
        }
        if let Some(_scope_name) = options.scope_name {
            return Err(Error::unimplemented("scope_name is unimplemented"));
        }
        if let Some(_collection_name) = options.collection_name {
            return Err(Error::unimplemented("collection_name is unimplemented"));
        }

        Ok(opts)
    }
}

impl TryFrom<BuildDeferredIndexesOptions>
    for couchbase::options::query_index_mgmt_options::BuildQueryIndexOptions
{
    type Error = Box<Error>;

    fn try_from(options: BuildDeferredIndexesOptions) -> Result<Self, Self::Error> {
        let opts = couchbase::options::query_index_mgmt_options::BuildQueryIndexOptions::default();

        if let Some(_scope_name) = options.scope_name {
            return Err(Error::unimplemented("scope_name is unimplemented"));
        }
        if let Some(_collection_name) = options.collection_name {
            return Err(Error::unimplemented("collection_name is unimplemented"));
        }
        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}
