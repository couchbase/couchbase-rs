use crate::errors::error::{Error, Result};
use crate::proto::protocol::sdk::kv::{
    AppendOptions, DecrementOptions, ExistsOptions, GetAndLockOptions, GetAndTouchOptions,
    GetOptions, IncrementOptions, InsertOptions, PrependOptions, RemoveOptions, ReplaceOptions,
    TouchOptions, UnlockOptions, UpsertOptions,
};
use couchbase::durability_level::DurabilityLevel;

impl TryFrom<GetOptions> for couchbase::options::kv_options::GetOptions {
    type Error = Box<Error>;

    fn try_from(proto: GetOptions) -> Result<Self> {
        let mut options = couchbase::options::kv_options::GetOptions::new();

        if let Some(with_expiry) = proto.with_expiry {
            options = options.expiry(with_expiry);
        }
        if let Some(_timeout) = proto.timeout_msecs {
            // TODO: Should we wrap operations in a performer timeout to simulate user behaviour?
            return Err(Error::unimplemented("timeout is unimplemented"));
        }
        if !proto.projection.is_empty() {
            options = options.projections(proto.projection);
        }

        Ok(options)
    }
}

impl TryFrom<InsertOptions> for couchbase::options::kv_options::InsertOptions {
    type Error = Box<Error>;

    fn try_from(proto: InsertOptions) -> Result<Self> {
        let mut options = couchbase::options::kv_options::InsertOptions::new();

        if let Some(_timeout) = proto.timeout_msecs {
            return Err(Error::unimplemented("timeout is unimplemented"));
        }
        if let Some(durability) = proto.durability {
            let level: DurabilityLevel = durability.try_into()?;
            options = options.durability_level(level);
        }
        if let Some(expiry) = proto.expiry {
            options = options.expiry(expiry.try_into()?)
        }

        Ok(options)
    }
}

impl TryFrom<ReplaceOptions> for couchbase::options::kv_options::ReplaceOptions {
    type Error = Box<Error>;

    fn try_from(proto: ReplaceOptions) -> Result<Self> {
        let mut options = couchbase::options::kv_options::ReplaceOptions::new();

        if let Some(_timeout) = proto.timeout_msecs {
            return Err(Error::unimplemented("timeout is unimplemented"));
        }
        if let Some(durability) = proto.durability {
            let level: DurabilityLevel = durability.try_into()?;
            options = options.durability_level(level);
        }
        if let Some(expiry) = proto.expiry {
            options = options.expiry(expiry.try_into()?)
        }
        if let Some(preserve_expiry) = proto.preserve_expiry {
            options = options.preserve_expiry(preserve_expiry);
        }
        if let Some(cas) = proto.cas {
            options = options.cas(cas as u64);
        }

        Ok(options)
    }
}

impl TryFrom<UpsertOptions> for couchbase::options::kv_options::UpsertOptions {
    type Error = Box<Error>;

    fn try_from(proto: UpsertOptions) -> Result<Self> {
        let mut options = couchbase::options::kv_options::UpsertOptions::new();

        if let Some(_timeout) = proto.timeout_msecs {
            return Err(Error::unimplemented("timeout is unimplemented"));
        }
        if let Some(durability) = proto.durability {
            let level: DurabilityLevel = durability.try_into()?;
            options = options.durability_level(level);
        }
        if let Some(expiry) = proto.expiry {
            options = options.expiry(expiry.try_into()?)
        }
        if let Some(preserve_expiry) = proto.preserve_expiry {
            options = options.preserve_expiry(preserve_expiry);
        }

        Ok(options)
    }
}

impl TryFrom<RemoveOptions> for couchbase::options::kv_options::RemoveOptions {
    type Error = Box<Error>;

    fn try_from(proto: RemoveOptions) -> Result<Self> {
        let mut options = couchbase::options::kv_options::RemoveOptions::new();

        if let Some(_timeout) = proto.timeout_msecs {
            return Err(Error::unimplemented("timeout is unimplemented"));
        }
        if let Some(durability) = proto.durability {
            let level: DurabilityLevel = durability.try_into()?;
            options = options.durability_level(level);
        }
        if let Some(cas) = proto.cas {
            options = options.cas(cas as u64);
        }

        Ok(options)
    }
}

impl TryFrom<GetAndLockOptions> for couchbase::options::kv_options::GetAndLockOptions {
    type Error = Box<Error>;

    fn try_from(proto: GetAndLockOptions) -> Result<Self> {
        let options = couchbase::options::kv_options::GetAndLockOptions::new();

        if let Some(_timeout) = proto.timeout_msecs {
            return Err(Error::unimplemented("timeout is unimplemented"));
        }

        Ok(options)
    }
}

impl TryFrom<GetAndTouchOptions> for couchbase::options::kv_options::GetAndTouchOptions {
    type Error = Box<Error>;

    fn try_from(proto: GetAndTouchOptions) -> Result<Self> {
        let options = couchbase::options::kv_options::GetAndTouchOptions::new();

        if let Some(_timeout) = proto.timeout_msecs {
            return Err(Error::unimplemented("timeout is unimplemented"));
        }

        Ok(options)
    }
}

impl TryFrom<UnlockOptions> for couchbase::options::kv_options::UnlockOptions {
    type Error = Box<Error>;

    fn try_from(proto: UnlockOptions) -> Result<Self> {
        let options = couchbase::options::kv_options::UnlockOptions::new();

        if let Some(_timeout) = proto.timeout_msecs {
            return Err(Error::unimplemented("timeout is unimplemented"));
        }

        Ok(options)
    }
}

impl TryFrom<TouchOptions> for couchbase::options::kv_options::TouchOptions {
    type Error = Box<Error>;

    fn try_from(proto: TouchOptions) -> Result<Self> {
        let options = couchbase::options::kv_options::TouchOptions::new();

        if let Some(_timeout) = proto.timeout_msecs {
            return Err(Error::unimplemented("timeout is unimplemented"));
        }

        Ok(options)
    }
}

impl TryFrom<ExistsOptions> for couchbase::options::kv_options::ExistsOptions {
    type Error = Box<Error>;

    fn try_from(proto: ExistsOptions) -> Result<Self> {
        let options = couchbase::options::kv_options::ExistsOptions::new();

        if let Some(_timeout) = proto.timeout_msecs {
            return Err(Error::unimplemented("timeout is unimplemented"));
        }

        Ok(options)
    }
}

impl TryFrom<AppendOptions> for couchbase::options::kv_binary_options::AppendOptions {
    type Error = Box<Error>;

    fn try_from(proto: AppendOptions) -> Result<Self> {
        let mut options = couchbase::options::kv_binary_options::AppendOptions::new();

        if let Some(_timeout) = proto.timeout_msecs {
            return Err(Error::unimplemented("timeout is unimplemented"));
        }
        if let Some(durability) = proto.durability {
            let level: DurabilityLevel = durability.try_into()?;
            options = options.durability_level(level);
        }
        if let Some(cas) = proto.cas {
            options = options.cas(cas as u64);
        }

        Ok(options)
    }
}

impl TryFrom<PrependOptions> for couchbase::options::kv_binary_options::PrependOptions {
    type Error = Box<Error>;

    fn try_from(proto: PrependOptions) -> Result<Self> {
        let mut options = couchbase::options::kv_binary_options::PrependOptions::new();

        if let Some(_timeout) = proto.timeout_msecs {
            return Err(Error::unimplemented("timeout is unimplemented"));
        }
        if let Some(durability) = proto.durability {
            let level: DurabilityLevel = durability.try_into()?;
            options = options.durability_level(level);
        }
        if let Some(cas) = proto.cas {
            options = options.cas(cas as u64);
        }

        Ok(options)
    }
}

impl TryFrom<IncrementOptions> for couchbase::options::kv_binary_options::IncrementOptions {
    type Error = Box<Error>;

    fn try_from(proto: IncrementOptions) -> Result<Self> {
        let mut options = couchbase::options::kv_binary_options::IncrementOptions::new();

        if let Some(_timeout) = proto.timeout_msecs {
            return Err(Error::unimplemented("timeout is unimplemented"));
        }
        if let Some(expiry) = proto.expiry {
            options = options.expiry(expiry.try_into()?)
        }
        if let Some(delta) = proto.delta {
            options = options.delta(delta as u64);
        }
        if let Some(initial) = proto.initial {
            options = options.initial(initial as u64);
        }
        if let Some(durability) = proto.durability {
            let level: DurabilityLevel = durability.try_into()?;
            options = options.durability_level(level);
        }

        Ok(options)
    }
}

impl TryFrom<DecrementOptions> for couchbase::options::kv_binary_options::DecrementOptions {
    type Error = Box<Error>;

    fn try_from(proto: DecrementOptions) -> Result<Self> {
        let mut options = couchbase::options::kv_binary_options::DecrementOptions::new();

        if let Some(_timeout) = proto.timeout_msecs {
            return Err(Error::unimplemented("timeout is unimplemented"));
        }
        if let Some(expiry) = proto.expiry {
            options = options.expiry(expiry.try_into()?)
        }
        if let Some(delta) = proto.delta {
            options = options.delta(delta as u64);
        }
        if let Some(initial) = proto.initial {
            options = options.initial(initial as u64);
        }
        if let Some(durability) = proto.durability {
            let level: DurabilityLevel = durability.try_into()?;
            options = options.durability_level(level);
        }

        Ok(options)
    }
}
