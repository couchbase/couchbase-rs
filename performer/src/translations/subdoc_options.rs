use crate::errors::error::Error;
use crate::proto::protocol::sdk::kv::lookup_in::LookupInOptions;
use crate::proto::protocol::sdk::kv::mutate_in::{MutateInOptions, StoreSemantics};
use couchbase::durability_level::DurabilityLevel;

impl TryFrom<LookupInOptions> for couchbase::options::kv_options::LookupInOptions {
    type Error = Box<Error>;

    fn try_from(opts: LookupInOptions) -> Result<Self, Self::Error> {
        let mut copts = couchbase::options::kv_options::LookupInOptions::new();

        if let Some(access_deleted) = opts.access_deleted {
            copts = copts.access_deleted(access_deleted);
        }

        if let Some(_timeout) = opts.timeout_millis {
            // TODO: Should we wrap operations in a performer timeout to simulate user behaviour?
            return Err(Error::unimplemented("timeout is unimplemented"));
        }

        Ok(copts)
    }
}

impl TryFrom<MutateInOptions> for couchbase::options::kv_options::MutateInOptions {
    type Error = Box<Error>;

    fn try_from(opts: MutateInOptions) -> Result<Self, Self::Error> {
        let mut copts = couchbase::options::kv_options::MutateInOptions::new();

        if let Some(expiry) = opts.expiry {
            copts = copts.expiry(expiry.try_into()?);
        }

        if let Some(cas) = opts.cas {
            copts = copts.cas(cas as u64);
        }

        if let Some(durability) = opts.durability {
            let level: DurabilityLevel = durability.try_into()?;
            copts = copts.durability_level(level);
        }

        if let Some(store_semantics) = opts.store_semantics {
            let semantic = match StoreSemantics::try_from(store_semantics) {
                Ok(StoreSemantics::Insert) => {
                    couchbase::options::kv_options::StoreSemantics::Insert
                }
                Ok(StoreSemantics::Replace) => {
                    couchbase::options::kv_options::StoreSemantics::Replace
                }
                Ok(StoreSemantics::Upsert) => {
                    couchbase::options::kv_options::StoreSemantics::Upsert
                }
                _ => return Err(Error::unimplemented("unknown store semantics")),
            };
            copts = copts.store_semantics(semantic);
        }

        if let Some(access_deleted) = opts.access_deleted {
            copts = copts.access_deleted(access_deleted);
        }

        if let Some(preserve_expiry) = opts.preserve_expiry {
            copts = copts.preserve_expiry(preserve_expiry);
        }

        if let Some(_create_as_deleted) = opts.create_as_deleted {
            return Err(Error::unimplemented("create_as_deleted is unimplemented"));
        }

        if let Some(_timeout) = opts.timeout_millis {
            // TODO: Should we wrap operations in a performer timeout to simulate user behaviour?
            return Err(Error::unimplemented("timeout is unimplemented"));
        }

        Ok(copts)
    }
}
