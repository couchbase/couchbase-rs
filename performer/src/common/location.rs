use crate::counters::counter;
use crate::errors::error::{Error, Result};
use crate::proto::protocol::shared;
use crate::proto::protocol::shared::doc_location;
use std::sync::Arc;
use uuid::Uuid;

pub struct DocLocation {
    collection: shared::Collection,
    id: String,
}

impl DocLocation {
    pub fn from_proto(
        location: &shared::DocLocation,
        counters: Arc<counter::Counters>,
    ) -> Result<Self> {
        match location.location.clone() {
            Some(doc_location::Location::Specific(specific)) => {
                let collection = specific.collection.ok_or_else(|| {
                    Error::invalid_argument("specific location must have a collection")
                })?;

                Ok(Self {
                    collection,
                    id: specific.id,
                })
            }
            Some(doc_location::Location::Pool(pool)) => {
                let next = match &pool.pool_selection_strategy {
                    Some(shared::doc_location_pool::PoolSelectionStrategy::Random(random)) => {
                        if random.distribution == shared::RandomDistribution::Uniform as i32 {
                            fastrand::i32(0..pool.pool_size as i32)
                        } else {
                            return Err(Error::invalid_argument(
                                "unrecognised random distribution",
                            ));
                        }
                    }
                    Some(shared::doc_location_pool::PoolSelectionStrategy::Counter(
                        counter_selection,
                    )) => {
                        let counter = counter_selection.counter.as_ref().ok_or_else(|| {
                            Error::invalid_argument("No counters in selection strategy")
                        })?;

                        let counter = counters.get(counter)?;
                        (counter.get_and_increment()) % pool.pool_size as i32
                    }
                    _ => {
                        return Err(Error::invalid_argument(
                            "unrecognised pool selection strategy",
                        ))
                    }
                };

                let collection = pool.collection.ok_or_else(|| {
                    Error::invalid_argument("pool location must have a collection")
                })?;

                Ok(Self {
                    collection,
                    id: format!("{}{}", pool.id_preface, next),
                })
            }
            Some(doc_location::Location::Uuid(uuid_loc)) => {
                let collection = uuid_loc.collection.ok_or_else(|| {
                    Error::invalid_argument("pool location must have a collection")
                })?;
                Ok(Self {
                    collection,
                    id: Uuid::new_v4().to_string(),
                })
            }
            None => Err(Error::invalid_argument("command had no valid location")),
        }
    }

    pub fn bucket(&self) -> &str {
        &self.collection.bucket_name
    }

    pub fn scope(&self) -> &str {
        &self.collection.scope_name
    }

    pub fn collection_name(&self) -> &str {
        &self.collection.collection_name
    }

    pub fn id(&self) -> &str {
        self.id.as_str()
    }
}
