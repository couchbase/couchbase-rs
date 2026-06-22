use crate::errors;
use crate::proto::protocol::sdk::cluster::bucket_manager;
use crate::proto::protocol::shared::Durability;
use couchbase::management::buckets::bucket_settings::*;

impl TryFrom<bucket_manager::CreateBucketSettings> for BucketSettings {
    type Error = Box<errors::error::Error>;

    fn try_from(settings: bucket_manager::CreateBucketSettings) -> Result<Self, Self::Error> {
        let conflict_resolution = settings.conflict_resolution_type;
        let settings = settings.settings.ok_or_else(|| {
            errors::error::Error::invalid_argument("create bucket settings is required")
        })?;

        let mut sdk_settings = BucketSettings::try_from(settings)?;

        if let Some(conflict_resolution) = conflict_resolution {
            let resolution_type =
                match bucket_manager::ConflictResolutionType::try_from(conflict_resolution) {
                    Ok(bucket_manager::ConflictResolutionType::Timestamp) => {
                        ConflictResolutionType::TIMESTAMP
                    }
                    Ok(bucket_manager::ConflictResolutionType::SequenceNumber) => {
                        ConflictResolutionType::SEQUENCE_NUMBER
                    }
                    Ok(bucket_manager::ConflictResolutionType::Custom) => {
                        ConflictResolutionType::CUSTOM
                    }
                    Err(_) => {
                        return Err(errors::error::Error::invalid_argument(
                            "invalid conflict resolution type",
                        ))
                    }
                };
            sdk_settings = sdk_settings.conflict_resolution_type(resolution_type);
        }

        Ok(sdk_settings)
    }
}

impl TryFrom<bucket_manager::BucketSettings> for BucketSettings {
    type Error = Box<errors::error::Error>;

    fn try_from(settings: bucket_manager::BucketSettings) -> Result<Self, Self::Error> {
        let mut sdk_settings = BucketSettings::new(settings.name);

        // Since ram_quota_mb is required in the gRPC, it will be 0 if not set by the driver.
        if settings.ram_quota_mb != 0 {
            sdk_settings = sdk_settings.ram_quota_mb(settings.ram_quota_mb as u64);
        }

        if let Some(flush_enabled) = settings.flush_enabled {
            sdk_settings = sdk_settings.flush_enabled(flush_enabled);
        }

        if let Some(num_replicas) = settings.num_replicas {
            sdk_settings = sdk_settings.num_replicas(num_replicas as u32);
        }

        if let Some(replica_indexes) = settings.replica_indexes {
            sdk_settings = sdk_settings.replica_indexes(replica_indexes);
        }

        if let Some(bucket_type) = settings.bucket_type {
            let bt = match bucket_manager::BucketType::try_from(bucket_type) {
                Ok(bucket_manager::BucketType::Couchbase) => BucketType::COUCHBASE,
                Ok(bucket_manager::BucketType::Ephemeral) => BucketType::EPHEMERAL,
                Ok(bucket_manager::BucketType::Memcached) => {
                    return Err(errors::error::Error::unimplemented(
                        "memcached bucket type not supported",
                    ))
                }
                Err(_) => {
                    return Err(errors::error::Error::invalid_argument(
                        "invalid bucket type",
                    ))
                }
            };
            sdk_settings = sdk_settings.bucket_type(bt);
        }

        if let Some(eviction_policy) = settings.eviction_policy {
            let policy = match bucket_manager::EvictionPolicyType::try_from(eviction_policy) {
                Ok(bucket_manager::EvictionPolicyType::Full) => EvictionPolicyType::FULL,
                Ok(bucket_manager::EvictionPolicyType::NoEviction) => {
                    EvictionPolicyType::NO_EVICTION
                }
                Ok(bucket_manager::EvictionPolicyType::NotRecentlyUsed) => {
                    EvictionPolicyType::NOT_RECENTLY_USED
                }
                Ok(bucket_manager::EvictionPolicyType::ValueOnly) => EvictionPolicyType::VALUE_ONLY,
                Err(_) => {
                    return Err(errors::error::Error::invalid_argument(
                        "invalid eviction policy",
                    ))
                }
            };
            sdk_settings = sdk_settings.eviction_policy(policy);
        }

        if let Some(max_expiry) = settings.max_expiry_seconds {
            sdk_settings =
                sdk_settings.max_expiry(std::time::Duration::from_secs(max_expiry as u64));
        }

        if let Some(compression_mode) = settings.compression_mode {
            let mode = match bucket_manager::CompressionMode::try_from(compression_mode) {
                Ok(bucket_manager::CompressionMode::Active) => CompressionMode::ACTIVE,
                Ok(bucket_manager::CompressionMode::Off) => CompressionMode::OFF,
                Ok(bucket_manager::CompressionMode::Passive) => CompressionMode::PASSIVE,
                Err(_) => {
                    return Err(errors::error::Error::invalid_argument(
                        "invalid compression mode",
                    ))
                }
            };
            sdk_settings = sdk_settings.compression_mode(mode);
        }

        if let Some(durability) = settings.minimum_durability_level {
            let durability_level = match Durability::try_from(durability) {
                Ok(Durability::None) => couchbase::durability_level::DurabilityLevel::NONE,
                Ok(Durability::Majority) => couchbase::durability_level::DurabilityLevel::MAJORITY,
                Ok(Durability::MajorityAndPersistToActive) => {
                    couchbase::durability_level::DurabilityLevel::MAJORITY_AND_PERSIST_ACTIVE
                }
                Ok(Durability::PersistToMajority) => {
                    couchbase::durability_level::DurabilityLevel::PERSIST_TO_MAJORITY
                }
                Err(_) => {
                    return Err(errors::error::Error::invalid_argument(
                        "invalid durability level",
                    ))
                }
            };
            sdk_settings = sdk_settings.minimum_durability_level(durability_level);
        }

        if let Some(storage_backend) = settings.storage_backend {
            let backend = match bucket_manager::StorageBackend::try_from(storage_backend) {
                Ok(bucket_manager::StorageBackend::Couchstore) => StorageBackend::COUCHSTORE,
                Ok(bucket_manager::StorageBackend::Magma) => StorageBackend::MAGMA,
                Err(_) => {
                    return Err(errors::error::Error::invalid_argument(
                        "invalid storage backend",
                    ))
                }
            };
            sdk_settings = sdk_settings.storage_backend(backend);
        }

        if let Some(history_retention_collection_default) =
            settings.history_retention_collection_default
        {
            sdk_settings = sdk_settings
                .history_retention_collection_default(history_retention_collection_default);
        }

        if let Some(history_retention_seconds) = settings.history_retention_seconds {
            sdk_settings = sdk_settings.history_retention_duration(std::time::Duration::from_secs(
                history_retention_seconds as u64,
            ));
        }

        if let Some(history_retention_bytes) = settings.history_retention_bytes {
            sdk_settings = sdk_settings.history_retention_bytes(history_retention_bytes);
        }

        if let Some(num_vbuckets) = settings.num_vbuckets {
            sdk_settings = sdk_settings.num_vbuckets(num_vbuckets as u16);
        }

        Ok(sdk_settings)
    }
}

impl TryFrom<BucketSettings> for bucket_manager::BucketSettings {
    type Error = Box<errors::error::Error>;

    fn try_from(settings: BucketSettings) -> Result<Self, Self::Error> {
        let proto_settings = bucket_manager::BucketSettings {
            name: settings.name.to_string(),
            ram_quota_mb: settings.ram_quota_mb.unwrap() as i64,
            flush_enabled: settings.flush_enabled,
            num_replicas: settings.num_replicas.map(|v| v as i32),
            replica_indexes: settings.replica_indexes,
            bucket_type: settings
                .bucket_type
                .map(|b| match b {
                    BucketType::COUCHBASE => Ok(bucket_manager::BucketType::Couchbase as i32),
                    BucketType::EPHEMERAL => Ok(bucket_manager::BucketType::Ephemeral as i32),
                    _ => Err(errors::error::Error::invalid_argument(
                        "invalid bucket type",
                    )),
                })
                .transpose()?,
            eviction_policy: settings
                .eviction_policy
                .map(|e| match e {
                    EvictionPolicyType::FULL => Ok(bucket_manager::EvictionPolicyType::Full as i32),
                    EvictionPolicyType::NO_EVICTION => {
                        Ok(bucket_manager::EvictionPolicyType::NoEviction as i32)
                    }
                    EvictionPolicyType::NOT_RECENTLY_USED => {
                        Ok(bucket_manager::EvictionPolicyType::NotRecentlyUsed as i32)
                    }
                    EvictionPolicyType::VALUE_ONLY => {
                        Ok(bucket_manager::EvictionPolicyType::ValueOnly as i32)
                    }
                    _ => Err(errors::error::Error::invalid_argument(
                        "invalid eviction policy",
                    )),
                })
                .transpose()?,
            max_expiry_seconds: settings.max_expiry.map(|d| d.as_secs() as i32),
            compression_mode: settings
                .compression_mode
                .map(|c| match c {
                    CompressionMode::ACTIVE => Ok(bucket_manager::CompressionMode::Active as i32),
                    CompressionMode::OFF => Ok(bucket_manager::CompressionMode::Off as i32),
                    CompressionMode::PASSIVE => Ok(bucket_manager::CompressionMode::Passive as i32),
                    _ => Err(errors::error::Error::invalid_argument(
                        "invalid compression mode",
                    )),
                })
                .transpose()?,
            minimum_durability_level: settings
                .minimum_durability_level
                .map(|d| match d {
                    couchbase::durability_level::DurabilityLevel::NONE => {
                        Ok(Durability::None as i32)
                    }
                    couchbase::durability_level::DurabilityLevel::MAJORITY => {
                        Ok(Durability::Majority as i32)
                    }
                    couchbase::durability_level::DurabilityLevel::MAJORITY_AND_PERSIST_ACTIVE => {
                        Ok(Durability::MajorityAndPersistToActive as i32)
                    }
                    couchbase::durability_level::DurabilityLevel::PERSIST_TO_MAJORITY => {
                        Ok(Durability::PersistToMajority as i32)
                    }
                    _ => Err(errors::error::Error::invalid_argument(
                        "invalid durability level",
                    )),
                })
                .transpose()?,
            storage_backend: settings
                .storage_backend
                .map(|s| match s {
                    StorageBackend::COUCHSTORE => {
                        Ok(bucket_manager::StorageBackend::Couchstore as i32)
                    }
                    StorageBackend::MAGMA => Ok(bucket_manager::StorageBackend::Magma as i32),
                    _ => Err(errors::error::Error::invalid_argument(
                        "invalid storage backend",
                    )),
                })
                .transpose()?,
            history_retention_collection_default: settings.history_retention_collection_default,
            history_retention_seconds: settings
                .history_retention_duration
                .map(|d| d.as_secs() as i64),
            history_retention_bytes: settings.history_retention_bytes,
            num_vbuckets: settings.num_vbuckets.map(|v| v as u32),
        };

        Ok(proto_settings)
    }
}
