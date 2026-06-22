use crate::errors::error::Error;
use crate::proto::protocol::sdk::bucket::collection_manager::{
    CreateCollectionSettings, UpdateCollectionSettings,
};
use couchbase::management::collections::collection_settings::MaxExpiryValue;

impl TryFrom<CreateCollectionSettings>
    for couchbase::management::collections::collection_settings::CreateCollectionSettings
{
    type Error = Box<Error>;

    fn try_from(settings: CreateCollectionSettings) -> Result<Self, Self::Error> {
        let mut opts = couchbase::management::collections::collection_settings::CreateCollectionSettings::default();

        if let Some(max_expiry_secs) = settings.expiry_secs {
            opts = opts.max_expiry(MaxExpiryValue::Seconds(std::time::Duration::from_secs(
                max_expiry_secs as u64,
            )));
        }

        if let Some(history) = settings.history {
            opts = opts.history(history);
        }

        Ok(opts)
    }
}

impl TryFrom<UpdateCollectionSettings>
    for couchbase::management::collections::collection_settings::UpdateCollectionSettings
{
    type Error = Box<Error>;

    fn try_from(settings: UpdateCollectionSettings) -> Result<Self, Self::Error> {
        let mut opts = couchbase::management::collections::collection_settings::UpdateCollectionSettings::default();

        if let Some(max_expiry_secs) = settings.expiry_secs {
            opts = opts.max_expiry(MaxExpiryValue::Seconds(std::time::Duration::from_secs(
                max_expiry_secs as u64,
            )));
        }

        if let Some(history) = settings.history {
            opts = opts.history(history);
        }

        Ok(opts)
    }
}
