use crate::commands::execution::execute_simple;
use crate::commands::helpers::create_success_sdk_result;
use crate::errors;
use crate::proto::protocol::shared::authenticator::Authenticator;
use crate::translations::cluster_options::map_authenticator;
use couchbase::cluster::Cluster;
use prost_types::Timestamp;

pub struct AuthenticatorCommand {
    cluster: Cluster,
    authenticator: Authenticator,
    return_result: bool,
    initiated: Timestamp,
}

impl AuthenticatorCommand {
    pub fn new(cluster: Cluster, authenticator: Authenticator, return_result: bool) -> Self {
        Self {
            cluster,
            authenticator,
            initiated: Timestamp::default(),
            return_result,
        }
    }

    pub async fn execute(
        self,
        batcher: &crate::common::batcher::Batcher,
    ) -> errors::error::Result<bool> {
        let auth = map_authenticator(&self.authenticator);
        execute_simple(
            self.initiated,
            batcher,
            self.return_result,
            self.cluster.set_authenticator(auth),
            |_| Ok(create_success_sdk_result()),
        )
        .await
    }
}
