use crate::commands::execution::execute_simple;
use crate::commands::helpers::create_success_sdk_result;
use crate::errors;
use crate::proto::protocol::sdk::cluster::wait_until_ready::WaitUntilReadyRequest as Command;
use couchbase::bucket::Bucket;
use couchbase::cluster::Cluster;
use prost_types::Timestamp;

#[derive(Clone)]
pub enum Location {
    Cluster(Cluster),
    Bucket(Bucket),
}

pub struct WaitUntilReadyCommand {
    location: Location,
    return_result: bool,
    initiated: Timestamp,
    command: Command,
}

impl WaitUntilReadyCommand {
    pub fn new(location: Location, command: Command, return_result: bool) -> Self {
        Self {
            location,
            return_result,
            initiated: Timestamp::default(),
            command,
        }
    }

    pub async fn execute(
        self,
        batcher: &crate::common::batcher::Batcher,
    ) -> errors::error::Result<bool> {
        match self.location {
            Location::Cluster(c) => {
                execute_simple(
                    self.initiated,
                    batcher,
                    self.return_result,
                    c.wait_until_ready(self.command.options.map(|o| o.try_into()).transpose()?),
                    |_| Ok(create_success_sdk_result()),
                )
                .await
            }
            Location::Bucket(b) => {
                execute_simple(
                    self.initiated,
                    batcher,
                    self.return_result,
                    b.wait_until_ready(self.command.options.map(|o| o.try_into()).transpose()?),
                    |_| Ok(create_success_sdk_result()),
                )
                .await
            }
        }
    }
}
