use crate::errors::error::Result;
use crate::proto::protocol::{run, sdk};
use crate::translations::common::sdk_error_to_proto_run_result;
use prost_types::Timestamp;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn create_run_result(initiated: Timestamp, elapsed: Duration) -> run::Result {
    run::Result {
        initiated: Some(initiated),
        elapsed_nanos: elapsed.as_nanos() as i64,
        result: None,
    }
}

pub fn create_success_sdk_result() -> run::result::Result {
    run::result::Result::Sdk(sdk::Result {
        result: Some(sdk::result::Result::Success(true)),
    })
}

pub async fn handle_simple_error(
    err: couchbase::error::Error,
    top_level_result: &mut run::Result,
    batcher: &crate::common::batcher::Batcher,
) -> Result<bool> {
    top_level_result.result = Some(sdk_error_to_proto_run_result(err));
    batcher.send(top_level_result.clone()).await;
    Ok(false)
}

pub fn current_timestamp() -> Timestamp {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    Timestamp {
        seconds: now.as_secs() as i64,
        nanos: now.subsec_nanos() as i32,
    }
}
