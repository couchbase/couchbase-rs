use crate::commands::helpers::{create_run_result, create_success_sdk_result, handle_simple_error};
use crate::commands::mutations::MutationCommand;
use crate::errors::error::Result;
use crate::proto::protocol::run;
use crate::translations::common::sdk_error_to_proto_run_result;
use prost_types::Timestamp;

pub async fn execute_simple<T, F, M>(
    initiated: Timestamp,
    batcher: &crate::common::batcher::Batcher,
    return_result: bool,
    op: F,
    map: M,
) -> Result<bool>
where
    F: std::future::Future<Output = std::result::Result<T, couchbase::error::Error>>,
    M: FnOnce(T) -> Result<run::result::Result>,
{
    let start = std::time::Instant::now();
    let res = op.await;
    let end = std::time::Instant::now();

    let mut top_level_result = create_run_result(initiated, end - start);

    match res {
        Ok(ok) => {
            if return_result {
                top_level_result.result = Some(map(ok)?);
            } else {
                top_level_result.result = Some(create_success_sdk_result());
            }
        }
        Err(err) => {
            top_level_result.result = Some(sdk_error_to_proto_run_result(err));
            batcher.send(top_level_result).await;
            return Ok(false);
        }
    }

    batcher.send(top_level_result).await;
    Ok(true)
}

pub async fn execute_mutation<C: MutationCommand>(
    command: C,
    batcher: &crate::common::batcher::Batcher,
) -> Result<bool> {
    let encoded = crate::common::content::encode_content(command.content(), command.transcoder())?;
    let start = std::time::Instant::now();

    let res = match encoded {
        crate::common::content::EncodedContent::Raw(bytes, flags) => {
            command
                .execute_raw_operation(command.doc_id(), &bytes, flags, command.options())
                .await
        }
        crate::common::content::EncodedContent::PassthroughString(s) => {
            command
                .execute_operation(command.doc_id(), &s, command.options())
                .await
        }
        crate::common::content::EncodedContent::Json(j) => {
            command
                .execute_operation(command.doc_id(), &j, command.options())
                .await
        }
        crate::common::content::EncodedContent::None => {
            command
                .execute_operation(command.doc_id(), &None::<()>, command.options())
                .await
        }
        crate::common::content::EncodedContent::ByteArray(b) => {
            command
                .execute_operation(command.doc_id(), &b, command.options())
                .await
        }
    };

    let end = std::time::Instant::now();

    let mut top_level_result = create_run_result(command.initiated(), end - start);

    match res {
        Ok(res) => {
            if command.return_result() {
                top_level_result.result = Some(res.into());
            } else {
                top_level_result.result = Some(create_success_sdk_result());
            }
        }
        Err(err) => {
            return handle_simple_error(err, &mut top_level_result, batcher).await;
        }
    }
    batcher.send(top_level_result).await;
    Ok(true)
}
