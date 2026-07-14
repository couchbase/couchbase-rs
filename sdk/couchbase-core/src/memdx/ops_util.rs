/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use crate::memdx::client::ResponseContext;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::error::Result;
use crate::memdx::magic::Magic;
use crate::memdx::opcode::OpCode;
use crate::memdx::packet::RequestPacket;
use crate::memdx::pendingop::StandardPendingOp;
use crate::memdx::request::{GetCollectionIdRequest, PingRequest};
use crate::memdx::response::{GetCollectionIdResponse, PingResponse};

pub struct OpsUtil {}

impl OpsUtil {
    pub async fn get_collection_id<D>(
        &self,
        dispatcher: &D,
        request: GetCollectionIdRequest<'_>,
    ) -> Result<StandardPendingOp<GetCollectionIdResponse>>
    where
        D: Dispatcher,
    {
        let full_name = format!("{}.{}", request.scope_name, request.collection_name);

        let op = dispatcher
            .dispatch(
                RequestPacket {
                    magic: Magic::Req,
                    op_code: OpCode::GetCollectionId,
                    datatype: 0,
                    vbucket_id: None,
                    cas: None,
                    extras: None,
                    key: None,
                    value: Some(full_name.as_bytes()),
                    framing_extras: None,
                    opaque: None,
                },
                false,
                Some(ResponseContext {
                    cas: None,
                    subdoc_info: None,
                    scope_name: Some(request.scope_name.to_string()),
                    collection_name: Some(request.collection_name.to_string()),
                }),
            )
            .await?;

        Ok(StandardPendingOp::new(op))
    }

    pub async fn ping<D>(
        &self,
        dispatcher: &D,
        _request: PingRequest<'_>,
    ) -> Result<StandardPendingOp<PingResponse>>
    where
        D: Dispatcher,
    {
        let op = dispatcher
            .dispatch(
                RequestPacket {
                    magic: Magic::Req,
                    op_code: OpCode::Noop,
                    datatype: 0,
                    vbucket_id: None,
                    cas: None,
                    extras: None,
                    key: None,
                    value: None,
                    framing_extras: None,
                    opaque: None,
                },
                false,
                None,
            )
            .await?;

        Ok(StandardPendingOp::new(op))
    }
}
