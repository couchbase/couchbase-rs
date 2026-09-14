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
use crate::memdx::durability_level::DurabilityLevel;
use crate::memdx::error::Result;
use crate::memdx::error::{Error, ServerError, ServerErrorKind};
use crate::memdx::ext_frame_code::ExtReqFrameCode;
use crate::memdx::extframe;
use crate::memdx::magic::Magic;
use crate::memdx::opcode::OpCode;
use crate::memdx::ops_core::OpsCore;
use crate::memdx::packet::{RequestPacket, ResponsePacket};
use crate::memdx::pendingop::StandardPendingOp;
use crate::memdx::request::{
    AddRequest, AppendRequest, DecrementRequest, DeleteRequest, GetAndLockRequest,
    GetAndTouchRequest, GetMetaRequest, GetReplicaRequest, GetRequest, IncrementRequest,
    LookupInRequest, MutateInRequest, PrependRequest, ReplaceRequest, SetRequest, TouchRequest,
    UnlockRequest,
};
use crate::memdx::response::{
    AddResponse, AppendResponse, DecrementResponse, DeleteResponse, GetAndLockResponse,
    GetAndTouchResponse, GetMetaResponse, GetReplicaResponse, GetResponse, IncrementResponse,
    LookupInResponse, MutateInResponse, PrependResponse, ReplaceResponse, SetResponse,
    TouchResponse, UnlockResponse,
};
use crate::memdx::status::Status;
use crate::memdx::subdoc::SubdocRequestInfo;
use bitflags::Flags;
use byteorder::{BigEndian, ByteOrder};
use std::time::Duration;

const MAX_KEY_SIZE: usize = 250;
const MAX_KEY_SIZE_USING_COLLECTIONS: usize = 246;

#[derive(Debug)]
pub struct OpsCrud {
    pub collections_enabled: bool,
    pub durability_enabled: bool,
    pub preserve_expiry_enabled: bool,
    pub ext_frames_enabled: bool,
}

impl OpsCrud {
    pub async fn set<D>(
        &self,
        dispatcher: &D,
        request: SetRequest<'_>,
    ) -> Result<StandardPendingOp<SetResponse>>
    where
        D: Dispatcher,
    {
        let mut ext_frame_buf = [0; 128];
        let (magic, used) = self.encode_req_ext_frames(
            request.durability_level,
            request.durability_level_timeout,
            request.preserve_expiry,
            request.on_behalf_of,
            &mut ext_frame_buf,
        )?;

        let framing_extras = if used > 0 {
            Some(&ext_frame_buf[..used])
        } else {
            None
        };

        let buf = &mut [0; 251];
        let key = self.encode_collection_and_key(request.collection_id, request.key, buf)?;

        let mut extra_buf = [0; 8];
        byteorder::BigEndian::write_u32(&mut extra_buf[0..4], request.flags);
        byteorder::BigEndian::write_u32(&mut extra_buf[4..8], request.expiry.unwrap_or_default());

        let packet = RequestPacket {
            magic,
            op_code: OpCode::Set,
            datatype: request.datatype,
            vbucket_id: Some(request.vbucket_id),
            cas: request.cas,
            extras: Some(&extra_buf),
            key: Some(key),
            value: Some(request.value),
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet, false, None).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn get<D>(
        &self,
        dispatcher: &D,
        request: GetRequest<'_>,
    ) -> Result<StandardPendingOp<GetResponse>>
    where
        D: Dispatcher,
    {
        let mut ext_frame_buf = [0; 128];
        let (magic, used) =
            self.encode_req_ext_frames(None, None, None, request.on_behalf_of, &mut ext_frame_buf)?;

        let buf = &mut [0; 251];
        let key = self.encode_collection_and_key(request.collection_id, request.key, buf)?;

        let framing_extras = if used > 0 {
            Some(&ext_frame_buf[..used])
        } else {
            None
        };

        let packet = RequestPacket {
            magic,
            op_code: OpCode::Get,
            datatype: 0,
            vbucket_id: Some(request.vbucket_id),
            cas: None,
            extras: None,
            key: Some(key),
            value: None,
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet, false, None).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn get_replica<D>(
        &self,
        dispatcher: &D,
        request: GetReplicaRequest<'_>,
    ) -> Result<StandardPendingOp<GetReplicaResponse>>
    where
        D: Dispatcher,
    {
        let mut ext_frame_buf = [0; 128];
        let (magic, used) =
            self.encode_req_ext_frames(None, None, None, request.on_behalf_of, &mut ext_frame_buf)?;

        let buf = &mut [0; 251];
        let key = self.encode_collection_and_key(request.collection_id, request.key, buf)?;

        let framing_extras = if used > 0 {
            Some(&ext_frame_buf[..used])
        } else {
            None
        };

        let packet = RequestPacket {
            magic,
            op_code: OpCode::GetReplica,
            datatype: 0,
            vbucket_id: Some(request.vbucket_id),
            cas: None,
            extras: None,
            key: Some(key),
            value: None,
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet, false, None).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn get_meta<D>(
        &self,
        dispatcher: &D,
        request: GetMetaRequest<'_>,
    ) -> Result<StandardPendingOp<GetMetaResponse>>
    where
        D: Dispatcher,
    {
        let mut ext_frame_buf = [0; 128];
        let (magic, used) =
            self.encode_req_ext_frames(None, None, None, request.on_behalf_of, &mut ext_frame_buf)?;

        let buf = &mut [0; 251];
        let key = self.encode_collection_and_key(request.collection_id, request.key, buf)?;

        let framing_extras = if used > 0 {
            Some(&ext_frame_buf[..used])
        } else {
            None
        };

        // This appears to be necessary to get the server to include the datatype in the response
        // extras.
        let extras = [2];

        let packet = RequestPacket {
            magic,
            op_code: OpCode::GetMeta,
            datatype: 0,
            vbucket_id: Some(request.vbucket_id),
            cas: None,
            extras: Some(&extras),
            key: Some(key),
            value: None,
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet, false, None).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn delete<D>(
        &self,
        dispatcher: &D,
        request: DeleteRequest<'_>,
    ) -> Result<StandardPendingOp<DeleteResponse>>
    where
        D: Dispatcher,
    {
        let mut ext_frame_buf = [0; 128];
        let (magic, used) = self.encode_req_ext_frames(
            request.durability_level,
            request.durability_level_timeout,
            None,
            request.on_behalf_of,
            &mut ext_frame_buf,
        )?;

        let framing_extras = if used > 0 {
            Some(&ext_frame_buf[..used])
        } else {
            None
        };

        let buf = &mut [0; 251];
        let key = self.encode_collection_and_key(request.collection_id, request.key, buf)?;

        let packet = RequestPacket {
            magic,
            op_code: OpCode::Delete,
            datatype: 0,
            vbucket_id: Some(request.vbucket_id),
            cas: request.cas,
            extras: None,
            key: Some(key),
            value: None,
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet, false, None).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn get_and_lock<D>(
        &self,
        dispatcher: &D,
        request: GetAndLockRequest<'_>,
    ) -> Result<StandardPendingOp<GetAndLockResponse>>
    where
        D: Dispatcher,
    {
        let mut ext_frame_buf = [0; 128];
        let (magic, used) =
            self.encode_req_ext_frames(None, None, None, request.on_behalf_of, &mut ext_frame_buf)?;

        let buf = &mut [0; 251];
        let key = self.encode_collection_and_key(request.collection_id, request.key, buf)?;

        let framing_extras = if used > 0 {
            Some(&ext_frame_buf[..used])
        } else {
            None
        };

        let mut extra_buf = [0; 4];
        byteorder::BigEndian::write_u32(&mut extra_buf[0..4], request.lock_time);

        let packet = RequestPacket {
            magic,
            op_code: OpCode::GetLocked,
            datatype: 0,
            vbucket_id: Some(request.vbucket_id),
            cas: None,
            extras: Some(&extra_buf),
            key: Some(key),
            value: None,
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet, false, None).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn get_and_touch<D>(
        &self,
        dispatcher: &D,
        request: GetAndTouchRequest<'_>,
    ) -> Result<StandardPendingOp<GetAndTouchResponse>>
    where
        D: Dispatcher,
    {
        let mut ext_frame_buf = [0; 128];
        let (magic, used) =
            self.encode_req_ext_frames(None, None, None, request.on_behalf_of, &mut ext_frame_buf)?;

        let buf = &mut [0; 251];
        let key = self.encode_collection_and_key(request.collection_id, request.key, buf)?;

        let framing_extras = if used > 0 {
            Some(&ext_frame_buf[..used])
        } else {
            None
        };

        let mut extra_buf = [0; 4];
        byteorder::BigEndian::write_u32(&mut extra_buf[0..4], request.expiry);

        let packet = RequestPacket {
            magic,
            op_code: OpCode::GAT,
            datatype: 0,
            vbucket_id: Some(request.vbucket_id),
            cas: None,
            extras: Some(&extra_buf),
            key: Some(key),
            value: None,
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet, false, None).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn unlock<D>(
        &self,
        dispatcher: &D,
        request: UnlockRequest<'_>,
    ) -> Result<StandardPendingOp<UnlockResponse>>
    where
        D: Dispatcher,
    {
        let mut ext_frame_buf = [0; 128];
        let (magic, used) =
            self.encode_req_ext_frames(None, None, None, request.on_behalf_of, &mut ext_frame_buf)?;

        let buf = &mut [0; 251];
        let key = self.encode_collection_and_key(request.collection_id, request.key, buf)?;

        let framing_extras = if used > 0 {
            Some(&ext_frame_buf[..used])
        } else {
            None
        };

        let packet = RequestPacket {
            magic,
            op_code: OpCode::UnlockKey,
            datatype: 0,
            vbucket_id: Some(request.vbucket_id),
            cas: Some(request.cas),
            extras: None,
            key: Some(key),
            value: None,
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet, false, None).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn touch<D>(
        &self,
        dispatcher: &D,
        request: TouchRequest<'_>,
    ) -> Result<StandardPendingOp<TouchResponse>>
    where
        D: Dispatcher,
    {
        let mut ext_frame_buf = [0; 128];
        let (magic, used) =
            self.encode_req_ext_frames(None, None, None, request.on_behalf_of, &mut ext_frame_buf)?;

        let buf = &mut [0; 251];
        let key = self.encode_collection_and_key(request.collection_id, request.key, buf)?;

        let framing_extras = if used > 0 {
            Some(&ext_frame_buf[..used])
        } else {
            None
        };

        let mut extra_buf = [0; 4];
        byteorder::BigEndian::write_u32(&mut extra_buf[0..4], request.expiry);

        let packet = RequestPacket {
            magic,
            op_code: OpCode::Touch,
            datatype: 0,
            vbucket_id: Some(request.vbucket_id),
            cas: None,
            extras: Some(&extra_buf),
            key: Some(key),
            value: None,
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet, false, None).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn add<D>(
        &self,
        dispatcher: &D,
        request: AddRequest<'_>,
    ) -> Result<StandardPendingOp<AddResponse>>
    where
        D: Dispatcher,
    {
        let mut ext_frame_buf = [0; 128];
        let (magic, used) = self.encode_req_ext_frames(
            request.durability_level,
            request.durability_level_timeout,
            None,
            request.on_behalf_of,
            &mut ext_frame_buf,
        )?;

        let framing_extras = if used > 0 {
            Some(&ext_frame_buf[..used])
        } else {
            None
        };

        let buf = &mut [0; 251];
        let key = self.encode_collection_and_key(request.collection_id, request.key, buf)?;

        let mut extra_buf = [0; 8];
        byteorder::BigEndian::write_u32(&mut extra_buf[0..4], request.flags);
        byteorder::BigEndian::write_u32(&mut extra_buf[4..8], request.expiry.unwrap_or_default());

        let packet = RequestPacket {
            magic,
            op_code: OpCode::Add,
            datatype: request.datatype,
            vbucket_id: Some(request.vbucket_id),
            cas: None,
            extras: Some(&extra_buf),
            key: Some(key),
            value: Some(request.value),
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet, false, None).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn replace<D>(
        &self,
        dispatcher: &D,
        request: ReplaceRequest<'_>,
    ) -> Result<StandardPendingOp<ReplaceResponse>>
    where
        D: Dispatcher,
    {
        if request.expiry.is_some() && request.preserve_expiry.is_some() {
            return Err(Error::new_invalid_argument_error(
                "cannot specify expiry and preserve expiry together",
                None,
            ));
        }

        let mut ext_frame_buf = [0; 128];
        let (magic, used) = self.encode_req_ext_frames(
            request.durability_level,
            request.durability_level_timeout,
            request.preserve_expiry,
            request.on_behalf_of,
            &mut ext_frame_buf,
        )?;

        let framing_extras = if used > 0 {
            Some(&ext_frame_buf[..used])
        } else {
            None
        };

        let buf = &mut [0; 251];
        let key = self.encode_collection_and_key(request.collection_id, request.key, buf)?;

        let mut extra_buf = [0; 8];
        byteorder::BigEndian::write_u32(&mut extra_buf[0..4], request.flags);
        byteorder::BigEndian::write_u32(&mut extra_buf[4..8], request.expiry.unwrap_or_default());

        let packet = RequestPacket {
            magic,
            op_code: OpCode::Replace,
            datatype: request.datatype,
            vbucket_id: Some(request.vbucket_id),
            cas: request.cas,
            extras: Some(&extra_buf),
            key: Some(key),
            value: Some(request.value),
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet, false, None).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn append<D>(
        &self,
        dispatcher: &D,
        request: AppendRequest<'_>,
    ) -> Result<StandardPendingOp<AppendResponse>>
    where
        D: Dispatcher,
    {
        let mut ext_frame_buf = [0; 128];
        let (magic, used) = self.encode_req_ext_frames(
            request.durability_level,
            request.durability_level_timeout,
            None,
            request.on_behalf_of,
            &mut ext_frame_buf,
        )?;

        let framing_extras = if used > 0 {
            Some(&ext_frame_buf[..used])
        } else {
            None
        };

        let buf = &mut [0; 251];
        let key = self.encode_collection_and_key(request.collection_id, request.key, buf)?;

        let packet = RequestPacket {
            magic,
            op_code: OpCode::Append,
            datatype: request.datatype,
            vbucket_id: Some(request.vbucket_id),
            cas: request.cas,
            extras: None,
            key: Some(key),
            value: Some(request.value),
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher
            .dispatch(
                packet,
                false,
                Some(ResponseContext {
                    cas: request.cas,
                    subdoc_info: None,
                    scope_name: None,
                    collection_name: None,
                }),
            )
            .await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn prepend<D>(
        &self,
        dispatcher: &D,
        request: PrependRequest<'_>,
    ) -> Result<StandardPendingOp<PrependResponse>>
    where
        D: Dispatcher,
    {
        let mut ext_frame_buf = [0; 128];
        let (magic, used) = self.encode_req_ext_frames(
            request.durability_level,
            request.durability_level_timeout,
            None,
            request.on_behalf_of,
            &mut ext_frame_buf,
        )?;

        let framing_extras = if used > 0 {
            Some(&ext_frame_buf[..used])
        } else {
            None
        };

        let buf = &mut [0; 251];
        let key = self.encode_collection_and_key(request.collection_id, request.key, buf)?;

        let packet = RequestPacket {
            magic,
            op_code: OpCode::Prepend,
            datatype: request.datatype,
            vbucket_id: Some(request.vbucket_id),
            cas: request.cas,
            extras: None,
            key: Some(key),
            value: Some(request.value),
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher
            .dispatch(
                packet,
                false,
                Some(ResponseContext {
                    cas: request.cas,
                    subdoc_info: None,
                    scope_name: None,
                    collection_name: None,
                }),
            )
            .await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    fn encode_counter_values(
        delta: u64,
        initial: Option<u64>,
        expiry: Option<u32>,
        buf: &mut [u8; 20],
    ) {
        byteorder::BigEndian::write_u64(&mut buf[0..8], delta);
        if let Some(initial) = initial {
            byteorder::BigEndian::write_u64(&mut buf[8..16], initial);
            byteorder::BigEndian::write_u32(&mut buf[16..20], expiry.unwrap_or_default());
        } else {
            byteorder::BigEndian::write_u64(&mut buf[8..16], 0x0000000000000000);
            byteorder::BigEndian::write_u32(&mut buf[16..20], 0xFFFFFFFF);
        }
    }

    pub async fn increment<D>(
        &self,
        dispatcher: &D,
        request: IncrementRequest<'_>,
    ) -> Result<StandardPendingOp<IncrementResponse>>
    where
        D: Dispatcher,
    {
        let mut ext_frame_buf = [0; 128];
        let (magic, used) = self.encode_req_ext_frames(
            request.durability_level,
            request.durability_level_timeout,
            None,
            request.on_behalf_of,
            &mut ext_frame_buf,
        )?;

        let framing_extras = if used > 0 {
            Some(&ext_frame_buf[..used])
        } else {
            None
        };

        let buf = &mut [0; 251];
        let key = self.encode_collection_and_key(request.collection_id, request.key, buf)?;

        let mut extra_buf = [0; 20];
        Self::encode_counter_values(
            request.delta,
            request.initial,
            request.expiry,
            &mut extra_buf,
        );

        let packet = RequestPacket {
            magic,
            op_code: OpCode::Increment,
            datatype: 0,
            vbucket_id: Some(request.vbucket_id),
            cas: None,
            extras: Some(&extra_buf),
            key: Some(key),
            value: None,
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet, false, None).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn decrement<D>(
        &self,
        dispatcher: &D,
        request: DecrementRequest<'_>,
    ) -> Result<StandardPendingOp<DecrementResponse>>
    where
        D: Dispatcher,
    {
        let mut ext_frame_buf = [0; 128];
        let (magic, used) = self.encode_req_ext_frames(
            request.durability_level,
            request.durability_level_timeout,
            None,
            request.on_behalf_of,
            &mut ext_frame_buf,
        )?;

        let framing_extras = if used > 0 {
            Some(&ext_frame_buf[..used])
        } else {
            None
        };

        let buf = &mut [0; 251];
        let key = self.encode_collection_and_key(request.collection_id, request.key, buf)?;

        let mut extra_buf = [0; 20];
        Self::encode_counter_values(
            request.delta,
            request.initial,
            request.expiry,
            &mut extra_buf,
        );

        let packet = RequestPacket {
            magic,
            op_code: OpCode::Decrement,
            datatype: 0,
            vbucket_id: Some(request.vbucket_id),
            cas: None,
            extras: Some(&extra_buf),
            key: Some(key),
            value: None,
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet, false, None).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn lookup_in<D>(
        &self,
        dispatcher: &D,
        request: LookupInRequest<'_>,
    ) -> Result<StandardPendingOp<LookupInResponse>>
    where
        D: Dispatcher,
    {
        let mut ext_frame_buf = [0; 128];
        let (magic, used) =
            self.encode_req_ext_frames(None, None, None, request.on_behalf_of, &mut ext_frame_buf)?;

        let framing_extras = if used > 0 {
            Some(&ext_frame_buf[..used])
        } else {
            None
        };

        let buf = &mut [0; 251];
        let key = self.encode_collection_and_key(request.collection_id, request.key, buf)?;

        let len_ops = request.ops.len();
        let mut path_bytes_list: Vec<Vec<u8>> = vec![vec![]; len_ops];
        let mut path_bytes_total = 0;

        for (i, op) in request.ops.iter().enumerate() {
            let path_bytes = op.path.to_vec();
            path_bytes_total += path_bytes.len();
            path_bytes_list[i] = path_bytes;
        }

        let mut value_buf: Vec<u8> = vec![0; len_ops * 4 + path_bytes_total];
        let mut value_iter = 0;
        for (i, op) in request.ops.iter().enumerate() {
            let path_bytes = &path_bytes_list[i];
            let path_bytes_len = path_bytes.len();

            value_buf[value_iter] = Into::<OpCode>::into(op.op).into();
            value_buf[value_iter + 1] = op.flags.bits();
            BigEndian::write_u16(
                &mut value_buf[value_iter + 2..value_iter + 4],
                path_bytes_len as u16,
            );
            value_buf[value_iter + 4..value_iter + 4 + path_bytes_len].copy_from_slice(path_bytes);
            value_iter += 4 + path_bytes_len;
        }

        let mut extra_buf = Vec::with_capacity(8);

        if !request.flags.is_empty() {
            extra_buf.push(request.flags.bits());
        }

        let packet = RequestPacket {
            magic,
            op_code: OpCode::SubDocMultiLookup,
            datatype: 0,
            vbucket_id: Some(request.vbucket_id),
            cas: None,
            extras: Some(&extra_buf),
            key: Some(key),
            value: Some(&value_buf),
            framing_extras,
            opaque: None,
        };

        let response_context = ResponseContext {
            cas: packet.cas,
            subdoc_info: Some(SubdocRequestInfo {
                flags: request.flags,
                op_count: request.ops.len() as u8,
            }),
            scope_name: None,
            collection_name: None,
        };

        let pending_op = dispatcher
            .dispatch(packet, false, Some(response_context))
            .await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn mutate_in<D>(
        &self,
        dispatcher: &D,
        request: MutateInRequest<'_>,
    ) -> Result<StandardPendingOp<MutateInResponse>>
    where
        D: Dispatcher,
    {
        if request.expiry.is_some() && request.preserve_expiry.is_some() {
            return Err(Error::new_invalid_argument_error(
                "cannot specify expiry and preserve expiry together",
                None,
            ));
        }

        let mut ext_frame_buf = [0; 128];
        let (magic, used) = self.encode_req_ext_frames(
            request.durability_level,
            request.durability_level_timeout,
            request.preserve_expiry,
            request.on_behalf_of,
            &mut ext_frame_buf,
        )?;

        let framing_extras = if used > 0 {
            Some(&ext_frame_buf[..used])
        } else {
            None
        };

        let buf = &mut [0; 251];
        let key = self.encode_collection_and_key(request.collection_id, request.key, buf)?;

        let len_ops = request.ops.len();
        let mut path_bytes_list: Vec<Vec<u8>> = vec![vec![]; len_ops];
        let mut path_bytes_total = 0;
        let mut value_bytes_total = 0;

        for (i, op) in request.ops.iter().enumerate() {
            let path_bytes = op.path.to_vec();
            path_bytes_total += path_bytes.len();
            path_bytes_list[i] = path_bytes;
            value_bytes_total += op.value.len();
        }

        let mut value_buf: Vec<u8> = vec![0; len_ops * 8 + path_bytes_total + value_bytes_total];
        let mut value_iter = 0;

        for (i, op) in request.ops.iter().enumerate() {
            let path_bytes = &path_bytes_list[i];
            let path_bytes_len = path_bytes.len();
            let value_bytes_len = op.value.len();

            value_buf[value_iter] = Into::<OpCode>::into(op.op).into();
            value_buf[value_iter + 1] = op.flags.bits();
            BigEndian::write_u16(
                &mut value_buf[value_iter + 2..value_iter + 4],
                path_bytes_len as u16,
            );
            BigEndian::write_u32(
                &mut value_buf[value_iter + 4..value_iter + 8],
                value_bytes_len as u32,
            );
            value_buf[value_iter + 8..value_iter + 8 + path_bytes_len].copy_from_slice(path_bytes);
            value_buf[value_iter + 8 + path_bytes_len
                ..value_iter + 8 + path_bytes_len + value_bytes_len]
                .copy_from_slice(op.value);
            value_iter += 8 + path_bytes_len + value_bytes_len;
        }

        let mut extra_buf = [0; 5];
        byteorder::BigEndian::write_u32(&mut extra_buf[0..4], request.expiry.unwrap_or_default());
        extra_buf[4] = request.flags.bits();

        let extra_buf = if request.flags.is_empty() {
            &extra_buf[..4]
        } else {
            extra_buf[4] = request.flags.bits();
            &extra_buf
        };

        let packet = RequestPacket {
            magic,
            op_code: OpCode::SubDocMultiMutation,
            datatype: 0,
            vbucket_id: Some(request.vbucket_id),
            cas: request.cas,
            extras: Some(extra_buf),
            key: Some(key),
            value: Some(&value_buf),
            framing_extras,
            opaque: None,
        };

        let response_context = ResponseContext {
            cas: request.cas,
            subdoc_info: Some(SubdocRequestInfo {
                flags: request.flags,
                op_count: request.ops.len() as u8,
            }),
            scope_name: None,
            collection_name: None,
        };

        let pending_op = dispatcher
            .dispatch(packet, false, Some(response_context))
            .await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    fn encode_collection_and_key<'a>(
        &self,
        collection_id: u32,
        key: &'a [u8],
        buf: &'a mut [u8],
    ) -> Result<&'a [u8]> {
        let key_len = key.len();
        if !self.collections_enabled {
            if key_len > MAX_KEY_SIZE {
                return Err(Error::new_invalid_argument_error(
                    format!("key too long, {MAX_KEY_SIZE} bytes max but is {key_len} bytes"),
                    "key".to_string(),
                ));
            }
            if collection_id != 0 {
                return Err(Error::new_invalid_argument_error(
                    "collections not enabled",
                    "collection_id".to_string(),
                ));
            }

            return Ok(key);
        }

        let max_key_size = if collection_id == 0 {
            MAX_KEY_SIZE
        } else {
            MAX_KEY_SIZE_USING_COLLECTIONS
        };

        let encoded_size = extframe::make_uleb128_32(collection_id, buf);
        if key_len > max_key_size {
            return Err(Error::new_invalid_argument_error(
                format!("key too long, {max_key_size} bytes max but is {key_len} bytes including collection prefix"),
                "key".to_string(),
            ));
        }

        for (i, k) in key.iter().enumerate() {
            buf[i + encoded_size] = *k;
        }
        Ok(&buf[0..key.len() + encoded_size])
    }

    fn encode_req_ext_frames(
        &self,
        durability_level: Option<DurabilityLevel>,
        durability_timeout: Option<Duration>,
        preserve_expiry: Option<bool>,
        on_behalf_of: Option<&str>,
        buf: &mut [u8],
    ) -> Result<(Magic, usize)> {
        let mut offset = 0;

        if let Some(obo) = on_behalf_of {
            extframe::append_ext_frame(
                ExtReqFrameCode::OnBehalfOf,
                obo.as_bytes(),
                buf,
                &mut offset,
            )?;
        }

        if let Some(dura) = durability_level {
            if !self.durability_enabled {
                return Err(Error::new_invalid_argument_error(
                    "cannot use synchronous durability when its not enabled",
                    "durability_level".to_string(),
                ));
            }

            let dura_buf = extframe::encode_durability_ext_frame(dura, durability_timeout)?;

            extframe::append_ext_frame(ExtReqFrameCode::Durability, &dura_buf, buf, &mut offset)?;
        } else if durability_timeout.is_some() {
            return Err(Error::new_invalid_argument_error(
                "cannot encode durability timeout without durability level",
                "durability_timeout".to_string(),
            ));
        }

        if preserve_expiry.is_some() {
            if !self.preserve_expiry_enabled {
                return Err(Error::new_invalid_argument_error(
                    "cannot use preserve expiry when its not enabled",
                    "preserve_expiry".to_string(),
                ));
            }

            extframe::append_ext_frame(ExtReqFrameCode::PreserveTTL, &[], buf, &mut offset)?;
        }

        let magic = if offset > 0 {
            if !self.ext_frames_enabled {
                return Err(Error::new_invalid_argument_error(
                    "cannot use framing extras when its not enabled",
                    "ext_frames_enabled".to_string(),
                ));
            }

            Magic::ReqExt
        } else {
            Magic::Req
        };

        Ok((magic, offset))
    }

    pub(crate) fn decode_common_mutation_status(
        resp: &ResponsePacket,
    ) -> std::result::Result<(), Error> {
        let kind = match resp.status {
            Status::DurabilityInvalidLevel => ServerErrorKind::DurabilityInvalid,
            Status::SyncWriteAmbiguous => ServerErrorKind::SyncWriteAmbiguous,
            Status::SyncWriteInProgress => ServerErrorKind::SyncWriteInProgress,
            Status::SyncWriteRecommitInProgress => ServerErrorKind::SyncWriteRecommitInProgress,
            Status::DurabilityImpossible => ServerErrorKind::DurabilityImpossible,
            _ => {
                return Self::decode_common_status(resp);
            }
        };

        let mut err = ServerError::new(kind, resp.op_code, resp.status, resp.opaque);
        if let Some(value) = &resp.value {
            err = err.with_context(value.to_vec());
        }

        Err(err.into())
    }

    pub(crate) fn decode_common_status(resp: &ResponsePacket) -> std::result::Result<(), Error> {
        let kind = match resp.status {
            Status::CollectionUnknown => ServerErrorKind::UnknownCollectionID,
            Status::AccessError => ServerErrorKind::Access,
            Status::NoBucket => ServerErrorKind::NoBucket,
            Status::RateLimitedMaxCommands => ServerErrorKind::RateLimitedMaxCommands,
            Status::RateLimitedMaxConnections => ServerErrorKind::RateLimitedMaxConnections,
            Status::RateLimitedNetworkEgress => ServerErrorKind::RateLimitedNetworkEgress,
            Status::RateLimitedNetworkIngress => ServerErrorKind::RateLimitedNetworkIngress,
            Status::RateLimitedScopeSizeLimitExceeded => {
                ServerErrorKind::RateLimitedScopeSizeLimitExceeded
            }
            _ => {
                return Ok(());
            }
        };

        let mut err = ServerError::new(kind, resp.op_code, resp.status, resp.opaque);
        if let Some(value) = &resp.value {
            err = err.with_context(value.to_vec());
        }

        Err(err.into())
    }

    pub(crate) fn decode_common_mutation_error(resp: &ResponsePacket) -> Error {
        if let Err(e) = Self::decode_common_mutation_status(resp) {
            return e;
        };

        OpsCore::decode_error(resp)
    }

    pub(crate) fn decode_common_error(resp: &ResponsePacket) -> Error {
        if let Err(e) = Self::decode_common_status(resp) {
            return e;
        };

        OpsCore::decode_error(resp)
    }
}
