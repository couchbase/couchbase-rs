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

use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::error::Result;
use crate::memdx::error::{Error, ServerError, ServerErrorKind};
use crate::memdx::magic::Magic;
use crate::memdx::op_auth_saslauto::OpSASLAutoEncoder;
use crate::memdx::op_auth_saslbyname::OpSASLAuthByNameEncoder;
use crate::memdx::op_auth_saslplain::OpSASLPlainEncoder;
use crate::memdx::op_auth_saslscram::OpSASLScramEncoder;
use crate::memdx::op_bootstrap::OpBootstrapEncoder;
use crate::memdx::opcode::OpCode;
use crate::memdx::packet::{RequestPacket, ResponsePacket};
use crate::memdx::pendingop::StandardPendingOp;
use crate::memdx::request::{
    GetClusterConfigRequest, GetErrorMapRequest, HelloRequest, SASLAuthRequest,
    SASLListMechsRequest, SASLStepRequest, SelectBucketRequest,
};
use crate::memdx::response::{
    GetClusterConfigResponse, GetErrorMapResponse, HelloResponse, SASLAuthResponse,
    SASLListMechsResponse, SASLStepResponse, SelectBucketResponse,
};
use crate::memdx::status::Status;
use byteorder::ByteOrder;

pub struct OpsCore {}

impl OpsCore {
    pub(crate) fn decode_error_context(
        resp: &ResponsePacket,
        kind: ServerErrorKind,
    ) -> ServerError {
        let mut base_cause = ServerError::new(kind, resp.op_code, resp.status, resp.opaque);

        if let Some(value) = &resp.value {
            if resp.status == Status::NotMyVbucket {
                base_cause = base_cause.with_config(value.to_vec());
            } else {
                base_cause = base_cause.with_context(value.to_vec());
            }
        }

        base_cause
    }

    pub(crate) fn decode_error(resp: &ResponsePacket) -> Error {
        let status = resp.status;
        let base_error_kind = if status == Status::NotMyVbucket {
            ServerErrorKind::NotMyVbucket
        } else if status == Status::TmpFail {
            ServerErrorKind::TmpFail
        } else if status == Status::NoBucket {
            ServerErrorKind::NoBucket
        } else if status == Status::AuthStale {
            ServerErrorKind::AuthStale
        } else if status == Status::InvalidArgs {
            return Error::new_invalid_argument_error(
                "the server rejected the request because one or more arguments were invalid",
                None,
            )
            .with(Self::decode_error_context(
                resp,
                ServerErrorKind::InvalidArgs,
            ));
        } else {
            ServerErrorKind::UnknownStatus { status }
        };

        Self::decode_error_context(resp, base_error_kind).into()
    }
}

impl OpBootstrapEncoder for OpsCore {
    async fn hello<D>(
        &self,
        dispatcher: &D,
        request: HelloRequest,
    ) -> Result<StandardPendingOp<HelloResponse>>
    where
        D: Dispatcher,
    {
        let mut features: Vec<u8> = Vec::new();
        for feature in request.requested_features {
            let feature: u16 = feature.into();
            let bytes = feature.to_be_bytes();
            features.extend_from_slice(&bytes);
        }

        let key = if request.client_name.is_empty() {
            None
        } else {
            Some(request.client_name.as_slice())
        };

        let op = dispatcher
            .dispatch(
                RequestPacket {
                    magic: Magic::Req,
                    op_code: OpCode::Hello,
                    datatype: 0,
                    vbucket_id: None,
                    cas: None,
                    extras: None,
                    key,
                    value: Some(&features),
                    framing_extras: None,
                    opaque: None,
                },
                false,
                None,
            )
            .await?;

        Ok(StandardPendingOp::new(op))
    }

    async fn get_error_map<D>(
        &self,
        dispatcher: &D,
        request: GetErrorMapRequest,
    ) -> Result<StandardPendingOp<GetErrorMapResponse>>
    where
        D: Dispatcher,
    {
        let version = request.version.to_be_bytes();

        let op = dispatcher
            .dispatch(
                RequestPacket {
                    magic: Magic::Req,
                    op_code: OpCode::GetErrorMap,
                    datatype: 0,
                    vbucket_id: None,
                    cas: None,
                    extras: None,
                    key: None,
                    value: Some(&version),
                    framing_extras: None,
                    opaque: None,
                },
                false,
                None,
            )
            .await?;

        Ok(StandardPendingOp::new(op))
    }

    async fn select_bucket<D>(
        &self,
        dispatcher: &D,
        request: SelectBucketRequest,
    ) -> Result<StandardPendingOp<SelectBucketResponse>>
    where
        D: Dispatcher,
    {
        let key = request.bucket_name.into_bytes();

        let op = dispatcher
            .dispatch(
                RequestPacket {
                    magic: Magic::Req,
                    op_code: OpCode::SelectBucket,
                    datatype: 0,
                    vbucket_id: None,
                    cas: None,
                    extras: None,
                    key: Some(&key),
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

    async fn get_cluster_config<D>(
        &self,
        dispatcher: &D,
        request: GetClusterConfigRequest,
    ) -> Result<StandardPendingOp<GetClusterConfigResponse>>
    where
        D: Dispatcher,
    {
        let mut extra_buf = [0; 16];
        let extras = if let Some(known_version) = request.known_version {
            byteorder::BigEndian::write_u64(&mut extra_buf[0..8], known_version.rev_epoch as u64);
            byteorder::BigEndian::write_u64(&mut extra_buf[8..16], known_version.rev_id as u64);

            Some(&extra_buf[..])
        } else {
            None
        };

        let op = dispatcher
            .dispatch(
                RequestPacket {
                    magic: Magic::Req,
                    op_code: OpCode::GetClusterConfig,
                    datatype: 0,
                    vbucket_id: None,
                    cas: None,
                    extras,
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

impl OpSASLPlainEncoder for OpsCore {
    async fn sasl_auth<D>(
        &self,
        dispatcher: &D,
        request: SASLAuthRequest,
    ) -> Result<StandardPendingOp<SASLAuthResponse>>
    where
        D: Dispatcher,
    {
        let mut value = Vec::new();
        value.extend_from_slice(request.payload.as_slice());
        let key: Vec<u8> = request.auth_mechanism.into();

        let op = dispatcher
            .dispatch(
                RequestPacket {
                    magic: Magic::Req,
                    op_code: OpCode::SASLAuth,
                    datatype: 0,
                    vbucket_id: None,
                    cas: None,
                    extras: None,
                    key: Some(&key),
                    value: Some(&value),
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

impl OpSASLAuthByNameEncoder for OpsCore {}

impl OpSASLAutoEncoder for OpsCore {
    async fn sasl_list_mechs<D>(
        &self,
        dispatcher: &D,
        _request: SASLListMechsRequest,
    ) -> Result<StandardPendingOp<SASLListMechsResponse>>
    where
        D: Dispatcher,
    {
        let op = dispatcher
            .dispatch(
                RequestPacket {
                    magic: Magic::Req,
                    op_code: OpCode::SASLListMechs,
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

impl OpSASLScramEncoder for OpsCore {
    async fn sasl_step<D>(
        &self,
        dispatcher: &D,
        request: SASLStepRequest,
    ) -> Result<StandardPendingOp<SASLStepResponse>>
    where
        D: Dispatcher,
    {
        let mut value = Vec::new();
        value.extend_from_slice(request.payload.as_slice());
        let key: Vec<u8> = request.auth_mechanism.into();

        let op = dispatcher
            .dispatch(
                RequestPacket {
                    magic: Magic::Req,
                    op_code: OpCode::SASLStep,
                    datatype: 0,
                    vbucket_id: None,
                    cas: None,
                    extras: None,
                    key: Some(&key),
                    value: Some(&value),
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
