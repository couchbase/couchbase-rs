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

use std::io::{Cursor, Read};
use std::time::Duration;

use bytes::Bytes;

use crate::memdx::auth_mechanism::AuthMechanism;
use crate::memdx::client_response::ClientResponse;
use crate::memdx::error::{
    Error, ResourceError, ServerError, ServerErrorKind, SubdocError, SubdocErrorKind,
};
use crate::memdx::extframe::decode_res_ext_frames;
use crate::memdx::hello_feature::HelloFeature;
use crate::memdx::ops_core::OpsCore;
use crate::memdx::ops_crud::OpsCrud;
use crate::memdx::status::Status;
use crate::memdx::subdoc::{SubDocResult, SubdocDocFlag};
use byteorder::{BigEndian, ReadBytesExt};
use bytes::Buf;

pub trait TryFromClientResponse: Sized {
    fn try_from(resp: ClientResponse) -> Result<Self, Error>;
}

pub trait TraceAttributes {
    fn server_duration(&self) -> Option<Duration>;
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct HelloResponse {
    pub enabled_features: Vec<HelloFeature>,
}

impl TryFromClientResponse for HelloResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;
        if status != Status::Success {
            return Err(OpsCore::decode_error(&packet));
        }

        let mut features: Vec<HelloFeature> = Vec::new();
        if let Some(value) = &packet.value {
            if value.len() % 2 != 0 {
                return Err(Error::new_protocol_error("invalid hello features length"));
            }

            let mut cursor = Cursor::new(value);
            while let Ok(code) = cursor.read_u16::<BigEndian>() {
                features.push(HelloFeature::from(code));
            }
        }
        let response = HelloResponse {
            enabled_features: features,
        };

        Ok(response)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GetErrorMapResponse {
    pub error_map: Bytes,
}

impl TryFromClientResponse for GetErrorMapResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;
        if status != Status::Success {
            return Err(OpsCore::decode_error(&packet));
        }

        let value = packet.value.unwrap_or_default();
        let response = GetErrorMapResponse { error_map: value };

        Ok(response)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SelectBucketResponse {}

impl TryFromClientResponse for SelectBucketResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;
        if status != Status::Success {
            if status == Status::AccessError || status == Status::KeyNotFound {
                return Err(ServerError::new(
                    ServerErrorKind::UnknownBucketName,
                    packet.op_code,
                    status,
                    packet.opaque,
                )
                .into());
            }
            return Err(OpsCore::decode_error(&packet));
        }

        Ok(SelectBucketResponse {})
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLAuthResponse {
    pub needs_more_steps: bool,
    pub payload: Bytes,
}

impl TryFromClientResponse for SASLAuthResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;
        if status == Status::AuthContinue {
            return Ok(SASLAuthResponse {
                needs_more_steps: true,
                payload: packet.value.unwrap_or_default(),
            });
        }

        if status != Status::Success {
            return Err(OpsCore::decode_error(&packet));
        }

        Ok(SASLAuthResponse {
            needs_more_steps: false,
            payload: packet.value.unwrap_or_default(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLStepResponse {
    pub needs_more_steps: bool,
    pub payload: Bytes,
}

impl TryFromClientResponse for SASLStepResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;
        if status != Status::Success {
            return Err(OpsCore::decode_error(&packet));
        }

        Ok(SASLStepResponse {
            needs_more_steps: false,
            payload: packet.value.unwrap_or_default(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLListMechsResponse {
    pub available_mechs: Vec<AuthMechanism>,
}

impl TryFromClientResponse for SASLListMechsResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;
        if status != Status::Success {
            if status == Status::KeyNotFound {
                // KeyNotFound appears here when the bucket was initialized by ns_server, but
                // ns_server has not posted a configuration for the bucket to kv_engine yet. We
                // transform this into a ErrTmpFail as we make the assumption that the
                // SelectBucket will have failed if this was anything but a transient issue.
                return Err(ServerError::new(
                    ServerErrorKind::ConfigNotSet,
                    packet.op_code,
                    status,
                    packet.opaque,
                )
                .into());
            }
            return Err(OpsCore::decode_error(&packet));
        }

        let value = packet.value.unwrap_or_default();
        let mechs_list_string = match String::from_utf8(value.to_vec()) {
            Ok(v) => v,
            Err(e) => {
                return Err(Error::new_protocol_error(
                    "failed to parse authentication mechanism list",
                )
                .with(e));
            }
        };
        let mechs_list_split = mechs_list_string.split(' ');
        let mut mechs_list = Vec::new();
        for item in mechs_list_split {
            mechs_list.push(AuthMechanism::try_from(item)?);
        }

        Ok(SASLListMechsResponse {
            available_mechs: mechs_list,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GetClusterConfigResponse {
    pub config: Bytes,
}

impl TryFromClientResponse for GetClusterConfigResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;
        if status != Status::Success {
            return Err(OpsCore::decode_error(&packet));
        }

        Ok(GetClusterConfigResponse {
            config: packet.value.unwrap_or_default(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct BootstrapResult {
    pub hello: Option<HelloResponse>,
    pub error_map: Option<GetErrorMapResponse>,
    pub cluster_config: Option<GetClusterConfigResponse>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MutationToken {
    pub vbuuid: u64,
    pub seqno: u64,
}

impl TryFrom<&[u8]> for MutationToken {
    type Error = Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != 16 {
            return Err(Error::new_protocol_error("bad extras length"));
        }

        let (vbuuid_bytes, seqno_bytes) = value.split_at(size_of::<u64>());
        let vbuuid = u64::from_be_bytes(vbuuid_bytes.try_into().unwrap());
        let seqno = u64::from_be_bytes(seqno_bytes.try_into().unwrap());

        Ok(MutationToken { vbuuid, seqno })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SetResponse {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
    pub server_duration: Option<Duration>,
}

impl TryFromClientResponse for SetResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        let kind = if status == Status::TooBig {
            Some(ServerErrorKind::TooBig)
        } else if status == Status::Locked {
            Some(ServerErrorKind::Locked)
        } else if status == Status::KeyExists {
            Some(ServerErrorKind::CasMismatch)
        } else if status == Status::Success {
            None
        } else {
            return Err(OpsCrud::decode_common_mutation_error(&packet));
        };

        if let Some(kind) = kind {
            return Err(ServerError::new(kind, packet.op_code, status, packet.opaque).into());
        }

        let mutation_token = if let Some(extras) = &packet.extras {
            Some(MutationToken::try_from(extras.as_ref())?)
        } else {
            None
        };

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(SetResponse {
            cas: packet.cas.unwrap_or_default(),
            mutation_token,
            server_duration,
        })
    }
}

impl TraceAttributes for SetResponse {
    fn server_duration(&self) -> Option<Duration> {
        self.server_duration
    }
}

fn parse_flags(extras: &Option<Bytes>) -> Result<u32, Error> {
    if let Some(extras) = &extras {
        if extras.len() != 4 {
            return Err(Error::new_protocol_error("bad extras length reading flags"));
        }

        Ok(u32::from_be_bytes(extras[..].try_into().unwrap()))
    } else {
        Err(Error::new_protocol_error("no extras in response"))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GetResponse {
    pub cas: u64,
    pub flags: u32,
    pub value: Bytes,
    pub datatype: u8,
    pub server_duration: Option<Duration>,
}

impl TryFromClientResponse for GetResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        if status == Status::KeyNotFound {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                packet.op_code,
                packet.status,
                packet.opaque,
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCrud::decode_common_error(&packet));
        }

        let flags = parse_flags(&packet.extras)?;

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        let value = packet.value.unwrap_or_default();

        Ok(GetResponse {
            cas: packet.cas.unwrap_or_default(),
            flags,
            value,
            datatype: packet.datatype,
            server_duration,
        })
    }
}

impl TraceAttributes for GetResponse {
    fn server_duration(&self) -> Option<Duration> {
        self.server_duration
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GetReplicaResponse {
    pub cas: u64,
    pub flags: u32,
    pub value: Bytes,
    pub datatype: u8,
    pub server_duration: Option<Duration>,
}

impl TryFromClientResponse for GetReplicaResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        if status == Status::KeyNotFound {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                packet.op_code,
                packet.status,
                packet.opaque,
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCrud::decode_common_error(&packet));
        }

        let flags = parse_flags(&packet.extras)?;

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        let value = packet.value.unwrap_or_default();

        Ok(GetReplicaResponse {
            cas: packet.cas.unwrap_or_default(),
            flags,
            value,
            datatype: packet.datatype,
            server_duration,
        })
    }
}

impl TraceAttributes for GetReplicaResponse {
    fn server_duration(&self) -> Option<Duration> {
        self.server_duration
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GetMetaResponse {
    pub cas: u64,
    pub flags: u32,
    pub value: Bytes,
    pub datatype: u8,
    pub server_duration: Option<Duration>,
    pub expiry: u32,
    pub seq_no: u64,
    pub deleted: bool,
}

impl TryFromClientResponse for GetMetaResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        if status == Status::KeyNotFound {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                packet.op_code,
                packet.status,
                packet.opaque,
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCrud::decode_common_error(&packet));
        }

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        let value = packet.value.unwrap_or_default();

        if let Some(extras) = &packet.extras {
            if extras.len() != 21 {
                return Err(Error::new_protocol_error("bad extras length"));
            }

            let mut extras = Cursor::new(extras);
            let deleted = extras.read_u32::<BigEndian>()?;
            let flags = extras.read_u32::<BigEndian>()?;
            let expiry = extras.read_u32::<BigEndian>()?;
            let seq_no = extras.read_u64::<BigEndian>()?;
            let datatype = extras.read_u8()?;

            Ok(GetMetaResponse {
                cas: packet.cas.unwrap_or_default(),
                flags,
                value,
                datatype,
                server_duration,
                expiry,
                seq_no,
                deleted: deleted != 0,
            })
        } else {
            Err(Error::new_protocol_error("no extras in response"))
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DeleteResponse {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
    pub server_duration: Option<Duration>,
}

impl TryFromClientResponse for DeleteResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        let kind = if status == Status::KeyNotFound {
            Some(ServerErrorKind::KeyNotFound)
        } else if status == Status::Locked {
            Some(ServerErrorKind::Locked)
        } else if status == Status::KeyExists {
            Some(ServerErrorKind::CasMismatch)
        } else if status == Status::Success {
            None
        } else {
            return Err(OpsCrud::decode_common_mutation_error(&packet));
        };

        if let Some(kind) = kind {
            return Err(ServerError::new(kind, packet.op_code, status, packet.opaque).into());
        }

        let mutation_token = if let Some(extras) = &packet.extras {
            Some(MutationToken::try_from(extras.as_ref())?)
        } else {
            None
        };

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(DeleteResponse {
            cas: packet.cas.unwrap_or_default(),
            mutation_token,
            server_duration,
        })
    }
}

impl TraceAttributes for DeleteResponse {
    fn server_duration(&self) -> Option<Duration> {
        self.server_duration
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GetAndLockResponse {
    pub cas: u64,
    pub flags: u32,
    pub value: Bytes,
    pub datatype: u8,
    pub server_duration: Option<Duration>,
}

impl TryFromClientResponse for GetAndLockResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        if status == Status::KeyNotFound {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                packet.op_code,
                packet.status,
                packet.opaque,
            )
            .into());
        } else if status == Status::Locked {
            return Err(ServerError::new(
                ServerErrorKind::Locked,
                packet.op_code,
                packet.status,
                packet.opaque,
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCrud::decode_common_error(&packet));
        }

        let flags = parse_flags(&packet.extras)?;

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        let value = packet.value.unwrap_or_default();

        Ok(GetAndLockResponse {
            cas: packet.cas.unwrap_or_default(),
            flags,
            value,
            datatype: packet.datatype,
            server_duration,
        })
    }
}

impl TraceAttributes for GetAndLockResponse {
    fn server_duration(&self) -> Option<Duration> {
        self.server_duration
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GetAndTouchResponse {
    pub cas: u64,
    pub flags: u32,
    pub value: Bytes,
    pub datatype: u8,
    pub server_duration: Option<Duration>,
}

impl TryFromClientResponse for GetAndTouchResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        if status == Status::KeyNotFound {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                packet.op_code,
                packet.status,
                packet.opaque,
            )
            .into());
        } else if status == Status::Locked {
            return Err(ServerError::new(
                ServerErrorKind::Locked,
                packet.op_code,
                packet.status,
                packet.opaque,
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCrud::decode_common_error(&packet));
        }

        let flags = parse_flags(&packet.extras)?;

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        let value = packet.value.unwrap_or_default();

        Ok(GetAndTouchResponse {
            cas: packet.cas.unwrap_or_default(),
            flags,
            value,
            datatype: packet.datatype,
            server_duration,
        })
    }
}

impl TraceAttributes for GetAndTouchResponse {
    fn server_duration(&self) -> Option<Duration> {
        self.server_duration
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct UnlockResponse {
    pub server_duration: Option<Duration>,
}

impl TryFromClientResponse for UnlockResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        if status == Status::KeyNotFound {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                packet.op_code,
                packet.status,
                packet.opaque,
            )
            .into());
        } else if status == Status::Locked {
            return Err(ServerError::new(
                ServerErrorKind::CasMismatch,
                packet.op_code,
                packet.status,
                packet.opaque,
            )
            .into());
        } else if status == Status::NotLocked {
            return Err(ServerError::new(
                ServerErrorKind::NotLocked,
                packet.op_code,
                packet.status,
                packet.opaque,
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCrud::decode_common_error(&packet));
        }

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(UnlockResponse { server_duration })
    }
}

impl TraceAttributes for UnlockResponse {
    fn server_duration(&self) -> Option<Duration> {
        self.server_duration
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TouchResponse {
    pub cas: u64,
    pub server_duration: Option<Duration>,
}

impl TryFromClientResponse for TouchResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        if status == Status::KeyNotFound {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                packet.op_code,
                packet.status,
                packet.opaque,
            )
            .into());
        } else if status == Status::Locked {
            return Err(ServerError::new(
                ServerErrorKind::Locked,
                packet.op_code,
                packet.status,
                packet.opaque,
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCrud::decode_common_error(&packet));
        }

        if let Some(extras) = &packet.extras {
            if !extras.is_empty() {
                return Err(Error::new_protocol_error("bad extras length"));
            }
        }

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(TouchResponse {
            cas: packet.cas.unwrap_or_default(),
            server_duration,
        })
    }
}

impl TraceAttributes for TouchResponse {
    fn server_duration(&self) -> Option<Duration> {
        self.server_duration
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct AddResponse {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
    pub server_duration: Option<Duration>,
}

impl TryFromClientResponse for AddResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        let kind = if status == Status::TooBig {
            Some(ServerErrorKind::TooBig)
        } else if status == Status::Locked {
            Some(ServerErrorKind::Locked)
        } else if status == Status::KeyExists {
            Some(ServerErrorKind::KeyExists)
        } else if status == Status::Success {
            None
        } else {
            return Err(OpsCrud::decode_common_mutation_error(&packet));
        };

        if let Some(kind) = kind {
            return Err(ServerError::new(kind, packet.op_code, status, packet.opaque).into());
        }

        let mutation_token = if let Some(extras) = &packet.extras {
            Some(MutationToken::try_from(extras.as_ref())?)
        } else {
            None
        };

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(AddResponse {
            cas: packet.cas.unwrap_or_default(),
            mutation_token,
            server_duration,
        })
    }
}

impl TraceAttributes for AddResponse {
    fn server_duration(&self) -> Option<Duration> {
        self.server_duration
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ReplaceResponse {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
    pub server_duration: Option<Duration>,
}

impl TryFromClientResponse for ReplaceResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        let kind = if status == Status::TooBig {
            Some(ServerErrorKind::TooBig)
        } else if status == Status::KeyNotFound {
            Some(ServerErrorKind::KeyNotFound)
        } else if status == Status::Locked {
            Some(ServerErrorKind::Locked)
        } else if status == Status::KeyExists {
            Some(ServerErrorKind::CasMismatch)
        } else if status == Status::Success {
            None
        } else {
            return Err(OpsCrud::decode_common_mutation_error(&packet));
        };

        if let Some(kind) = kind {
            return Err(ServerError::new(kind, packet.op_code, status, packet.opaque).into());
        }

        let mutation_token = if let Some(extras) = &packet.extras {
            Some(MutationToken::try_from(extras.as_ref())?)
        } else {
            None
        };

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(ReplaceResponse {
            cas: packet.cas.unwrap_or_default(),
            mutation_token,
            server_duration,
        })
    }
}

impl TraceAttributes for ReplaceResponse {
    fn server_duration(&self) -> Option<Duration> {
        self.server_duration
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct AppendResponse {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
    pub server_duration: Option<Duration>,
}

impl TryFromClientResponse for AppendResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let cas = resp
            .response_context()
            .expect("response did not have a response context")
            .cas;
        let packet = resp.packet();
        let status = packet.status;

        let kind = if status == Status::TooBig {
            Some(ServerErrorKind::TooBig)
        } else if status == Status::NotStored {
            Some(ServerErrorKind::NotStored)
        } else if status == Status::Locked {
            Some(ServerErrorKind::Locked)
        } else if status == Status::KeyExists && cas.is_some() {
            // KeyExists without a request cas would be an odd error to receive so we don't
            // handle that case.
            Some(ServerErrorKind::CasMismatch)
        } else if status == Status::Success {
            None
        } else {
            return Err(OpsCrud::decode_common_mutation_error(&packet));
        };

        if let Some(kind) = kind {
            return Err(ServerError::new(kind, packet.op_code, status, packet.opaque).into());
        }

        let mutation_token = if let Some(extras) = &packet.extras {
            Some(MutationToken::try_from(extras.as_ref())?)
        } else {
            None
        };

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(AppendResponse {
            cas: packet.cas.unwrap_or_default(),
            mutation_token,
            server_duration,
        })
    }
}

impl TraceAttributes for AppendResponse {
    fn server_duration(&self) -> Option<Duration> {
        self.server_duration
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PrependResponse {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
    pub server_duration: Option<Duration>,
}

impl TryFromClientResponse for PrependResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let cas = resp
            .response_context()
            .expect("response did not have a response context")
            .cas;
        let packet = resp.packet();
        let status = packet.status;

        let kind = if status == Status::TooBig {
            Some(ServerErrorKind::TooBig)
        } else if status == Status::NotStored {
            Some(ServerErrorKind::NotStored)
        } else if status == Status::Locked {
            Some(ServerErrorKind::Locked)
        } else if status == Status::KeyExists && cas.is_some() {
            // KeyExists without a request cas would be an odd error to receive so we don't
            // handle that case.
            Some(ServerErrorKind::CasMismatch)
        } else if status == Status::Success {
            None
        } else {
            return Err(OpsCrud::decode_common_mutation_error(&packet));
        };

        if let Some(kind) = kind {
            return Err(ServerError::new(kind, packet.op_code, status, packet.opaque).into());
        }

        let mutation_token = if let Some(extras) = &packet.extras {
            Some(MutationToken::try_from(extras.as_ref())?)
        } else {
            None
        };

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(PrependResponse {
            cas: packet.cas.unwrap_or_default(),
            mutation_token,
            server_duration,
        })
    }
}

impl TraceAttributes for PrependResponse {
    fn server_duration(&self) -> Option<Duration> {
        self.server_duration
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct IncrementResponse {
    pub cas: u64,
    pub value: u64,
    pub mutation_token: Option<MutationToken>,
    pub server_duration: Option<Duration>,
}

impl TryFromClientResponse for IncrementResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        let kind = if status == Status::KeyNotFound {
            Some(ServerErrorKind::KeyNotFound)
        } else if status == Status::Locked {
            Some(ServerErrorKind::Locked)
        } else if status == Status::BadDelta {
            Some(ServerErrorKind::BadDelta)
        } else if status == Status::Success {
            None
        } else {
            return Err(OpsCrud::decode_common_mutation_error(&packet));
        };

        if let Some(kind) = kind {
            return Err(ServerError::new(kind, packet.op_code, status, packet.opaque).into());
        }

        let value = if let Some(val) = &packet.value {
            if val.len() != 8 {
                return Err(Error::new_protocol_error(
                    "bad counter value length in response",
                ));
            }

            u64::from_be_bytes(val[..].try_into().unwrap())
        } else {
            0
        };

        let mutation_token = if let Some(extras) = &packet.extras {
            Some(MutationToken::try_from(extras.as_ref())?)
        } else {
            None
        };

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(IncrementResponse {
            cas: packet.cas.unwrap_or_default(),
            value,
            mutation_token,
            server_duration,
        })
    }
}

impl TraceAttributes for IncrementResponse {
    fn server_duration(&self) -> Option<Duration> {
        self.server_duration
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DecrementResponse {
    pub cas: u64,
    pub value: u64,
    pub mutation_token: Option<MutationToken>,
    pub server_duration: Option<Duration>,
}

impl TryFromClientResponse for DecrementResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        let kind = if status == Status::KeyNotFound {
            Some(ServerErrorKind::KeyNotFound)
        } else if status == Status::Locked {
            Some(ServerErrorKind::Locked)
        } else if status == Status::BadDelta {
            Some(ServerErrorKind::BadDelta)
        } else if status == Status::Success {
            None
        } else {
            return Err(OpsCrud::decode_common_mutation_error(&packet));
        };

        if let Some(kind) = kind {
            return Err(ServerError::new(kind, packet.op_code, status, packet.opaque).into());
        }

        let value = if let Some(val) = &packet.value {
            if val.len() != 8 {
                return Err(Error::new_protocol_error(
                    "bad counter value length in response",
                ));
            }

            u64::from_be_bytes(val[..].try_into().unwrap())
        } else {
            0
        };

        let mutation_token = if let Some(extras) = &packet.extras {
            Some(MutationToken::try_from(extras.as_ref())?)
        } else {
            None
        };

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(DecrementResponse {
            cas: packet.cas.unwrap_or_default(),
            value,
            mutation_token,
            server_duration,
        })
    }
}

impl TraceAttributes for DecrementResponse {
    fn server_duration(&self) -> Option<Duration> {
        self.server_duration
    }
}

pub struct LookupInResponse {
    pub cas: u64,
    pub ops: Vec<SubDocResult>,
    pub doc_is_deleted: bool,
    pub server_duration: Option<Duration>,
}

impl TryFromClientResponse for LookupInResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let subdoc_info = resp
            .response_context()
            .expect("response did not have a response context")
            .subdoc_info
            .expect("missing subdoc info in response context");
        let packet = resp.packet();
        let cas = packet.cas;
        let status = packet.status;

        if status == Status::KeyNotFound {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                packet.op_code,
                packet.status,
                packet.opaque,
            )
            .into());
        } else if status == Status::Locked {
            return Err(ServerError::new(
                ServerErrorKind::Locked,
                packet.op_code,
                packet.status,
                packet.opaque,
            )
            .into());
        } else if status == Status::SubDocInvalidCombo {
            return Err(ServerError::new(
                ServerErrorKind::Subdoc {
                    error: SubdocError::new(SubdocErrorKind::InvalidCombo, None),
                },
                packet.op_code,
                packet.status,
                packet.opaque,
            )
            .into());
        } else if status == Status::SubDocInvalidXattrOrder {
            return Err(ServerError::new(
                ServerErrorKind::Subdoc {
                    error: SubdocError::new(SubdocErrorKind::InvalidXattrOrder, None),
                },
                packet.op_code,
                packet.status,
                packet.opaque,
            )
            .into());
        } else if status == Status::SubDocXattrInvalidKeyCombo {
            return Err(ServerError::new(
                ServerErrorKind::Subdoc {
                    error: SubdocError::new(SubdocErrorKind::XattrInvalidKeyCombo, None),
                },
                packet.op_code,
                packet.status,
                packet.opaque,
            )
            .into());
        } else if status == Status::SubDocXattrInvalidFlagCombo {
            return Err(ServerError::new(
                ServerErrorKind::Subdoc {
                    error: SubdocError::new(SubdocErrorKind::XattrInvalidFlagCombo, None),
                },
                packet.op_code,
                packet.status,
                packet.opaque,
            )
            .into());
        }

        let mut doc_is_deleted = false;

        if status == Status::SubDocSuccessDeleted || status == Status::SubDocMultiPathFailureDeleted
        {
            doc_is_deleted = true;
            // still considered a success
        } else if status != Status::Success && status != Status::SubDocMultiPathFailure {
            return Err(OpsCrud::decode_common_error(&packet));
        }

        let mut results: Vec<SubDocResult> = Vec::with_capacity(subdoc_info.op_count as usize);
        let mut op_index = 0;

        let value = packet
            .value
            .as_ref()
            .ok_or_else(|| Error::new_protocol_error("missing value"))?;

        let mut cursor = Cursor::new(value);
        while cursor.position() < cursor.get_ref().len() as u64 {
            if cursor.remaining() < 6 {
                return Err(Error::new_protocol_error("bad value length"));
            }

            let res_status = cursor.read_u16::<BigEndian>()?;
            let res_status = Status::from(res_status);
            let res_value_len = cursor.read_u32::<BigEndian>()?;

            if cursor.remaining() < res_value_len as usize {
                return Err(Error::new_protocol_error("bad value length"));
            }

            let value = if res_value_len > 0 {
                let mut tmp_val = vec![0; res_value_len as usize];
                cursor.read_exact(&mut tmp_val)?;
                Some(tmp_val)
            } else {
                None
            };

            let err_kind: Option<SubdocErrorKind> = match res_status {
                Status::Success => None,
                Status::SubDocDocTooDeep => Some(SubdocErrorKind::DocTooDeep),
                Status::SubDocNotJSON => Some(SubdocErrorKind::NotJSON),
                Status::SubDocPathNotFound => Some(SubdocErrorKind::PathNotFound),
                Status::SubDocPathMismatch => Some(SubdocErrorKind::PathMismatch),
                Status::SubDocPathInvalid => Some(SubdocErrorKind::PathInvalid),
                Status::SubDocPathTooBig => Some(SubdocErrorKind::PathTooBig),
                Status::SubDocXattrUnknownVAttr => Some(SubdocErrorKind::XattrUnknownVAttr),
                _ => Some(SubdocErrorKind::UnknownStatus { status: res_status }),
            };

            let err = err_kind.map(|kind| {
                ServerError::new(
                    ServerErrorKind::Subdoc {
                        error: SubdocError::new(kind, op_index),
                    },
                    packet.op_code,
                    packet.status,
                    packet.opaque,
                )
                .into()
            });

            results.push(SubDocResult { value, err });
            op_index += 1;
        }

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(LookupInResponse {
            cas: cas.unwrap_or_default(),
            ops: results,
            doc_is_deleted,
            server_duration,
        })
    }
}

impl TraceAttributes for LookupInResponse {
    fn server_duration(&self) -> Option<Duration> {
        self.server_duration
    }
}

pub struct MutateInResponse {
    pub cas: u64,
    pub ops: Vec<SubDocResult>,
    pub doc_is_deleted: bool,
    pub mutation_token: Option<MutationToken>,
    pub server_duration: Option<Duration>,
}

impl TryFromClientResponse for MutateInResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let subdoc_info = resp
            .response_context()
            .expect("response did not have a response context")
            .subdoc_info
            .expect("missing subdoc info in response context");

        let packet = resp.packet();
        let cas = packet.cas;
        let status = packet.status;

        let kind = if status == Status::KeyNotFound {
            Some(ServerErrorKind::KeyNotFound)
        } else if status == Status::KeyExists && cas.is_some() {
            Some(ServerErrorKind::CasMismatch)
        } else if status == Status::KeyExists {
            Some(ServerErrorKind::KeyExists)
        } else if status == Status::Locked {
            Some(ServerErrorKind::Locked)
        } else if status == Status::TooBig {
            Some(ServerErrorKind::TooBig)
        } else if status == Status::SubDocInvalidCombo {
            Some(ServerErrorKind::Subdoc {
                error: SubdocError::new(SubdocErrorKind::InvalidCombo, None),
            })
        } else if status == Status::SubDocInvalidXattrOrder {
            Some(ServerErrorKind::Subdoc {
                error: SubdocError::new(SubdocErrorKind::InvalidXattrOrder, None),
            })
        } else if status == Status::SubDocXattrInvalidKeyCombo {
            Some(ServerErrorKind::Subdoc {
                error: SubdocError::new(SubdocErrorKind::XattrInvalidKeyCombo, None),
            })
        } else if status == Status::SubDocXattrInvalidFlagCombo {
            Some(ServerErrorKind::Subdoc {
                error: SubdocError::new(SubdocErrorKind::XattrInvalidFlagCombo, None),
            })
        } else if status == Status::SubDocXattrUnknownMacro {
            Some(ServerErrorKind::Subdoc {
                error: SubdocError::new(SubdocErrorKind::XattrUnknownMacro, None),
            })
        } else if status == Status::SubDocXattrUnknownVattrMacro {
            Some(ServerErrorKind::Subdoc {
                error: SubdocError::new(SubdocErrorKind::XattrUnknownVattrMacro, None),
            })
        } else if status == Status::SubDocXattrCannotModifyVAttr {
            Some(ServerErrorKind::Subdoc {
                error: SubdocError::new(SubdocErrorKind::XattrCannotModifyVAttr, None),
            })
        } else if status == Status::SubDocCanOnlyReviveDeletedDocuments {
            Some(ServerErrorKind::Subdoc {
                error: SubdocError::new(SubdocErrorKind::CanOnlyReviveDeletedDocuments, None),
            })
        } else if status == Status::SubDocDeletedDocumentCantHaveValue {
            Some(ServerErrorKind::Subdoc {
                error: SubdocError::new(SubdocErrorKind::DeletedDocumentCantHaveValue, None),
            })
        } else if status == Status::NotStored {
            if subdoc_info.flags.contains(SubdocDocFlag::AddDoc) {
                Some(ServerErrorKind::KeyExists)
            } else {
                Some(ServerErrorKind::NotStored)
            }
        } else if status == Status::SubDocMultiPathFailure {
            if let Some(value) = &packet.value {
                if value.len() != 3 {
                    return Err(Error::new_protocol_error("bad value length"));
                }

                let mut cursor = Cursor::new(value);
                let op_index = cursor.read_u8()?;
                let res_status = cursor.read_u16::<BigEndian>()?;

                let res_status = Status::from(res_status);

                let err_kind: SubdocErrorKind = match res_status {
                    Status::SubDocDocTooDeep => SubdocErrorKind::DocTooDeep,
                    Status::SubDocNotJSON => SubdocErrorKind::NotJSON,
                    Status::SubDocPathNotFound => SubdocErrorKind::PathNotFound,
                    Status::SubDocPathMismatch => SubdocErrorKind::PathMismatch,
                    Status::SubDocPathInvalid => SubdocErrorKind::PathInvalid,
                    Status::SubDocPathTooBig => SubdocErrorKind::PathTooBig,
                    Status::SubDocPathExists => SubdocErrorKind::PathExists,
                    Status::SubDocCantInsert => SubdocErrorKind::CantInsert,
                    Status::SubDocBadRange => SubdocErrorKind::BadRange,
                    Status::SubDocBadDelta => SubdocErrorKind::BadDelta,
                    Status::SubDocValueTooDeep => SubdocErrorKind::ValueTooDeep,
                    _ => SubdocErrorKind::UnknownStatus { status: res_status },
                };

                Some(ServerErrorKind::Subdoc {
                    error: SubdocError::new(err_kind, Some(op_index)),
                })
            } else {
                return Err(Error::new_protocol_error("bad value length"));
            }
        } else {
            None
        };

        if let Some(kind) = kind {
            return Err(ServerError::new(kind, packet.op_code, status, packet.opaque).into());
        }

        let mut doc_is_deleted = false;
        if status == Status::SubDocSuccessDeleted {
            doc_is_deleted = true;
            // still considered a success
        } else if status != Status::Success && status != Status::SubDocMultiPathFailure {
            return Err(OpsCrud::decode_common_mutation_error(&packet));
        }

        let mut results: Vec<SubDocResult> = Vec::with_capacity(subdoc_info.op_count as usize);

        if let Some(value) = &packet.value {
            let mut cursor = Cursor::new(value);

            while cursor.position() < cursor.get_ref().len() as u64 {
                if cursor.remaining() < 3 {
                    return Err(Error::new_protocol_error("bad value length"));
                }

                let op_index = cursor.read_u8()?;

                if op_index > results.len() as u8 {
                    for _ in results.len() as u8..op_index {
                        results.push(SubDocResult {
                            err: None,
                            value: None,
                        });
                    }
                }

                let op_status = cursor.read_u16::<BigEndian>()?;
                let op_status = Status::from(op_status);

                if op_status == Status::Success {
                    let val_length = cursor.read_u32::<BigEndian>()?;

                    let mut value = vec![0; val_length as usize];
                    cursor.read_exact(&mut value)?;

                    results.push(SubDocResult {
                        err: None,
                        value: Some(value),
                    });
                } else {
                    return Err(Error::new_protocol_error(
                        "subdoc mutatein op illegally provided an error",
                    ));
                }
            }
        }

        if results.len() < subdoc_info.op_count as usize {
            for _ in results.len()..subdoc_info.op_count as usize {
                results.push(SubDocResult {
                    err: None,
                    value: None,
                });
            }
        }

        let mutation_token = if let Some(extras) = &packet.extras {
            if extras.len() != 16 {
                return Err(Error::new_protocol_error("bad extras length"));
            }

            let (vbuuid_bytes, seqno_bytes) = extras.split_at(size_of::<u64>());
            let vbuuid = u64::from_be_bytes(vbuuid_bytes.try_into().unwrap());
            let seqno = u64::from_be_bytes(seqno_bytes.try_into().unwrap());

            Some(MutationToken { vbuuid, seqno })
        } else {
            None
        };

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(MutateInResponse {
            cas: cas.unwrap_or_default(),
            ops: results,
            mutation_token,
            doc_is_deleted,
            server_duration,
        })
    }
}

impl TraceAttributes for MutateInResponse {
    fn server_duration(&self) -> Option<Duration> {
        self.server_duration
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GetCollectionIdResponse {
    pub manifest_rev: u64,
    pub collection_id: u32,
}

impl TryFromClientResponse for GetCollectionIdResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let (scope_name, collection_name) = {
            let context = resp
                .response_context()
                .expect("response did not have a response context");
            let scope_name = context.scope_name.clone().expect("missing scope name");
            let collection_name = context
                .collection_name
                .clone()
                .expect("missing collection name");

            (scope_name, collection_name)
        };
        let packet = resp.packet();
        let status = packet.status;

        if status == Status::ScopeUnknown {
            return Err(ResourceError::new(
                ServerError::new(
                    ServerErrorKind::UnknownScopeName,
                    packet.op_code,
                    packet.status,
                    packet.opaque,
                ),
                scope_name,
                collection_name,
            )
            .into());
        } else if status == Status::CollectionUnknown {
            return Err(ResourceError::new(
                ServerError::new(
                    ServerErrorKind::UnknownCollectionName,
                    packet.op_code,
                    packet.status,
                    packet.opaque,
                ),
                scope_name,
                collection_name,
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCore::decode_error(&packet));
        }

        let extras = if let Some(extras) = &packet.extras {
            if extras.len() != 12 {
                return Err(Error::new_protocol_error("invalid extras length"));
            }
            extras
        } else {
            return Err(Error::new_protocol_error("no extras in response"));
        };

        let mut extras = Cursor::new(extras);
        let manifest_rev = extras.read_u64::<BigEndian>()?;
        let collection_id = extras.read_u32::<BigEndian>()?;

        Ok(GetCollectionIdResponse {
            manifest_rev,
            collection_id,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PingResponse {}

impl TryFromClientResponse for PingResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        if status != Status::Success {
            return Err(OpsCore::decode_error(&packet));
        }

        Ok(PingResponse {})
    }
}

impl TraceAttributes for PingResponse {
    fn server_duration(&self) -> Option<Duration> {
        None
    }
}
