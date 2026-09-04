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

use std::fmt::{Display, Formatter};

use crate::memdx::error::Error;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum OpCode {
    Get,
    GetReplica,
    Set,
    Add,
    Replace,
    Delete,
    Increment,
    Decrement,
    Noop,
    Touch,
    GAT,
    Append,
    Prepend,
    Hello,
    GetClusterConfig,
    GetCollectionId,
    SubDocGet,
    SubDocExists,
    SubDocDictAdd,
    SubDocDictSet,
    SubDocDelete,
    SubDocReplace,
    SubDocArrayPushLast,
    SubDocArrayPushFirst,
    SubDocArrayInsert,
    SubDocArrayAddUnique,
    SubDocCounter,
    SubDocMultiLookup,
    SubDocMultiMutation,
    SubDocGetCount,
    SubDocReplaceBodyWithXattr,
    GetErrorMap,
    SelectBucket,
    GetLocked,
    UnlockKey,
    GetMeta,
    SASLAuth,
    SASLListMechs,
    SASLStep,
    Unknown(u8),
}

impl From<OpCode> for u8 {
    fn from(value: OpCode) -> Self {
        match value {
            OpCode::Get => 0x00,
            OpCode::Set => 0x01,
            OpCode::Add => 0x02,
            OpCode::Replace => 0x03,
            OpCode::Delete => 0x04,
            OpCode::Increment => 0x05,
            OpCode::Decrement => 0x06,
            OpCode::Noop => 0x0a,
            OpCode::Append => 0x0e,
            OpCode::Prepend => 0x0f,
            OpCode::Touch => 0x1c,
            OpCode::GAT => 0x1d,
            OpCode::Hello => 0x1f,
            OpCode::SASLListMechs => 0x20,
            OpCode::SASLAuth => 0x21,
            OpCode::SASLStep => 0x22,
            OpCode::GetReplica => 0x83,
            OpCode::SelectBucket => 0x89,
            OpCode::GetLocked => 0x94,
            OpCode::UnlockKey => 0x95,
            OpCode::GetMeta => 0xa0,
            OpCode::GetClusterConfig => 0xb5,
            OpCode::GetCollectionId => 0xbb,
            OpCode::SubDocGet => 0xc5,
            OpCode::SubDocExists => 0xc6,
            OpCode::SubDocDictAdd => 0xc7,
            OpCode::SubDocDictSet => 0xc8,
            OpCode::SubDocDelete => 0xc9,
            OpCode::SubDocReplace => 0xca,
            OpCode::SubDocArrayPushLast => 0xcb,
            OpCode::SubDocArrayPushFirst => 0xcc,
            OpCode::SubDocArrayInsert => 0xcd,
            OpCode::SubDocArrayAddUnique => 0xce,
            OpCode::SubDocCounter => 0xcf,
            OpCode::SubDocMultiLookup => 0xd0,
            OpCode::SubDocMultiMutation => 0xd1,
            OpCode::SubDocGetCount => 0xd2,
            OpCode::SubDocReplaceBodyWithXattr => 0xd3,
            OpCode::GetErrorMap => 0xfe,
            OpCode::Unknown(code) => code,
        }
    }
}

impl TryFrom<u8> for OpCode {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        let code = match value {
            0x00 => OpCode::Get,
            0x83 => OpCode::GetReplica,
            0x01 => OpCode::Set,
            0x02 => OpCode::Add,
            0x03 => OpCode::Replace,
            0x04 => OpCode::Delete,
            0x05 => OpCode::Increment,
            0x06 => OpCode::Decrement,
            0x0a => OpCode::Noop,
            0x0e => OpCode::Append,
            0x0f => OpCode::Prepend,
            0x1c => OpCode::Touch,
            0x1d => OpCode::GAT,
            0x1f => OpCode::Hello,
            0x20 => OpCode::SASLListMechs,
            0x21 => OpCode::SASLAuth,
            0x22 => OpCode::SASLStep,
            0x89 => OpCode::SelectBucket,
            0x94 => OpCode::GetLocked,
            0x95 => OpCode::UnlockKey,
            0xb5 => OpCode::GetClusterConfig,
            0xbb => OpCode::GetCollectionId,
            0xc5 => OpCode::SubDocGet,
            0xc6 => OpCode::SubDocExists,
            0xc7 => OpCode::SubDocDictAdd,
            0xc8 => OpCode::SubDocDictSet,
            0xc9 => OpCode::SubDocDelete,
            0xca => OpCode::SubDocReplace,
            0xcb => OpCode::SubDocArrayPushLast,
            0xcc => OpCode::SubDocArrayPushFirst,
            0xcd => OpCode::SubDocArrayInsert,
            0xce => OpCode::SubDocArrayAddUnique,
            0xcf => OpCode::SubDocCounter,
            0xd0 => OpCode::SubDocMultiLookup,
            0xd1 => OpCode::SubDocMultiMutation,
            0xd2 => OpCode::SubDocGetCount,
            0xd3 => OpCode::SubDocReplaceBodyWithXattr,
            0xfe => OpCode::GetErrorMap,
            _ => OpCode::Unknown(value),
        };

        Ok(code)
    }
}

impl Display for OpCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let txt = match self {
            OpCode::Get => "Get",
            OpCode::GetReplica => "Get replica",
            OpCode::Set => "Set",
            OpCode::Add => "Add",
            OpCode::Replace => "Replace",
            OpCode::Delete => "Delete",
            OpCode::Increment => "Increment",
            OpCode::Decrement => "Decrement",
            OpCode::Noop => "Noop",
            OpCode::Append => "Append",
            OpCode::Prepend => "Prepend",
            OpCode::Touch => "Touch",
            OpCode::GAT => "GAT",
            OpCode::GetMeta => "Get meta",
            OpCode::Hello => "Hello",
            OpCode::GetClusterConfig => "Get cluster config",
            OpCode::GetCollectionId => "Get collection id",
            OpCode::GetErrorMap => "Get error map",
            OpCode::SelectBucket => "Select bucket",
            OpCode::GetLocked => "Get locked",
            OpCode::UnlockKey => "Unlock key",
            OpCode::SASLAuth => "SASL auth",
            OpCode::SASLListMechs => "SASL list mechanisms",
            OpCode::SASLStep => "SASL step",
            OpCode::SubDocGet => "SubDoc get",
            OpCode::SubDocExists => "SubDoc exists",
            OpCode::SubDocDictAdd => "SubDoc dict add",
            OpCode::SubDocDictSet => "SubDoc dict set",
            OpCode::SubDocDelete => "SubDoc delete",
            OpCode::SubDocReplace => "SubDoc replace",
            OpCode::SubDocArrayPushLast => "SubDoc array push last",
            OpCode::SubDocArrayPushFirst => "SubDoc array push first",
            OpCode::SubDocArrayInsert => "SubDoc array insert",
            OpCode::SubDocArrayAddUnique => "SubDoc array add unique",
            OpCode::SubDocCounter => "SubDoc counter",
            OpCode::SubDocMultiLookup => "SubDoc multi lookup",
            OpCode::SubDocMultiMutation => "SubDoc multi mutation",
            OpCode::SubDocGetCount => "SubDoc get count",
            OpCode::SubDocReplaceBodyWithXattr => "SubDoc replace body with Xattr",
            OpCode::Unknown(code) => {
                return write!(f, "x{code:02x}");
            }
        };
        write!(f, "{txt}")
    }
}
