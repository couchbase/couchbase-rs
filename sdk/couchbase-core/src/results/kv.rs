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

use bytes::Bytes;

use crate::error;
use crate::mutationtoken::MutationToken;
use std::time::Duration;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct GetResult {
    pub value: Bytes,
    pub flags: u32,
    pub datatype: u8,
    pub cas: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct GetMetaResult {
    pub cas: u64,
    pub flags: u32,
    pub value: Bytes,
    pub datatype: u8,
    pub server_duration: Option<Duration>,
    pub expiry: u32,
    pub seq_no: u64,
    pub deleted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct UpsertResult {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DeleteResult {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct GetAndLockResult {
    pub value: Bytes,
    pub flags: u32,
    pub datatype: u8,
    pub cas: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct GetAndTouchResult {
    pub value: Bytes,
    pub flags: u32,
    pub datatype: u8,
    pub cas: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct UnlockResult {}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct TouchResult {
    pub cas: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AddResult {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ReplaceResult {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AppendResult {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct PrependResult {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct IncrementResult {
    pub cas: u64,
    pub value: u64,
    pub mutation_token: Option<MutationToken>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DecrementResult {
    pub cas: u64,
    pub value: u64,
    pub mutation_token: Option<MutationToken>,
}

#[derive(Debug)]
pub struct SubDocResult {
    pub err: Option<error::MemdxError>,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug)]
pub struct LookupInResult {
    pub value: Vec<SubDocResult>,
    pub cas: u64,
    pub doc_is_deleted: bool,
}

#[derive(Debug)]
pub struct MutateInResult {
    pub value: Vec<SubDocResult>,
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct GetCollectionIdResult {
    pub manifest_rev: u64,
    pub collection_id: u32,
}
