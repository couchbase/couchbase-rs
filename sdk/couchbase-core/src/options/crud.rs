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

use std::sync::Arc;

use crate::memdx::datatype::DataTypeFlag;
use crate::memdx::durability_level::DurabilityLevel;
use crate::memdx::subdoc::{LookupInOp, MutateInOp, SubdocDocFlag};
use crate::retry::{RetryStrategy, DEFAULT_RETRY_STRATEGY};

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct GetOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> GetOptions<'a> {
    pub fn new(key: &'a [u8], scope_name: &'a str, collection_name: &'a str) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum GetReplicaStrategy {
    FromIndex { replica_index: u32, wrap: bool },
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct GetReplicaOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub strategy: GetReplicaStrategy,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> GetReplicaOptions<'a> {
    pub fn new(
        key: &'a [u8],
        scope_name: &'a str,
        collection_name: &'a str,
        strategy: GetReplicaStrategy,
    ) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            strategy,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct GetMetaOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> GetMetaOptions<'a> {
    pub fn new(key: &'a [u8], scope_name: &'a str, collection_name: &'a str) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct UpsertOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub value: &'a [u8],
    pub flags: u32,
    pub datatype: DataTypeFlag,
    pub expiry: Option<u32>,
    pub preserve_expiry: Option<bool>,
    pub cas: Option<u64>,
    pub durability_level: Option<DurabilityLevel>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> UpsertOptions<'a> {
    pub fn new(
        key: &'a [u8],
        scope_name: &'a str,
        collection_name: &'a str,
        value: &'a [u8],
    ) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            value,
            flags: 0,
            datatype: DataTypeFlag::default(),
            expiry: None,
            preserve_expiry: None,
            cas: None,
            durability_level: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn flags(mut self, flags: u32) -> Self {
        self.flags = flags;
        self
    }

    pub fn datatype(mut self, datatype: DataTypeFlag) -> Self {
        self.datatype = datatype;
        self
    }

    pub fn expiry(mut self, expiry: impl Into<Option<u32>>) -> Self {
        self.expiry = expiry.into();
        self
    }

    pub fn preserve_expiry(mut self, preserve_expiry: impl Into<Option<bool>>) -> Self {
        self.preserve_expiry = preserve_expiry.into();
        self
    }

    pub fn cas(mut self, cas: impl Into<Option<u64>>) -> Self {
        self.cas = cas.into();
        self
    }

    pub fn durability_level(
        mut self,
        durability_level: impl Into<Option<DurabilityLevel>>,
    ) -> Self {
        self.durability_level = durability_level.into();
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct DeleteOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub cas: Option<u64>,
    pub durability_level: Option<DurabilityLevel>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> DeleteOptions<'a> {
    pub fn new(key: &'a [u8], scope_name: &'a str, collection_name: &'a str) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            cas: None,
            durability_level: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn cas(mut self, cas: impl Into<Option<u64>>) -> Self {
        self.cas = cas.into();
        self
    }

    pub fn durability_level(
        mut self,
        durability_level: impl Into<Option<DurabilityLevel>>,
    ) -> Self {
        self.durability_level = durability_level.into();
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct GetAndLockOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub lock_time: u32,
    pub collection_id: Option<u32>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> GetAndLockOptions<'a> {
    pub fn new(
        key: &'a [u8],
        scope_name: &'a str,
        collection_name: &'a str,
        lock_time: u32,
    ) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            lock_time,
            collection_id: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn collection_id(mut self, collection_id: impl Into<Option<u32>>) -> Self {
        self.collection_id = collection_id.into();
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct GetAndTouchOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub expiry: u32,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> GetAndTouchOptions<'a> {
    pub fn new(key: &'a [u8], scope_name: &'a str, collection_name: &'a str, expiry: u32) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            expiry,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct UnlockOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub cas: u64,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> UnlockOptions<'a> {
    pub fn new(key: &'a [u8], scope_name: &'a str, collection_name: &'a str, cas: u64) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            cas,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct TouchOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub expiry: u32,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> TouchOptions<'a> {
    pub fn new(key: &'a [u8], scope_name: &'a str, collection_name: &'a str, expiry: u32) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            expiry,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct AddOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub value: &'a [u8],
    pub flags: u32,
    pub datatype: DataTypeFlag,
    pub expiry: Option<u32>,
    pub durability_level: Option<DurabilityLevel>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> AddOptions<'a> {
    pub fn new(
        key: &'a [u8],
        scope_name: &'a str,
        collection_name: &'a str,
        value: &'a [u8],
    ) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            value,
            flags: 0,
            datatype: DataTypeFlag::default(),
            expiry: None,
            durability_level: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn flags(mut self, flags: u32) -> Self {
        self.flags = flags;
        self
    }

    pub fn datatype(mut self, datatype: DataTypeFlag) -> Self {
        self.datatype = datatype;
        self
    }

    pub fn expiry(mut self, expiry: impl Into<Option<u32>>) -> Self {
        self.expiry = expiry.into();
        self
    }

    pub fn durability_level(
        mut self,
        durability_level: impl Into<Option<DurabilityLevel>>,
    ) -> Self {
        self.durability_level = durability_level.into();
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ReplaceOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub value: &'a [u8],
    pub flags: u32,
    pub datatype: DataTypeFlag,
    pub expiry: Option<u32>,
    pub preserve_expiry: Option<bool>,
    pub cas: Option<u64>,
    pub durability_level: Option<DurabilityLevel>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> ReplaceOptions<'a> {
    pub fn new(
        key: &'a [u8],
        scope_name: &'a str,
        collection_name: &'a str,
        value: &'a [u8],
    ) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            value,
            flags: 0,
            datatype: DataTypeFlag::default(),
            expiry: None,
            preserve_expiry: None,
            cas: None,
            durability_level: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn flags(mut self, flags: u32) -> Self {
        self.flags = flags;
        self
    }

    pub fn datatype(mut self, datatype: DataTypeFlag) -> Self {
        self.datatype = datatype;
        self
    }

    pub fn expiry(mut self, expiry: impl Into<Option<u32>>) -> Self {
        self.expiry = expiry.into();
        self
    }

    pub fn preserve_expiry(mut self, preserve_expiry: impl Into<Option<bool>>) -> Self {
        self.preserve_expiry = preserve_expiry.into();
        self
    }

    pub fn cas(mut self, cas: impl Into<Option<u64>>) -> Self {
        self.cas = cas.into();
        self
    }

    pub fn durability_level(
        mut self,
        durability_level: impl Into<Option<DurabilityLevel>>,
    ) -> Self {
        self.durability_level = durability_level.into();
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct AppendOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub value: &'a [u8],
    pub cas: Option<u64>,
    pub durability_level: Option<DurabilityLevel>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> AppendOptions<'a> {
    pub fn new(
        key: &'a [u8],
        scope_name: &'a str,
        collection_name: &'a str,
        value: &'a [u8],
    ) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            value,
            cas: None,
            durability_level: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn cas(mut self, cas: impl Into<Option<u64>>) -> Self {
        self.cas = cas.into();
        self
    }

    pub fn durability_level(
        mut self,
        durability_level: impl Into<Option<DurabilityLevel>>,
    ) -> Self {
        self.durability_level = durability_level.into();
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct PrependOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub value: &'a [u8],
    pub cas: Option<u64>,
    pub durability_level: Option<DurabilityLevel>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> PrependOptions<'a> {
    pub fn new(
        key: &'a [u8],
        scope_name: &'a str,
        collection_name: &'a str,
        value: &'a [u8],
    ) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            value,
            cas: None,
            durability_level: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn cas(mut self, cas: impl Into<Option<u64>>) -> Self {
        self.cas = cas.into();
        self
    }

    pub fn durability_level(
        mut self,
        durability_level: impl Into<Option<DurabilityLevel>>,
    ) -> Self {
        self.durability_level = durability_level.into();
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct IncrementOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub initial: Option<u64>,
    pub delta: u64,
    pub expiry: Option<u32>,
    pub durability_level: Option<DurabilityLevel>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> IncrementOptions<'a> {
    pub fn new(key: &'a [u8], delta: u64, scope_name: &'a str, collection_name: &'a str) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            initial: None,
            delta,
            expiry: None,
            durability_level: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn initial(mut self, initial: impl Into<Option<u64>>) -> Self {
        self.initial = initial.into();
        self
    }

    pub fn expiry(mut self, expiry: impl Into<Option<u32>>) -> Self {
        self.expiry = expiry.into();
        self
    }

    pub fn durability_level(
        mut self,
        durability_level: impl Into<Option<DurabilityLevel>>,
    ) -> Self {
        self.durability_level = durability_level.into();
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct DecrementOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub initial: Option<u64>,
    pub delta: u64,
    pub expiry: Option<u32>,
    pub durability_level: Option<DurabilityLevel>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> DecrementOptions<'a> {
    pub fn new(key: &'a [u8], delta: u64, scope_name: &'a str, collection_name: &'a str) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            initial: None,
            delta,
            expiry: None,
            durability_level: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn initial(mut self, initial: impl Into<Option<u64>>) -> Self {
        self.initial = initial.into();
        self
    }

    pub fn expiry(mut self, expiry: impl Into<Option<u32>>) -> Self {
        self.expiry = expiry.into();
        self
    }

    pub fn durability_level(
        mut self,
        durability_level: impl Into<Option<DurabilityLevel>>,
    ) -> Self {
        self.durability_level = durability_level.into();
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct LookupInOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub ops: &'a [LookupInOp<'a>],
    pub flags: SubdocDocFlag,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> LookupInOptions<'a> {
    pub fn new(
        key: &'a [u8],
        scope_name: &'a str,
        collection_name: &'a str,
        ops: &'a [LookupInOp<'a>],
    ) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            ops,
            flags: SubdocDocFlag::empty(),
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn flags(mut self, flags: SubdocDocFlag) -> Self {
        self.flags = flags;
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct MutateInOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub ops: &'a [MutateInOp<'a>],
    pub flags: SubdocDocFlag,
    pub expiry: Option<u32>,
    pub preserve_expiry: Option<bool>,
    pub cas: Option<u64>,
    pub durability_level: Option<DurabilityLevel>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> MutateInOptions<'a> {
    pub fn new(
        key: &'a [u8],
        scope_name: &'a str,
        collection_name: &'a str,
        ops: &'a [MutateInOp<'a>],
    ) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            ops,
            flags: SubdocDocFlag::empty(),
            expiry: None,
            preserve_expiry: None,
            cas: None,
            durability_level: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn flags(mut self, flags: SubdocDocFlag) -> Self {
        self.flags = flags;
        self
    }

    pub fn expiry(mut self, expiry: impl Into<Option<u32>>) -> Self {
        self.expiry = expiry.into();
        self
    }

    pub fn preserve_expiry(mut self, preserve_expiry: impl Into<Option<bool>>) -> Self {
        self.preserve_expiry = preserve_expiry.into();
        self
    }

    pub fn cas(mut self, cas: impl Into<Option<u64>>) -> Self {
        self.cas = cas.into();
        self
    }

    pub fn durability_level(
        mut self,
        durability_level: impl Into<Option<DurabilityLevel>>,
    ) -> Self {
        self.durability_level = durability_level.into();
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct GetCollectionIdOptions<'a> {
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> GetCollectionIdOptions<'a> {
    pub fn new(scope_name: &'a str, collection_name: &'a str) -> Self {
        Self {
            scope_name,
            collection_name,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}
