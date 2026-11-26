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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};
use serde_json::Value;

use crate::helpers;
use crate::httpx::client::Client;
use crate::httpx::request::OnBehalfOfInfo;
use crate::mgmtx::node_target::NodeTarget;
use crate::queryx::ensure_index_helper::DesiredState;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ScanConsistency {
    NotBounded,
    RequestPlus,
    AtPlus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum ProfileMode {
    Off,
    Phases,
    Timings,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
#[non_exhaustive]
pub enum Compression {
    Zip,
    Rle,
    Lzma,
    Lzo,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub enum DurabilityLevel {
    None,
    Majority,
    MajorityAndPersistActive,
    PersistToMajority,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub enum Encoding {
    #[serde(rename = "UTF-8")]
    Utf8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
#[non_exhaustive]
pub enum Format {
    Json,
    Xml,
    Csv,
    Tsv,
}

#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct CredsJson {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub(crate) user: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub(crate) pass: String,
}

impl CredsJson {
    pub fn new(user: impl Into<String>, pass: impl Into<String>) -> Self {
        Self {
            user: user.into(),
            pass: pass.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum ReplicaLevel {
    Off,
    On,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ScanVectorEntry {
    pub(crate) seq_no: u64,
    pub(crate) vb_uuid: String,
}

impl ScanVectorEntry {
    pub fn new(seq_no: u64, vb_uuid: impl Into<String>) -> Self {
        Self {
            seq_no,
            vb_uuid: vb_uuid.into(),
        }
    }
}

impl Serialize for ScanVectorEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(2))?;
        seq.serialize_element(&self.seq_no)?;
        seq.serialize_element(&self.vb_uuid)?;

        seq.end()
    }
}

#[derive(Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct NaturalLanguageOptions {
    pub(crate) cred: Option<String>,
    pub(crate) org_id: Option<String>,
    pub(crate) vendor: Option<String>,
    pub(crate) model: Option<String>,
    pub(crate) config: Option<serde_json::Map<String, Value>>,
    pub(crate) chat_id: Option<String>,
}

impl NaturalLanguageOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn cred(mut self, cred: impl Into<Option<String>>) -> Self {
        self.cred = cred.into();
        self
    }

    pub fn org_id(mut self, org_id: impl Into<Option<String>>) -> Self {
        self.org_id = org_id.into();
        self
    }

    pub fn vendor(mut self, vendor: impl Into<Option<String>>) -> Self {
        self.vendor = vendor.into();
        self
    }

    pub fn model(mut self, model: impl Into<Option<String>>) -> Self {
        self.model = model.into();
        self
    }

    pub fn config(mut self, config: impl Into<Option<serde_json::Map<String, Value>>>) -> Self {
        self.config = config.into();
        self
    }

    pub fn chat_id(mut self, chat_id: impl Into<Option<String>>) -> Self {
        self.chat_id = chat_id.into();
        self
    }

    fn serialize_into<M: SerializeMap>(&self, map: &mut M) -> Result<(), M::Error> {
        if let Some(cred) = &self.cred {
            map.serialize_entry("natural_cred", cred)?;
        }
        if let Some(org_id) = &self.org_id {
            map.serialize_entry("natural_orgid", org_id)?;
        }
        if let Some(vendor) = &self.vendor {
            map.serialize_entry("natural_vendor", vendor)?;
        }
        if let Some(model) = &self.model {
            map.serialize_entry("natural_model", model)?;
        }
        if let Some(config) = self.config.as_ref().filter(|config| !config.is_empty()) {
            map.serialize_entry("natural_config", config)?;
        }
        if let Some(chat_id) = &self.chat_id {
            map.serialize_entry("natural_chatid", chat_id)?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for NaturalLanguageOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const REDACTED: &str = "<redacted>";
        f.debug_struct("NaturalLanguageOptions")
            .field("cred", &self.cred.as_ref().map(|_| REDACTED))
            .field("org_id", &self.org_id)
            .field("vendor", &self.vendor)
            .field("model", &self.model)
            .field("config", &self.config.as_ref().map(|_| REDACTED))
            .field("chat_id", &self.chat_id)
            .finish()
    }
}

pub type FullScanVectors = Vec<ScanVectorEntry>;
pub type SparseScanVectors = HashMap<String, ScanVectorEntry>;

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct QueryOptions {
    pub(crate) args: Option<Vec<Value>>,
    pub(crate) atr_collection: Option<String>,
    pub(crate) auto_execute: Option<bool>,
    pub(crate) client_context_id: Option<String>,
    pub(crate) compression: Option<Compression>,
    pub(crate) controls: Option<bool>,
    pub(crate) creds: Option<Vec<CredsJson>>,
    pub(crate) durability_level: Option<DurabilityLevel>,
    pub(crate) encoded_plan: Option<String>,
    pub(crate) encoding: Option<Encoding>,
    pub(crate) format: Option<Format>,
    pub(crate) kv_timeout: Option<Duration>,
    pub(crate) max_parallelism: Option<u32>,
    pub(crate) memory_quota: Option<u32>,
    pub(crate) metrics: Option<bool>,
    pub(crate) namespace: Option<String>,
    pub(crate) num_atrs: Option<u32>,
    pub(crate) pipeline_batch: Option<u32>,
    pub(crate) pipeline_cap: Option<u32>,
    pub(crate) prepared: Option<String>,
    pub(crate) preserve_expiry: Option<bool>,
    pub(crate) pretty: Option<bool>,
    pub(crate) profile: Option<ProfileMode>,
    pub(crate) query_context: Option<String>,
    pub(crate) read_only: Option<bool>,
    pub(crate) scan_cap: Option<u32>,
    pub(crate) scan_consistency: Option<ScanConsistency>,
    pub(crate) sparse_scan_vector: Option<SparseScanVectors>,
    pub(crate) full_scan_vector: Option<FullScanVectors>,
    pub(crate) sparse_scan_vectors: Option<HashMap<String, SparseScanVectors>>,
    pub(crate) full_scan_vectors: Option<HashMap<String, FullScanVectors>>,
    pub(crate) scan_wait: Option<Duration>,
    pub(crate) signature: Option<bool>,
    pub(crate) statement: Option<String>,
    pub(crate) timeout: Option<Duration>,
    pub(crate) tx_data: Option<Vec<u8>>,
    pub(crate) tx_id: Option<String>,
    pub(crate) tx_implicit: Option<bool>,
    pub(crate) tx_stmt_num: Option<u32>,
    pub(crate) tx_timeout: Option<Duration>,
    pub(crate) use_cbo: Option<bool>,
    pub(crate) use_fts: Option<bool>,
    pub(crate) use_replica: Option<ReplicaLevel>,

    pub(crate) named_args: Option<HashMap<String, Value>>,
    pub(crate) raw: Option<HashMap<String, Value>>,
    pub(crate) natural_language: Option<NaturalLanguageOptions>,

    pub(crate) on_behalf_of: Option<OnBehalfOfInfo>,
}

impl QueryOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn args(mut self, args: impl Into<Option<Vec<Value>>>) -> Self {
        self.args = args.into();
        self
    }

    pub fn atr_collection(mut self, atr_collection: impl Into<Option<String>>) -> Self {
        self.atr_collection = atr_collection.into();
        self
    }

    pub fn auto_execute(mut self, auto_execute: impl Into<Option<bool>>) -> Self {
        self.auto_execute = auto_execute.into();
        self
    }

    pub fn client_context_id(mut self, client_context_id: impl Into<Option<String>>) -> Self {
        self.client_context_id = client_context_id.into();
        self
    }

    pub fn compression(mut self, compression: impl Into<Option<Compression>>) -> Self {
        self.compression = compression.into();
        self
    }

    pub fn controls(mut self, controls: impl Into<Option<bool>>) -> Self {
        self.controls = controls.into();
        self
    }

    pub fn creds(mut self, creds: impl Into<Option<Vec<CredsJson>>>) -> Self {
        self.creds = creds.into();
        self
    }

    pub fn durability_level(
        mut self,
        durability_level: impl Into<Option<DurabilityLevel>>,
    ) -> Self {
        self.durability_level = durability_level.into();
        self
    }

    pub fn encoded_plan(mut self, encoded_plan: impl Into<Option<String>>) -> Self {
        self.encoded_plan = encoded_plan.into();
        self
    }

    pub fn encoding(mut self, encoding: impl Into<Option<Encoding>>) -> Self {
        self.encoding = encoding.into();
        self
    }

    pub fn format(mut self, format: impl Into<Option<Format>>) -> Self {
        self.format = format.into();
        self
    }

    pub fn kv_timeout(mut self, kv_timeout: impl Into<Option<Duration>>) -> Self {
        self.kv_timeout = kv_timeout.into();
        self
    }

    pub fn max_parallelism(mut self, max_parallelism: impl Into<Option<u32>>) -> Self {
        self.max_parallelism = max_parallelism.into();
        self
    }

    pub fn memory_quota(mut self, memory_quota: impl Into<Option<u32>>) -> Self {
        self.memory_quota = memory_quota.into();
        self
    }

    pub fn metrics(mut self, metrics: impl Into<Option<bool>>) -> Self {
        self.metrics = metrics.into();
        self
    }

    pub fn namespace(mut self, namespace: impl Into<Option<String>>) -> Self {
        self.namespace = namespace.into();
        self
    }

    pub fn num_atrs(mut self, num_atrs: impl Into<Option<u32>>) -> Self {
        self.num_atrs = num_atrs.into();
        self
    }

    pub fn pipeline_batch(mut self, pipeline_batch: impl Into<Option<u32>>) -> Self {
        self.pipeline_batch = pipeline_batch.into();
        self
    }

    pub fn pipeline_cap(mut self, pipeline_cap: impl Into<Option<u32>>) -> Self {
        self.pipeline_cap = pipeline_cap.into();
        self
    }

    pub fn prepared(mut self, prepared: impl Into<Option<String>>) -> Self {
        self.prepared = prepared.into();
        self
    }

    pub fn preserve_expiry(mut self, preserve_expiry: impl Into<Option<bool>>) -> Self {
        self.preserve_expiry = preserve_expiry.into();
        self
    }

    pub fn pretty(mut self, pretty: impl Into<Option<bool>>) -> Self {
        self.pretty = pretty.into();
        self
    }

    pub fn profile(mut self, profile: impl Into<Option<ProfileMode>>) -> Self {
        self.profile = profile.into();
        self
    }

    pub fn query_context(mut self, query_context: impl Into<Option<String>>) -> Self {
        self.query_context = query_context.into();
        self
    }

    pub fn read_only(mut self, read_only: impl Into<Option<bool>>) -> Self {
        self.read_only = read_only.into();
        self
    }

    pub fn scan_cap(mut self, scan_cap: impl Into<Option<u32>>) -> Self {
        self.scan_cap = scan_cap.into();
        self
    }

    pub fn scan_consistency(
        mut self,
        scan_consistency: impl Into<Option<ScanConsistency>>,
    ) -> Self {
        self.scan_consistency = scan_consistency.into();
        self
    }

    pub fn sparse_scan_vector(
        mut self,
        sparse_scan_vector: impl Into<Option<SparseScanVectors>>,
    ) -> Self {
        self.sparse_scan_vector = sparse_scan_vector.into();
        self
    }

    pub fn full_scan_vector(
        mut self,
        full_scan_vector: impl Into<Option<FullScanVectors>>,
    ) -> Self {
        self.full_scan_vector = full_scan_vector.into();
        self
    }

    pub fn sparse_scan_vectors(
        mut self,
        sparse_scan_vectors: impl Into<Option<HashMap<String, SparseScanVectors>>>,
    ) -> Self {
        self.sparse_scan_vectors = sparse_scan_vectors.into();
        self
    }

    pub fn full_scan_vectors(
        mut self,
        full_scan_vectors: impl Into<Option<HashMap<String, FullScanVectors>>>,
    ) -> Self {
        self.full_scan_vectors = full_scan_vectors.into();
        self
    }

    pub fn scan_wait(mut self, scan_wait: impl Into<Option<Duration>>) -> Self {
        self.scan_wait = scan_wait.into();
        self
    }

    pub fn signature(mut self, signature: impl Into<Option<bool>>) -> Self {
        self.signature = signature.into();
        self
    }

    pub fn statement(mut self, statement: impl Into<Option<String>>) -> Self {
        self.statement = statement.into();
        self
    }

    pub fn timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
        self.timeout = timeout.into();
        self
    }

    pub fn tx_data(mut self, tx_data: impl Into<Option<Vec<u8>>>) -> Self {
        self.tx_data = tx_data.into();
        self
    }

    pub fn tx_id(mut self, tx_id: impl Into<Option<String>>) -> Self {
        self.tx_id = tx_id.into();
        self
    }

    pub fn tx_implicit(mut self, tx_implicit: impl Into<Option<bool>>) -> Self {
        self.tx_implicit = tx_implicit.into();
        self
    }

    pub fn tx_stmt_num(mut self, tx_stmt_num: impl Into<Option<u32>>) -> Self {
        self.tx_stmt_num = tx_stmt_num.into();
        self
    }

    pub fn tx_timeout(mut self, tx_timeout: impl Into<Option<Duration>>) -> Self {
        self.tx_timeout = tx_timeout.into();
        self
    }

    pub fn use_cbo(mut self, use_cbo: impl Into<Option<bool>>) -> Self {
        self.use_cbo = use_cbo.into();
        self
    }

    pub fn use_fts(mut self, use_fts: impl Into<Option<bool>>) -> Self {
        self.use_fts = use_fts.into();
        self
    }

    pub fn use_replica(mut self, use_replica: impl Into<Option<ReplicaLevel>>) -> Self {
        self.use_replica = use_replica.into();
        self
    }

    pub fn named_args(mut self, named_args: impl Into<Option<HashMap<String, Value>>>) -> Self {
        self.named_args = named_args.into();
        self
    }

    pub fn raw(mut self, raw: impl Into<Option<HashMap<String, Value>>>) -> Self {
        self.raw = raw.into();
        self
    }

    pub fn natural_language(
        mut self,
        natural_language: impl Into<Option<NaturalLanguageOptions>>,
    ) -> Self {
        self.natural_language = natural_language.into();
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}

impl Serialize for QueryOptions {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use helpers::durations;
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(None)?;

        macro_rules! serialize_if_not_none {
            ($field:expr, $name:expr) => {
                if !$field.is_none() {
                    map.serialize_entry($name, &$field)?;
                }
            };
        }

        macro_rules! serialize_duration_if_not_none {
            ($field:expr, $name:expr) => {
                if let Some(f) = $field {
                    map.serialize_entry($name, &durations::duration_to_golang_string(&f))?;
                }
            };
        }

        serialize_if_not_none!(self.args, "args");
        serialize_if_not_none!(self.atr_collection, "atr_collection");
        serialize_if_not_none!(self.auto_execute, "auto_execute");
        serialize_if_not_none!(self.client_context_id, "client_context_id");
        serialize_if_not_none!(self.compression, "compression");
        serialize_if_not_none!(self.controls, "controls");
        serialize_if_not_none!(self.creds, "creds");
        serialize_if_not_none!(self.durability_level, "durability_level");
        serialize_if_not_none!(self.encoded_plan, "encoded_plan");
        serialize_if_not_none!(self.encoding, "encoding");
        serialize_if_not_none!(self.format, "format");
        serialize_duration_if_not_none!(self.kv_timeout, "kvtimeout");
        serialize_if_not_none!(self.max_parallelism, "max_parallelism");
        serialize_if_not_none!(self.memory_quota, "memory_quota");
        serialize_if_not_none!(self.metrics, "metrics");
        serialize_if_not_none!(self.namespace, "namespace");
        serialize_if_not_none!(self.num_atrs, "num_atrs");
        serialize_if_not_none!(self.pipeline_batch, "pipeline_batch");
        serialize_if_not_none!(self.pipeline_cap, "pipeline_cap");
        serialize_if_not_none!(self.prepared, "prepared");
        serialize_if_not_none!(self.preserve_expiry, "preserve_expiry");
        serialize_if_not_none!(self.pretty, "pretty");
        serialize_if_not_none!(self.profile, "profile");
        serialize_if_not_none!(self.query_context, "query_context");
        serialize_if_not_none!(self.read_only, "readonly");
        serialize_if_not_none!(self.scan_cap, "scan_cap");
        serialize_if_not_none!(self.scan_consistency, "scan_consistency");
        serialize_if_not_none!(self.sparse_scan_vector, "scan_vector");
        serialize_if_not_none!(self.full_scan_vector, "scan_vector");
        serialize_if_not_none!(self.sparse_scan_vectors, "scan_vectors");
        serialize_if_not_none!(self.full_scan_vectors, "scan_vectors");
        serialize_duration_if_not_none!(self.scan_wait, "scan_wait");
        serialize_if_not_none!(self.signature, "signature");
        serialize_if_not_none!(self.statement, "statement");
        serialize_duration_if_not_none!(self.timeout, "timeout");
        serialize_if_not_none!(self.tx_data, "txdata");
        serialize_if_not_none!(self.tx_id, "txid");
        serialize_if_not_none!(self.tx_implicit, "tximplicit");
        serialize_if_not_none!(self.tx_stmt_num, "txstmtnum");
        serialize_duration_if_not_none!(self.tx_timeout, "txtimeout");
        serialize_if_not_none!(self.use_cbo, "use_cbo");
        serialize_if_not_none!(self.use_fts, "use_fts");
        serialize_if_not_none!(self.use_replica, "use_replica");

        if let Some(args) = &self.named_args {
            // Prefix each named_arg with "$" if not already prefixed.
            for (key, value) in args {
                let key = if key.starts_with('$') {
                    key
                } else {
                    &format!("${key}")
                };
                map.serialize_entry(key, value)?;
            }
        }

        if let Some(natural_language) = &self.natural_language {
            natural_language.serialize_into(&mut map)?;
        }

        if let Some(raw) = &self.raw {
            // Move raw fields to the top level.
            for (key, value) in raw {
                map.serialize_entry(key, value)?;
            }
        }

        map.end()
    }
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct GetAllIndexesOptions<'a> {
    pub(crate) bucket_name: &'a str,
    pub(crate) scope_name: Option<&'a str>,
    pub(crate) collection_name: Option<&'a str>,
    pub(crate) on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> GetAllIndexesOptions<'a> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn bucket_name(mut self, bucket_name: &'a str) -> Self {
        self.bucket_name = bucket_name;
        self
    }

    pub fn scope_name(mut self, scope_name: impl Into<Option<&'a str>>) -> Self {
        self.scope_name = scope_name.into();
        self
    }

    pub fn collection_name(mut self, collection_name: impl Into<Option<&'a str>>) -> Self {
        self.collection_name = collection_name.into();
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct CreatePrimaryIndexOptions<'a> {
    pub(crate) bucket_name: &'a str,
    pub(crate) scope_name: Option<&'a str>,
    pub(crate) collection_name: Option<&'a str>,
    pub(crate) index_name: Option<&'a str>,
    pub(crate) num_replicas: Option<u32>,
    pub(crate) deferred: Option<bool>,
    pub(crate) ignore_if_exists: Option<bool>,
    pub(crate) on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> CreatePrimaryIndexOptions<'a> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn bucket_name(mut self, bucket_name: &'a str) -> Self {
        self.bucket_name = bucket_name;
        self
    }

    pub fn scope_name(mut self, scope_name: impl Into<Option<&'a str>>) -> Self {
        self.scope_name = scope_name.into();
        self
    }

    pub fn collection_name(mut self, collection_name: impl Into<Option<&'a str>>) -> Self {
        self.collection_name = collection_name.into();
        self
    }

    pub fn index_name(mut self, index_name: impl Into<Option<&'a str>>) -> Self {
        self.index_name = index_name.into();
        self
    }

    pub fn num_replicas(mut self, num_replicas: impl Into<Option<u32>>) -> Self {
        self.num_replicas = num_replicas.into();
        self
    }

    pub fn deferred(mut self, deferred: impl Into<Option<bool>>) -> Self {
        self.deferred = deferred.into();
        self
    }

    pub fn ignore_if_exists(mut self, ignore_if_exists: impl Into<Option<bool>>) -> Self {
        self.ignore_if_exists = ignore_if_exists.into();
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct CreateIndexOptions<'a> {
    pub(crate) bucket_name: &'a str,
    pub(crate) scope_name: Option<&'a str>,
    pub(crate) collection_name: Option<&'a str>,
    pub(crate) index_name: &'a str,
    pub(crate) num_replicas: Option<u32>,
    pub(crate) fields: &'a [&'a str],
    pub(crate) deferred: Option<bool>,
    pub(crate) ignore_if_exists: Option<bool>,
    pub(crate) on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> CreateIndexOptions<'a> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn bucket_name(mut self, bucket_name: &'a str) -> Self {
        self.bucket_name = bucket_name;
        self
    }

    pub fn scope_name(mut self, scope_name: impl Into<Option<&'a str>>) -> Self {
        self.scope_name = scope_name.into();
        self
    }

    pub fn collection_name(mut self, collection_name: impl Into<Option<&'a str>>) -> Self {
        self.collection_name = collection_name.into();
        self
    }

    pub fn index_name(mut self, index_name: &'a str) -> Self {
        self.index_name = index_name;
        self
    }

    pub fn num_replicas(mut self, num_replicas: impl Into<Option<u32>>) -> Self {
        self.num_replicas = num_replicas.into();
        self
    }

    pub fn fields(mut self, fields: &'a [&'a str]) -> Self {
        self.fields = fields;
        self
    }

    pub fn deferred(mut self, deferred: impl Into<Option<bool>>) -> Self {
        self.deferred = deferred.into();
        self
    }

    pub fn ignore_if_exists(mut self, ignore_if_exists: impl Into<Option<bool>>) -> Self {
        self.ignore_if_exists = ignore_if_exists.into();
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct DropPrimaryIndexOptions<'a> {
    pub(crate) bucket_name: &'a str,
    pub(crate) scope_name: Option<&'a str>,
    pub(crate) collection_name: Option<&'a str>,
    pub(crate) index_name: Option<&'a str>,
    pub(crate) ignore_if_not_exists: Option<bool>,
    pub(crate) on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> DropPrimaryIndexOptions<'a> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn bucket_name(mut self, bucket_name: &'a str) -> Self {
        self.bucket_name = bucket_name;
        self
    }

    pub fn scope_name(mut self, scope_name: impl Into<Option<&'a str>>) -> Self {
        self.scope_name = scope_name.into();
        self
    }

    pub fn collection_name(mut self, collection_name: impl Into<Option<&'a str>>) -> Self {
        self.collection_name = collection_name.into();
        self
    }

    pub fn index_name(mut self, index_name: impl Into<Option<&'a str>>) -> Self {
        self.index_name = index_name.into();
        self
    }

    pub fn ignore_if_not_exists(mut self, ignore_if_not_exists: impl Into<Option<bool>>) -> Self {
        self.ignore_if_not_exists = ignore_if_not_exists.into();
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct DropIndexOptions<'a> {
    pub(crate) bucket_name: &'a str,
    pub(crate) scope_name: Option<&'a str>,
    pub(crate) collection_name: Option<&'a str>,
    pub(crate) index_name: &'a str,
    pub(crate) ignore_if_not_exists: Option<bool>,
    pub(crate) on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> DropIndexOptions<'a> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn bucket_name(mut self, bucket_name: &'a str) -> Self {
        self.bucket_name = bucket_name;
        self
    }

    pub fn scope_name(mut self, scope_name: impl Into<Option<&'a str>>) -> Self {
        self.scope_name = scope_name.into();
        self
    }

    pub fn collection_name(mut self, collection_name: impl Into<Option<&'a str>>) -> Self {
        self.collection_name = collection_name.into();
        self
    }

    pub fn index_name(mut self, index_name: &'a str) -> Self {
        self.index_name = index_name;
        self
    }

    pub fn ignore_if_not_exists(mut self, ignore_if_not_exists: impl Into<Option<bool>>) -> Self {
        self.ignore_if_not_exists = ignore_if_not_exists.into();
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct BuildDeferredIndexesOptions<'a> {
    pub(crate) bucket_name: &'a str,
    pub(crate) scope_name: Option<&'a str>,
    pub(crate) collection_name: Option<&'a str>,
    pub(crate) on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> BuildDeferredIndexesOptions<'a> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn bucket_name(mut self, bucket_name: &'a str) -> Self {
        self.bucket_name = bucket_name;
        self
    }

    pub fn scope_name(mut self, scope_name: impl Into<Option<&'a str>>) -> Self {
        self.scope_name = scope_name.into();
        self
    }

    pub fn collection_name(mut self, collection_name: impl Into<Option<&'a str>>) -> Self {
        self.collection_name = collection_name.into();
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct WatchIndexesOptions<'a> {
    pub(crate) bucket_name: &'a str,
    pub(crate) scope_name: Option<&'a str>,
    pub(crate) collection_name: Option<&'a str>,
    pub(crate) indexes: &'a [&'a str],
    pub(crate) watch_primary: Option<bool>,
    pub(crate) on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> WatchIndexesOptions<'a> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn bucket_name(mut self, bucket_name: &'a str) -> Self {
        self.bucket_name = bucket_name;
        self
    }

    pub fn scope_name(mut self, scope_name: impl Into<Option<&'a str>>) -> Self {
        self.scope_name = scope_name.into();
        self
    }

    pub fn collection_name(mut self, collection_name: impl Into<Option<&'a str>>) -> Self {
        self.collection_name = collection_name.into();
        self
    }

    pub fn indexes(mut self, indexes: &'a [&'a str]) -> Self {
        self.indexes = indexes;
        self
    }

    pub fn watch_primary(mut self, watch_primary: impl Into<Option<bool>>) -> Self {
        self.watch_primary = watch_primary.into();
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EnsureIndexPollOptions<C: Client> {
    pub desired_state: DesiredState,
    pub client: Arc<C>,
    pub targets: Vec<NodeTarget>,
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct PingOptions<'a> {
    pub(crate) on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> PingOptions<'a> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}

#[cfg(test)]
mod tests {
    use super::{NaturalLanguageOptions, QueryOptions};
    use serde_json::{json, Map, Value};
    use std::collections::HashMap;

    fn config() -> Map<String, Value> {
        let mut config = Map::new();
        config.insert("api_key".into(), json!("sk-secret"));
        config.insert("model".into(), json!("gpt"));
        config
    }

    #[test]
    fn natural_language_options_serialize_to_top_level_params() {
        let opts = QueryOptions::new()
            .statement("USING AI WITH {} hello".to_string())
            .natural_language(
                NaturalLanguageOptions::new()
                    .cred("me@example.com:hunter2".to_string())
                    .org_id("org".to_string())
                    .vendor("openai".to_string())
                    .model("gpt".to_string())
                    .config(config())
                    .chat_id("chat-1".to_string()),
            );

        assert_eq!(
            json!({
                "statement": "USING AI WITH {} hello",
                "natural_cred": "me@example.com:hunter2",
                "natural_orgid": "org",
                "natural_vendor": "openai",
                "natural_model": "gpt",
                "natural_config": {"api_key": "sk-secret", "model": "gpt"},
                "natural_chatid": "chat-1",
            }),
            serde_json::to_value(&opts).unwrap()
        );
    }

    #[test]
    fn empty_natural_config_is_omitted() {
        let opts =
            QueryOptions::new().natural_language(NaturalLanguageOptions::new().config(Map::new()));
        assert_eq!(json!({}), serde_json::to_value(&opts).unwrap());
    }

    #[test]
    fn raw_params_take_precedence_over_natural_language_options() {
        let opts = QueryOptions::new()
            .natural_language(NaturalLanguageOptions::new().chat_id("typed".to_string()))
            .raw(HashMap::from([(
                "natural_chatid".to_string(),
                json!("raw"),
            )]));
        assert_eq!(
            json!({"natural_chatid": "raw"}),
            serde_json::to_value(&opts).unwrap()
        );
    }

    #[test]
    fn debug_redacts_model_credentials() {
        let opts = QueryOptions::new().natural_language(
            NaturalLanguageOptions::new()
                .cred("me@example.com:hunter2".to_string())
                .config(config())
                .chat_id("chat-1".to_string()),
        );

        let debug = format!("{opts:?}");
        assert!(!debug.contains("hunter2"), "{debug}");
        assert!(!debug.contains("sk-secret"), "{debug}");
        assert!(debug.contains("<redacted>"), "{debug}");
        assert!(debug.contains("chat-1"), "{debug}");

        let core_opts = crate::options::query::QueryOptions::new().natural_language(
            NaturalLanguageOptions::new().cred("me@example.com:hunter2".to_string()),
        );
        assert!(!format!("{core_opts:?}").contains("hunter2"));
    }
}
