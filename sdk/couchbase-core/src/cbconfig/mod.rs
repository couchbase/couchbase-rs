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

pub mod parse;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct VBucketServerMap {
    #[serde(alias = "hashAlgorithm")]
    pub hash_algorithm: String,
    #[serde(alias = "numReplicas")]
    pub num_replicas: usize,
    #[serde(alias = "serverList")]
    pub server_list: Vec<String>,
    #[serde(alias = "vBucketMap")]
    pub vbucket_map: Vec<Vec<i16>>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ConfigDDocs {
    #[serde(alias = "uri")]
    pub uri: String,
}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct TerseExtNodePorts {
    #[serde(alias = "kv")]
    pub kv: Option<u16>,
    #[serde(alias = "capi")]
    pub capi: Option<u16>,
    #[serde(alias = "mgmt")]
    pub mgmt: Option<u16>,
    #[serde(alias = "n1ql")]
    pub n1ql: Option<u16>,
    #[serde(alias = "fts")]
    pub fts: Option<u16>,
    #[serde(alias = "cbas")]
    pub cbas: Option<u16>,
    #[serde(alias = "eventingAdminPort")]
    pub eventing: Option<u16>,
    #[serde(alias = "indexHttp")]
    pub gsi: Option<u16>,
    #[serde(alias = "backupAPI")]
    pub backup: Option<u16>,

    #[serde(alias = "kvSSL")]
    pub kv_ssl: Option<u16>,
    #[serde(alias = "capiSSL")]
    pub capi_ssl: Option<u16>,
    #[serde(alias = "mgmtSSL")]
    pub mgmt_ssl: Option<u16>,
    #[serde(alias = "n1qlSSL")]
    pub n1ql_ssl: Option<u16>,
    #[serde(alias = "ftsSSL")]
    pub fts_ssl: Option<u16>,
    #[serde(alias = "cbasSSL")]
    pub cbas_ssl: Option<u16>,
    #[serde(alias = "eventingSSL")]
    pub eventing_ssl: Option<u16>,
    #[serde(alias = "indexHttps")]
    pub gsi_ssl: Option<u16>,
    #[serde(alias = "backupAPIHTTPS")]
    pub backup_ssl: Option<u16>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TerseExtNodeAltAddresses {
    #[serde(alias = "ports")]
    pub ports: TerseExtNodePorts,
    #[serde(alias = "hostname")]
    pub hostname: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TerseNodePorts {
    #[serde(alias = "direct")]
    pub direct: Option<u16>,
    #[serde(alias = "proxy")]
    pub proxy: Option<u16>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TerseNodeConfig {
    #[serde(alias = "couchbaseApiBase")]
    pub couchbase_api_base: Option<String>,
    #[serde(alias = "hostname")]
    pub hostname: Option<String>,
    #[serde(alias = "ports")]
    pub ports: Option<TerseNodePorts>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TerseNodeExtConfig {
    #[serde(alias = "services")]
    pub services: Option<TerseExtNodePorts>,
    #[serde(alias = "thisNode")]
    pub this_node: Option<bool>,
    #[serde(alias = "hostname")]
    pub hostname: Option<String>,
    #[serde(alias = "alternateAddresses", default)]
    pub alternate_addresses: HashMap<String, TerseExtNodeAltAddresses>,
    #[serde(alias = "nodeUUID", default)]
    pub node_uuid: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TerseConfig {
    #[serde(alias = "rev")]
    pub rev: i64,
    #[serde(alias = "revEpoch")]
    pub rev_epoch: Option<i64>,
    #[serde(alias = "name")]
    pub name: Option<String>,
    #[serde(alias = "nodeLocator")]
    pub node_locator: Option<String>,
    #[serde(alias = "uuid")]
    pub uuid: Option<String>,
    #[serde(alias = "uri")]
    pub uri: Option<String>,
    #[serde(alias = "streamingUri")]
    pub streaming_uri: Option<String>,
    #[serde(alias = "bucketCapabilitiesVer")]
    pub bucket_capabilities_ver: Option<String>,
    #[serde(alias = "bucketCapabilities")]
    pub bucket_capabilities: Option<Vec<String>>,
    #[serde(alias = "collectionsManifestUid")]
    pub collections_manifest_uuid: Option<String>,
    #[serde(alias = "ddocs")]
    pub ddocs: Option<ConfigDDocs>,
    #[serde(alias = "vBucketServerMap")]
    pub vbucket_server_map: Option<VBucketServerMap>,
    #[serde(alias = "nodes")]
    pub nodes: Option<Vec<TerseNodeConfig>>,
    #[serde(alias = "nodesExt")]
    pub nodes_ext: Vec<TerseNodeExtConfig>,
    #[serde(alias = "clusterCapabilitiesVer")]
    pub cluster_capabilities_ver: Vec<i64>,
    #[serde(alias = "clusterCapabilities")]
    pub cluster_capabilities: HashMap<String, Vec<String>>,
    #[serde(alias = "clusterUUID")]
    pub cluster_uuid: Option<String>,
    #[serde(alias = "clusterName")]
    pub cluster_name: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[non_exhaustive]
pub struct CollectionManifestCollection {
    #[serde(rename = "uid")]
    pub uid: String,
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "maxTTL", default)]
    pub max_ttl: Option<i32>,
    #[serde(rename = "history", default)]
    pub history: Option<bool>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[non_exhaustive]
pub struct CollectionManifestScope {
    #[serde(rename = "uid")]
    pub uid: String,
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "collections", default)]
    pub collections: Vec<CollectionManifestCollection>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[non_exhaustive]
pub struct CollectionManifest {
    #[serde(rename = "uid")]
    pub uid: String,
    #[serde(rename = "scopes", default)]
    pub scopes: Vec<CollectionManifestScope>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FullNode {
    #[serde(rename = "clusterMembership", default)]
    pub cluster_membership: Option<String>,
    #[serde(rename = "recoveryType", default)]
    pub recovery_type: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(rename = "otpNode", default)]
    pub otp_node: Option<String>,
    #[serde(default)]
    pub hostname: Option<String>,
    #[serde(rename = "nodeUUID", default)]
    pub node_uuid: Option<String>,
    #[serde(rename = "clusterCompatibility", default)]
    pub cluster_compatibility: Option<u64>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub os: Option<String>,
    #[serde(rename = "cpuCount", default)]
    pub cpu_count: Option<u32>,
    #[serde(default)]
    pub ports: Option<HashMap<String, i32>>,
    #[serde(default)]
    pub services: Vec<String>,
    #[serde(rename = "nodeEncryption", default)]
    pub node_encryption: Option<bool>,
    #[serde(rename = "nodeEncryptionClientCertVerification", default)]
    pub node_encryption_client_cert_verification: Option<bool>,
    #[serde(rename = "addressFamilyOnly", default)]
    pub address_family_only: Option<bool>,
    #[serde(rename = "configuredHostname", default)]
    pub configured_hostname: Option<String>,
    #[serde(rename = "addressFamily", default)]
    pub address_family: Option<String>,
    #[serde(rename = "externalListeners", default)]
    pub external_listeners: Option<Vec<ExternalListener>>,
    #[serde(rename = "serverGroup", default)]
    pub server_group: Option<String>,
    #[serde(rename = "couchApiBase", default)]
    pub couch_api_base: Option<String>,
    #[serde(rename = "couchApiBaseHTTPS", default)]
    pub couch_api_base_https: Option<String>,
    #[serde(rename = "nodeHash", default)]
    pub node_hash: Option<u64>,
    #[serde(rename = "systemStats", default)]
    pub system_stats: Option<SystemStats>,
    #[serde(rename = "interestingStats", default)]
    pub interesting_stats: Option<InterestingStats>,
    #[serde(default)]
    pub uptime: Option<String>,
    #[serde(rename = "memoryTotal", default)]
    pub memory_total: Option<u64>,
    #[serde(rename = "memoryFree", default)]
    pub memory_free: Option<u64>,
    #[serde(rename = "mcdMemoryReserved", default)]
    pub mcd_memory_reserved: Option<u64>,
    #[serde(rename = "mcdMemoryAllocated", default)]
    pub mcd_memory_allocated: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExternalListener {
    #[serde(rename = "afamily", default)]
    pub afamily: Option<String>,
    #[serde(rename = "nodeEncryption", default)]
    pub node_encryption: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SystemStats {
    #[serde(rename = "cpu_utilization_rate", default)]
    pub cpu_utilization_rate: Option<f64>,
    #[serde(rename = "cpu_stolen_rate", default)]
    pub cpu_stolen_rate: Option<f64>,
    #[serde(rename = "swap_total", default)]
    pub swap_total: Option<u64>,
    #[serde(rename = "swap_used", default)]
    pub swap_used: Option<u64>,
    #[serde(rename = "mem_total", default)]
    pub mem_total: Option<u64>,
    #[serde(rename = "mem_free", default)]
    pub mem_free: Option<u64>,
    #[serde(rename = "mem_limit", default)]
    pub mem_limit: Option<u64>,
    #[serde(rename = "cpu_cores_available", default)]
    pub cpu_cores_available: Option<u32>,
    #[serde(default)]
    pub allocstall: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct InterestingStats {
    #[serde(default)]
    pub cmd_get: Option<u64>,
    #[serde(default)]
    pub couch_docs_actual_disk_size: Option<u64>,
    #[serde(default)]
    pub couch_docs_data_size: Option<u64>,
    #[serde(default)]
    pub couch_spatial_data_size: Option<u64>,
    #[serde(default)]
    pub couch_spatial_disk_size: Option<u64>,
    #[serde(default)]
    pub couch_views_actual_disk_size: Option<u64>,
    #[serde(default)]
    pub couch_views_data_size: Option<u64>,
    #[serde(default)]
    pub curr_items: Option<u64>,
    #[serde(default)]
    pub curr_items_tot: Option<u64>,
    #[serde(default)]
    pub ep_bg_fetched: Option<u64>,
    #[serde(default)]
    pub get_hits: Option<u64>,
    #[serde(default)]
    pub index_data_size: Option<u64>,
    #[serde(default)]
    pub index_disk_size: Option<u64>,
    #[serde(default)]
    pub mem_used: Option<u64>,
    #[serde(default)]
    pub ops: Option<u64>,
    #[serde(default)]
    pub vb_active_num_non_resident: Option<u64>,
    #[serde(default)]
    pub vb_replica_curr_items: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct FullBucketControllers {
    #[serde(rename = "compactAll", default)]
    pub compact_all: Option<String>,
    #[serde(rename = "compactDB", default)]
    pub compact_db: Option<String>,
    #[serde(rename = "purgeDeletes", default)]
    pub purge_deletes: Option<String>,
    #[serde(rename = "startRecovery", default)]
    pub start_recovery: Option<String>,
    #[serde(default)]
    pub flush: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct BucketStatsUris {
    #[serde(default)]
    pub uri: Option<String>,
    #[serde(rename = "directoryURI", default)]
    pub directory_uri: Option<String>,
    #[serde(rename = "nodeStatsListURI", default)]
    pub node_stats_list_uri: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct BucketBasicStats {
    #[serde(rename = "quotaPercentUsed", default)]
    pub quota_percent_used: Option<f64>,
    #[serde(rename = "opsPerSec", default)]
    pub ops_per_sec: Option<u64>,
    #[serde(rename = "diskFetches", default)]
    pub disk_fetches: Option<u64>,
    #[serde(rename = "itemCount", default)]
    pub item_count: Option<u64>,
    #[serde(rename = "diskUsed", default)]
    pub disk_used: Option<u64>,
    #[serde(rename = "dataUsed", default)]
    pub data_used: Option<u64>,
    #[serde(rename = "memUsed", default)]
    pub mem_used: Option<u64>,
    #[serde(rename = "vbActiveNumNonResident", default)]
    pub vb_active_num_non_resident: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FullBucketConfig {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(rename = "nodeLocator", default)]
    pub node_locator: Option<String>,
    #[serde(rename = "bucketType")]
    pub bucket_type: String,
    #[serde(rename = "storageBackend")]
    pub storage_backend: String,

    #[serde(default)]
    pub uuid: Option<String>,
    #[serde(default)]
    pub uri: Option<String>,
    #[serde(rename = "streamingUri", default)]
    pub streaming_uri: Option<String>,

    #[serde(rename = "numVBuckets", default)]
    pub num_vbuckets: Option<u32>,

    #[serde(rename = "bucketCapabilitiesVer", default)]
    pub bucket_capabilities_ver: Option<String>,
    #[serde(rename = "bucketCapabilities", default)]
    pub bucket_capabilities: Vec<String>,

    #[serde(rename = "collectionsManifestUid", default)]
    pub collections_manifest_uid: Option<String>,

    #[serde(default)]
    pub ddocs: Option<ConfigDDocs>,
    #[serde(rename = "vBucketServerMap", default)]
    pub vbucket_server_map: Option<VBucketServerMap>,

    #[serde(default)]
    pub nodes: Vec<FullNode>,

    #[serde(rename = "localRandomKeyUri", default)]
    pub local_random_key_uri: Option<String>,

    #[serde(default)]
    pub controllers: FullBucketControllers,

    #[serde(rename = "stats", default)]
    pub stats: Option<BucketStatsUris>,

    #[serde(rename = "authType", default)]
    pub auth_type: Option<String>,
    #[serde(rename = "autoCompactionSettings", default)]
    pub auto_compaction_settings: Option<bool>,

    #[serde(rename = "replicaIndex")]
    pub replica_index: bool,

    pub quota: FullBucketQuota,

    #[serde(default)]
    pub rank: Option<i64>,
    #[serde(rename = "enableCrossClusterVersioning", default)]
    pub enable_cross_cluster_versioning: Option<bool>,
    #[serde(rename = "versionPruningWindowHrs", default)]
    pub version_pruning_window_hrs: Option<u32>,

    #[serde(rename = "replicaNumber")]
    pub replica_number: u32,
    #[serde(rename = "threadsNumber", default)]
    pub threads_number: Option<u32>,

    #[serde(rename = "basicStats", default)]
    pub basic_stats: Option<BucketBasicStats>,

    #[serde(rename = "evictionPolicy")]
    pub eviction_policy: String,
    #[serde(rename = "durabilityMinLevel")]
    pub minimum_durability_level: String,
    #[serde(rename = "pitrEnabled", default)]
    pub pitr_enabled: Option<bool>,
    #[serde(rename = "pitrGranularity", default)]
    pub pitr_granularity: Option<u32>,
    #[serde(rename = "pitrMaxHistoryAge", default)]
    pub pitr_max_history_age: Option<u32>,

    #[serde(rename = "conflictResolutionType")]
    pub conflict_resolution_type: String,
    #[serde(rename = "maxTTL")]
    pub max_ttl: u32,
    #[serde(rename = "compressionMode")]
    pub compression_mode: String,

    #[serde(rename = "historyRetentionCollectionDefault", default)]
    pub history_retention_collection_default: Option<bool>,
    #[serde(rename = "historyRetentionBytes", default)]
    pub history_retention_bytes: Option<u64>,
    #[serde(rename = "historyRetentionSeconds", default)]
    pub history_retention_seconds: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FullBucketQuota {
    pub ram: u64,
    #[serde(rename = "rawRAM")]
    pub raw_ram: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BucketNames {
    #[serde(rename = "bucketName")]
    pub bucket_name: String,
    pub uuid: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FullClusterConfig {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub nodes: Vec<FullNode>,
    #[serde(rename = "bucketNames")]
    pub bucket_names: Vec<BucketNames>,
}
