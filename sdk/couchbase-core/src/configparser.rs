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

use crate::cbconfig::{TerseConfig, TerseExtNodePorts, VBucketServerMap};
use crate::clusterlabels::ClusterLabels;
use crate::error;
use crate::error::{ErrorKind, Result};
use crate::parsedconfig::{
    BucketType, ParsedConfig, ParsedConfigBucket, ParsedConfigBucketFeature, ParsedConfigFeature,
    ParsedConfigNode, ParsedConfigNodeAddresses, ParsedConfigNodePorts,
};
use crate::vbucketmap::VbucketMap;

pub(crate) struct ConfigParser {}

impl ConfigParser {
    pub fn parse_terse_config(config: TerseConfig, source_hostname: &str) -> Result<ParsedConfig> {
        let rev_id = config.rev;
        let rev_epoch = config.rev_epoch.unwrap_or_default();

        let len_nodes = if let Some(nodes) = config.nodes {
            nodes.len()
        } else {
            0
        };
        let mut nodes = Vec::with_capacity(config.nodes_ext.len());
        for (node_idx, node) in config.nodes_ext.into_iter().enumerate() {
            let node_hostname = Self::parse_config_hostname(&node.hostname, source_hostname);

            let mut alt_addresses = HashMap::new();
            for (network_type, alt_addrs) in node.alternate_addresses {
                let alt_hostname = Self::parse_config_hostname(&alt_addrs.hostname, &node_hostname);
                let this_address = Self::parse_config_hosts_into(&alt_hostname, alt_addrs.ports);

                alt_addresses.insert(network_type, this_address);
            }

            let this_node = ParsedConfigNode {
                has_data: node_idx < len_nodes,
                addresses: Self::parse_config_hosts_into(
                    &node_hostname,
                    node.services.unwrap_or_default(),
                ),
                alt_addresses,
            };

            nodes.push(this_node);
        }

        let bucket = if let Some(bucket_name) = config.name {
            let bucket_uuid = config.uuid.unwrap_or_default();
            let (bucket_type, vbucket_map) = if let Some(locator) = config.node_locator {
                match locator.as_str() {
                    "vbucket" => (
                        BucketType::Couchbase,
                        Self::parse_vbucket_server_map(config.vbucket_server_map)?,
                    ),
                    _ => (BucketType::Invalid, None),
                }
            } else {
                (BucketType::Invalid, None)
            };

            let mut features = vec![];
            if let Some(bucket_capabilities) = config.bucket_capabilities {
                for cap in bucket_capabilities {
                    let feat = ParsedConfigBucketFeature::from(cap);
                    if feat != ParsedConfigBucketFeature::Unknown {
                        features.push(feat);
                    }
                }
            }

            Some(ParsedConfigBucket {
                bucket_uuid,
                bucket_name,
                bucket_type,
                vbucket_map,
                features,
            })
        } else {
            None
        };

        let mut features = vec![];
        if let Some(caps) = config.cluster_capabilities.get("search") {
            if caps.contains(&"vectorSearch".to_string()) {
                features.push(ParsedConfigFeature::FtsVectorSearch);
            }
            if caps.contains(&"scoreFusion".to_string()) {
                features.push(ParsedConfigFeature::FtsScoreFusion);
            }
        }

        let cluster_labels = if config.cluster_name.is_some() || config.cluster_uuid.is_some() {
            Some(ClusterLabels {
                cluster_name: config.cluster_name,
                cluster_uuid: config.cluster_uuid,
            })
        } else {
            None
        };

        Ok(ParsedConfig {
            rev_id,
            rev_epoch,
            bucket,
            nodes,
            features,
            source_hostname: source_hostname.to_string(),
            cluster_labels,
        })
    }

    fn parse_config_hostname(hostname: &Option<String>, source_hostname: &str) -> String {
        if let Some(hostname) = hostname {
            if hostname.contains(':') {
                return format!("[{hostname}]");
            }

            hostname.to_string()
        } else {
            source_hostname.to_string()
        }
    }

    fn parse_config_hosts_into(
        hostname: &str,
        ports: TerseExtNodePorts,
    ) -> ParsedConfigNodeAddresses {
        ParsedConfigNodeAddresses {
            hostname: hostname.to_string(),
            non_ssl_ports: ParsedConfigNodePorts {
                kv: ports.kv,
                mgmt: ports.mgmt,
                analytics: ports.cbas,
                query: ports.n1ql,
                search: ports.fts,
            },
            ssl_ports: ParsedConfigNodePorts {
                kv: ports.kv_ssl,
                mgmt: ports.mgmt_ssl,
                analytics: ports.cbas_ssl,
                query: ports.n1ql_ssl,
                search: ports.fts_ssl,
            },
        }
    }

    fn parse_vbucket_server_map(
        vbucket_server_map: Option<VBucketServerMap>,
    ) -> Result<Option<VbucketMap>> {
        if let Some(vbucket_server_map) = vbucket_server_map {
            if vbucket_server_map.vbucket_map.is_empty() {
                return Err(error::Error::from(ErrorKind::NoVbucketMap));
            }

            return Ok(Some(VbucketMap::new(
                vbucket_server_map.vbucket_map,
                vbucket_server_map.num_replicas,
            )?));
        }

        Ok(None)
    }
}
