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
use crate::address::Address;
use crate::analyticscomponent::AnalyticsComponentConfig;
use crate::authenticator::Authenticator;
use crate::clusterlabels::ClusterLabels;
use crate::configmanager::ConfigManagerMemdConfig;
use crate::diagnosticscomponent::DiagnosticsComponentConfig;
use crate::kvclient_babysitter::KvTarget;
use crate::mgmtcomponent::MgmtComponentConfig;
use crate::parsedconfig::{ParsedConfig, ParsedConfigFeature};
use crate::querycomponent::{QueryComponentConfig, QueryNodeUuids};
use crate::searchcomponent::SearchComponentConfig;
use crate::service_type::ServiceType;
use crate::tls_config::TlsConfig;
use crate::tracingcomponent::TracingComponentConfig;
use crate::vbucketrouter::VbucketRoutingInfo;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub(crate) struct AgentComponentConfigs {
    pub kv_targets: HashMap<String, KvTarget>,
    pub auth: Authenticator,
    pub selected_bucket: Option<String>,

    pub config_manager_memd_config: ConfigManagerMemdConfig,
    pub vbucket_routing_info: VbucketRoutingInfo,
    pub analytics_config: AnalyticsComponentConfig,
    pub query_config: QueryComponentConfig,
    pub search_config: SearchComponentConfig,
    pub mgmt_config: MgmtComponentConfig,
    pub diagnostics_config: DiagnosticsComponentConfig,
    pub tracing_config: TracingComponentConfig,
}

pub(crate) struct HttpClientConfig {
    pub idle_connection_timeout: Duration,
    pub max_idle_connections_per_host: Option<usize>,
    pub tcp_keep_alive_time: Duration,
}

struct NetworkAndCanonicalAddress {
    network_address: Address,
    canonical_address: Address,
}

pub(crate) struct NetworkAndCanonicalEndpoint {
    pub(crate) network_endpoint: String,
    pub(crate) canonical_endpoint: String,
}

impl AgentComponentConfigs {
    pub fn gen_from_config(
        config: &ParsedConfig,
        network_type: &str,
        tls_config: Option<TlsConfig>,
        bucket_name: Option<String>,
        authenticator: Authenticator,
    ) -> AgentComponentConfigs {
        let rev_id = config.rev_id;
        let network_info = config.addresses_group_for_network_type(network_type);

        let mut gcccp_node_ids = Vec::new();
        let mut kv_data_node_ids: Vec<Arc<str>> = Vec::new();
        let mut kv_data_hosts: HashMap<String, NetworkAndCanonicalAddress> = HashMap::new();
        let mut mgmt_endpoints: HashMap<String, NetworkAndCanonicalEndpoint> = HashMap::new();
        let mut analytics_endpoints: HashMap<String, NetworkAndCanonicalEndpoint> = HashMap::new();
        let mut query_endpoints: HashMap<String, NetworkAndCanonicalEndpoint> = HashMap::new();
        let mut search_endpoints: HashMap<String, NetworkAndCanonicalEndpoint> = HashMap::new();
        let mut query_node_uuids: HashMap<String, String> = HashMap::new();

        for node in network_info.nodes {
            let kv_ep_id = format!("kv{}", node.node_id);
            let mgmt_ep_id = format!("mgmt{}", node.node_id);
            let analytics_ep_id = format!("analytics{}", node.node_id);
            let query_ep_id = format!("query{}", node.node_id);
            let search_ep_id = format!("search{}", node.node_id);

            gcccp_node_ids.push(kv_ep_id.clone());

            if node.has_data {
                kv_data_node_ids.push(Arc::from(kv_ep_id.as_str()));
            }

            if tls_config.is_some() {
                if let Some(p) = node.ssl_ports.kv {
                    kv_data_hosts.insert(
                        kv_ep_id,
                        NetworkAndCanonicalAddress {
                            network_address: Address {
                                host: node.hostname.clone(),
                                port: p,
                            },
                            canonical_address: Address {
                                host: node.canonical_node_info.hostname.clone(),
                                port: node.canonical_node_info.ssl_ports.kv.unwrap_or(p),
                            },
                        },
                    );
                }
                if let Some(p) = node.ssl_ports.mgmt {
                    mgmt_endpoints.insert(
                        mgmt_ep_id,
                        NetworkAndCanonicalEndpoint {
                            network_endpoint: format!("https://{}:{}", node.hostname, p),
                            canonical_endpoint: format!(
                                "https://{}:{}",
                                node.canonical_node_info.hostname,
                                node.canonical_node_info.ssl_ports.mgmt.unwrap_or(p),
                            ),
                        },
                    );
                }
                if let Some(p) = node.ssl_ports.analytics {
                    analytics_endpoints.insert(
                        analytics_ep_id,
                        NetworkAndCanonicalEndpoint {
                            network_endpoint: format!("https://{}:{}", node.hostname, p),
                            canonical_endpoint: format!(
                                "https://{}:{}",
                                node.canonical_node_info.hostname,
                                node.canonical_node_info.ssl_ports.analytics.unwrap_or(p),
                            ),
                        },
                    );
                }
                if let Some(p) = node.ssl_ports.query {
                    query_endpoints.insert(
                        query_ep_id,
                        NetworkAndCanonicalEndpoint {
                            network_endpoint: format!("https://{}:{}", node.hostname, p),
                            canonical_endpoint: format!(
                                "https://{}:{}",
                                node.canonical_node_info.hostname,
                                node.canonical_node_info.ssl_ports.query.unwrap_or(p),
                            ),
                        },
                    );
                }
                if let Some(p) = node.ssl_ports.search {
                    search_endpoints.insert(
                        search_ep_id,
                        NetworkAndCanonicalEndpoint {
                            network_endpoint: format!("https://{}:{}", node.hostname, p),
                            canonical_endpoint: format!(
                                "https://{}:{}",
                                node.canonical_node_info.hostname,
                                node.canonical_node_info.ssl_ports.search.unwrap_or(p),
                            ),
                        },
                    );
                }
            } else {
                if let Some(p) = node.non_ssl_ports.kv {
                    kv_data_hosts.insert(
                        kv_ep_id,
                        NetworkAndCanonicalAddress {
                            network_address: Address {
                                host: node.hostname.clone(),
                                port: p,
                            },
                            canonical_address: Address {
                                host: node.canonical_node_info.hostname.clone(),
                                port: node.canonical_node_info.non_ssl_ports.kv.unwrap_or(p),
                            },
                        },
                    );
                }
                if let Some(p) = node.non_ssl_ports.mgmt {
                    mgmt_endpoints.insert(
                        mgmt_ep_id,
                        NetworkAndCanonicalEndpoint {
                            network_endpoint: format!("http://{}:{}", node.hostname, p),
                            canonical_endpoint: format!(
                                "http://{}:{}",
                                node.canonical_node_info.hostname,
                                node.canonical_node_info.non_ssl_ports.mgmt.unwrap_or(p),
                            ),
                        },
                    );
                }
                if let Some(p) = node.non_ssl_ports.analytics {
                    analytics_endpoints.insert(
                        analytics_ep_id,
                        NetworkAndCanonicalEndpoint {
                            network_endpoint: format!("http://{}:{}", node.hostname, p),
                            canonical_endpoint: format!(
                                "http://{}:{}",
                                node.canonical_node_info.hostname,
                                node.canonical_node_info
                                    .non_ssl_ports
                                    .analytics
                                    .unwrap_or(p)
                            ),
                        },
                    );
                }
                if let Some(p) = node.non_ssl_ports.query {
                    query_endpoints.insert(
                        query_ep_id,
                        NetworkAndCanonicalEndpoint {
                            network_endpoint: format!("http://{}:{}", node.hostname, p),
                            canonical_endpoint: format!(
                                "http://{}:{}",
                                node.canonical_node_info.hostname,
                                node.canonical_node_info.non_ssl_ports.query.unwrap_or(p),
                            ),
                        },
                    );
                }
                if let Some(p) = node.non_ssl_ports.search {
                    search_endpoints.insert(
                        search_ep_id,
                        NetworkAndCanonicalEndpoint {
                            network_endpoint: format!("http://{}:{}", node.hostname, p),
                            canonical_endpoint: format!(
                                "http://{}:{}",
                                node.canonical_node_info.hostname,
                                node.canonical_node_info.non_ssl_ports.search.unwrap_or(p),
                            ),
                        },
                    );
                }
            }

            if let Some(node_uuid) = node.node_uuid {
                let query_ep_id = format!("query{}", node.node_id);
                if query_endpoints.contains_key(&query_ep_id) {
                    query_node_uuids.insert(node_uuid, query_ep_id);
                }
            }
        }

        let mut kv_targets = HashMap::new();
        for (node_id, addresses) in kv_data_hosts {
            let target = KvTarget {
                address: addresses.network_address,
                canonical_address: addresses.canonical_address,
                tls_config: tls_config.clone(),
            };

            kv_targets.insert(node_id, target);
        }

        let vbucket_routing_info = if let Some(info) = &config.bucket {
            VbucketRoutingInfo {
                vbucket_info: info.vbucket_map.clone(),
                server_list: kv_data_node_ids,
                bucket_selected: true,
            }
        } else {
            VbucketRoutingInfo {
                vbucket_info: None,
                server_list: kv_data_node_ids,
                bucket_selected: false,
            }
        };

        let mut available_services = vec![ServiceType::MEMD];
        if !query_endpoints.is_empty() {
            available_services.push(ServiceType::QUERY)
        }
        if !search_endpoints.is_empty() {
            available_services.push(ServiceType::SEARCH)
        }

        let cluster_labels = config
            .cluster_labels
            .as_ref()
            .map(|cluster_labels| ClusterLabels {
                cluster_uuid: cluster_labels.cluster_uuid.clone(),
                cluster_name: cluster_labels.cluster_name.clone(),
            });

        AgentComponentConfigs {
            kv_targets,
            auth: authenticator.clone(),
            selected_bucket: bucket_name.clone(),
            config_manager_memd_config: ConfigManagerMemdConfig {
                endpoints: gcccp_node_ids,
            },

            vbucket_routing_info,
            analytics_config: AnalyticsComponentConfig {
                endpoints: analytics_endpoints,
                authenticator: authenticator.clone(),
            },
            query_config: QueryComponentConfig {
                node_uuids: QueryNodeUuids::new(query_node_uuids, &query_endpoints),
                endpoints: query_endpoints,
                authenticator: authenticator.clone(),
                conversational_query_supported: config
                    .features
                    .contains(&ParsedConfigFeature::QueryConversationalQuery),
            },
            search_config: SearchComponentConfig {
                endpoints: search_endpoints,
                authenticator: authenticator.clone(),
                vector_search_enabled: config
                    .features
                    .contains(&ParsedConfigFeature::FtsVectorSearch),
                score_fusion_enabled: config
                    .features
                    .contains(&ParsedConfigFeature::FtsScoreFusion),
                custom_script_queries_enabled: config
                    .features
                    .contains(&ParsedConfigFeature::FtsCustomScriptQueries),
            },
            mgmt_config: MgmtComponentConfig {
                endpoints: mgmt_endpoints,
                authenticator: authenticator.clone(),
            },
            diagnostics_config: DiagnosticsComponentConfig {
                bucket: bucket_name,
                services: available_services,
                rev_id,
            },
            tracing_config: TracingComponentConfig { cluster_labels },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::authenticator::{Authenticator, PasswordAuthenticator};
    use crate::cbconfig;
    use crate::componentconfigs::AgentComponentConfigs;
    use crate::configparser::ConfigParser;
    use std::collections::HashMap;

    fn gen_query_node_uuids(network_type: &str) -> HashMap<String, String> {
        let config = br#"{
            "rev": 1,
            "nodesExt": [
                {
                    "services": {"mgmt": 8091, "kv": 11210, "n1ql": 8093},
                    "hostname": "10.0.0.1",
                    "nodeUUID": "uuid-a",
                    "alternateAddresses": {
                        "external": {"hostname": "ext-a", "ports": {"mgmt": 18091, "n1ql": 18093}}
                    }
                },
                {
                    "services": {"mgmt": 8091, "kv": 11210, "n1ql": 8093},
                    "hostname": "10.0.0.2",
                    "nodeUUID": "uuid-b",
                    "alternateAddresses": {
                        "external": {"hostname": "ext-b", "ports": {"mgmt": 18091}}
                    }
                },
                {
                    "services": {"mgmt": 8091, "kv": 11210, "n1ql": 8093},
                    "hostname": "10.0.0.3"
                },
                {
                    "services": {"mgmt": 8091, "kv": 11210},
                    "hostname": "10.0.0.4",
                    "nodeUUID": "uuid-d"
                }
            ],
            "clusterCapabilitiesVer": [],
            "clusterCapabilities": {}
        }"#;

        let config = cbconfig::parse::parse_terse_config(config, "10.0.0.1").unwrap();
        let parsed = ConfigParser::parse_terse_config(config, "10.0.0.1").unwrap();

        let configs = AgentComponentConfigs::gen_from_config(
            &parsed,
            network_type,
            None,
            None,
            Authenticator::PasswordAuthenticator(PasswordAuthenticator {
                username: "user".to_string(),
                password: "pass".to_string(),
            }),
        );

        let query_config = configs.query_config;
        for endpoint_id in query_config.node_uuids.endpoint_ids.values() {
            assert!(query_config.endpoints.contains_key(endpoint_id));
        }

        assert!(!query_config.node_uuids.shared_endpoint);
        query_config.node_uuids.endpoint_ids
    }

    #[test]
    fn query_node_uuids_only_include_nodes_with_query_endpoints() {
        let node_uuids = gen_query_node_uuids("default");

        assert_eq!(
            HashMap::from([
                ("uuid-a".to_string(), "queryep-10.0.0.1-8091".to_string()),
                ("uuid-b".to_string(), "queryep-10.0.0.2-8091".to_string()),
            ]),
            node_uuids
        );
    }

    #[test]
    fn query_node_uuids_follow_selected_network() {
        let node_uuids = gen_query_node_uuids("external");

        assert_eq!(
            HashMap::from([("uuid-a".to_string(), "queryep-10.0.0.1-8091".to_string())]),
            node_uuids
        );
    }

    #[test]
    fn query_node_uuids_detect_shared_endpoint() {
        // Capella Private Link exposes every node through the same alternate address.
        let config = br#"{
            "rev": 1,
            "nodesExt": [
                {
                    "services": {"mgmt": 8091, "n1ql": 8093},
                    "hostname": "10.0.0.1",
                    "nodeUUID": "uuid-a",
                    "alternateAddresses": {
                        "external": {"hostname": "pl.example", "ports": {"mgmt": 18091, "n1ql": 18093}}
                    }
                },
                {
                    "services": {"mgmt": 8091, "n1ql": 8093},
                    "hostname": "10.0.0.2",
                    "nodeUUID": "uuid-b",
                    "alternateAddresses": {
                        "external": {"hostname": "pl.example", "ports": {"mgmt": 18091, "n1ql": 18093}}
                    }
                }
            ],
            "clusterCapabilitiesVer": [],
            "clusterCapabilities": {}
        }"#;

        let config = cbconfig::parse::parse_terse_config(config, "10.0.0.1").unwrap();
        let parsed = ConfigParser::parse_terse_config(config, "10.0.0.1").unwrap();

        let gen = |network_type: &str| {
            AgentComponentConfigs::gen_from_config(
                &parsed,
                network_type,
                None,
                None,
                Authenticator::PasswordAuthenticator(PasswordAuthenticator {
                    username: "user".to_string(),
                    password: "pass".to_string(),
                }),
            )
            .query_config
            .node_uuids
        };

        let external = gen("external");
        assert_eq!(2, external.endpoint_ids.len());
        assert!(external.shared_endpoint);

        // The internal addresses are distinct, so the default network is unaffected.
        assert!(!gen("default").shared_endpoint);
    }
}
