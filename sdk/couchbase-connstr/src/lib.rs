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

pub mod error;

use error::ErrorKind;
use regex::Regex;
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Display, Formatter};
use url::form_urlencoded;
#[cfg(feature = "dns-srv")]
use {
    hickory_resolver::config::*, hickory_resolver::name_server::TokioConnectionProvider,
    hickory_resolver::proto::xfer::Protocol, hickory_resolver::system_conf::read_system_conf,
    hickory_resolver::TokioResolver, std::net::SocketAddr, std::time::Duration, tracing::debug,
};

pub const DEFAULT_LEGACY_HTTP_PORT: u16 = 8091;
pub const DEFAULT_LEGACY_HTTPS_PORT: u16 = 18091;
pub const DEFAULT_MEMD_PORT: u16 = 11210;
pub const DEFAULT_SSL_MEMD_PORT: u16 = 11207;
pub const DEFAULT_COUCHBASE2_PORT: u16 = 18098;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ConnSpec {
    scheme: Option<String>,
    hosts: Vec<ConnSpecAddress>,
    options: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Address {
    pub host: String,
    pub port: u16,
}

#[cfg(feature = "dns-srv")]
#[derive(Debug, Clone, PartialEq)]
pub struct DnsConfig {
    pub namespace: SocketAddr,
    pub timeout: Option<Duration>,
}

impl Display for Address {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') && !self.host.starts_with('[') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ConnSpecAddress {
    host: String,
    port: Option<u16>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SrvRecord {
    pub proto: String,
    pub scheme: String,
    pub host: String,
}

#[cfg(feature = "dns-srv")]
impl ConnSpec {
    fn srv_record(&self) -> Option<SrvRecord> {
        if let Some(scheme_type) = &self.scheme {
            let scheme = scheme_type.as_str();
            if (scheme != "couchbase" && scheme != "couchbases")
                || self.hosts.len() != 1
                || self.hosts[0].port.is_some()
            {
                return None;
            }

            let host = &self.hosts[0].host;
            if host_is_ip_address(host) {
                return None;
            }

            return Some(SrvRecord {
                scheme: scheme_type.clone(),
                proto: "tcp".to_string(),
                host: host.clone(),
            });
        }

        None
    }
}

impl Display for ConnSpec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let scheme = self
            .scheme
            .clone()
            .map(|scheme| format!("{scheme}://"))
            .unwrap_or_default();

        let hosts = self
            .hosts
            .iter()
            .map(|host| {
                if let Some(port) = &host.port {
                    if host.host.contains(':') && !host.host.starts_with('[') {
                        format!("[{}]:{}", host.host, port)
                    } else {
                        format!("{}:{}", host.host, port)
                    }
                } else {
                    host.host.clone()
                }
            })
            .collect::<Vec<String>>()
            .join(",");

        let mut url_options = self.options.iter().fold(String::new(), |acc, (k, v)| {
            let values = v
                .iter()
                .map(|value| format!("{k}={value}"))
                .collect::<Vec<String>>()
                .join("&");
            if acc.is_empty() {
                values
            } else {
                format!("{acc}&{values}")
            }
        });
        if !url_options.is_empty() {
            url_options = format!("?{url_options}");
        }

        let out = format!("{scheme}{hosts}{url_options}");

        write!(f, "{out}")
    }
}

pub fn parse(conn_str: impl AsRef<str>) -> error::Result<ConnSpec> {
    let conn_str = conn_str.as_ref();

    let parts_matcher =
        Regex::new(r"((.*)://)?(([^/?:]*)(:([^/?:@]*))?@)?([^/?]*)(/([^?]*))?(\?(.*))?").unwrap();
    let host_matcher = Regex::new(r"((\[[^]]+]+)|([^;,:]+))(:([0-9]*))?(;,)?").unwrap();

    if let Some(parts) = parts_matcher.captures(conn_str) {
        let scheme = parts.get(2).map(|m| m.as_str().to_string());

        let hosts = if let Some(hosts) = parts.get(7) {
            let mut addresses = vec![];
            for host_info in host_matcher.captures_iter(hosts.as_str()) {
                let mut address = ConnSpecAddress {
                    host: host_info[1].to_string(),
                    port: None,
                };

                if let Some(port) = host_info.get(5) {
                    address.port = Some(
                        port.as_str()
                            .parse()
                            .map_err(|e| ErrorKind::Parse(format!("failed to parse port: {e}")))?,
                    );
                }

                addresses.push(address);
            }
            addresses
        } else {
            vec![]
        };

        let options = if let Some(options) = parts.get(11) {
            form_urlencoded::parse(options.as_str().as_bytes())
                .into_owned()
                .fold(
                    HashMap::new(),
                    |mut acc: HashMap<String, Vec<String>>, (k, v)| {
                        acc.entry(k).or_default().push(v);
                        acc
                    },
                )
        } else {
            HashMap::default()
        };

        return Ok(ConnSpec {
            scheme,
            hosts,
            options,
        });
    }

    Ok(ConnSpec::default())
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ResolvedConnSpec {
    pub use_ssl: bool,
    pub memd_hosts: Vec<Address>,
    pub http_hosts: Vec<Address>,
    pub couchbase2_host: Option<Address>,
    pub srv_record: Option<SrvRecord>,
    pub options: HashMap<String, Vec<String>>,
}

#[cfg(feature = "dns-srv")]
pub async fn resolve(
    conn_spec: ConnSpec,
    dns_config: impl Into<Option<DnsConfig>>,
) -> error::Result<ResolvedConnSpec> {
    let (default_port, has_explicit_scheme, use_ssl) = if let Some(scheme) = &conn_spec.scheme {
        match scheme.as_str() {
            "couchbase" => (DEFAULT_MEMD_PORT, true, false),
            "couchbases" => (DEFAULT_SSL_MEMD_PORT, true, true),
            "couchbase2" => {
                return handle_couchbase2_scheme(conn_spec);
            }
            "" => (DEFAULT_MEMD_PORT, false, false),
            _ => {
                return Err(ErrorKind::InvalidArgument {
                    msg: "unrecognized scheme".to_string(),
                    arg: "scheme".to_string(),
                }
                .into());
            }
        }
    } else {
        (DEFAULT_MEMD_PORT, false, false)
    };

    if let Some(srv_record) = conn_spec.srv_record() {
        match lookup_srv(
            &srv_record.scheme,
            &srv_record.proto,
            &srv_record.host,
            dns_config.into(),
        )
        .await
        {
            Ok(srv_records) => {
                return Ok(ResolvedConnSpec {
                    use_ssl,
                    memd_hosts: srv_records,
                    http_hosts: vec![],
                    couchbase2_host: None,
                    srv_record: Some(SrvRecord {
                        proto: srv_record.proto,
                        scheme: srv_record.scheme,
                        host: srv_record.host,
                    }),
                    options: conn_spec.options,
                });
            }
            Err(e) => {
                debug!("Srv lookup failed {e}");
            }
        };
    };

    resolve_without_srv(conn_spec, default_port, has_explicit_scheme, use_ssl)
}

#[cfg(not(feature = "dns-srv"))]
pub async fn resolve(conn_spec: ConnSpec) -> error::Result<ResolvedConnSpec> {
    let (default_port, has_explicit_scheme, use_ssl) = if let Some(scheme) = &conn_spec.scheme {
        match scheme.as_str() {
            "couchbase" => (DEFAULT_MEMD_PORT, true, false),
            "couchbases" => (DEFAULT_SSL_MEMD_PORT, true, true),
            "couchbase2" => {
                return handle_couchbase2_scheme(conn_spec);
            }
            "" => (DEFAULT_MEMD_PORT, false, false),
            _ => {
                return Err(ErrorKind::InvalidArgument {
                    msg: "unrecognized scheme".to_string(),
                    arg: "scheme".to_string(),
                }
                .into());
            }
        }
    } else {
        (DEFAULT_MEMD_PORT, false, false)
    };

    resolve_without_srv(conn_spec, default_port, has_explicit_scheme, use_ssl)
}

fn resolve_without_srv(
    conn_spec: ConnSpec,
    default_port: u16,
    has_explicit_scheme: bool,
    use_ssl: bool,
) -> error::Result<ResolvedConnSpec> {
    if conn_spec.hosts.is_empty() {
        let (memd_port, http_port) = if use_ssl {
            (DEFAULT_SSL_MEMD_PORT, DEFAULT_LEGACY_HTTPS_PORT)
        } else {
            (DEFAULT_MEMD_PORT, DEFAULT_LEGACY_HTTP_PORT)
        };

        return Ok(ResolvedConnSpec {
            use_ssl,
            memd_hosts: vec![Address {
                host: "127.0.0.1".to_string(),
                port: memd_port,
            }],
            http_hosts: vec![Address {
                host: "127.0.0.1".to_string(),
                port: http_port,
            }],
            couchbase2_host: None,
            srv_record: None,
            options: conn_spec.options,
        });
    }

    let mut memd_hosts = vec![];
    let mut http_hosts = vec![];
    for address in conn_spec.hosts {
        if let Some(port) = &address.port {
            if *port == DEFAULT_LEGACY_HTTP_PORT {
                return Err(ErrorKind::InvalidArgument{msg: "couchbase://host:8091 not supported for couchbase:// scheme. Use couchbase://host".to_string(), arg: "port".to_string()}.into());
            }

            if !has_explicit_scheme && address.port != Some(default_port) {
                return Err(ErrorKind::InvalidArgument {
                    msg: "ambiguous port without scheme".to_string(),
                    arg: "port".to_string(),
                }
                .into());
            }

            memd_hosts.push(Address {
                host: address.host,
                port: *port,
            });
        } else {
            let (memd_port, http_port) = if use_ssl {
                (DEFAULT_SSL_MEMD_PORT, DEFAULT_LEGACY_HTTPS_PORT)
            } else {
                (DEFAULT_MEMD_PORT, DEFAULT_LEGACY_HTTP_PORT)
            };

            memd_hosts.push(Address {
                host: address.host.clone(),
                port: memd_port,
            });

            http_hosts.push(Address {
                host: address.host,
                port: http_port,
            });
        }
    }

    Ok(ResolvedConnSpec {
        use_ssl,
        memd_hosts,
        http_hosts,
        couchbase2_host: None,
        srv_record: None,
        options: conn_spec.options,
    })
}

fn handle_couchbase2_scheme(conn_spec: ConnSpec) -> error::Result<ResolvedConnSpec> {
    if conn_spec.hosts.len() > 1 {
        return Err(ErrorKind::InvalidArgument {
            msg: "couchbase2 scheme can only be used with a single host".to_string(),
            arg: "scheme".to_string(),
        }
        .into());
    }

    let host = if conn_spec.hosts.is_empty() {
        Address {
            host: "127.0.0.1".to_string(),
            port: DEFAULT_COUCHBASE2_PORT,
        }
    } else {
        let address = conn_spec.hosts[0].clone();
        if let Some(port) = &address.port {
            Address {
                host: address.host,
                port: *port,
            }
        } else {
            Address {
                host: address.host,
                port: DEFAULT_COUCHBASE2_PORT,
            }
        }
    };

    Ok(ResolvedConnSpec {
        use_ssl: true,
        memd_hosts: vec![],
        http_hosts: vec![],
        couchbase2_host: Some(host),
        srv_record: None,
        options: conn_spec.options,
    })
}

#[cfg(feature = "dns-srv")]
async fn lookup_srv(
    scheme: &str,
    proto: &str,
    host: &str,
    dns_config: Option<DnsConfig>,
) -> error::Result<Vec<Address>> {
    let (resolver_config, resolver_opts) = match dns_config {
        Some(dns) => {
            let mut group = NameServerConfigGroup::with_capacity(2);
            let udp = NameServerConfig::new(dns.namespace, Protocol::Udp);
            let tcp = NameServerConfig::new(dns.namespace, Protocol::Tcp);
            group.push(udp);
            group.push(tcp);

            let config = ResolverConfig::from_parts(None, vec![], group);

            let mut opts = ResolverOpts::default();
            if let Some(timeout) = dns.timeout {
                opts.timeout = timeout;
            }

            (config, opts)
        }
        None => read_system_conf().map_err(ErrorKind::Resolve)?,
    };

    let resolver =
        TokioResolver::builder_with_config(resolver_config, TokioConnectionProvider::default())
            .with_options(resolver_opts)
            .build();

    let name = format!("_{scheme}._{proto}.{host}");
    let response = resolver.srv_lookup(name).await?;

    let mut addresses = vec![];
    for addr in response.iter() {
        addresses.push(Address {
            host: addr.target().to_string(),
            port: addr.port(),
        });
    }

    Ok(addresses)
}

#[cfg(feature = "dns-srv")]
fn host_is_ip_address(host: &str) -> bool {
    host.starts_with('[') || host.parse::<std::net::IpAddr>().is_ok()
}

#[cfg(test)]
mod test {
    use crate::{
        parse, resolve, Address, ConnSpec, ConnSpecAddress, ResolvedConnSpec,
        DEFAULT_COUCHBASE2_PORT, DEFAULT_MEMD_PORT, DEFAULT_SSL_MEMD_PORT,
    };
    use std::collections::HashMap;

    fn parse_or_die(conn_str: &str) -> ConnSpec {
        parse(conn_str).unwrap_or_else(|e| panic!("Failed to parse {conn_str}: {e:?}"))
    }

    async fn resolve_or_die(conn_spec: ConnSpec) -> ResolvedConnSpec {
        #[cfg(feature = "dns-srv")]
        let result = resolve(conn_spec.clone(), None)
            .await
            .unwrap_or_else(|e| panic!("Failed to resolve {conn_spec:?}: {e:?}"));
        #[cfg(not(feature = "dns-srv"))]
        let result = resolve(conn_spec.clone())
            .await
            .unwrap_or_else(|e| panic!("Failed to resolve {conn_spec:?}: {e:?}"));
        result
    }

    async fn resolve_is_err(conn_spec: ConnSpec) -> bool {
        #[cfg(feature = "dns-srv")]
        return resolve(conn_spec, None).await.is_err();
        #[cfg(not(feature = "dns-srv"))]
        return resolve(conn_spec).await.is_err();
    }

    fn check_address_parsing(
        conn_str: &str,
        cs: &ConnSpec,
        expected_spec: &ConnSpec,
        check_str: bool,
    ) {
        if check_str && cs.to_string() != conn_str {
            panic!("ConnStr round-trip should match. {cs} != {conn_str}");
        }

        assert_eq!(cs.scheme, expected_spec.scheme, "Parsed incorrect scheme");
        assert_eq!(
            cs.hosts.len(),
            expected_spec.hosts.len(),
            "Some addresses were not parsed"
        );

        for (cs_addr, expected_addr) in cs.hosts.iter().zip(expected_spec.hosts.iter()) {
            assert_eq!(cs_addr.host, expected_addr.host, "Parsed incorrect host");
            assert_eq!(cs_addr.port, expected_addr.port, "Parsed incorrect port");
        }
    }

    fn check_option_parsing(cs: &ConnSpec, expected_spec: &ConnSpec) {
        assert_eq!(
            cs.options.len(),
            expected_spec.options.len(),
            "Some options were not parsed"
        );

        for (key, opts) in &cs.options {
            let expected_opts = expected_spec
                .options
                .get(key)
                .expect("Missing expected option");
            assert_eq!(
                opts.len(),
                expected_opts.len(),
                "Some option values were not parsed"
            );

            for (opt, expected_opt) in opts.iter().zip(expected_opts.iter()) {
                assert_eq!(opt, expected_opt, "Parsed incorrect option value");
            }
        }
    }

    async fn check_default_spec(
        conn_str: &str,
        expected_spec: ConnSpec,
        expect_memd_hosts: Vec<Address>,
        use_ssl: bool,
        check_hosts: bool,
        check_str: bool,
    ) {
        let cs = parse_or_die(conn_str);

        check_address_parsing(conn_str, &cs, &expected_spec, check_str);
        check_option_parsing(&cs, &expected_spec);

        let rcs = resolve_or_die(cs).await;

        assert_eq!(rcs.use_ssl, use_ssl, "Did not correctly mark SSL");

        if check_hosts {
            assert_eq!(
                rcs.memd_hosts.len(),
                expect_memd_hosts.len(),
                "Some memd hosts were missing"
            );
            for (host, expect_host) in rcs.memd_hosts.iter().zip(expect_memd_hosts.iter()) {
                assert_eq!(host.host, expect_host.host, "Resolved incorrect memd host");
                assert_eq!(host.port, expect_host.port, "Resolved incorrect memd port");
            }
        }
    }

    async fn check_couchbase2_server_spec(
        conn_str: &str,
        expected_spec: ConnSpec,
        expect_address: Address,
    ) {
        let cs = parse_or_die(conn_str);

        check_address_parsing(conn_str, &cs, &expected_spec, true);
        check_option_parsing(&cs, &expected_spec);

        let rcs = resolve_or_die(cs).await;

        assert!(rcs.couchbase2_host.is_some(), "Couchbase2 host was missing");
        let couchbase2_host = rcs.couchbase2_host.unwrap();
        assert_eq!(
            couchbase2_host.host, expect_address.host,
            "Resolved incorrect couchbase2 host"
        );
        assert_eq!(
            couchbase2_host.port, expect_address.port,
            "Resolved incorrect couchbase2 port"
        );
    }

    #[tokio::test]
    async fn test_parse_basic() {
        check_default_spec(
            "couchbase://1.2.3.4",
            ConnSpec {
                scheme: Some("couchbase".to_string()),
                hosts: vec![ConnSpecAddress {
                    host: "1.2.3.4".to_string(),
                    port: None,
                }],
                ..Default::default()
            },
            vec![Address {
                host: "1.2.3.4".to_string(),
                port: DEFAULT_MEMD_PORT,
            }],
            false,
            true,
            true,
        )
        .await;

        check_default_spec(
            "couchbase://[2001:4860:4860::8888]",
            ConnSpec {
                scheme: Some("couchbase".to_string()),
                hosts: vec![ConnSpecAddress {
                    host: "[2001:4860:4860::8888]".to_string(),
                    port: None,
                }],
                ..Default::default()
            },
            vec![Address {
                host: "[2001:4860:4860::8888]".to_string(),
                port: DEFAULT_MEMD_PORT,
            }],
            false,
            true,
            true,
        )
        .await;

        check_default_spec(
            "couchbase://",
            ConnSpec {
                scheme: Some("couchbase".to_string()),
                ..Default::default()
            },
            vec![Address {
                host: "127.0.0.1".to_string(),
                port: DEFAULT_MEMD_PORT,
            }],
            false,
            true,
            true,
        )
        .await;

        check_default_spec(
            "couchbase://?",
            ConnSpec {
                scheme: Some("couchbase".to_string()),
                ..Default::default()
            },
            vec![Address {
                host: "127.0.0.1".to_string(),
                port: DEFAULT_MEMD_PORT,
            }],
            false,
            true,
            false,
        )
        .await;

        check_default_spec(
            "1.2.3.4",
            ConnSpec {
                hosts: vec![ConnSpecAddress {
                    host: "1.2.3.4".to_string(),
                    port: None,
                }],
                ..Default::default()
            },
            vec![Address {
                host: "1.2.3.4".to_string(),
                port: DEFAULT_MEMD_PORT,
            }],
            false,
            true,
            true,
        )
        .await;

        check_default_spec(
            "[2001:4860:4860::8888]",
            ConnSpec {
                hosts: vec![ConnSpecAddress {
                    host: "[2001:4860:4860::8888]".to_string(),
                    port: None,
                }],
                ..Default::default()
            },
            vec![Address {
                host: "[2001:4860:4860::8888]".to_string(),
                port: DEFAULT_MEMD_PORT,
            }],
            false,
            true,
            true,
        )
        .await;

        let cs = parse_or_die("1.2.3.4:8091");
        assert!(resolve_is_err(cs).await, "Expected error with http port");

        let cs = parse_or_die("1.2.3.4:999");
        assert!(
            resolve_is_err(cs).await,
            "Expected error with non-default port without scheme"
        );
    }

    #[tokio::test]
    async fn test_parse_hosts() {
        check_default_spec(
            "couchbase://foo.com,bar.com,baz.com",
            ConnSpec {
                scheme: Some("couchbase".to_string()),
                hosts: vec![
                    ConnSpecAddress {
                        host: "foo.com".to_string(),
                        port: None,
                    },
                    ConnSpecAddress {
                        host: "bar.com".to_string(),
                        port: None,
                    },
                    ConnSpecAddress {
                        host: "baz.com".to_string(),
                        port: None,
                    },
                ],
                ..Default::default()
            },
            vec![
                Address {
                    host: "foo.com".to_string(),
                    port: DEFAULT_MEMD_PORT,
                },
                Address {
                    host: "bar.com".to_string(),
                    port: DEFAULT_MEMD_PORT,
                },
                Address {
                    host: "baz.com".to_string(),
                    port: DEFAULT_MEMD_PORT,
                },
            ],
            false,
            true,
            true,
        )
        .await;

        check_default_spec(
            "couchbase://[2001:4860:4860::8822],[2001:4860:4860::8833]:888",
            ConnSpec {
                scheme: Some("couchbase".to_string()),
                hosts: vec![
                    ConnSpecAddress {
                        host: "[2001:4860:4860::8822]".to_string(),
                        port: None,
                    },
                    ConnSpecAddress {
                        host: "[2001:4860:4860::8833]".to_string(),
                        port: Some(888),
                    },
                ],
                ..Default::default()
            },
            vec![
                Address {
                    host: "[2001:4860:4860::8822]".to_string(),
                    port: DEFAULT_MEMD_PORT,
                },
                Address {
                    host: "[2001:4860:4860::8833]".to_string(),
                    port: 888,
                },
            ],
            false,
            true,
            true,
        )
        .await;

        let cs = parse_or_die("couchbase://foo.com:8091");
        assert!(
            resolve_is_err(cs).await,
            "Expected error for couchbase://XXX:8091"
        );

        check_default_spec(
            "couchbase://foo.com:4444",
            ConnSpec {
                scheme: Some("couchbase".to_string()),
                hosts: vec![ConnSpecAddress {
                    host: "foo.com".to_string(),
                    port: Some(4444),
                }],
                ..Default::default()
            },
            vec![Address {
                host: "foo.com".to_string(),
                port: 4444,
            }],
            false,
            true,
            true,
        )
        .await;

        check_default_spec(
            "couchbases://foo.com:4444",
            ConnSpec {
                scheme: Some("couchbases".to_string()),
                hosts: vec![ConnSpecAddress {
                    host: "foo.com".to_string(),
                    port: Some(4444),
                }],
                ..Default::default()
            },
            vec![Address {
                host: "foo.com".to_string(),
                port: 4444,
            }],
            true,
            true,
            true,
        )
        .await;

        check_default_spec(
            "couchbases://",
            ConnSpec {
                scheme: Some("couchbases".to_string()),
                ..Default::default()
            },
            vec![Address {
                host: "127.0.0.1".to_string(),
                port: DEFAULT_SSL_MEMD_PORT,
            }],
            true,
            true,
            true,
        )
        .await;

        check_default_spec(
            "couchbase://foo.com,bar.com:4444",
            ConnSpec {
                scheme: Some("couchbase".to_string()),
                hosts: vec![
                    ConnSpecAddress {
                        host: "foo.com".to_string(),
                        port: None,
                    },
                    ConnSpecAddress {
                        host: "bar.com".to_string(),
                        port: Some(4444),
                    },
                ],
                ..Default::default()
            },
            vec![
                Address {
                    host: "foo.com".to_string(),
                    port: DEFAULT_MEMD_PORT,
                },
                Address {
                    host: "bar.com".to_string(),
                    port: 4444,
                },
            ],
            false,
            true,
            true,
        )
        .await;

        check_default_spec(
            "couchbase://foo.com;bar.com;baz.com",
            ConnSpec {
                scheme: Some("couchbase".to_string()),
                hosts: vec![
                    ConnSpecAddress {
                        host: "foo.com".to_string(),
                        port: None,
                    },
                    ConnSpecAddress {
                        host: "bar.com".to_string(),
                        port: None,
                    },
                    ConnSpecAddress {
                        host: "baz.com".to_string(),
                        port: None,
                    },
                ],
                ..Default::default()
            },
            vec![
                Address {
                    host: "foo.com".to_string(),
                    port: DEFAULT_MEMD_PORT,
                },
                Address {
                    host: "bar.com".to_string(),
                    port: DEFAULT_MEMD_PORT,
                },
                Address {
                    host: "baz.com".to_string(),
                    port: DEFAULT_MEMD_PORT,
                },
            ],
            false,
            true,
            false,
        )
        .await;
    }

    #[tokio::test]
    async fn test_options_passthrough() {
        check_default_spec(
            "couchbase:///?foo=bar",
            ConnSpec {
                scheme: Some("couchbase".to_string()),
                options: {
                    let mut map = HashMap::new();
                    map.insert("foo".to_string(), vec!["bar".to_string()]);
                    map
                },
                ..Default::default()
            },
            vec![],
            false,
            false,
            false,
        )
        .await;

        check_default_spec(
            "couchbase://?foo=bar",
            ConnSpec {
                scheme: Some("couchbase".to_string()),
                options: {
                    let mut map = HashMap::new();
                    map.insert("foo".to_string(), vec!["bar".to_string()]);
                    map
                },
                ..Default::default()
            },
            vec![],
            false,
            false,
            true,
        )
        .await;

        check_default_spec(
            "couchbase://?foo=fooval&bar=barval",
            ConnSpec {
                scheme: Some("couchbase".to_string()),
                options: {
                    let mut map = HashMap::new();
                    map.insert("foo".to_string(), vec!["fooval".to_string()]);
                    map.insert("bar".to_string(), vec!["barval".to_string()]);
                    map
                },
                ..Default::default()
            },
            vec![],
            false,
            false,
            false,
        )
        .await;

        check_default_spec(
            "couchbase://?foo=fooval&bar=barval&",
            ConnSpec {
                scheme: Some("couchbase".to_string()),
                options: {
                    let mut map = HashMap::new();
                    map.insert("foo".to_string(), vec!["fooval".to_string()]);
                    map.insert("bar".to_string(), vec!["barval".to_string()]);
                    map
                },
                ..Default::default()
            },
            vec![],
            false,
            false,
            false,
        )
        .await;

        check_default_spec(
            "couchbase://?foo=val1&foo=val2&",
            ConnSpec {
                scheme: Some("couchbase".to_string()),
                options: {
                    let mut map = HashMap::new();
                    map.insert(
                        "foo".to_string(),
                        vec!["val1".to_string(), "val2".to_string()],
                    );
                    map
                },
                ..Default::default()
            },
            vec![],
            false,
            false,
            false,
        )
        .await;
    }

    #[tokio::test]
    async fn test_parse_couchbase2() {
        check_couchbase2_server_spec(
            "couchbase2://1.2.3.4",
            ConnSpec {
                scheme: Some("couchbase2".to_string()),
                hosts: vec![ConnSpecAddress {
                    host: "1.2.3.4".to_string(),
                    port: None,
                }],
                ..Default::default()
            },
            Address {
                host: "1.2.3.4".to_string(),
                port: DEFAULT_COUCHBASE2_PORT,
            },
        )
        .await;

        check_couchbase2_server_spec(
            "couchbase2://",
            ConnSpec {
                scheme: Some("couchbase2".to_string()),
                ..Default::default()
            },
            Address {
                host: "127.0.0.1".to_string(),
                port: DEFAULT_COUCHBASE2_PORT,
            },
        )
        .await;

        check_couchbase2_server_spec(
            "couchbase2://1.2.3.4:1234",
            ConnSpec {
                scheme: Some("couchbase2".to_string()),
                hosts: vec![ConnSpecAddress {
                    host: "1.2.3.4".to_string(),
                    port: Some(1234),
                }],
                ..Default::default()
            },
            Address {
                host: "1.2.3.4".to_string(),
                port: 1234,
            },
        )
        .await;

        check_couchbase2_server_spec(
            "couchbase2://1.2.3.4:18098",
            ConnSpec {
                scheme: Some("couchbase2".to_string()),
                hosts: vec![ConnSpecAddress {
                    host: "1.2.3.4".to_string(),
                    port: Some(18098),
                }],
                ..Default::default()
            },
            Address {
                host: "1.2.3.4".to_string(),
                port: DEFAULT_COUCHBASE2_PORT,
            },
        )
        .await;
    }
}
