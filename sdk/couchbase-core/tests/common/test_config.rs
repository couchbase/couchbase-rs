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

use crate::common::default_agent_options;
use crate::common::node_version::NodeVersion;
use crate::common::test_agent::TestAgent;
use couchbase_connstr::ResolvedConnSpec;
use couchbase_core::address::Address;
use couchbase_core::agent::Agent;
use couchbase_core::options::waituntilready::WaitUntilReadyOptions;
use envconfig::Envconfig;
use log::LevelFilter;
use std::env;
use std::future::Future;
use std::io::Write;
use std::ops::{Add, Deref};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;
use tokio::time::{timeout_at, Instant};

pub static TEST_AGENT: LazyLock<RwLock<Option<TestAgent>>> = LazyLock::new(|| RwLock::new(None));
pub static LOGGER_INITIATED: LazyLock<AtomicBool> = LazyLock::new(|| AtomicBool::new(false));

#[derive(Debug, Clone, Envconfig)]
pub struct EnvTestConfig {
    #[envconfig(from = "RCBUSERNAME", default = "Administrator")]
    pub username: String,
    #[envconfig(from = "RCBPASSWORD", default = "password")]
    pub password: String,
    #[envconfig(from = "RCBCONNSTR", default = "couchbase://192.168.107.128")]
    pub conn_string: String,
    #[envconfig(from = "RCBBUCKET", default = "default")]
    pub default_bucket: String,
    #[envconfig(from = "RCBSCOPE", default = "_default")]
    pub default_scope: String,
    #[envconfig(from = "RCBCOLLECTION", default = "_default")]
    pub default_collection: String,
    #[envconfig(from = "RCBDATA_TIMEOUT", default = "2500")]
    pub data_timeout: String,
    #[envconfig(from = "RCBSERVER_VERSION", default = "7.6.2")]
    pub server_version: String,
}

#[derive(Debug, Clone)]
pub struct TestSetupConfig {
    pub username: String,
    pub password: String,
    pub memd_addrs: Vec<Address>,
    pub data_timeout: String,
    pub use_ssl: bool,
    pub bucket: String,
    pub scope: String,
    pub collection: String,
    pub resolved_conn_spec: ResolvedConnSpec,
}

impl TestSetupConfig {
    pub async fn setup_agent(&self) -> Agent {
        Agent::new(default_agent_options::create_default_options(self.clone()).await)
            .await
            .unwrap()
    }
}

static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
});

pub fn run_test<T, Fut>(test: T)
where
    T: FnOnce(TestAgent) -> Fut,
    Fut: Future<Output = ()>,
{
    RUNTIME.block_on(async {
        let mut config = TEST_AGENT.write().await;

        if let Some(agent) = config.deref() {
            let agent = agent.clone();
            drop(config);
            test(agent).await;
            return;
        }

        if LOGGER_INITIATED.compare_exchange(false, true, SeqCst, SeqCst) == Ok(false) {
            env_logger::Builder::new()
                .format(|buf, record| {
                    writeln!(
                        buf,
                        "{}:{} {} [{}] - {}",
                        record.file().unwrap_or("unknown"),
                        record.line().unwrap_or(0),
                        chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%.3f"),
                        record.level(),
                        record.args()
                    )
                })
                .filter(Some("rustls"), LevelFilter::Warn)
                .filter_level(
                    env::var("RUST_LOG")
                        .unwrap_or("TRACE".to_string())
                        .parse()
                        .unwrap(),
                )
                .init();
        }

        let test_agent = timeout_at(
            Instant::now().add(Duration::from_secs(7)),
            create_test_agent(),
        )
        .await
        .unwrap();

        test_agent
            .wait_until_ready(&WaitUntilReadyOptions::new())
            .await
            .unwrap();

        *config = Some(test_agent.clone());
        drop(config);

        test(test_agent).await;
    });
}

pub fn setup_test<T, Fut>(test: T)
where
    T: FnOnce(TestSetupConfig) -> Fut,
    Fut: Future<Output = ()>,
{
    RUNTIME.block_on(async {
        if LOGGER_INITIATED.compare_exchange(false, true, SeqCst, SeqCst) == Ok(false) {
            env_logger::Builder::new()
                .format(|buf, record| {
                    writeln!(
                        buf,
                        "{}:{} {} [{}] - {}",
                        record.file().unwrap_or("unknown"),
                        record.line().unwrap_or(0),
                        chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%.3f"),
                        record.level(),
                        record.args()
                    )
                })
                .filter(Some("rustls"), LevelFilter::Warn)
                .filter_level(
                    env::var("RUST_LOG")
                        .unwrap_or("TRACE".to_string())
                        .parse()
                        .unwrap(),
                )
                .init();
        }

        let test_config = EnvTestConfig::init_from_env().unwrap();
        test(create_test_config(&test_config).await).await;
    });
}

pub async fn create_test_config(test_config: &EnvTestConfig) -> TestSetupConfig {
    let conn_spec = couchbase_connstr::parse(&test_config.conn_string).unwrap();

    let resolved_conn_spec = couchbase_connstr::resolve(conn_spec, None).await.unwrap();

    TestSetupConfig {
        username: test_config.username.clone(),
        password: test_config.password.clone(),
        memd_addrs: resolved_conn_spec
            .memd_hosts
            .iter()
            .map(|h| Address {
                host: h.host.clone(),
                port: h.port,
            })
            .collect(),
        data_timeout: test_config.data_timeout.clone(),
        use_ssl: resolved_conn_spec.use_ssl,
        bucket: test_config.default_bucket.clone(),
        scope: test_config.default_scope.clone(),
        collection: test_config.default_collection.clone(),
        resolved_conn_spec,
    }
}

pub async fn create_test_agent() -> TestAgent {
    let test_config = EnvTestConfig::init_from_env().unwrap();

    let test_setup_config = create_test_config(&test_config).await;

    let agent = test_setup_config.setup_agent().await;

    TestAgent::new(
        test_setup_config,
        NodeVersion::from(test_config.server_version.clone()),
        agent,
    )
}
