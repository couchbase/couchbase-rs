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

use std::future::Future;
use std::ops::Deref;
use std::sync::LazyLock;

use crate::common::default_cluster_options;
use crate::common::node_version::NodeVersion;
use crate::common::test_bucket::TestBucket;
use crate::common::test_cluster::TestCluster;
use couchbase::cluster::Cluster;
use couchbase::options::diagnostic_options::WaitUntilReadyOptions;
use couchbase::service_type::ServiceType;
use couchbase_connstr::ResolvedConnSpec;
use envconfig::Envconfig;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;

pub static TEST_CONFIG: LazyLock<RwLock<Option<TestCluster>>> = LazyLock::new(|| RwLock::new(None));

#[derive(Debug, Clone, Envconfig)]
pub struct EnvTestConfig {
    #[envconfig(from = "RCBUSERNAME", default = "Administrator")]
    pub username: String,
    #[envconfig(from = "RCBPASSWORD", default = "password")]
    pub password: String,
    #[envconfig(from = "RCBCONNSTR", default = "couchbases://192.168.107.128")]
    pub conn_string: String,
    #[envconfig(from = "RCBBUCKET", default = "default")]
    pub default_bucket: String,
    #[envconfig(from = "RCBSCOPE", default = "_default")]
    pub default_scope: String,
    #[envconfig(from = "RCBCOLLECTION", default = "_default")]
    pub default_collection: String,
    #[envconfig(from = "RCBSERVER_VERSION", default = "7.6.2")]
    pub server_version: String,
}

#[derive(Debug, Clone)]
pub struct TestSetupConfig {
    pub username: String,
    pub password: String,
    pub conn_str: String,
    pub resolved_conn_spec: ResolvedConnSpec,
    pub default_bucket: String,
    pub default_scope: String,
    pub default_collection: String,
}

impl TestSetupConfig {
    pub async fn setup_cluster(&self) -> Cluster {
        Cluster::connect(
            self.conn_str.clone(),
            default_cluster_options::create_default_options(self.clone()).await,
        )
        .await
        .unwrap()
    }
}

pub static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
});

pub fn run_test<T, Fut>(test: T)
where
    T: FnOnce(TestCluster, TestBucket) -> Fut,
    Fut: Future<Output = ()>,
{
    RUNTIME.block_on(async {
        let mut config = TEST_CONFIG.write().await;

        if let Some(cluster) = config.deref() {
            let cluster = cluster.clone();
            let bucket = cluster.bucket(cluster.default_bucket());
            drop(config);

            test(cluster, bucket).await;
            return;
        }

        let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            tracing_subscriber::EnvFilter::new("off")
                .add_directive("couchbase::metrics=off".parse().unwrap())
                .add_directive("couchbase=trace".parse().unwrap())
                .add_directive("couchbase_core=trace".parse().unwrap())
                .add_directive("couchbase_connstr=trace".parse().unwrap())
                .add_directive("rustls=warn".parse().unwrap())
        });

        tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_target(false)
            .with_thread_ids(true)
            .with_env_filter(filter)
            .try_init()
            .ok();

        let test_cluster = create_test_cluster().await;

        test_cluster.wait_until_ready(None).await.unwrap();

        let bucket = test_cluster.bucket(test_cluster.default_bucket());

        bucket.wait_until_ready(None).await.unwrap();

        *config = Some(test_cluster.clone());
        drop(config);

        test(test_cluster, bucket).await;
    });
}

pub async fn create_test_cluster() -> TestCluster {
    let test_config = EnvTestConfig::init_from_env().unwrap();

    let conn_spec = couchbase_connstr::parse(&test_config.conn_string).unwrap();

    let test_setup_config = TestSetupConfig {
        default_bucket: test_config.default_bucket,
        default_scope: test_config.default_scope,
        default_collection: test_config.default_collection,
        username: test_config.username,
        password: test_config.password,
        conn_str: test_config.conn_string,
        resolved_conn_spec: couchbase_connstr::resolve(conn_spec, None).await.unwrap(),
    };

    TestCluster::new(
        NodeVersion::from(test_config.server_version.clone()),
        test_setup_config.clone(),
    )
    .await
}

pub fn get_bucket(rt: &Runtime) -> (TestCluster, TestBucket) {
    rt.block_on(async {
        let cluster = create_test_cluster().await;

        cluster
            .wait_until_ready(WaitUntilReadyOptions::new().service_types(vec![ServiceType::KV]))
            .await
            .unwrap();

        let bucket = cluster.clone().bucket(cluster.default_bucket());

        (cluster, bucket)
    })
}

impl TestCluster {}
