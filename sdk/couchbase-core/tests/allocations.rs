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
use crate::common::helpers::{
    ensure_manifest, generate_bytes_value, generate_key, generate_string_value,
};
use crate::common::test_agent::TestAgent;
use crate::common::test_config::run_test;
use couchbase_core::options::crud::{AddOptions, GetOptions, ReplaceOptions, UpsertOptions};
use couchbase_core::options::management::CreateCollectionOptions;
use couchbase_core::options::waituntilready::WaitUntilReadyOptions;
use couchbase_core::retryfailfast::FailFastRetryStrategy;
use couchbase_core::service_type::ServiceType;
use serial_test::serial;
use std::future::Future;
use std::sync::Arc;

mod common;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[serial]
#[cfg(feature = "dhat-heap")]
#[test]
fn upsert() {
    run_test(async |mut agent| {
        let key = generate_key();
        let value = generate_bytes_value(32);
        let key_clone = key.clone();
        let value_clone = value.clone();

        let upsert_opts = UpsertOptions::new(key_clone.as_slice(), "", "", value_clone.as_slice())
            .retry_strategy(Arc::new(FailFastRetryStrategy::default()));
        let expected_allocs: u64 = if agent.test_setup_config.use_ssl {
            13
        } else {
            11
        };

        ensure_agent_ready(&agent).await;

        create_doc(key, value, "", &agent).await;

        run_allocation_test(agent, expected_allocs, async |agent| {
            agent.upsert(upsert_opts).await.unwrap();
        })
        .await
    });
}

#[serial]
#[cfg(feature = "dhat-heap")]
#[test]
fn upsert_against_new_collection() {
    run_test(async |mut agent| {
        let key = generate_key();
        let value = generate_bytes_value(32);
        let key_clone = key.clone();
        let value_clone = value.clone();

        let collection_name = generate_string_value(10);

        let resp = agent
            .create_collection(&CreateCollectionOptions::new(
                &agent.test_setup_config.bucket,
                &agent.test_setup_config.scope,
                &collection_name,
            ))
            .await
            .unwrap();

        ensure_manifest(&agent, &agent.test_setup_config.bucket, resp.manifest_uid).await;

        let upsert_opts = UpsertOptions::new(
            key_clone.as_slice(),
            "",
            &collection_name,
            value_clone.as_slice(),
        )
        .retry_strategy(Arc::new(FailFastRetryStrategy::default()));

        let expected_allocs: u64 = if agent.test_setup_config.use_ssl {
            15
        } else {
            13
        };

        ensure_agent_ready(&agent).await;

        create_doc(key, value, &collection_name, &agent).await;

        run_allocation_test(agent, expected_allocs, async |agent| {
            agent.upsert(upsert_opts).await.unwrap();
        })
        .await
    });
}

#[serial]
#[cfg(feature = "dhat-heap")]
#[test]
fn add() {
    run_test(async |mut agent| {
        let key = generate_key();
        let value = generate_bytes_value(32);
        let key_clone = key.clone();
        let value_clone = value.clone();

        let add_opts = AddOptions::new(key_clone.as_slice(), "", "", value_clone.as_slice())
            .retry_strategy(Arc::new(FailFastRetryStrategy::default()));
        let expected_allocs: u64 = if agent.test_setup_config.use_ssl {
            13
        } else {
            11
        };

        ensure_agent_ready(&agent).await;

        run_allocation_test(agent, expected_allocs, async |agent| {
            agent.add(add_opts).await.unwrap();
        })
        .await
    });
}

#[serial]
#[cfg(feature = "dhat-heap")]
#[test]
fn replace() {
    run_test(async |mut agent| {
        let key = generate_key();
        let value = generate_bytes_value(32);
        let key_clone = key.clone();
        let value_clone = value.clone();

        let opts = ReplaceOptions::new(key_clone.as_slice(), "", "", value_clone.as_slice())
            .retry_strategy(Arc::new(FailFastRetryStrategy::default()));
        let expected_allocs: u64 = if agent.test_setup_config.use_ssl {
            13
        } else {
            11
        };

        ensure_agent_ready(&agent).await;

        create_doc(key, value, "", &agent).await;

        run_allocation_test(agent, expected_allocs, async |agent| {
            agent.replace(opts).await.unwrap();
        })
        .await
    });
}

#[serial]
#[cfg(feature = "dhat-heap")]
#[test]
fn get() {
    run_test(async |mut agent| {
        let key = generate_key();
        let value = generate_bytes_value(32);
        let key_clone = key.clone();

        let opts = GetOptions::new(key_clone.as_slice(), "", "")
            .retry_strategy(Arc::new(FailFastRetryStrategy::default()));

        let expected_allocs: u64 = if agent.test_setup_config.use_ssl {
            13
        } else {
            11
        };

        ensure_agent_ready(&agent).await;

        create_doc(key, value, "", &agent).await;

        run_allocation_test(agent, expected_allocs, async |agent| {
            agent.get(opts).await.unwrap();
        })
        .await
    });
}

async fn ensure_agent_ready(agent: &TestAgent) {
    agent
        .wait_until_ready(&WaitUntilReadyOptions::new().service_types(vec![ServiceType::MEMD]))
        .await
        .unwrap();
}

async fn create_doc(key: Vec<u8>, value: Vec<u8>, collection: &str, agent: &TestAgent) {
    let strat = Arc::new(FailFastRetryStrategy::default());

    let upsert_opts = UpsertOptions::new(key.as_slice(), "", collection, value.as_slice())
        .retry_strategy(strat.clone());

    // make sure that all the underlying resources are setup.
    agent.upsert(upsert_opts.clone()).await.unwrap();
}

#[cfg(feature = "dhat-heap")]
async fn run_allocation_test<Fn, F>(agent: TestAgent, expected_allocs: u64, fut: Fn)
where
    Fn: FnOnce(TestAgent) -> F,
    F: Future<Output = ()>,
{
    let profiler = dhat::Profiler::builder().testing().build();

    let stats1 = dhat::HeapStats::get();

    fut(agent).await;

    let stats2 = dhat::HeapStats::get();

    let total_allocs = stats2.total_blocks - stats1.total_blocks;

    dhat::assert_eq!(
        total_allocs,
        expected_allocs,
        "Expected max {} allocations, was {}",
        expected_allocs,
        total_allocs
    );

    drop(profiler);
}
