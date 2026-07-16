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

use crate::common::consistency_utils::{
    verify_collection_created, verify_scope_created, verify_scope_dropped,
};
use crate::common::test_config::run_test;
use crate::common::try_until;
use couchbase::error::ErrorKind;
use couchbase::management::collections::collection_manager::CollectionManager;
use couchbase::options::query_index_mgmt_options::{
    CreatePrimaryQueryIndexOptions, CreateQueryIndexOptions,
};
use couchbase::options::query_options::QueryOptions;
use couchbase::results::query_results::{QueryMetaData, QueryStatus};
use futures::StreamExt;
use serde_json::value::RawValue;
use serde_json::Value;
use std::time::Duration;

mod common;

#[test]
fn test_query_basic() {
    run_test(async |cluster, bucket| {
        let scope = bucket.scope(cluster.default_scope());
        let opts = QueryOptions::new().metrics(true);
        let mut res = scope.query("SELECT 1=1", opts).await.unwrap();

        let mut rows: Vec<Value> = vec![];
        while let Some(row) = res.rows().next().await {
            rows.push(row.unwrap());
        }

        assert_eq!(1, rows.len());

        let row = rows.first().unwrap();

        let row_obj = row.as_object().unwrap();

        assert!(row_obj.get("$1").unwrap().as_bool().unwrap());

        let meta = res.metadata().unwrap();
        assert_metadata(meta);
    })
}

#[test]
fn test_query_empty_result() {
    run_test(async |cluster, bucket| {
        let scope = bucket.scope(cluster.default_scope());
        let opts = QueryOptions::new().metrics(true);
        let mut res = scope
            .query("SELECT * FROM ARRAY_RANGE(0, 0) AS x", opts)
            .await
            .unwrap();

        let mut rows: Vec<Value> = vec![];
        while let Some(row) = res.rows().next().await {
            rows.push(row.unwrap());
        }

        assert_eq!(0, rows.len());
    })
}

#[test]
fn test_query_error() {
    run_test(async |cluster, bucket| {
        let scope = bucket.scope(cluster.default_scope());
        let opts = QueryOptions::new().metrics(true);
        let mut res = scope.query("SELEC 1=1", opts).await;

        let e = res.err().unwrap();
        assert_eq!(&ErrorKind::ParsingFailure, e.kind());

        assert!(e.to_string().contains("3000"));
        assert!(e.to_string().contains("syntax error"));
    })
}

#[test]
fn test_query_raw_result() {
    run_test(async |cluster, bucket| {
        let scope = bucket.scope(cluster.default_scope());
        let opts = QueryOptions::new().metrics(true);
        let mut res = scope.query("SELECT 1=1", opts).await.unwrap();

        let mut rows: Vec<Box<RawValue>> = vec![];
        while let Some(row) = res.rows().next().await {
            rows.push(row.unwrap());
        }

        assert_eq!(1, rows.len());

        let row = rows.first().unwrap();

        let row_value: Value = serde_json::from_str(row.get()).unwrap();
        let row_obj = row_value.as_object().unwrap();

        assert!(row_obj.get("$1").unwrap().as_bool().unwrap());

        let meta = res.metadata().unwrap();
        assert_metadata(meta);
    })
}

#[test]
fn test_prepared_query_basic() {
    run_test(async |cluster, bucket| {
        let scope = bucket.scope(cluster.default_scope());
        let opts = QueryOptions::new().metrics(true);
        let mut res = scope.query("SELECT 1=1", opts).await.unwrap();

        let mut rows: Vec<Value> = vec![];
        while let Some(row) = res.rows().next().await {
            rows.push(row.unwrap());
        }

        assert_eq!(1, rows.len());

        let row = rows.first().unwrap();

        let row_obj = row.as_object().unwrap();

        assert!(row_obj.get("$1").unwrap().as_bool().unwrap());

        let meta = res.metadata().unwrap();
        assert_metadata(meta);
    })
}

#[test]
fn test_query_basic_cluster() {
    run_test(async |cluster, bucket| {
        let opts = QueryOptions::new().metrics(true);
        let mut res = cluster.query("SELECT 1=1", opts).await.unwrap();

        let mut rows: Vec<Value> = vec![];
        while let Some(row) = res.rows().next().await {
            rows.push(row.unwrap());
        }

        assert_eq!(1, rows.len());

        let row = rows.first().unwrap();

        let row_obj = row.as_object().unwrap();

        assert!(row_obj.get("$1").unwrap().as_bool().unwrap());

        let meta = res.metadata().unwrap();
        assert_metadata(meta);
    })
}

#[test]
fn test_query_indexes() {
    run_test(async |cluster, bucket| {
        let coll_manager = bucket.collections();
        let (scope, collection) = create_collection(&coll_manager).await;

        let manager = bucket.scope(&scope).collection(&collection).query_indexes();

        let opts = CreatePrimaryQueryIndexOptions::new().ignore_if_exists(true);

        // Allow time for server to sync with the new collection
        try_until(
            tokio::time::Instant::now() + Duration::from_secs(30),
            Duration::from_millis(100),
            "Primary index was not created in time",
            async || {
                let res = manager.create_primary_index(opts.clone()).await;
                if res.is_ok() {
                    Ok(Some(()))
                } else {
                    Ok(None)
                }
            },
        )
        .await;

        let opts = CreateQueryIndexOptions::new()
            .ignore_if_exists(true)
            .deferred(true);

        // Allow time for server to sync with the new collection, this request might go
        // somewhere different to the one above.
        try_until(
            tokio::time::Instant::now() + Duration::from_secs(30),
            Duration::from_millis(100),
            "Primary index was not created in time",
            async || {
                let res = manager
                    .create_index(
                        "test_index".to_string(),
                        vec!["name".to_string()],
                        opts.clone(),
                    )
                    .await;
                if res.is_ok() {
                    Ok(Some(()))
                } else {
                    Ok(None)
                }
            },
        )
        .await;

        manager
            .create_index(
                "test_index_nested".to_string(),
                vec![
                    "d.firstName".to_string(),
                    "d.permissions.`/admin/role`".to_string(),
                    "`d.firstName`".to_string(),
                ],
                opts.clone(),
            )
            .await
            .unwrap();

        let indexes = manager.get_all_indexes(None).await.unwrap();

        assert_eq!(3, indexes.len());

        let primary_index = indexes.iter().find(|idx| idx.name() == "#primary").unwrap();
        assert!(primary_index.is_primary());
        assert_eq!(primary_index.state(), "online");

        let test_index = indexes
            .iter()
            .find(|idx| idx.name() == "test_index")
            .unwrap();
        assert!(!test_index.is_primary());
        assert_eq!(test_index.state(), "deferred");
        assert_eq!(test_index.keyspace(), &collection);

        let nested_index = indexes
            .iter()
            .find(|idx| idx.name() == "test_index_nested")
            .unwrap();
        let nested_key = nested_index.index_key();
        assert_eq!(3, nested_key.len());
        assert!(nested_key[0].contains("`d`"));
        assert!(nested_key[0].contains("`firstName`"));
        assert!(nested_key[1].contains("`d`"));
        assert!(nested_key[1].contains("`permissions`"));
        assert!(nested_key[1].contains("`/admin/role`"));
        // distinct from the nested `d`.`firstName` path above.
        assert_eq!("`d.firstName`", nested_key[2]);

        manager.build_deferred_indexes(None).await.unwrap();

        manager
            .watch_indexes(
                vec!["test_index".to_string(), "test_index_nested".to_string()],
                None,
            )
            .await
            .unwrap();

        manager.drop_primary_index(None).await.unwrap();

        manager
            .drop_index("test_index".to_string(), None)
            .await
            .unwrap();

        manager
            .drop_index("test_index_nested".to_string(), None)
            .await
            .unwrap();

        let indexes = manager.get_all_indexes(None).await.unwrap();
        assert_eq!(0, indexes.len());

        drop_scope(&coll_manager, &scope).await;
    })
}

async fn create_collection(manager: &CollectionManager) -> (String, String) {
    let scope_name = common::generate_string_value(10);
    let collection_name = common::generate_string_value(10);

    manager.create_scope(&scope_name, None).await.unwrap();
    verify_scope_created(manager, &scope_name).await;

    let settings =
        couchbase::management::collections::collection_settings::CreateCollectionSettings::new();
    manager
        .create_collection(&scope_name, &collection_name, settings, None)
        .await
        .unwrap();

    verify_collection_created(manager, &scope_name, &collection_name).await;

    (scope_name, collection_name)
}

async fn drop_scope(manager: &CollectionManager, scope_name: &str) {
    manager.drop_scope(scope_name, None).await.unwrap();
    verify_scope_dropped(manager, scope_name).await;
}

fn assert_metadata(meta: QueryMetaData) {
    assert!(!meta.request_id.is_empty());
    assert!(!meta.client_context_id.is_empty());
    assert_eq!(QueryStatus::Success, meta.status);
    assert!(meta.profile.is_none());
    assert!(meta.warnings.is_empty());

    let metrics = meta
        .metrics
        .as_ref()
        .expect("expected metrics to be present");
    assert!(!metrics.elapsed_time.is_zero());
    assert!(!metrics.execution_time.is_zero());
    assert_eq!(1, metrics.result_count);
    assert_ne!(0, metrics.result_size);
    assert_eq!(0, metrics.mutation_count);
    assert_eq!(0, metrics.sort_count);
    assert_eq!(0, metrics.error_count);
    assert_eq!(0, metrics.warning_count);

    assert_eq!(
        "{\"$1\":\"boolean\"}",
        meta.signature.as_ref().unwrap().get()
    );
}
