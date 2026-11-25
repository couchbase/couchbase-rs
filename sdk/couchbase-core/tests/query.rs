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

extern crate core;

use futures::StreamExt;
use serde_json::Value;

use crate::common::helpers::generate_string_key;
use crate::common::test_config::run_test;
use couchbase_core::options::query::{
    BuildDeferredIndexesOptions, CreateIndexOptions, CreatePrimaryIndexOptions, DropIndexOptions,
    DropPrimaryIndexOptions, EnsureIndexOptions, GetAllIndexesOptions, QueryOptions,
    WatchIndexesOptions,
};
use couchbase_core::queryx::ensure_index_helper::DesiredState;
use couchbase_core::queryx::query_result::Status;
use couchbase_core::results::query::QueryResultStream;

mod common;

#[test]
fn test_query_basic() {
    run_test(async |mut agent| {
        let opts = QueryOptions::default().statement("SELECT 1=1".to_string());
        let mut res = agent.query(opts).await.unwrap();

        let mut rows = vec![];
        while let Some(row) = res.next().await {
            rows.push(row.unwrap());
        }

        assert_eq!(1, rows.len());

        let row = rows.first().unwrap();

        let row_value: Value = serde_json::from_slice(row).unwrap();
        let row_obj = row_value.as_object().unwrap();

        assert!(row_obj.get("$1").unwrap().as_bool().unwrap());

        let meta = res.metadata().unwrap();
        assert!(meta.prepared.is_none());
        assert!(!meta.request_id.is_empty());
        assert!(!meta.client_context_id.is_empty());
        assert_eq!(Status::Success, meta.status);
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
    });
}

#[test]
fn test_prepared_query_basic() {
    run_test(async |mut agent| {
        let opts = QueryOptions::default().statement("SELECT 1=1".to_string());
        let mut res = agent.prepared_query(opts).await.unwrap();

        let mut rows = vec![];
        while let Some(row) = res.next().await {
            rows.push(row.unwrap());
        }

        assert_eq!(1, rows.len());

        let row = rows.first().unwrap();

        let row_value: Value = serde_json::from_slice(row).unwrap();
        let row_obj = row_value.as_object().unwrap();

        assert!(row_obj.get("$1").unwrap().as_bool().unwrap());

        let meta = res.metadata().unwrap();
        assert!(meta.prepared.is_some());
        assert!(!meta.request_id.is_empty());
        assert!(!meta.client_context_id.is_empty());
        assert_eq!(Status::Success, meta.status);
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

        dbg!(&meta.signature);

        // MB-65750: signature is inconsistent in format.
        assert!(!meta.signature.as_ref().unwrap().get().is_empty());
    });
}

#[test]
fn test_prepared_query_reprepares_after_plan_removed() {
    run_test(async |mut agent| {
        // Use a unique statement so that the shared agent's prepared statement cache is not
        // affected by, and does not affect, other tests.
        let value = generate_string_key();
        let statement = format!("SELECT {} AS v", serde_json::to_string(&value).unwrap());

        // First execution prepares the statement and caches the prepared name.
        let mut res = agent
            .prepared_query(QueryOptions::default().statement(statement.clone()))
            .await
            .unwrap();
        let rows = collect_rows(&mut res).await;
        assert_eq!(vec![serde_json::json!({ "v": value })], rows);
        assert!(res.metadata().unwrap().prepared.is_some());

        // Remove the plan from every query node so that executing the cached prepared name fails,
        // forcing the SDK to fall back to re-preparing the statement.
        // Match on the statement rather than the name: the name returned to the SDK is prefixed
        // with the preparing node (e.g. `[host:8091]<uuid>`), but `system:prepareds.name` is not.
        let mut res = agent
            .query(
                QueryOptions::default()
                    .statement("DELETE FROM system:prepareds WHERE statement = $1".to_string())
                    .args(vec![Value::String(format!("PREPARE {statement}"))]),
            )
            .await
            .unwrap();
        collect_rows(&mut res).await;
        let mutation_count = res
            .metadata()
            .unwrap()
            .metrics
            .as_ref()
            .expect("expected metrics to be present")
            .mutation_count;
        assert_ne!(
            0, mutation_count,
            "expected the prepared plan to be deleted"
        );

        // Second execution must transparently re-prepare and succeed.
        let mut res = agent
            .prepared_query(QueryOptions::default().statement(statement))
            .await
            .unwrap();
        let rows = collect_rows(&mut res).await;
        assert_eq!(vec![serde_json::json!({ "v": value })], rows);

        let meta = res.metadata().unwrap();
        assert_eq!(Status::Success, meta.status);
        assert!(meta.prepared.is_some());
    });
}

async fn collect_rows(res: &mut QueryResultStream) -> Vec<Value> {
    let mut rows = vec![];
    while let Some(row) = res.next().await {
        rows.push(serde_json::from_slice(&row.unwrap()).unwrap());
    }
    rows
}

#[test]
fn test_query_indexes() {
    run_test(async |mut agent| {
        let bucket_name = agent.test_setup_config.bucket.clone();

        let opts = CreatePrimaryIndexOptions::new(bucket_name.as_str()).ignore_if_exists(true);

        agent.create_primary_index(&opts).await.unwrap();

        agent
            .ensure_index(&EnsureIndexOptions::new(
                "#primary",
                bucket_name.as_str(),
                None,
                None,
                DesiredState::Created,
            ))
            .await
            .unwrap();

        let index_name = generate_string_key();
        let index_name = index_name.as_str();

        let opts = CreateIndexOptions::new(bucket_name.as_str(), index_name, &["name"])
            .ignore_if_exists(true)
            .deferred(true);

        agent.create_index(&opts).await.unwrap();

        agent
            .ensure_index(&EnsureIndexOptions::new(
                index_name,
                bucket_name.as_str(),
                None,
                None,
                DesiredState::Created,
            ))
            .await
            .unwrap();

        let opts = GetAllIndexesOptions::new(bucket_name.as_str());

        let indexes = agent.get_all_indexes(&opts).await.unwrap();

        let num_indexes = indexes.len();
        assert!(num_indexes >= 2);

        let opts = BuildDeferredIndexesOptions::new(bucket_name.as_str());

        agent.build_deferred_indexes(&opts).await.unwrap();

        let index_names = &[index_name];
        let opts = WatchIndexesOptions::new(bucket_name.as_str(), index_names);

        tokio::time::timeout(
            std::time::Duration::from_secs(15),
            agent.watch_indexes(&opts),
        )
        .await
        .unwrap()
        .unwrap();

        let opts = DropPrimaryIndexOptions::new(bucket_name.as_str()).ignore_if_not_exists(true);

        agent.drop_primary_index(&opts).await.unwrap();

        agent
            .ensure_index(&EnsureIndexOptions::new(
                "#primary",
                bucket_name.as_str(),
                None,
                None,
                DesiredState::Deleted,
            ))
            .await
            .unwrap();

        let opts =
            DropIndexOptions::new(bucket_name.as_str(), index_name).ignore_if_not_exists(true);

        agent.drop_index(&opts).await.unwrap();

        agent
            .ensure_index(&EnsureIndexOptions::new(
                index_name,
                bucket_name.as_str(),
                None,
                None,
                DesiredState::Deleted,
            ))
            .await
            .unwrap();

        let opts = GetAllIndexesOptions::new(bucket_name.as_str());

        let indexes = agent.get_all_indexes(&opts).await.unwrap();

        assert_eq!(
            num_indexes - 2,
            indexes.len(),
            "Indexes were not deleted as expected"
        );
    });
}
