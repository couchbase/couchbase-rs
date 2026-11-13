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

use crate::common::doc_generation::{
    import_projection_doc, HobbyDetails, Location, Person, PersonAttributes, PersonDimensions,
    PersonHobbies,
};
use crate::common::new_key;
use crate::common::test_config::run_test;
use chrono::Utc;
use couchbase::error::ErrorKind;
use couchbase::options::kv_binary_options::{DecrementOptions, IncrementOptions};
use couchbase::options::kv_options::{GetOptions, UpsertOptions};
use couchbase::retry::{RetryAction, RetryReason, RetryRequest, RetryStrategy};
use couchbase::subdoc::lookup_in_specs::{GetSpecOptions, LookupInSpec};
use couchbase::subdoc::macros::{LookupInMacros, MutateInMacros};
use couchbase::subdoc::mutate_in_specs::MutateInSpec;
use couchbase::transcoding;
use couchbase::transcoding::{encode_common_flags, DataType};
use couchbase_core::memdx::subdoc::SubdocOp;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::collections::{BTreeMap, HashSet};
use std::ops::{Add, Deref};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::{timeout_at, Instant};

mod common;

#[test]
fn test_upsert() {
    run_test(async |cluster, bucket| {
        let collection = bucket
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());

        let key = new_key();

        collection.upsert(&key, "test", None).await.unwrap();

        let res = collection.get(key, None).await.unwrap();

        let content: String = res.content_as().unwrap();

        assert_eq!("test", content);
    });
}

#[test]
fn test_upsert_operation_cancellation() {
    run_test(async |cluster, bucket| {
        let collection = bucket
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());

        let key = new_key();

        collection.upsert(&key, "test", None).await.unwrap();

        let _res = timeout_at(
            Instant::now().add(Duration::from_micros(1)),
            collection.deref().get(&key, None),
        )
        .await;

        let res = collection.get(key, None).await.unwrap();

        let content: String = res.content_as().unwrap();

        assert_eq!("test", content);
    })
}

#[test]
fn test_upsert_with_transcoder() {
    run_test(async |cluster, bucket| {
        let collection = bucket
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());

        let value = RawValue::from_string(r#"{"test": "test"}"#.to_string()).unwrap();

        let key = new_key();

        let (content, flags) = transcoding::json::encode(value).unwrap();
        collection
            .upsert_raw(&key, &content, flags, None)
            .await
            .unwrap();

        let res = collection.get(key, None).await.unwrap();

        let content: Box<RawValue> = res.content_as().unwrap();

        assert_eq!(r#"{"test": "test"}"#, content.get());
    })
}

#[test]
fn test_upsert_with_custom_transcoder() {
    run_test(async |cluster, bucket| {
        let collection = bucket
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());

        let mut value = BTreeMap::new();
        value.insert("x".to_string(), 1.0);
        value.insert("y".to_string(), 2.0);

        let key = new_key();

        collection
            .upsert_raw(
                &key,
                serde_yaml::to_string(&value).unwrap().as_bytes(),
                encode_common_flags(DataType::Binary),
                None,
            )
            .await
            .unwrap();

        let res = collection.get(key, None).await.unwrap();

        let (content, flags) = res.content_as_raw();
        let content: BTreeMap<String, f64> = serde_yaml::from_slice(content).unwrap();

        assert_eq!(value, content);
    })
}

#[test]
fn test_insert() {
    run_test(async |cluster, bucket| {
        let collection = bucket
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());

        let key = new_key();

        collection.insert(&key, "test", None).await.unwrap();

        let res = collection.get(key, None).await.unwrap();

        let content: String = res.content_as().unwrap();

        assert_eq!("test", content);
    })
}

#[test]
fn test_replace() {
    run_test(async |cluster, bucket| {
        let collection = bucket
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());

        let key = new_key();

        collection.insert(&key, "test", None).await.unwrap();
        collection
            .replace(&key, "test_replaced", None)
            .await
            .unwrap();

        let res = collection.get(key, None).await.unwrap();

        let content: String = res.content_as().unwrap();

        assert_eq!("test_replaced", content);
    })
}

#[test]
fn test_remove() {
    run_test(async |cluster, bucket| {
        let collection = bucket
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());

        let key = new_key();

        collection.insert(&key, "test", None).await.unwrap();
        collection.remove(&key, None).await.unwrap();

        let res = collection.get(key, None).await;

        assert!(res.is_err());
    })
}

#[test]
fn test_exists() {
    run_test(async |cluster, bucket| {
        let collection = bucket
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());

        let key = new_key();

        collection.insert(&key, "test", None).await.unwrap();

        let res = collection.exists(&key, None).await.unwrap();

        assert!(res.exists());

        collection.remove(&key, None).await.unwrap();

        let res = collection.exists(&key, None).await.unwrap();
        assert!(!res.exists());
    })
}

#[test]
fn test_get_and_touch() {
    run_test(async |cluster, bucket| {
        let collection = bucket
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());

        let key = new_key();

        collection.insert(&key, "test", None).await.unwrap();

        let res = collection
            .get_and_touch(&key, Duration::from_secs(10), None)
            .await
            .unwrap();

        let content: String = res.content_as().unwrap();

        assert_eq!("test", content);
    })
}

#[test]
fn test_get_and_lock() {
    run_test(async |cluster, bucket| {
        let collection = bucket
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());

        let key = new_key();

        collection.insert(&key, "test", None).await.unwrap();

        let res = collection
            .get_and_lock(&key, Duration::from_secs(10), None)
            .await
            .unwrap();

        let content: String = res.content_as().unwrap();

        assert_eq!("test", content);
    })
}

#[test]
fn test_unlock() {
    run_test(async |cluster, bucket| {
        let collection = bucket
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());

        let key = new_key();

        collection.insert(&key, "test", None).await.unwrap();

        let lock_res = collection
            .get_and_lock(&key, Duration::from_secs(10), None)
            .await
            .unwrap();

        collection.unlock(&key, lock_res.cas(), None).await.unwrap();
    })
}

#[test]
fn test_touch() {
    run_test(async |cluster, bucket| {
        let collection = bucket
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());

        let key = new_key();

        collection.insert(&key, "test", None).await.unwrap();

        collection
            .touch(&key, Duration::from_secs(10), None)
            .await
            .unwrap();
    })
}

#[test]
fn test_append() {
    run_test(async |cluster, bucket| {
        let collection = bucket
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());

        let key = new_key();

        let (content, flags) = transcoding::raw_binary::encode("test".as_bytes()).unwrap();
        collection
            .insert_raw(&key, content, flags, None)
            .await
            .unwrap();

        collection
            .binary()
            .append(&key, "append".as_bytes(), None)
            .await
            .unwrap();

        let res = collection.get(key, None).await.unwrap();

        let (raw, flags) = res.content_as_raw();
        let content = transcoding::raw_binary::decode(raw, flags).unwrap();

        assert_eq!("testappend".as_bytes(), content);
    })
}

#[test]
fn test_prepend() {
    run_test(async |cluster, bucket| {
        let collection = bucket
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());
        let key = new_key();

        let (content, flags) = transcoding::raw_binary::encode("test".as_bytes()).unwrap();
        collection
            .insert_raw(&key, content, flags, None)
            .await
            .unwrap();

        collection
            .binary()
            .prepend(&key, "prepend".as_bytes(), None)
            .await
            .unwrap();

        let res = collection.get(key, None).await.unwrap();

        let (raw, flags) = res.content_as_raw();
        let content = transcoding::raw_binary::decode(raw, flags).unwrap();

        assert_eq!("prependtest".as_bytes(), content);
    })
}

#[test]
fn test_increment() {
    run_test(async |cluster, bucket| {
        let collection = bucket
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());

        let key = new_key();

        collection.insert(&key, 0, None).await.unwrap();

        let res = collection
            .binary()
            .increment(&key, IncrementOptions::new().delta(1u64))
            .await
            .unwrap();

        assert_eq!(1, res.content());
    })
}

#[test]
fn test_decrement() {
    run_test(async |cluster, bucket| {
        let collection = bucket
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());

        let key = new_key();

        collection.insert(&key, 1, None).await.unwrap();

        let res = collection
            .binary()
            .decrement(&key, DecrementOptions::new().delta(1u64))
            .await
            .unwrap();

        assert_eq!(0, res.content());
    })
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
struct SubdocObject {
    foo: u32,
    bar: u32,
    baz: String,
    arr: Vec<u32>,
}

#[test]
fn test_lookup_in() {
    run_test(async |cluster, bucket| {
        let collection = bucket
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());

        let key = new_key();

        let obj = SubdocObject {
            foo: 14,
            bar: 2,
            baz: "hello".to_string(),
            arr: vec![1, 2, 3],
        };

        collection.upsert(&key, &obj, None).await.unwrap();

        let ops = [
            LookupInSpec::get("baz", None),
            LookupInSpec::exists("not-exists", None),
            LookupInSpec::count("arr", None),
            LookupInSpec::get(LookupInMacros::IsDeleted, GetSpecOptions::new().xattr(true)),
            LookupInSpec::get("", None),
        ];

        let result = collection.lookup_in(&key, &ops, None).await.unwrap();

        assert_eq!(result.content_as::<String>(0).unwrap(), "hello".to_string());
        assert!(result.exists(0).unwrap());
        assert!(!result.content_as::<bool>(1).unwrap());
        assert!(!result.exists(1).unwrap());
        assert_eq!(result.content_as::<u32>(2).unwrap(), 3);
        assert!(result.exists(2).unwrap());
        assert!(!result.content_as::<bool>(3).unwrap());
        assert!(result.exists(3).unwrap());
        assert_eq!(
            result.content_as_raw(4).unwrap(),
            serde_json::to_vec(&obj).unwrap()
        );
        assert!(result.exists(4).unwrap());
    })
}

#[test]
fn test_mutate_in() {
    run_test(async |cluster, bucket| {
        let collection = bucket
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());

        let key = new_key();

        let obj = SubdocObject {
            foo: 14,
            bar: 2,
            baz: "hello".to_string(),
            arr: vec![3],
        };

        collection.upsert(&key, &obj, None).await.unwrap();

        let xattr_spec = MutateInSpec::insert("my-cas", MutateInMacros::CAS, None).unwrap();

        let ops = [
            MutateInSpec::decrement("bar", 1, None).unwrap(),
            MutateInSpec::increment("bar", 2, None).unwrap(),
            MutateInSpec::upsert("baz", "world", None).unwrap(),
            xattr_spec,
            MutateInSpec::array_prepend("arr", &[1, 2], None).unwrap(),
            MutateInSpec::array_append("arr", &[5, 6], None).unwrap(),
        ];

        let result = collection.mutate_in(&key, &ops, None).await.unwrap();

        assert_eq!(result.entries().len(), 6);
        assert!(result.mutation_token().is_some());
        assert_ne!(result.cas(), 0);
        assert_eq!(result.content_as::<u32>(0).unwrap(), 1);
        assert_eq!(result.content_as::<u32>(1).unwrap(), 3);

        let res = collection.get(key, None).await.unwrap();
        let content = res.content_as::<SubdocObject>().unwrap();

        assert_eq!(
            content,
            SubdocObject {
                foo: 14,
                bar: 3,
                baz: "world".to_string(),
                arr: vec![1, 2, 3, 5, 6],
            }
        );
    })
}

#[test]
fn get_with_expiry() {
    run_test(async |cluster, bucket| {
        let collection = bucket
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());

        let key = new_key();

        collection
            .upsert(
                &key,
                "test",
                UpsertOptions::new().expiry(Duration::from_secs(30)),
            )
            .await
            .unwrap();

        let res = collection
            .get(key, GetOptions::new().expiry(true))
            .await
            .unwrap();

        let expiry = *res.expiry_time().expect("Expected expiry time to be set");

        let now = Utc::now();

        assert!(expiry > now, "Expiry time should be in the future");
        assert!(
            // We use 31 seconds to allow for inaccuracies in precision when applying the expiry.
            expiry < now.add(Duration::from_secs(31)),
            "Expiry time should be within 30 seconds: {expiry} vs {now}"
        );

        let content: String = res.content_as().unwrap();

        assert_eq!("test", content);
    })
}

#[test]
fn get_with_projections() {
    run_test(async |cluster, bucket| {
        let collection = bucket
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());

        let projection_id = import_projection_doc(&collection).await;

        // Load baseline person for expected values
        let content = std::fs::read_to_string("tests/testdata/projection_doc.json").unwrap();
        let person: Person = serde_json::from_str(content.as_str()).unwrap();

        #[derive(Debug)]
        struct TCase {
            name: &'static str,
            project: Vec<String>,
            expected: Person,
        }

        let cases: Vec<TCase> = vec![
            TCase {
                name: "string",
                project: vec!["name".to_string()],
                expected: Person {
                    name: person.name.clone(),
                    ..Default::default()
                },
            },
            TCase {
                name: "int",
                project: vec!["age".to_string()],
                expected: Person {
                    age: person.age,
                    ..Default::default()
                },
            },
            TCase {
                name: "array",
                project: vec!["animals".to_string()],
                expected: Person {
                    animals: person.animals.clone(),
                    ..Default::default()
                },
            },
            TCase {
                name: "array-index1",
                project: vec!["animals[0]".to_string()],
                expected: Person {
                    animals: vec![person.animals[0].clone()],
                    ..Default::default()
                },
            },
            TCase {
                name: "array-index2",
                project: vec!["animals[1]".to_string()],
                expected: Person {
                    animals: vec![person.animals[1].clone()],
                    ..Default::default()
                },
            },
            TCase {
                name: "array-index3",
                project: vec!["animals[2]".to_string()],
                expected: Person {
                    animals: vec![person.animals[2].clone()],
                    ..Default::default()
                },
            },
            TCase {
                name: "full-object-field",
                project: vec!["attributes".to_string()],
                expected: Person {
                    attributes: person.attributes.clone(),
                    ..Default::default()
                },
            },
            TCase {
                name: "nested-object-field1",
                project: vec!["attributes.hair".to_string()],
                expected: Person {
                    attributes: PersonAttributes {
                        hair: person.attributes.hair.clone(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            },
            TCase {
                name: "nested-object-field2",
                project: vec!["attributes.dimensions".to_string()],
                expected: Person {
                    attributes: PersonAttributes {
                        dimensions: person.attributes.dimensions.clone(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            },
            TCase {
                name: "nested-object-field3",
                project: vec!["attributes.dimensions.height".to_string()],
                expected: Person {
                    attributes: PersonAttributes {
                        dimensions: PersonDimensions {
                            height: person.attributes.dimensions.height,
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    ..Default::default()
                },
            },
            TCase {
                name: "nested-object-field3",
                project: vec!["attributes.dimensions.weight".to_string()],
                expected: Person {
                    attributes: PersonAttributes {
                        dimensions: PersonDimensions {
                            weight: person.attributes.dimensions.weight,
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    ..Default::default()
                },
            },
            TCase {
                name: "nested-object-field4",
                project: vec!["attributes.hobbies".to_string()],
                expected: Person {
                    attributes: PersonAttributes {
                        hobbies: person.attributes.hobbies.clone(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            },
            TCase {
                name: "nested-array-object-field1",
                project: vec!["attributes.hobbies[0].type".to_string()],
                expected: Person {
                    attributes: PersonAttributes {
                        hobbies: vec![PersonHobbies {
                            r#type: person.attributes.hobbies[0].r#type.clone(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            },
            TCase {
                name: "nested-array-object-field2",
                project: vec!["attributes.hobbies[1].type".to_string()],
                expected: Person {
                    attributes: PersonAttributes {
                        hobbies: vec![PersonHobbies {
                            r#type: person.attributes.hobbies[1].r#type.clone(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            },
            TCase {
                name: "nested-array-object-field3",
                project: vec!["attributes.hobbies[0].name".to_string()],
                expected: Person {
                    attributes: PersonAttributes {
                        hobbies: vec![PersonHobbies {
                            name: person.attributes.hobbies[0].name.clone(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            },
            TCase {
                name: "nested-array-object-field4",
                project: vec!["attributes.hobbies[1].name".to_string()],
                expected: Person {
                    attributes: PersonAttributes {
                        hobbies: vec![PersonHobbies {
                            name: person.attributes.hobbies[1].name.clone(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            },
            TCase {
                name: "nested-array-object-field5",
                project: vec!["attributes.hobbies[1].details".to_string()],
                expected: Person {
                    attributes: PersonAttributes {
                        hobbies: vec![PersonHobbies {
                            details: person.attributes.hobbies[1].details.clone(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            },
            TCase {
                name: "nested-array-object-nested-field1",
                project: vec!["attributes.hobbies[1].details.location".to_string()],
                expected: Person {
                    attributes: PersonAttributes {
                        hobbies: vec![PersonHobbies {
                            details: HobbyDetails {
                                location: person.attributes.hobbies[1].details.location.clone(),
                            },
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            },
            TCase {
                name: "nested-array-object-nested-nested-field1",
                project: vec!["attributes.hobbies[1].details.location.lat".to_string()],
                expected: Person {
                    attributes: PersonAttributes {
                        hobbies: vec![PersonHobbies {
                            details: HobbyDetails {
                                location: Location {
                                    lat: person.attributes.hobbies[1].details.location.lat,
                                    ..Default::default()
                                },
                            },
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            },
            TCase {
                name: "nested-array-object-nested-nested-field2",
                project: vec!["attributes.hobbies[1].details.location.long".to_string()],
                expected: Person {
                    attributes: PersonAttributes {
                        hobbies: vec![PersonHobbies {
                            details: HobbyDetails {
                                location: Location {
                                    long: person.attributes.hobbies[1].details.location.long,
                                    ..Default::default()
                                },
                            },
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            },
            TCase {
                name: "array-of-arrays-object",
                project: vec!["tracking.locations[1][1].lat".to_string()],
                expected: Person {
                    tracking: crate::common::doc_generation::Tracking {
                        locations: vec![vec![Location {
                            lat: person.tracking.locations[1][1].lat,
                            ..Default::default()
                        }]],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            },
            TCase {
                name: "array-of-arrays-native",
                project: vec!["tracking.raw[1][1]".to_string()],
                expected: Person {
                    tracking: crate::common::doc_generation::Tracking {
                        raw: vec![vec![person.tracking.raw[1][1]]],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            },
        ];

        let mut errors: Vec<String> = Vec::new();
        for case in cases.into_iter() {
            match collection
                .get(&projection_id, GetOptions::new().projections(case.project))
                .await
            {
                Ok(res) => match res.content_as::<Person>() {
                    Ok(got) => {
                        if got != case.expected {
                            errors.push(format!(
                                "{}: mismatch\n expected: {:?}\n got: {:?}",
                                case.name, case.expected, got
                            ));
                        }
                    }
                    Err(e) => errors.push(format!("{}: deserialization error: {:?}", case.name, e)),
                },
                Err(e) => errors.push(format!("{}: got error: {:?}", case.name, e)),
            }
        }
        if !errors.is_empty() {
            panic!(
                "projection cases failed ({}):\n{}",
                errors.len(),
                errors.join("\n")
            );
        }
    })
}

#[test]
fn test_retry_locked_until_cancel() {
    run_test(async |cluster, bucket| {
        let collection = bucket
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());

        let key = new_key();

        collection.insert(&key, "test", None).await.unwrap();

        collection
            .get_and_lock(&key, Duration::from_secs(10), None)
            .await
            .unwrap();

        // The upsert retries on the locked document until its timeout elapses.
        let err = collection
            .deref()
            .upsert(
                &key,
                "test",
                UpsertOptions::new().timeout(Duration::from_millis(2500)),
            )
            .await
            .unwrap_err();

        assert_eq!(&ErrorKind::AmbiguousTimeout, err.kind());
    })
}

#[test]
fn test_no_retry_locked_fail_fast() {
    run_test(async |cluster, bucket| {
        let collection = bucket
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());

        let key = new_key();

        collection.insert(&key, "test", None).await.unwrap();

        collection
            .get_and_lock(&key, Duration::from_secs(10), None)
            .await
            .unwrap();

        let strat = Arc::new(FailFastStrategy {
            record: Mutex::new(FailFastStrategyRecord {
                attempts: 0,
                reasons: HashSet::new(),
            }),
        });
        let res = timeout_at(
            Instant::now().add(Duration::from_millis(2500)),
            collection.deref().upsert(
                &key,
                "test",
                UpsertOptions::new().retry_strategy(strat.clone()),
            ),
        )
        .await
        .unwrap();

        assert!(res.is_err());
        assert_eq!(&ErrorKind::DocumentLocked, res.err().unwrap().kind());

        let guard = strat.record.lock().unwrap();
        assert_eq!(1, guard.attempts);
        assert_eq!(
            &RetryReason::KvLocked,
            guard.reasons.get(&RetryReason::KvLocked).unwrap()
        );
    })
}

#[derive(Debug)]
struct FailFastStrategyRecord {
    pub attempts: usize,
    pub reasons: HashSet<RetryReason>,
}

#[derive(Debug)]
struct FailFastStrategy {
    pub record: Mutex<FailFastStrategyRecord>,
}

impl RetryStrategy for FailFastStrategy {
    fn retry_after(&self, _info: &RetryRequest, reason: &RetryReason) -> Option<RetryAction> {
        let mut guard = self.record.lock().unwrap();
        guard.reasons.insert(*reason);
        guard.attempts += 1;

        None
    }
}
