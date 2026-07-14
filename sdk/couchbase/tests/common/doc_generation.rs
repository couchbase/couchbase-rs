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

use crate::common::new_key;
use couchbase::collection::Collection;
use couchbase::results::kv_results::MutationResult;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::read_to_string;

pub fn load_sample_beer_dataset(for_service: &str) -> Vec<TestBreweryDocumentJson> {
    let data = include_bytes!("../testdata/beer_sample_brewery_five.json");
    let mut docs: Vec<TestBreweryDocumentJson> = serde_json::from_slice(data).unwrap();
    for mut doc in docs.iter_mut() {
        doc.service = for_service.to_string();
    }

    docs
}

pub async fn import_color_sample(
    for_service: &str,
    collection: &Collection,
) -> HashMap<String, TestMutationResult> {
    let content = read_to_string("tests/testdata/color_landmarks_five.jsonl").unwrap();
    let mut results = HashMap::new();

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let mut doc: serde_json::Value = serde_json::from_str(line).unwrap();

        if let Some(obj) = doc.as_object_mut() {
            obj.insert(
                "service".to_string(),
                serde_json::Value::String(for_service.to_string()),
            );

            let key = obj
                .get("id")
                .and_then(|v| v.as_str())
                .expect("Document must have an 'id' field")
                .to_string();
            let result = collection.upsert(&key, &obj, None).await.unwrap();

            results.insert(
                key,
                TestMutationResult {
                    mutation_result: result,
                    doc: obj.clone(),
                },
            );
        }
    }

    results
}

#[derive(Debug, Clone)]
pub struct TestMutationResult {
    pub mutation_result: MutationResult,
    pub doc: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct TestBreweryMutationResult {
    pub mutation_result: MutationResult,
    pub doc: TestBreweryDocumentJson,
}

pub async fn import_sample_beer_dataset(
    for_service: &str,
    collection: &Collection,
) -> HashMap<String, TestBreweryMutationResult> {
    let docs = load_sample_beer_dataset(for_service);
    let mut results = HashMap::new();
    for doc in docs.into_iter() {
        let key = format!("{}-{}", doc.service, doc.name);
        let result = collection.upsert(&key, &doc, None).await.unwrap();
        results.insert(
            key,
            TestBreweryMutationResult {
                mutation_result: result,
                doc: doc.clone(),
            },
        );
    }

    results
}

pub async fn import_projection_doc(collection: &Collection) -> String {
    let content = read_to_string("tests/testdata/projection_doc.json").unwrap();
    let doc: serde_json::Value = serde_json::from_str(content.as_str()).unwrap();

    let key = new_key();
    collection.upsert(key.clone(), &doc, None).await.unwrap();
    key
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd, Default)]
#[serde(default)]
pub struct PersonDimensions {
    pub height: i32,
    pub weight: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd, Default)]
#[serde(default)]
pub struct Location {
    pub lat: f32,
    pub long: f32,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd, Default)]
#[serde(default)]
pub struct HobbyDetails {
    pub location: Location,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd, Default)]
#[serde(default)]
pub struct PersonHobbies {
    pub r#type: String,
    pub name: String,
    pub details: HobbyDetails,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd, Default)]
#[serde(default)]
pub struct PersonAttributes {
    pub hair: String,
    pub dimensions: PersonDimensions,
    pub hobbies: Vec<PersonHobbies>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd, Default)]
#[serde(default)]
pub struct Tracking {
    pub locations: Vec<Vec<Location>>, // [][]Location
    pub raw: Vec<Vec<f32>>,            // [][]float32
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd, Default)]
#[serde(default)]
pub struct Person {
    pub name: String,
    pub age: i32,
    pub animals: Vec<String>,
    pub attributes: PersonAttributes,
    pub tracking: Tracking,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd)]
pub struct TestBreweryDocumentJson {
    pub city: String,
    pub code: String,
    pub country: String,
    pub description: String,
    pub geo: TestBreweryGeoJson,
    pub name: String,
    pub phone: String,
    pub state: String,
    pub r#type: String,
    pub updated: String,
    pub website: String,
    #[serde(default)]
    pub service: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd)]
pub struct TestBreweryGeoJson {
    pub accuracy: String,
    pub lat: f32,
    pub lon: f32,
}
