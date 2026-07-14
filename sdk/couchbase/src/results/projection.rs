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

use crate::error;
use crate::results::kv_results::{LookupInResult, LookupInResultEntry};
use crate::subdoc::lookup_in_specs::LookupInSpec;
use serde_json::value::Map;
use serde_json::{from_slice, to_vec, Value};
use tracing::{error, warn};

pub(crate) fn build_from_subdoc_entries(
    specs: &[LookupInSpec],
    results: &[LookupInResultEntry],
) -> error::Result<Vec<u8>> {
    let mut content: Value = Value::Object(Default::default());
    for (i, spec) in specs.iter().enumerate() {
        if let Some(err) = &results[i].error {
            match err.kind() {
                error::ErrorKind::PathNotFound => {
                    warn!(
                        "projection path '{}' not found in subdoc response; skipping",
                        spec.path
                    );
                    continue;
                }
                _ => return Err(err.clone()),
            }
        }

        let parts = SubdocPath::path_parts(spec.path.as_str());
        let value_bytes = results[i]
            .value
            .as_deref()
            .ok_or_else(|| error::Error::other_failure("missing subdoc value".to_string()))?;

        let value: Value =
            from_slice(value_bytes).map_err(error::Error::decoding_failure_from_serde)?;

        content = SubdocPath::set_value(&parts, content, value);
    }
    to_vec(&content).map_err(error::Error::decoding_failure_from_serde)
}

pub(crate) fn build_from_full_doc(
    result: &LookupInResult,
    projections: Option<&[String]>,
) -> error::Result<Vec<u8>> {
    if projections.is_none() {
        // This is a special case where user specified a full doc fetch with expiration.
        let raw = result.content_as_raw(0)?;
        return Ok(raw.to_vec());
    }

    if result.entries.len() != 1 {
        return Err(error::Error::other_failure(
            "expected exactly one entry for full document".to_string(),
        ));
    }

    let content: Value = result.content_as(0)?;
    let mut projection_content: Value = Value::Object(Default::default());
    for projection in projections.unwrap() {
        let parts = SubdocPath::path_parts(projection.as_str());
        if let Some(value) = resolve_projection_value(&content, &parts).cloned() {
            projection_content = SubdocPath::set_value(&parts, projection_content, value);
        } else {
            warn!(
                "projection path '{}' not found in document; skipping",
                projection
            );
            continue;
        }
    }
    to_vec(&projection_content).map_err(error::Error::decoding_failure_from_serde)
}

#[derive(Debug, Clone)]
pub(crate) struct SubdocPath {
    path: String,
    is_array: bool,
}

impl SubdocPath {
    fn new(path: impl Into<String>, is_array: bool) -> Self {
        Self {
            path: path.into(),
            is_array,
        }
    }

    fn path_parts(path_str: &str) -> Vec<SubdocPath> {
        let mut parts = Vec::new();
        let mut current = String::new();
        let mut chars = path_str.chars().peekable();

        while let Some(ch) = chars.next() {
            match ch {
                '.' => {
                    if !current.is_empty() {
                        parts.push(SubdocPath::new(std::mem::take(&mut current), false));
                    }
                }
                '[' => {
                    // finalize what came before the array or add anonymous array segment
                    if !current.is_empty() {
                        parts.push(SubdocPath::new(std::mem::take(&mut current), true));
                    } else {
                        // consecutive array segment without a new key (e.g., [1][2])
                        parts.push(SubdocPath::new("", true));
                    }

                    // consume until closing bracket or end
                    for inner in chars.by_ref() {
                        if inner == ']' {
                            break;
                        }
                    }

                    // skip optional '.' after array
                    if matches!(chars.peek(), Some('.')) {
                        chars.next();
                    }
                }
                _ => current.push(ch),
            }
        }

        // push last element if any
        if !current.is_empty() {
            parts.push(SubdocPath::new(current, false));
        }

        parts
    }

    fn set_value(paths: &[SubdocPath], mut content: Value, value: Value) -> Value {
        let path = &paths[0];

        if paths.len() == 1 {
            if path.is_array {
                let mut arr = Vec::new();
                arr.push(value);
                match content {
                    Value::Object(ref mut obj) => {
                        obj.insert(path.path.clone(), Value::Array(arr));
                    }
                    Value::Array(ref mut vec) => {
                        vec.push(Value::Array(arr));
                    }
                    _ => {}
                }
            } else {
                match content {
                    Value::Array(ref mut vec) => {
                        let mut elem = Map::new();
                        elem.insert(path.path.clone(), value);
                        vec.push(Value::Object(elem));
                    }
                    Value::Object(ref mut obj) => {
                        obj.insert(path.path.clone(), value);
                    }
                    _ => {}
                }
            }
            return content;
        }

        if path.is_array {
            match &mut content {
                Value::Array(vec) => {
                    let child = Self::set_value(&paths[1..], Value::Array(Vec::new()), value);
                    vec.push(child);
                    content
                }
                Value::Object(obj) => {
                    let child = Self::set_value(&paths[1..], Value::Array(Vec::new()), value);
                    obj.insert(path.path.clone(), child);
                    content
                }
                _ => content,
            }
        } else {
            match &mut content {
                Value::Array(vec) => {
                    let child = Self::set_value(&paths[1..], Value::Object(Map::new()), value);
                    let mut obj = Map::new();
                    obj.insert(path.path.clone(), child);
                    vec.push(Value::Object(obj));
                    content
                }
                Value::Object(obj) => {
                    let child = Self::set_value(&paths[1..], Value::Object(Map::new()), value);
                    obj.insert(path.path.clone(), child);
                    content
                }
                _ => content,
            }
        }
    }
}

fn resolve_projection_value<'a>(root: &'a Value, parts: &[SubdocPath]) -> Option<&'a Value> {
    let mut current = root;
    for part in parts {
        current = current.get(part.path.as_str())?;
    }
    Some(current)
}
