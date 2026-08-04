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

use crate::httpx::request::OnBehalfOfInfo;
use crate::searchx::facets::Facet;
use crate::searchx::queries::Query;
use crate::searchx::sort::Sort;
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Location {
    pub lat: f64,
    pub lon: f64,
}

impl Location {
    pub fn new(lat: f64, lon: f64) -> Self {
        Self { lat, lon }
    }
}

impl Serialize for Location {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(2))?;
        seq.serialize_element(&self.lon)?;
        seq.serialize_element(&self.lat)?;

        seq.end()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum HighlightStyle {
    Html,
    Ansi,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub enum ConsistencyLevel {
    #[serde(rename = "")]
    NotBounded,
    #[serde(rename = "at_plus")]
    AtPlus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum ConsistencyResults {
    Complete,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct Highlight {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub style: Option<HighlightStyle>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<String>>,
}

impl Highlight {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn style(mut self, style: impl Into<Option<HighlightStyle>>) -> Self {
        self.style = style.into();
        self
    }

    pub fn fields(mut self, fields: impl Into<Option<Vec<String>>>) -> Self {
        self.fields = fields.into();
        self
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct Params {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score_rank_constant: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score_window_size: Option<u32>,
}

impl Params {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn score_rank_constant(mut self, value: impl Into<Option<u32>>) -> Self {
        self.score_rank_constant = value.into();
        self
    }

    pub fn score_window_size(mut self, value: impl Into<Option<u32>>) -> Self {
        self.score_window_size = value.into();
        self
    }
}

pub type ConsistencyVectors = HashMap<String, HashMap<String, u64>>;

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct Consistency {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub level: Option<ConsistencyLevel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub results: Option<ConsistencyResults>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vectors: Option<ConsistencyVectors>,
}

impl Consistency {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn level(mut self, level: impl Into<Option<ConsistencyLevel>>) -> Self {
        self.level = level.into();
        self
    }

    pub fn results(mut self, results: impl Into<Option<ConsistencyResults>>) -> Self {
        self.results = results.into();
        self
    }

    pub fn vectors(mut self, vectors: impl Into<Option<ConsistencyVectors>>) -> Self {
        self.vectors = vectors.into();
        self
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct Control {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consistency: Option<Consistency>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u64>,
}

impl Control {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn consistency(mut self, consistency: impl Into<Option<Consistency>>) -> Self {
        self.consistency = consistency.into();
        self
    }

    pub fn timeout(mut self, timeout: impl Into<Option<u64>>) -> Self {
        self.timeout = timeout.into();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct KnnQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    pub field: String,
    pub k: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector: Option<Vec<f32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_base64: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<Query>,
}

impl KnnQuery {
    pub fn new(field: impl Into<String>, k: impl Into<i64>) -> Self {
        Self {
            boost: None,
            field: field.into(),
            k: k.into(),
            vector: None,
            vector_base64: None,
            filter: None,
        }
    }

    pub fn boost(mut self, boost: impl Into<Option<f32>>) -> Self {
        self.boost = boost.into();
        self
    }

    pub fn k(mut self, k: impl Into<i64>) -> Self {
        self.k = k.into();
        self
    }

    pub fn vector(mut self, vector: impl Into<Option<Vec<f32>>>) -> Self {
        self.vector = vector.into();
        self
    }

    pub fn vector_base64(mut self, vector_base64: impl Into<Option<String>>) -> Self {
        self.vector_base64 = vector_base64.into();
        self
    }

    pub fn filter(mut self, filter: impl Into<Option<Query>>) -> Self {
        self.filter = filter.into();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum KnnOperator {
    Or,
    And,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct QueryOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) collections: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "ctl")]
    pub(crate) control: Option<Control>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) explain: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) facets: Option<HashMap<String, Facet>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) fields: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) from: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) highlight: Option<Highlight>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "includeLocations")]
    pub(crate) include_locations: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) query: Option<Query>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) score: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) search_after: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) search_before: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "showrequest")]
    pub(crate) show_request: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) size: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) sort: Option<Vec<Sort>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) knn: Option<Vec<KnnQuery>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) knn_operator: Option<KnnOperator>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) params: Option<Params>,

    #[serde(skip_serializing_if = "Option::is_none", flatten)]
    pub(crate) raw: Option<HashMap<String, serde_json::Value>>,

    #[serde(skip_serializing)]
    pub(crate) index_name: String,
    #[serde(skip_serializing)]
    pub(crate) scope_name: Option<String>,
    #[serde(skip_serializing)]
    pub(crate) bucket_name: Option<String>,

    #[serde(skip_serializing)]
    pub(crate) on_behalf_of: Option<OnBehalfOfInfo>,
}

impl QueryOptions {
    pub fn new(index_name: impl Into<String>) -> Self {
        Self {
            collections: None,
            control: None,
            explain: None,
            facets: None,
            fields: None,
            from: None,
            highlight: None,
            include_locations: None,
            query: None,
            score: None,
            search_after: None,
            search_before: None,
            show_request: None,
            size: None,
            sort: None,
            knn: None,
            knn_operator: None,
            params: None,
            raw: None,
            index_name: index_name.into(),
            scope_name: None,
            bucket_name: None,
            on_behalf_of: None,
        }
    }

    pub fn collections(mut self, collections: impl Into<Option<Vec<String>>>) -> Self {
        self.collections = collections.into();
        self
    }

    pub fn control(mut self, control: impl Into<Option<Control>>) -> Self {
        self.control = control.into();
        self
    }

    pub fn explain(mut self, explain: impl Into<Option<bool>>) -> Self {
        self.explain = explain.into();
        self
    }

    pub fn facets(mut self, facets: impl Into<Option<HashMap<String, Facet>>>) -> Self {
        self.facets = facets.into();
        self
    }

    pub fn fields(mut self, fields: impl Into<Option<Vec<String>>>) -> Self {
        self.fields = fields.into();
        self
    }

    pub fn from(mut self, from: impl Into<Option<u32>>) -> Self {
        self.from = from.into();
        self
    }

    pub fn highlight(mut self, highlight: impl Into<Option<Highlight>>) -> Self {
        self.highlight = highlight.into();
        self
    }

    pub fn include_locations(mut self, include_locations: impl Into<Option<bool>>) -> Self {
        self.include_locations = include_locations.into();
        self
    }

    pub fn query(mut self, query: impl Into<Option<Query>>) -> Self {
        self.query = query.into();
        self
    }

    pub fn score(mut self, score: impl Into<Option<String>>) -> Self {
        self.score = score.into();
        self
    }

    pub fn search_after(mut self, search_after: impl Into<Option<Vec<String>>>) -> Self {
        self.search_after = search_after.into();
        self
    }

    pub fn search_before(mut self, search_before: impl Into<Option<Vec<String>>>) -> Self {
        self.search_before = search_before.into();
        self
    }

    pub fn show_request(mut self, show_request: impl Into<Option<bool>>) -> Self {
        self.show_request = show_request.into();
        self
    }

    pub fn size(mut self, size: impl Into<Option<u32>>) -> Self {
        self.size = size.into();
        self
    }

    pub fn sort(mut self, sort: impl Into<Option<Vec<Sort>>>) -> Self {
        self.sort = sort.into();
        self
    }

    pub fn knn(mut self, knn: impl Into<Option<Vec<KnnQuery>>>) -> Self {
        self.knn = knn.into();
        self
    }

    pub fn knn_operator(mut self, knn_operator: impl Into<Option<KnnOperator>>) -> Self {
        self.knn_operator = knn_operator.into();
        self
    }

    pub fn params(mut self, params: impl Into<Option<Params>>) -> Self {
        self.params = params.into();
        self
    }

    pub fn raw(mut self, raw: impl Into<Option<HashMap<String, serde_json::Value>>>) -> Self {
        self.raw = raw.into();
        self
    }

    pub fn scope_name(mut self, scope_name: impl Into<Option<String>>) -> Self {
        self.scope_name = scope_name.into();
        self
    }

    pub fn bucket_name(mut self, bucket_name: impl Into<Option<String>>) -> Self {
        self.bucket_name = bucket_name.into();
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}
