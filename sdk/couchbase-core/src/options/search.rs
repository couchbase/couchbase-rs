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
use crate::retry::{RetryStrategy, DEFAULT_RETRY_STRATEGY};
use crate::searchx;
use crate::searchx::facets::Facet;
use crate::searchx::queries::Query;
use crate::searchx::query_options::{Control, Highlight, KnnOperator, KnnQuery, Params};
use crate::searchx::sort::Sort;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SearchOptions {
    pub collections: Option<Vec<String>>,
    pub control: Option<Control>,
    pub explain: Option<bool>,
    pub facets: Option<HashMap<String, Facet>>,
    pub fields: Option<Vec<String>>,
    pub from: Option<u32>,
    pub highlight: Option<Highlight>,
    pub include_locations: Option<bool>,
    pub query: Option<Query>,
    pub score: Option<String>,
    pub search_after: Option<Vec<String>>,
    pub search_before: Option<Vec<String>>,
    pub show_request: Option<bool>,
    pub size: Option<u32>,
    pub sort: Option<Vec<Sort>>,
    pub knn: Option<Vec<KnnQuery>>,
    pub knn_operator: Option<KnnOperator>,
    pub params: Option<Params>,

    pub raw: Option<HashMap<String, serde_json::Value>>,

    pub index_name: String,
    pub scope_name: Option<String>,
    pub bucket_name: Option<String>,

    pub on_behalf_of: Option<OnBehalfOfInfo>,

    pub endpoint: Option<String>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl SearchOptions {
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
            endpoint: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
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

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }

    pub fn endpoint(mut self, endpoint: impl Into<Option<String>>) -> Self {
        self.endpoint = endpoint.into();
        self
    }
}

impl From<SearchOptions> for searchx::query_options::QueryOptions {
    fn from(opts: SearchOptions) -> Self {
        searchx::query_options::QueryOptions {
            collections: opts.collections,
            control: opts.control,
            explain: opts.explain,
            facets: opts.facets,
            fields: opts.fields,
            from: opts.from,
            highlight: opts.highlight,
            include_locations: opts.include_locations,
            query: opts.query,
            score: opts.score,
            search_after: opts.search_after,
            search_before: opts.search_before,
            show_request: opts.show_request,
            size: opts.size,
            sort: opts.sort,
            knn: opts.knn,
            knn_operator: opts.knn_operator,
            params: opts.params,
            raw: opts.raw,
            index_name: opts.index_name,
            scope_name: opts.scope_name,
            bucket_name: opts.bucket_name,
            on_behalf_of: opts.on_behalf_of,
        }
    }
}
