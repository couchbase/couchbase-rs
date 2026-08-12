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

//! Options for Full-Text Search operations.

use crate::error::Error;
use crate::mutation_state::MutationState;
use crate::retry::RetryStrategy;
use crate::search::facets::Facet;
use crate::search::scoring::Scoring;
use crate::search::sort::Sort;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Scan consistency for search queries.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScanConsistency {
    /// No consistency requirement — results may be stale.
    NotBounded,
}

impl From<ScanConsistency> for couchbase_core::searchx::query_options::ConsistencyLevel {
    fn from(sc: ScanConsistency) -> Self {
        match sc {
            ScanConsistency::NotBounded => {
                couchbase_core::searchx::query_options::ConsistencyLevel::NotBounded
            }
        }
    }
}

impl From<ScanConsistency> for Option<couchbase_core::searchx::query_options::ConsistencyLevel> {
    fn from(sc: ScanConsistency) -> Self {
        match sc {
            ScanConsistency::NotBounded => {
                Some(couchbase_core::searchx::query_options::ConsistencyLevel::NotBounded)
            }
        }
    }
}

/// Highlight style for search result snippets.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum HighlightStyle {
    /// HTML-formatted highlights.
    Html,
    /// ANSI terminal-formatted highlights.
    Ansi,
}

impl From<HighlightStyle> for couchbase_core::searchx::query_options::HighlightStyle {
    fn from(hs: HighlightStyle) -> Self {
        match hs {
            HighlightStyle::Html => couchbase_core::searchx::query_options::HighlightStyle::Html,
            HighlightStyle::Ansi => couchbase_core::searchx::query_options::HighlightStyle::Ansi,
        }
    }
}

impl From<HighlightStyle> for Option<couchbase_core::searchx::query_options::HighlightStyle> {
    fn from(hs: HighlightStyle) -> Self {
        Some(hs.into())
    }
}

/// Highlight configuration for search results.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Highlight {
    /// The highlight style to use (HTML or ANSI).
    pub style: Option<HighlightStyle>,
    /// The fields to highlight. If `None`, all fields are highlighted.
    pub fields: Option<Vec<String>>,
}

impl Highlight {
    /// Creates a new `Highlight` with default values.
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets the highlight style.
    pub fn style(mut self, style: HighlightStyle) -> Self {
        self.style = Some(style);
        self
    }

    /// Sets the fields to highlight.
    pub fn fields(mut self, fields: Vec<String>) -> Self {
        self.fields = Some(fields);
        self
    }
}
impl From<Highlight> for couchbase_core::searchx::query_options::Highlight {
    fn from(h: Highlight) -> Self {
        couchbase_core::searchx::query_options::Highlight::default()
            .style(h.style.map(|s| s.into()))
            .fields(h.fields)
    }
}

impl From<Highlight> for Option<couchbase_core::searchx::query_options::Highlight> {
    fn from(h: Highlight) -> Self {
        Some(h.into())
    }
}

/// Options for Full-Text Search queries executed via
/// [`Scope::search`](crate::scope::Scope::search).
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct SearchOptions {
    /// Restrict the search to specific collections within the scope.
    pub collections: Option<Vec<String>>,
    /// Maximum number of results to return.
    pub limit: Option<u32>,
    /// Number of results to skip (for pagination).
    pub skip: Option<u32>,
    /// If `true`, includes the query execution explanation in the result.
    pub explain: Option<bool>,
    /// Highlight configuration for search result snippets.
    pub highlight: Option<Highlight>,
    /// Document fields to include in the result.
    pub fields: Option<Vec<String>>,
    /// Scan consistency level for the search.
    pub scan_consistency: Option<ScanConsistency>,
    /// Mutation state for consistent-with (at_plus) consistency.
    pub consistent_with: Option<MutationState>,
    /// Sort order for results.
    pub sort: Option<Vec<Sort>>,
    /// Facets to include in the result for aggregation.
    pub facets: Option<HashMap<String, Facet>>,
    /// Controls how results are scored. See [`Scoring`] for the available modes.
    ///
    /// **Volatile: This API is subject to change at any time.**
    pub scoring: Option<Scoring>,
    /// Raw key/value parameters passed directly to the search request body.
    pub raw: Option<HashMap<String, Value>>,
    /// If `true`, includes term location information in the result.
    pub include_locations: Option<bool>,
    /// If `true`, disables scoring (useful when only sorting or filtering).
    #[deprecated(since = "1.1.0", note = "Use `scoring(Scoring::None)` instead")]
    pub disable_scoring: Option<bool>,
    /// Server-side timeout for the search.
    pub server_timeout: Option<Duration>,
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl SearchOptions {
    /// Creates a new `SearchOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Restricts the search to specific collections.
    pub fn collections(mut self, collections: Vec<String>) -> Self {
        self.collections = Some(collections);
        self
    }

    /// Sets the maximum number of results to return.
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Sets the number of results to skip (for pagination).
    pub fn skip(mut self, skip: u32) -> Self {
        self.skip = Some(skip);
        self
    }

    /// If `true`, includes the query execution explanation in the result.
    pub fn explain(mut self, explain: bool) -> Self {
        self.explain = Some(explain);
        self
    }

    /// Sets the result highlighting configuration.
    pub fn highlight(mut self, highlight: Highlight) -> Self {
        self.highlight = Some(highlight);
        self
    }

    /// Sets the document fields to include in the result.
    pub fn fields(mut self, fields: Vec<String>) -> Self {
        self.fields = Some(fields);
        self
    }

    /// Sets the scan consistency level for the search.
    pub fn scan_consistency(mut self, scan_consistency: ScanConsistency) -> Self {
        self.scan_consistency = Some(scan_consistency);
        self
    }

    /// Sets a mutation state for consistent-with (at_plus) consistency.
    pub fn consistent_with(mut self, consistent_with: MutationState) -> Self {
        self.consistent_with = Some(consistent_with);
        self
    }

    /// Sets the sort order for results.
    pub fn sort(mut self, sort: Vec<Sort>) -> Self {
        self.sort = Some(sort);
        self
    }

    /// Sets the facets to include in the result.
    pub fn facets(mut self, facets: HashMap<String, Facet>) -> Self {
        self.facets = Some(facets);
        self
    }

    /// Sets how results are scored. See [`Scoring`] for the available modes.
    ///
    /// **Volatile: This API is subject to change at any time.**
    pub fn scoring(mut self, scoring: Scoring) -> Self {
        self.scoring = Some(scoring);
        self
    }

    /// Adds a raw key/value parameter to the search request body.
    pub fn add_raw<T: Serialize>(
        mut self,
        key: impl Into<String>,
        value: T,
    ) -> crate::error::Result<Self> {
        let value = serde_json::to_value(&value).map_err(Error::encoding_failure_from_serde)?;

        match self.raw {
            Some(mut params) => {
                params.insert(key.into(), value);
                self.raw = Some(params);
            }
            None => {
                let mut params = HashMap::new();
                params.insert(key.into(), value);
                self.raw = Some(params);
            }
        }
        Ok(self)
    }

    /// If `true`, includes term location information in the result.
    pub fn include_locations(mut self, include_locations: bool) -> Self {
        self.include_locations = Some(include_locations);
        self
    }

    /// If `true`, disables scoring (useful when only sorting or filtering).
    #[deprecated(since = "1.1.0", note = "Use `scoring(Scoring::None)` instead")]
    #[allow(deprecated)]
    pub fn disable_scoring(mut self, disable_scoring: bool) -> Self {
        self.disable_scoring = Some(disable_scoring);
        self
    }

    /// Sets the server-side timeout for the search.
    pub fn server_timeout(mut self, server_timeout: Duration) -> Self {
        self.server_timeout = Some(server_timeout);
        self
    }

    /// Sets a custom retry strategy for this operation.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}
